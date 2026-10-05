"""FalkorDB benchmark for LDBC Graphalytics."""

import time
import os
import shutil

from ._common import VERTEX_FILE, EDGE_FILE, bench_common

FALKORDB_DATA_DIR = "/tmp/falkordb_benchmark"

# The Graphalytics datasets store every undirected edge once, while FalkorDB's algorithms and its BFS follow the
# stored direction: the load therefore creates every edge in both directions (a graph is reused only when it holds
# exactly twice the edges of the file, so a partially saved database is reloaded instead of patched).
PAGERANK_QUERY = "CALL algo.pageRank('Node', 'EDGE') YIELD node, score RETURN node.id AS id, score"
WCC_QUERY = "CALL algo.WCC(null) YIELD node, componentId RETURN node.id AS id, componentId"
CDLP_QUERY = ("CALL algo.labelPropagation({nodeLabels: ['Node'], relationshipTypes: ['EDGE'], maxIterations: 10}) "
              "YIELD node, communityId RETURN node.id AS id, communityId")
BFS_QUERY = ("MATCH (src:Node {id: 6}) CALL algo.BFS(src, 999, 'EDGE') YIELD nodes "
             "RETURN size(nodes) AS reached")


def _file_lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def _dump_all(g):
    """Full per-vertex outputs of the exact calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q):
        return g.ro_query(q).result_set
    bench_common.dump_safely("falkordb", "PR", lambda: bench_common.dump_rows(
        "falkordb", "PR", ((r[0], float(r[1])) for r in rows(PAGERANK_QUERY))))
    bench_common.dump_safely("falkordb", "WCC", lambda: bench_common.dump_rows(
        "falkordb", "WCC", ((r[0], r[1]) for r in rows(WCC_QUERY))))
    bench_common.dump_safely("falkordb", "CDLP", lambda: bench_common.dump_rows(
        "falkordb", "CDLP", ((r[0], r[1]) for r in rows(CDLP_QUERY))))
    def bfs():
        # algo.BFS returns the reached nodes without levels: the distance of a vertex is the smallest depth
        # limit whose result contains it
        dist = {6: 0}
        depth = 1
        while True:
            reached = {r[0] for r in rows(
                f"MATCH (src:Node {{id: 6}}) CALL algo.BFS(src, {depth}, 'EDGE') YIELD nodes "
                "UNWIND nodes AS n RETURN n.id")}
            new = [v for v in reached if v not in dist]
            if not new:
                break
            for v in new:
                dist[v] = depth
            depth += 1
        bench_common.dump_bfs("falkordb", dist, [r[0] for r in rows("MATCH (n:Node) RETURN n.id")], 6)
    bench_common.dump_safely("falkordb", "BFS", bfs)


def run_benchmark():
    import redis
    from falkordb import FalkorDB
    print("\n" + "=" * 70)
    print("FALKORDB BENCHMARK")
    print("=" * 70)

    results = {}

    if bench_common.RESET and os.path.isdir(FALKORDB_DATA_DIR):
        print("  [FalkorDB] --reset: removing persisted data...")
        shutil.rmtree(FALKORDB_DATA_DIR)

    os.makedirs(FALKORDB_DATA_DIR, exist_ok=True)

    try:
        fdb = FalkorDB(host='localhost', port=6379)
        g = fdb.select_graph('bench')
    except Exception as e:
        print(f"  Cannot connect to FalkorDB: {e}")
        print(f"  Start with: docker run -d --name falkordb -p 6379:6379 -v {FALKORDB_DATA_DIR}:/var/lib/falkordb/data falkordb/falkordb")
        return {"error": str(e)}

    # Disable query timeout (default 1000ms is too low for large graphs)
    try:
        rc = redis.Redis(host='localhost', port=6379)
        rc.execute_command("GRAPH.CONFIG", "SET", "TIMEOUT", 0)
        print("  Query timeout disabled")
        # the default RESULTSET_SIZE of 10000 rows silently truncates every full per-vertex output
        rc.execute_command("GRAPH.CONFIG", "SET", "RESULTSET_SIZE", -1)
    except Exception:
        pass

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = g.ro_query("MATCH ()-[e]->() RETURN count(e) AS c")
            if r.result_set and r.result_set[0][0] == 2 * _file_lines(EDGE_FILE):
                needs_load = False
                print(f"\n[FalkorDB] Data already loaded ({r.result_set[0][0]} edges), skipping import")
        except Exception:
            pass

    if bench_common.RESET:
        # Delete existing graph if present
        try:
            g.delete()
            g = fdb.select_graph('bench')
        except Exception:
            pass

    if needs_load:
        print("\n[FalkorDB] Loading data...")
        start = time.perf_counter()

        # Load vertices in batches
        print("  Loading vertices...")
        batch_size = 5000
        with open(VERTEX_FILE) as f:
            batch = []
            for line in f:
                vid = int(line.strip())
                batch.append(vid)
                if len(batch) >= batch_size:
                    g.query("UNWIND $ids AS id CREATE (:Node {id: id})", {"ids": batch})
                    batch = []
            if batch:
                g.query("UNWIND $ids AS id CREATE (:Node {id: id})", {"ids": batch})

        # Create index on Node.id for edge linking
        try:
            g.query("CREATE INDEX FOR (n:Node) ON (n.id)")
        except Exception:
            pass

        # Load edges in batches
        print("  Loading edges...")
        with open(EDGE_FILE) as f:
            batch = []
            loaded = 0
            for line in f:
                parts = line.strip().split()
                batch.append([int(parts[0]), int(parts[1]), float(parts[2])])
                batch.append([int(parts[1]), int(parts[0]), float(parts[2])])
                if len(batch) >= batch_size:
                    loaded += len(batch)
                    if loaded % 500000 < batch_size:  # progress output also keeps the idle watchdog of the orchestrator quiet
                        print(f"    {loaded} edges loaded ({time.perf_counter() - start:.0f}s)", flush=True)
                    g.query("""
                        UNWIND $edges AS e
                        MATCH (a:Node {id: e[0]}), (b:Node {id: e[1]})
                        CREATE (a)-[:EDGE {weight: e[2]}]->(b)
                    """, {"edges": batch})
                    batch = []
            if batch:
                g.query("""
                    UNWIND $edges AS e
                    MATCH (a:Node {id: e[0]}), (b:Node {id: e[1]})
                    CREATE (a)-[:EDGE {weight: e[2]}]->(b)
                """, {"edges": batch})

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    r = g.ro_query("MATCH (n:Node) RETURN count(n) AS c")
    print(f"  Vertices: {r.result_set[0][0]}")
    r = g.ro_query("MATCH ()-[e:EDGE]->() RETURN count(e) AS c")
    print(f"  Edges: {r.result_set[0][0]}")

    if bench_common.dump_only():
        _dump_all(g)
        bench_common.cleanup_docker("falkordb")
        return results

    # --- PageRank ---
    print("\n[FalkorDB] Running PageRank...")
    def _run_pagerank():
        r = g.ro_query(PAGERANK_QUERY)
        print(f"  PageRank: {len(r.result_set)} rows")
        return r
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC (Weakly Connected Components) ---
    print("\n[FalkorDB] Running WCC...")
    def _run_wcc():
        r = g.ro_query(WCC_QUERY)
        print(f"  WCC: {len(r.result_set)} rows")
        return r
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- BFS ---
    print("\n[FalkorDB] Running BFS from vertex 6...")
    def _run_bfs():
        r = g.ro_query(BFS_QUERY)
        print(f"  Reached {r.result_set[0][0]} nodes")
        return r
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # --- SSSP ---
    # FalkorDB's algo.SSpaths does not support full single-source Dijkstra;
    # it only returns paths to direct neighbors. Not usable for SSSP benchmark.
    print("\n[FalkorDB] SSSP: not supported (algo.SSpaths is pair-oriented, not full SSSP)")
    results["sssp"] = "N/A"

    # --- CDLP (Community Detection via Label Propagation) ---
    print("\n[FalkorDB] Running CDLP...")
    def _run_cdlp():
        r = g.ro_query(CDLP_QUERY)
        print(f"  CDLP: {len(r.result_set)} rows")
        return r
    elapsed, _ = bench_common.run_timed("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    # --- LCC (Local Clustering Coefficient) ---
    # FalkorDB has no built-in LCC algorithm. Cypher-based triangle counting
    # is infeasible on 34M edges (would require enumerating all triangles).
    print("\n[FalkorDB] LCC: not supported (no built-in algorithm, Cypher too slow)")
    results["lcc"] = "N/A"

    _dump_all(g)
    bench_common.cleanup_docker("falkordb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("falkordb")
