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


# Compute-only variants (GRAPHALYTICS_OUTPUT=count, the default): same call, reduced to (rows, aggregate over every value).
COUNT_QUERIES = {
    "PageRank": "CALL algo.pageRank('Node', 'EDGE') YIELD node, score RETURN count(*) AS n, sum(score) AS agg",
    "WCC": "CALL algo.WCC(null) YIELD node, componentId RETURN count(*) AS n, max(componentId) AS agg",
    "CDLP": ("CALL algo.labelPropagation({nodeLabels: ['Node'], relationshipTypes: ['EDGE'], maxIterations: 10}) "
             "YIELD node, communityId RETURN count(*) AS n, max(communityId) AS agg"),
    "BFS": BFS_QUERY,    # already a count: the BFS procedure returns the list of reached nodes, the query returns its size
}
DISTINCT_QUERIES = {   # untimed verification of the number of distinct labels (the timed aggregate is a cheap max)
    "WCC": "CALL algo.WCC(null) YIELD node, componentId RETURN count(*) AS n, count(DISTINCT componentId) AS agg",
    "CDLP": ("CALL algo.labelPropagation({nodeLabels: ['Node'], relationshipTypes: ['EDGE'], maxIterations: 10}) "
             "YIELD node, communityId RETURN count(*) AS n, count(DISTINCT communityId) AS agg"),
}
FULL_QUERIES = {"PageRank": PAGERANK_QUERY, "WCC": WCC_QUERY, "CDLP": CDLP_QUERY, "BFS": BFS_QUERY}


def _save(rc):
    """Synchronous snapshot: Redis only writes its dump on the configured save points, so without this a stopped
    container comes back with an older, partial graph (observed: 62.8M of 68.4M edges) and the load is repeated."""
    try:
        rc.execute_command("SAVE")
        print("  [FalkorDB] graph snapshot saved")
    except Exception as e:
        print(f"  [FalkorDB] SAVE failed: {e}")


def _bulk_insert_command():
    import shutil
    import sys
    exe = os.environ.get("FALKORDB_BULK_INSERT") or shutil.which("falkordb-bulk-insert") \
        or os.path.join(os.path.dirname(sys.executable), "falkordb-bulk-insert")
    return exe


def _bulk_load(graph="bench"):
    """Load with FalkorDB's bulk loader (GRAPH.BULK) instead of one write query per 5000 edges.

    The per-query path costs two index lookups and one edge insertion per edge in the single write thread (about
    25k edges/s here: 53 minutes for the 68M both-direction edge records). The bulk loader needs 116 s for the same
    graph (84 s inside the server) and the outputs validate identically. It is the default; FALKORDB_BULK_LOAD=0 or a
    missing `falkordb-bulk-insert` (pip install falkordb-bulk-loader) selects the per-query path. The bulk loader sends binary node and edge
    tokens, resolves the endpoints through its own id map and builds the matrices directly.
    Schemaless mode: the first node column is the id and also stored as property `id` (named by the header); the first
    two edge columns are the endpoints and the other columns are properties. Both directions are written by awk.
    """
    import subprocess
    import bench_state
    work = bench_state.state_path("tmp", "falkordb-bulk", create=True)
    nodes, edges = os.path.join(work, "Node.csv"), os.path.join(work, "EDGE.csv")
    subprocess.run(["awk", "BEGIN{print \"id\"} {print $1}", VERTEX_FILE], stdout=open(nodes, "w"), check=True)
    subprocess.run(["awk", "BEGIN{print \"src dst weight\"} {print $1\" \"$2\" \"$3; print $2\" \"$1\" \"$3}", EDGE_FILE],
                   stdout=open(edges, "w"), check=True)
    # The loader's client has a short default socket timeout: while the server builds the matrices of a big edge batch it
    # does not read, and the send then fails with "Timeout writing to socket" (graph500-22, 128M edge records).
    cmd = [_bulk_insert_command(), graph, "-u", "redis://127.0.0.1:6379?socket_timeout=3600", "-o", " ", "-n", nodes, "-r", edges]
    print("  " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)
    for f in (nodes, edges):
        os.remove(f)


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
        # No automatic background snapshots: with the default save points a BGSAVE forks the 20+ GB process during the
        # load, the fork fails, Redis marks the save as failed and answers every write with MISCONF (this stopped the
        # load of 2026-10-05 at 35M edges). The graph is written once with a synchronous SAVE after the load.
        rc.config_set("save", "")
        rc.config_set("stop-writes-on-bgsave-error", "no")
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

    if needs_load and os.environ.get("FALKORDB_BULK_LOAD", "1") != "0" and os.path.exists(_bulk_insert_command()):
        print("\n[FalkorDB] Loading data with the bulk loader...")
        start = time.perf_counter()
        _bulk_load()
        g = fdb.select_graph('bench')
        g.query("CREATE INDEX FOR (n:Node) ON (n.id)")
        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
        _save(redis.Redis(host='localhost', port=6379, socket_timeout=3600))
    elif needs_load:
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
        _save(redis.Redis(host='localhost', port=6379, socket_timeout=3600))

    r = g.ro_query("MATCH (n:Node) RETURN count(n) AS c")
    print(f"  Vertices: {r.result_set[0][0]}")
    r = g.ro_query("MATCH ()-[e:EDGE]->() RETURN count(e) AS c")
    print(f"  Edges: {r.result_set[0][0]}")

    if bench_common.dump_only():
        _dump_all(g)
        bench_common.cleanup_docker("falkordb")
        return results

    # GRAPHALYTICS_OUTPUT=count (default): the call returns a summary row; =full: every row (BFS stays the reached-count query).
    def timed(name, key):
        print(f"\n[FalkorDB] Running {name}...")
        count_only = bench_common.compute_only()
        q = (COUNT_QUERIES if count_only else FULL_QUERIES)[name]
        def _run():
            r = g.ro_query(q)
            if name == "BFS":
                return r.result_set[0][0], None
            return tuple(r.result_set[0]) if count_only else (len(r.result_set), None)
        elapsed, summary = bench_common.run_timed_warm(name, _run)
        results[key] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {name} time: {elapsed:.2f}s  (summary {summary})")
            if count_only:
                n, agg = summary
                if name in DISTINCT_QUERIES:
                    n, agg = g.ro_query(DISTINCT_QUERIES[name]).result_set[0]
                # BFS: the reached list does not contain the source vertex (the export adds it); the summary has no distance aggregate
                bench_common.check_summary("falkordb", name, n + 1 if name == "BFS" else n, None if name == "BFS" else agg)

    timed("PageRank", "pagerank")
    timed("WCC", "wcc")
    timed("BFS", "bfs")

    # --- SSSP ---
    # FalkorDB's algo.SSpaths does not support full single-source Dijkstra;
    # it only returns paths to direct neighbors. Not usable for SSSP benchmark.
    print("\n[FalkorDB] SSSP: not supported (algo.SSpaths is pair-oriented, not full SSSP)")
    results["sssp"] = "N/A"

    # --- CDLP (Community Detection via Label Propagation) ---
    timed("CDLP", "cdlp")

    # --- LCC (Local Clustering Coefficient) ---
    # FalkorDB has no built-in LCC algorithm. Cypher-based triangle counting
    # is infeasible on 34M edges (would require enumerating all triangles).
    print("\n[FalkorDB] LCC: not supported (no built-in algorithm, Cypher too slow)")
    results["lcc"] = "N/A"

    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_output"] = bench_common.OUTPUT_MODE
    _dump_all(g)
    bench_common.cleanup_docker("falkordb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("falkordb")
