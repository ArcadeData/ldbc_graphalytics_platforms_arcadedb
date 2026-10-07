"""Memgraph benchmark for LDBC Graphalytics."""

import os
import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# The Graphalytics datasets store every undirected edge once, while Memgraph's MAGE algorithms and its *BFS /
# *wShortest expansions follow the stored direction. After the import every edge therefore also exists in the
# opposite direction (marked rev: true), and all algorithms run on that symmetric graph.
# PageRank: the reference does 10 iterations from the uniform vector with damping 0.85.
PAGERANK_ITERATIONS = int(os.environ.get("MEMGRAPH_PR_ITERATIONS", "10"))
PAGERANK_QUERY = (f"CALL pagerank.get({PAGERANK_ITERATIONS}, 0.85, 0.0) YIELD node, rank "
                  "RETURN node.id AS id, rank")
WCC_QUERY = ("CALL weakly_connected_components.get() YIELD node, component_id "
             "RETURN node.id AS id, component_id")
CDLP_QUERY = "CALL community_detection.get() YIELD node, community_id RETURN node.id AS id, community_id"
BFS_QUERY = "MATCH (a:Node {id: 6})-[e:EDGE *BFS]->(b:Node) RETURN b.id, size(e) AS dist"
SSSP_QUERY = ("MATCH (a:Node {id: 6})-[e:EDGE *wShortest (e, n | e.weight)]->(b:Node) "
              "RETURN b.id, reduce(w = 0.0, x IN e | w + x.weight) AS dist")


# Compute-only variants (GRAPHALYTICS_OUTPUT=count, the default): same call, reduced to (rows, aggregate over every value).
COUNT_QUERIES = {
    "PageRank": (f"CALL pagerank.get({PAGERANK_ITERATIONS}, 0.85, 0.0) YIELD node, rank RETURN count(*) AS n, sum(rank) AS agg"),
    "WCC": "CALL weakly_connected_components.get() YIELD node, component_id RETURN count(*) AS n, max(component_id) AS agg",
    "CDLP": "CALL community_detection.get() YIELD node, community_id RETURN count(*) AS n, max(community_id) AS agg",
    "BFS": "MATCH (a:Node {id: 6})-[e:EDGE *BFS]->(b:Node) RETURN count(*) AS n, max(size(e)) AS agg",
    "SSSP": ("MATCH (a:Node {id: 6})-[e:EDGE *wShortest (e, n | e.weight)]->(b:Node) "
             "RETURN count(*) AS n, max(reduce(w = 0.0, x IN e | w + x.weight)) AS agg"),
}
DISTINCT_QUERIES = {   # untimed verification of the number of distinct labels (the timed aggregate is a cheap max)
    "WCC": "CALL weakly_connected_components.get() YIELD node, component_id RETURN count(*) AS n, count(DISTINCT component_id) AS agg",
    "CDLP": "CALL community_detection.get() YIELD node, community_id RETURN count(*) AS n, count(DISTINCT community_id) AS agg",
}
FULL_QUERIES = {}   # filled below once the query constants exist


def _add_reverse_edges(conn, cursor, batch_size=50000):
    """Create the opposite direction of every imported edge (separate step so that a loaded database is reused)."""
    conn.commit()
    with open(EDGE_FILE) as f:
        batch = []
        for line in f:
            parts = line.strip().split()
            batch.append({"src": int(parts[1]), "dst": int(parts[0]), "weight": float(parts[2])})
            if len(batch) >= batch_size:
                cursor.execute("""
                    UNWIND $edges AS e
                    MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                    CREATE (a)-[:EDGE {weight: e.weight, rev: true}]->(b)
                """, {"edges": batch})
                conn.commit()
                batch = []
        if batch:
            cursor.execute("""
                UNWIND $edges AS e
                MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                CREATE (a)-[:EDGE {weight: e.weight, rev: true}]->(b)
            """, {"edges": batch})
            conn.commit()


FULL_QUERIES.update({"PageRank": PAGERANK_QUERY, "WCC": WCC_QUERY, "CDLP": CDLP_QUERY, "BFS": BFS_QUERY, "SSSP": SSSP_QUERY})


def _dump_all(cursor, skip_cdlp=False):
    """Full per-vertex outputs of the exact calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q):
        cursor.execute(q)
        return cursor.fetchall()
    bench_common.dump_safely("memgraph", "PR", lambda: bench_common.dump_rows("memgraph", "PR", (
        (r[0], float(r[1])) for r in rows(PAGERANK_QUERY))))
    # An algorithm that is skipped (GRAPHALYTICS_SKIP) is not exported either: on graph500-22 the WCC and CDLP procedures run out
    # of memory, and that kills the session the remaining exports need.
    if "wcc" not in bench_common.GRAPHALYTICS_SKIP:
        bench_common.dump_safely("memgraph", "WCC", lambda: bench_common.dump_rows("memgraph", "WCC", (
            (r[0], r[1]) for r in rows(WCC_QUERY))))
    if not skip_cdlp and "cdlp" not in bench_common.GRAPHALYTICS_SKIP:  # a timed run that hit the limit is not repeated for the export
        bench_common.dump_safely("memgraph", "CDLP", lambda: bench_common.dump_rows("memgraph", "CDLP", (
            (r[0], r[1]) for r in rows(CDLP_QUERY))))
    def bfs():
        reached = {r[0]: r[1] for r in rows(BFS_QUERY)}
        bench_common.dump_bfs("memgraph", reached, [r[0] for r in rows("MATCH (n:Node) RETURN n.id")], 6)
    bench_common.dump_safely("memgraph", "BFS", bfs)
    def sssp():
        dist = {r[0]: float(r[1]) for r in rows(SSSP_QUERY)}
        dist[6] = 0.0
        bench_common.dump_rows("memgraph", "SSSP", (
            (r[0], dist.get(r[0], "infinity")) for r in rows("MATCH (n:Node) RETURN n.id")))
    bench_common.dump_safely("memgraph", "SSSP", sssp)
    # LCC (nxalg.clustering) is not exported: it did not finish within the time limit on this graph.


def run_benchmark():
    import mgclient
    print("\n" + "=" * 70)
    print("MEMGRAPH BENCHMARK")
    print("=" * 70)

    results = {}

    try:
        conn = mgclient.connect(host='127.0.0.1', port=7687, sslmode=mgclient.MG_SSLMODE_DISABLE)
    except Exception as e:
        print(f"  Cannot connect to Memgraph: {e}")
        print("  Start with: docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage")
        return {"error": str(e)}

    cursor = conn.cursor()

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            cursor.execute("MATCH ()-[e]->() RETURN count(e) AS c")
            edge_count = cursor.fetchone()[0]
            if edge_count > 0:
                needs_load = False
                print(f"\n[Memgraph] Data already loaded ({edge_count} edges), skipping import")
        except Exception:
            pass

    if needs_load:
        # --- LOAD DATA ---
        print("\n[Memgraph] Loading data...")
        start = time.perf_counter()

        conn.commit()
        conn.autocommit = True
        cursor.execute("MATCH (n) DETACH DELETE n")
        try:
            cursor.execute("DROP INDEX ON :Node(id)")
        except Exception:
            pass
        conn.autocommit = False

        # Load vertices in batches
        print("  Loading vertices...")
        batch_size = 50000
        with open(VERTEX_FILE) as f:
            batch = []
            for line in f:
                vid = int(line.strip())
                batch.append(vid)
                if len(batch) >= batch_size:
                    cursor.execute(
                        "UNWIND $ids AS id CREATE (:Node {id: id})",
                        {"ids": batch}
                    )
                    conn.commit()
                    batch = []
            if batch:
                cursor.execute("UNWIND $ids AS id CREATE (:Node {id: id})", {"ids": batch})
                conn.commit()

        conn.commit()
        conn.autocommit = True
        cursor.execute("CREATE INDEX ON :Node(id)")
        conn.autocommit = False

        # Load edges in batches
        print("  Loading edges...")
        with open(EDGE_FILE) as f:
            batch = []
            for line in f:
                parts = line.strip().split()
                batch.append({"src": int(parts[0]), "dst": int(parts[1]),
                              "weight": float(parts[2])})
                if len(batch) >= batch_size:
                    cursor.execute("""
                        UNWIND $edges AS e
                        MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                        CREATE (a)-[:EDGE {weight: e.weight}]->(b)
                    """, {"edges": batch})
                    conn.commit()
                    batch = []
            if batch:
                cursor.execute("""
                    UNWIND $edges AS e
                    MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                    CREATE (a)-[:EDGE {weight: e.weight}]->(b)
                """, {"edges": batch})
                conn.commit()

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

        cursor.execute("MATCH (n) RETURN count(n)")
        print(f"  Vertices: {cursor.fetchone()[0]}")
        cursor.execute("MATCH ()-[e]->() RETURN count(e)")
        print(f"  Edges: {cursor.fetchone()[0]}")

    cursor.execute("MATCH ()-[e:EDGE {rev: true}]->() RETURN count(e)")
    if cursor.fetchone()[0] == 0:
        print("\n[Memgraph] Adding the reverse direction of every edge...")
        # Part of the one-off load, so it is not bound by the 5-minute per-operation limit (the 64M edges of graph500-22
        # need more than that): a timeout here left a graph with only some reverse edges and invalid algorithm results.
        elapsed, _ = bench_common.run_timed("Reverse edges", lambda: _add_reverse_edges(conn, cursor), timeout=2400)
        if isinstance(elapsed, (int, float)):
            if "load" in results:
                results["load"] += elapsed
            print(f"  Reverse edges time: {elapsed:.2f}s (one-time step, add it to the recorded load time)")
        else:
            # A half-built graph gives invalid algorithm results: report the load and stop.
            print("  Reverse edges did not finish; stopping, the algorithms would run on a graph with only some reverse edges")
            conn.close()
            bench_common.cleanup_docker("memgraph")
            return results
    cursor.execute("MATCH ()-[e]->() RETURN count(e)")
    print(f"  Edges (both directions): {cursor.fetchone()[0]}")

    if bench_common.dump_only():
        _dump_all(cursor)
        conn.close()
        bench_common.cleanup_docker("memgraph")
        return results

    # The timed call is the same procedure call as the export; GRAPHALYTICS_OUTPUT=count (default) reduces it to a summary row,
    # =full returns every row.
    def timed(name, key):
        print(f"\n[Memgraph] Running {name}...")
        count_only = bench_common.compute_only()
        q = (COUNT_QUERIES if count_only else FULL_QUERIES)[name]
        def _run():
            cursor.execute(q)
            rows = cursor.fetchall()
            return rows[0] if count_only else (len(rows), None)

        def _profile():   # engine-reported time of the procedure call / expansion operator (PROFILE, absolute time in ms)
            import re
            pconn = mgclient.connect(host='127.0.0.1', port=7687, sslmode=mgclient.MG_SSLMODE_DISABLE)
            pconn.autocommit = True   # PROFILE is not allowed inside the multicommand transaction of the benchmark connection
            pcur = pconn.cursor()
            pcur.execute("PROFILE " + q)
            best = 0.0
            rows_ = pcur.fetchall()
            pconn.close()
            for row in rows_:
                op, absolute = str(row[0]), str(row[-1])
                m = re.search(r"([0-9.]+)\s*ms", absolute)
                if m and ("CallProcedure" in op or "Expand" in op or "BFS" in op or "ShortestPath" in op or "WeightedShortestPath" in op):
                    best = max(best, float(m.group(1)) / 1000.0)
            return best
        elapsed, summary = bench_common.run_timed_warm(name, _run)
        results[key] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {name} time: {elapsed:.2f}s  (summary {summary})")
            bench_common.measure_server_time("memgraph", name, _profile)
            if count_only:
                n, agg = summary
                if name in DISTINCT_QUERIES:
                    cursor.execute(DISTINCT_QUERIES[name])
                    n, agg = cursor.fetchall()[0]
                # BFS/SSSP: the source vertex is not an expansion result row (the export adds it)
                bench_common.check_summary("memgraph", name, n + 1 if name in ("BFS", "SSSP") else n, agg)

    timed("BFS", "bfs")
    timed("PageRank", "pagerank")
    timed("WCC", "wcc")

    # --- LCC ---
    # MAGE only ships nxalg.clustering (NetworkX, pure Python, not installed in the image). A failed procedure call
    # also kills the Bolt session, so the call is not attempted: LCC stays N/A for Memgraph.
    print("\n[Memgraph] LCC: no native procedure in the MAGE image, skipped (N/A)")

    timed("SSSP", "sssp")
    timed("CDLP", "cdlp")
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_server_time"] = dict(bench_common.SERVER_TIMES)
    results["_output"] = bench_common.OUTPUT_MODE

    if results.get("cdlp") == "timeout":  # the server may still be busy with the abandoned query: new session
        conn.close()
        conn = mgclient.connect(host='127.0.0.1', port=7687, sslmode=mgclient.MG_SSLMODE_DISABLE)
        conn.autocommit = True
        cursor = conn.cursor()
    _dump_all(cursor, skip_cdlp=results.get("cdlp") == "timeout")
    conn.close()
    bench_common.cleanup_docker("memgraph")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("memgraph")
