"""DuckPGQ benchmark for LDBC Graphalytics."""

import os
import threading
import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# The Graphalytics datasets store every undirected edge once; DuckPGQ follows the stored direction. PageRank and
# BFS therefore run on `ldbc2`, a property graph over `edges2` (every edge in both directions). WCC and LCC are
# direction-agnostic and keep using `ldbc`.
PAGERANK_QUERY = "SELECT id, pagerank FROM pagerank(ldbc2, nodes, edges2)"
WCC_QUERY = "SELECT id, componentId FROM weakly_connected_component(ldbc, nodes, edges)"
LCC_QUERY = "SELECT id, local_clustering_coefficient FROM local_clustering_coefficient(ldbc, nodes, edges)"
BFS_QUERY = """
    FROM GRAPH_TABLE(ldbc2
        MATCH p = ANY SHORTEST (a:nodes WHERE a.id = 6)-[e:edges2]->{1,30}(b:nodes)
        COLUMNS (b.id AS dst, path_length(p) AS dist)
    )
"""


# Compute-only variants (GRAPHALYTICS_OUTPUT=count, the default): same table function, reduced to (rows, aggregate over a value column).
COUNT_QUERIES = {
    "PageRank": "SELECT count(*) AS n, sum(pagerank) AS agg FROM pagerank(ldbc2, nodes, edges2)",
    "WCC": "SELECT count(*) AS n, count(DISTINCT componentId) AS agg FROM weakly_connected_component(ldbc, nodes, edges)",
    "LCC": "SELECT count(*) AS n, sum(local_clustering_coefficient) AS agg FROM local_clustering_coefficient(ldbc, nodes, edges)",
    "BFS": """
    SELECT count(*) AS n, max(dist) AS agg FROM GRAPH_TABLE(ldbc2
        MATCH p = ANY SHORTEST (a:nodes WHERE a.id = 6)-[e:edges2]->{1,30}(b:nodes)
        COLUMNS (b.id AS dst, path_length(p) AS dist)
    )
""",
}


def _dump_all(conn):
    """Full per-vertex outputs of the exact calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    bench_common.dump_safely("duckpgq", "PR", lambda: bench_common.dump_rows(
        "duckpgq", "PR", conn.execute(PAGERANK_QUERY).fetchall()))
    bench_common.dump_safely("duckpgq", "WCC", lambda: bench_common.dump_rows(
        "duckpgq", "WCC", conn.execute(WCC_QUERY).fetchall()))
    bench_common.dump_safely("duckpgq", "LCC", lambda: bench_common.dump_rows(
        "duckpgq", "LCC", conn.execute(LCC_QUERY).fetchall()))
    # BFS is exported by the timed run itself when it finishes: the uncapped shortest-path query can exceed
    # the time limit in DuckPGQ, and it is not run again for the export.


def run_benchmark():
    import duckdb
    print("\n" + "=" * 70)
    print("DuckPGQ BENCHMARK")
    print("=" * 70)

    db_path = bench_common.embedded_db_path("graphalytics", "duckpgq", "db.duckdb")
    if os.path.exists(db_path):
        os.remove(db_path)

    results = {}
    conn = duckdb.connect(db_path)

    # Install and load DuckPGQ
    print("\n[DuckPGQ] Setting up extension...")
    try:
        conn.execute("INSTALL duckpgq FROM community")
    except Exception:
        pass
    conn.execute("LOAD duckpgq")

    # --- LOAD DATA ---
    print("\n[DuckPGQ] Loading data...")
    start = time.perf_counter()

    conn.execute(f"""
        CREATE TABLE nodes AS
        SELECT column0::BIGINT AS id FROM read_csv('{VERTEX_FILE}',
            delim=' ', header=false, auto_detect=false, columns={{'column0': 'BIGINT'}})
    """)

    conn.execute(f"""
        CREATE TABLE edges AS
        SELECT column0::BIGINT AS src, column1::BIGINT AS dst, column2::DOUBLE AS weight
        FROM read_csv('{EDGE_FILE}',
            delim=' ', header=false, auto_detect=false,
            columns={{'column0': 'BIGINT', 'column1': 'BIGINT', 'column2': 'DOUBLE'}})
    """)

    # Create property graph using -CREATE syntax (DuckPGQ requirement)
    conn.execute("""
        -CREATE PROPERTY GRAPH ldbc
        VERTEX TABLES (nodes)
        EDGE TABLES (edges SOURCE KEY (src) REFERENCES nodes (id)
                          DESTINATION KEY (dst) REFERENCES nodes (id))
    """)

    conn.execute("CREATE TABLE edges2 AS SELECT src, dst, weight FROM edges UNION ALL SELECT dst, src, weight FROM edges")
    conn.execute("""
        -CREATE PROPERTY GRAPH ldbc2
        VERTEX TABLES (nodes)
        EDGE TABLES (edges2 SOURCE KEY (src) REFERENCES nodes (id)
                           DESTINATION KEY (dst) REFERENCES nodes (id))
    """)

    load_time = time.perf_counter() - start
    results["load"] = load_time
    print(f"  Load time: {load_time:.2f}s")

    r = conn.execute("SELECT count(*) FROM nodes").fetchone()
    print(f"  Vertices: {r[0]}")
    r = conn.execute("SELECT count(*) FROM edges").fetchone()
    print(f"  Edges: {r[0]}")

    if bench_common.dump_only():
        _dump_all(conn)
        conn.close()
        os.remove(db_path)
        return results

    def timed(name, label, query, key, keep=False):
        print(f"\n[DuckPGQ] Running {label}...")
        count_only = bench_common.compute_only()
        q = COUNT_QUERIES[name] if count_only else query
        def _run():
            # SIGALRM cannot interrupt a query that runs inside DuckDB, so the limit is enforced with interrupt()
            timer = threading.Timer(bench_common.QUERY_TIMEOUT, conn.interrupt)
            timer.start()
            try:
                r = conn.execute(q).fetchall()
            finally:
                timer.cancel()
            return r
        elapsed, rows = bench_common.run_timed_warm(name, _run)
        results[key] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {label} time: {elapsed:.2f}s  ({len(rows)} rows returned)")
            if count_only:
                n, agg = rows[0]
                bench_common.check_summary("duckpgq", name, n + 1 if name == "BFS" else n, agg)   # BFS: the source is not a result row
                rows = None
        return rows if keep else None

    # GRAPHALYTICS_OUTPUT=full: every timed call is the exact query that _dump_all exports and the validator checks.
    timed("PageRank", "PageRank", PAGERANK_QUERY, "pagerank")
    timed("WCC", "WCC", WCC_QUERY, "wcc")
    timed("LCC", "LCC", LCC_QUERY, "lcc")
    _dump_all(conn)  # PR, WCC, LCC outputs are exported before the BFS, which can hit the time limit
    bfs_rows = timed("BFS", "BFS (undirected shortest paths from vertex 6)", BFS_QUERY, "bfs", keep=True)
    if bench_common.compute_only() and isinstance(results.get("bfs"), (int, float)) and bench_common.dump_enabled():
        bfs_rows = conn.execute(BFS_QUERY).fetchall()   # the full output for the export, untimed (the timed call returned a summary)
    if bfs_rows:
        bench_common.dump_safely("duckpgq", "BFS", lambda: bench_common.dump_bfs(
            "duckpgq", {r[0]: r[1] for r in bfs_rows},
            [row[0] for row in conn.execute("SELECT id FROM nodes").fetchall()], 6))
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_output"] = bench_common.OUTPUT_MODE

    conn.close()
    os.remove(db_path)
    return results
