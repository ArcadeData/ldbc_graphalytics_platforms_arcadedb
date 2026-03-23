"""DuckPGQ benchmark for LDBC Graphalytics."""

import time
import os

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


def run_benchmark():
    import duckdb
    print("\n" + "=" * 70)
    print("DuckPGQ BENCHMARK")
    print("=" * 70)

    db_path = "/tmp/duckpgq_benchmark.db"
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

    load_time = time.perf_counter() - start
    results["load"] = load_time
    print(f"  Load time: {load_time:.2f}s")

    r = conn.execute("SELECT count(*) FROM nodes").fetchone()
    print(f"  Vertices: {r[0]}")
    r = conn.execute("SELECT count(*) FROM edges").fetchone()
    print(f"  Edges: {r[0]}")

    # --- PageRank ---
    print("\n[DuckPGQ] Running PageRank...")
    def _run_pagerank():
        r = conn.execute("""
            SELECT * FROM pagerank(ldbc, nodes, edges)
            ORDER BY pagerank DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
        return r
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[DuckPGQ] Running WCC...")
    def _run_wcc():
        r = conn.execute("""
            SELECT componentId, count(*) AS size
            FROM weakly_connected_component(ldbc, nodes, edges)
            GROUP BY componentId
            ORDER BY size DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Component: id={row[0]}, size={row[1]}")
        return r
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- LCC ---
    print("\n[DuckPGQ] Running LCC...")
    def _run_lcc():
        r = conn.execute("""
            SELECT * FROM local_clustering_coefficient(ldbc, nodes, edges)
            ORDER BY local_clustering_coefficient DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Top LCC: node={row[0]}, coeff={row[1]:.6f}")
        return r
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- BFS / Shortest Path ---
    print("\n[DuckPGQ] Running Shortest Path from vertex 6...")
    def _run_bfs():
        r = conn.execute("""
            FROM GRAPH_TABLE(ldbc
                MATCH p = ANY SHORTEST (a:nodes WHERE a.id = 6)-[e:edges]->{1,30}(b:nodes)
                COLUMNS (b.id AS dst, path_length(p) AS dist)
            ) LIMIT 50000
        """).fetchall()
        print(f"  Reached {len(r)} nodes")
        return r
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    conn.close()
    os.remove(db_path)
    return results
