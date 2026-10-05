"""LadybugDB benchmark for LDBC Graphalytics."""

import time
import os
import shutil

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# Undirected shortest paths from the official source, full output (the datasets store each undirected edge once).
BFS_QUERY = "MATCH (a:Node {id: 6})-[e:Edge* SHORTEST 1..30]-(b:Node) RETURN b.id, length(e)"


def _dump_bfs(conn):
    """Full BFS output of the exact query that is timed (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q):
        r = conn.execute(q)
        while r.has_next():
            yield r.get_next()
    bench_common.dump_safely("ladybug", "BFS", lambda: bench_common.dump_bfs(
        "ladybug", {r[0]: r[1] for r in rows(BFS_QUERY)},
        [row[0] for row in rows("MATCH (n:Node) RETURN n.id")], 6))


def run_benchmark():
    import ladybug as kuzu
    print("\n" + "=" * 70)
    print("LADYBUGDB BENCHMARK")
    print("=" * 70)

    db_path = bench_common.embedded_db_path("graphalytics", "ladybug", "db")
    results = {}

    if bench_common.RESET:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        elif os.path.exists(db_path):
            os.remove(db_path)

    # Check if data already loaded
    needs_load = True
    if os.path.exists(db_path) and not bench_common.RESET:
        try:
            db = kuzu.Database(db_path)
            conn = kuzu.Connection(db)
            r = conn.execute("MATCH ()-[e:Edge]->() RETURN count(e) AS cnt")
            if r.has_next() and r.get_next()[0] > 0:
                needs_load = False
                print("\n[LadybugDB] Data already loaded, skipping import")
        except Exception:
            needs_load = True

    if needs_load:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        elif os.path.exists(db_path):
            os.remove(db_path)

        print("\n[LadybugDB] Loading data...")
        start = time.perf_counter()
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)

        conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE Edge(FROM Node TO Node, weight DOUBLE)")

        v_csv = "/tmp/ldbc_vertices.csv"
        e_csv = "/tmp/ldbc_edges.csv"
        shutil.copy(VERTEX_FILE, v_csv)
        shutil.copy(EDGE_FILE, e_csv)

        conn.execute(f"COPY Node FROM '{v_csv}' (HEADER=false)")
        conn.execute(f"COPY Edge FROM '{e_csv}' (HEADER=false, DELIM=' ')")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Verify
    r = conn.execute("MATCH (n:Node) RETURN count(n) AS cnt")
    while r.has_next():
        print(f"  Vertices: {r.get_next()[0]}")
    r = conn.execute("MATCH ()-[e:Edge]->() RETURN count(e) AS cnt")
    while r.has_next():
        print(f"  Edges: {r.get_next()[0]}")

    # Load algo extension
    try:
        conn.execute("INSTALL algo")
    except Exception:
        pass
    try:
        conn.execute("LOAD EXTENSION algo")
    except Exception:
        pass

    # Create projected graph for algorithms
    try:
        conn.execute("CALL project_graph('pg', ['Node'], ['Edge'])")
        print("  Projected graph created")
    except Exception as e:
        print(f"  Project graph failed: {e}")

    # --- PageRank ---
    print("\n[LadybugDB] Running PageRank...")
    def _run_pagerank():
        r = conn.execute("""
            CALL page_rank('pg') RETURN node.id, rank
            ORDER BY rank DESC LIMIT 10
        """)
        count = 0
        while r.has_next():
            row = r.get_next()
            if count < 3:
                print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
            count += 1
        return count
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC (Weakly Connected Components) ---
    print("\n[LadybugDB] Running WCC...")
    def _run_wcc():
        r = conn.execute("""
            CALL weakly_connected_components('pg')
            RETURN group_id, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        count = 0
        while r.has_next():
            row = r.get_next()
            if count < 3:
                print(f"    Component: group={row[0]}, size={row[1]}")
            count += 1
        return count
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- LCC (Local Clustering Coefficient) ---
    print("\n[LadybugDB] Running LCC...")
    def _run_lcc():
        r = conn.execute("""
            CALL local_clustering_coefficient('pg')
            RETURN node.id, coefficient
            ORDER BY coefficient DESC LIMIT 10
        """)
        count = 0
        while r.has_next():
            row = r.get_next()
            if count < 3:
                print(f"    Top LCC: node={row[0]}, coeff={row[1]:.6f}")
            count += 1
        return count
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- BFS (shortest path from source) ---
    print("\n[LadybugDB] Running BFS/Shortest Path from vertex 6...")
    def _run_bfs():
        r = conn.execute(BFS_QUERY)
        count = 0
        while r.has_next():
            r.get_next()
            count += 1
        print(f"  Reached {count} nodes")
        return count
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    _dump_bfs(conn)

    # The database stays for the next run (load once); --reset deletes it.
    del conn
    del db
    return results
