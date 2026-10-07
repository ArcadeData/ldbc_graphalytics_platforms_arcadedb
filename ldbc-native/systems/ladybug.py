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

    # The timed calls are compute only (GRAPHALYTICS_OUTPUT=count, the default): the same call as the export, reduced to
    # (rows, aggregate over every value). GRAPHALYTICS_OUTPUT=full returns the full output (BFS) instead.
    # PageRank/WCC/LCC need the algo extension, which does not load on macOS arm64 (they fail and are reported as N/A).
    COUNT = {
        "PageRank": ("CALL page_rank('pg') RETURN count(*) AS n, sum(rank) AS agg", "pagerank"),
        "WCC": ("CALL weakly_connected_components('pg') RETURN count(*) AS n, count(DISTINCT group_id) AS agg", "wcc"),
        "LCC": ("CALL local_clustering_coefficient('pg') RETURN count(*) AS n, sum(coefficient) AS agg", "lcc"),
        "BFS": ("MATCH (a:Node {id: 6})-[e:Edge* SHORTEST 1..30]-(b:Node) RETURN count(*) AS n, max(length(e)) AS agg", "bfs"),
    }
    for name, (query, key) in COUNT.items():
        print(f"\n[LadybugDB] Running {name}...")
        count_only = bench_common.compute_only()
        q = query if (count_only or name != "BFS") else BFS_QUERY
        def _run(q=q):
            r = conn.execute(q)
            if count_only or name != "BFS":
                n, agg = r.get_next()
                return n, agg
            n = 0
            while r.has_next():
                r.get_next()
                n += 1
            return n, None
        elapsed, summary = bench_common.run_timed_warm(name, _run)
        results[key] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {name} time: {elapsed:.2f}s  (summary {summary})")
            if count_only:
                n, agg = summary
                bench_common.check_summary("ladybug", name, n + 1 if name == "BFS" else n, agg)
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_output"] = bench_common.OUTPUT_MODE

    _dump_bfs(conn)

    # The database stays for the next run (load once); --reset deletes it.
    del conn
    del db
    return results
