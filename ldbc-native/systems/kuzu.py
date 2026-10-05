"""Kuzu benchmark for LDBC Graphalytics."""

import time
import os
import shutil
import subprocess

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# The Graphalytics datasets store every undirected edge once. Kuzu's algo extension and its recursive
# patterns follow the stored direction, so the load adds a reversed copy of the edge table (EdgeRev) and the
# algorithms run over both. PageRank: the reference does 10 iterations; Kuzu counts the initialisation as the
# first one, so maxIterations is 11 (verified: output equals the reference to machine precision).
PAGERANK_QUERY = ("CALL page_rank('pg', dampingFactor := 0.85, maxIterations := 11, tolerance := 0.0) "
                  "RETURN node.id, rank")
WCC_QUERY = "CALL weakly_connected_components('pg') RETURN node.id, group_id"
LCC_QUERY = "CALL local_clustering_coefficient('pg') RETURN node.id, coefficient"
BFS_QUERY = "MATCH (a:Node {id: 6})-[e:Edge* SHORTEST 1..30]-(b:Node) RETURN b.id, length(e)"


def _reverse_edges(src, dst):
    """Write `dst src weight` for every `src dst weight` line (the other direction of each edge)."""
    subprocess.run(["awk", "-F", " ", "{print $2\" \"$1\" \"$3}", src], stdout=open(dst, "w"), check=True)


def _dump_all(conn):
    """Full per-vertex outputs of the exact calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q):
        r = conn.execute(q)
        while r.has_next():
            yield r.get_next()
    def ids():
        return [row[0] for row in rows("MATCH (n:Node) RETURN n.id")]
    bench_common.dump_safely("kuzu", "PR", lambda: bench_common.dump_rows(
        "kuzu", "PR", ((r[0], float(r[1])) for r in rows(PAGERANK_QUERY))))
    bench_common.dump_safely("kuzu", "WCC", lambda: bench_common.dump_rows(
        "kuzu", "WCC", ((r[0], r[1]) for r in rows(WCC_QUERY))))
    bench_common.dump_safely("kuzu", "LCC", lambda: bench_common.dump_rows(
        "kuzu", "LCC", ((r[0], float(r[1])) for r in rows(LCC_QUERY))))
    bench_common.dump_safely("kuzu", "BFS", lambda: bench_common.dump_bfs(
        "kuzu", {r[0]: r[1] for r in rows(BFS_QUERY)}, ids(), 6))


def run_benchmark():
    import kuzu
    print("\n" + "=" * 70)
    print("KUZU BENCHMARK")
    print("=" * 70)

    db_path = bench_common.embedded_db_path("graphalytics", "kuzu", "db")
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
            r = conn.execute("MATCH ()-[e:EdgeRev]->() RETURN count(e) AS cnt")
            if r.has_next() and r.get_next()[0] > 0:
                needs_load = False
                print("\n[Kuzu] Data already loaded, skipping import")
        except Exception:
            needs_load = True

    if needs_load:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        elif os.path.exists(db_path):
            os.remove(db_path)

        print("\n[Kuzu] Loading data...")
        start = time.perf_counter()
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)

        conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE Edge(FROM Node TO Node, weight DOUBLE)")
        conn.execute("CREATE REL TABLE EdgeRev(FROM Node TO Node, weight DOUBLE)")

        v_csv = "/tmp/ldbc_vertices.csv"
        e_csv = "/tmp/ldbc_edges.csv"
        shutil.copy(VERTEX_FILE, v_csv)
        shutil.copy(EDGE_FILE, e_csv)

        conn.execute(f"COPY Node FROM '{v_csv}' (HEADER=false)")
        conn.execute(f"COPY Edge FROM '{e_csv}' (HEADER=false, DELIM=' ')")
        rev_csv = "/tmp/ldbc_edges_rev.csv"
        _reverse_edges(EDGE_FILE, rev_csv)
        conn.execute(f"COPY EdgeRev FROM '{rev_csv}' (HEADER=false, DELIM=' ')")
        os.remove(rev_csv)

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
        conn.execute("CALL project_graph('pg', ['Node'], ['Edge', 'EdgeRev'])")
        print("  Projected graph created")
    except Exception as e:
        print(f"  Project graph failed: {e}")

    def timed(name, label, query, key):
        print(f"\n[Kuzu] Running {label}...")
        def _run():
            r = conn.execute(query)
            count = 0
            while r.has_next():
                r.get_next()
                count += 1
            print(f"  {label}: {count} rows")
            return count
        elapsed, _ = bench_common.run_timed_warm(name, _run)
        results[key] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {label} time: {elapsed:.2f}s")

    # Every timed call is the exact query that _dump_all exports and the validator checks: full output, no LIMIT.
    timed("PageRank", "PageRank", PAGERANK_QUERY, "pagerank")
    timed("WCC", "WCC", WCC_QUERY, "wcc")
    timed("LCC", "LCC", LCC_QUERY, "lcc")
    timed("BFS", "BFS (undirected shortest paths from vertex 6)", BFS_QUERY, "bfs")

    _dump_all(conn)

    # The database stays for the next run (load once); --reset deletes it.
    del conn
    del db
    return results
