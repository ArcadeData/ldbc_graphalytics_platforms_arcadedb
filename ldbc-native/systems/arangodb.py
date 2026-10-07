"""ArangoDB benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# Pregel counts the initialisation as the first superstep: the reference's 10 PageRank iterations are 11 supersteps
# (verified: output equals the reference within 1e-4 for every vertex; 10 and 12 do not).
PAGERANK_SUPERSTEPS = 11
SSSP_COUNT_QUERY = """
    FOR v, e, p IN 0..100 OUTBOUND 'nodes/6' GRAPH 'bench'
        OPTIONS {order: 'weighted', weightAttribute: 'weight', uniqueVertices: 'global'}
        COLLECT AGGREGATE n = COUNT(1), m = MAX(LAST(p.weights))
        RETURN {n: n, agg: m}
"""
# Pregel's sssp is unweighted (hop count); the weighted single-source distances come from a weighted AQL traversal
SSSP_QUERY = """
    FOR v, e, p IN 0..100 OUTBOUND 'nodes/6' GRAPH 'bench'
        OPTIONS {order: 'weighted', weightAttribute: 'weight', uniqueVertices: 'global'}
        RETURN [v.vid, LAST(p.weights)]
"""


def _release_pregel_job(db, job_id):
    """A finished Pregel job keeps its in-memory graph copy until its ttl (10 minutes) expires. Repeated runs
    (warm-up + timed runs) then pile up several GiB each and the container is OOM-killed (observed: 5 -> 34 GiB in
    7 minutes at the 36 GiB limit), so every finished job is deleted explicitly."""
    import time as t
    try:
        db.pregel.delete_job(job_id)
    except Exception:  # noqa: BLE001
        pass
    t.sleep(2)


def _long_db(timeout):
    from arango import ArangoClient
    return ArangoClient(hosts='http://localhost:8529', request_timeout=timeout).db(
        '_system', username='root', password='benchmark')   # the default client times out after 60 s


def _dump_all(db, run_pregel):
    """Full per-vertex outputs (Pregel stores its results in a field of each vertex document)."""
    if not bench_common.dump_enabled():
        return
    def pregel(vendor_algo, algo, field, **kw):
        params = dict(kw.pop("algo_params", {}) or {})
        params["resultField"] = field
        job_id = db.pregel.create_job(graph='bench', algorithm=algo, store=True, algorithm_params=params, **kw)
        import time as t
        while True:
            job = db.pregel.job(job_id)
            if job['state'] in ('done', 'canceled', 'fatal error'):
                break
            t.sleep(1)
        _release_pregel_job(db, job_id)
        if job['state'] != 'done':
            raise RuntimeError(f"{vendor_algo} export failed: {job['state']}")
        t.sleep(2)
        return db.aql.execute(f"FOR v IN nodes RETURN [v.vid, v.`{field}`]", ttl=600, batch_size=100000)
    bench_common.dump_safely("arangodb", "PR", lambda: bench_common.dump_rows("arangodb", "PR", (
        (r[0], float(r[1])) for r in pregel("PR", "pagerank", "pr", max_gss=PAGERANK_SUPERSTEPS, algo_params={'threshold': 0.0}))))
    bench_common.dump_safely("arangodb", "WCC", lambda: bench_common.dump_rows("arangodb", "WCC", (
        (r[0], r[1]) for r in pregel("WCC", "connectedcomponents", "wcc"))))
    bench_common.dump_safely("arangodb", "CDLP", lambda: bench_common.dump_rows("arangodb", "CDLP", (
        (r[0], r[1]) for r in pregel("CDLP", "labelpropagation", "cdlp", max_gss=10))))
    def sssp():
        rows = list(_long_db(1800).aql.execute(SSSP_QUERY, ttl=1800, batch_size=100000, max_runtime=1700))
        bench_common.dump_rows("arangodb", "SSSP", ((r[0], float(r[1])) for r in rows))
    bench_common.dump_safely("arangodb", "SSSP", sssp)
    def bfs():
        from arango import ArangoClient
        long_db = ArangoClient(hosts='http://localhost:8529', request_timeout=1800).db(
            '_system', username='root', password='benchmark')   # the benchmark client times out after 60 s
        cur = long_db.aql.execute("""
            FOR v, e, p IN 0..100 OUTBOUND 'nodes/6' GRAPH 'bench'
                OPTIONS {bfs: true, uniqueVertices: 'global'}
                RETURN [v.vid, LENGTH(p.edges)]
        """, ttl=1800, batch_size=100000)
        reached = {r[0]: r[1] for r in cur}
        bench_common.dump_bfs("arangodb", reached, [r for r in long_db.aql.execute("FOR v IN nodes RETURN v.vid", ttl=600, batch_size=100000)], 6)
    bench_common.dump_safely("arangodb", "BFS", bfs)


def run_benchmark():
    from arango import ArangoClient
    print("\n" + "=" * 70)
    print("ARANGODB BENCHMARK")
    print("=" * 70)

    results = {}

    try:
        client = ArangoClient(hosts='http://localhost:8529')
        db = client.db('_system', username='root', password='benchmark')
        db.version()
    except Exception as e:
        print(f"  Cannot connect to ArangoDB: {e}")
        print("  Start with: docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb/arangodb:3.11.12")
        return {"error": str(e)}

    # The Graphalytics datasets store every undirected edge once, while Pregel follows the stored direction. The
    # load therefore writes every edge in both directions; the `meta` document marks a database loaded that way.
    needs_load = True
    if not bench_common.RESET:
        try:
            if (db.has_collection('edges') and db.collection('edges').count() > 0
                    and db.has_collection('meta') and db.collection('meta').has('both_directions')):
                needs_load = False
                print(f"\n[ArangoDB] Data already loaded ({db.collection('edges').count()} edges, both directions), "
                      "skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[ArangoDB] Loading data...")
        start = time.perf_counter()

        # Create graph collections
        if db.has_collection('nodes'):
            db.delete_collection('nodes')
        if db.has_collection('edges'):
            db.delete_collection('edges')
        if db.has_collection('meta'):
            db.delete_collection('meta')
        if db.has_graph('bench'):
            db.delete_graph('bench')

        nodes_col = db.create_collection('nodes')
        edges_col = db.create_collection('edges', edge=True)

        # Load vertices in batches
        print("  Loading vertices...")
        batch_size = 50000
        with open(VERTEX_FILE) as f:
            batch = []
            for line in f:
                vid = int(line.strip())
                batch.append({"_key": str(vid), "vid": vid})
                if len(batch) >= batch_size:
                    nodes_col.import_bulk(batch, on_duplicate='replace')
                    batch = []
            if batch:
                nodes_col.import_bulk(batch, on_duplicate='replace')

        # Load edges in batches
        print("  Loading edges...")
        with open(EDGE_FILE) as f:
            batch = []
            loaded = 0
            for line in f:
                parts = line.strip().split()
                weight = float(parts[2])
                batch.append({"_from": f"nodes/{parts[0]}", "_to": f"nodes/{parts[1]}", "weight": weight})
                batch.append({"_from": f"nodes/{parts[1]}", "_to": f"nodes/{parts[0]}", "weight": weight})
                if len(batch) >= batch_size:
                    edges_col.import_bulk(batch, on_duplicate='replace')
                    loaded += len(batch)
                    batch = []
                    if loaded % 2000000 < batch_size:
                        print(f"    {loaded} edges loaded ({time.perf_counter() - start:.0f}s)", flush=True)
            if batch:
                edges_col.import_bulk(batch, on_duplicate='replace')
        db.create_collection('meta').insert({"_key": "both_directions"})

        # Create named graph for Pregel
        db.create_graph('bench', edge_definitions=[{
            'edge_collection': 'edges',
            'from_vertex_collections': ['nodes'],
            'to_vertex_collections': ['nodes']
        }])

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
        print(f"  Vertices: {nodes_col.count()}")
        print(f"  Edges: {edges_col.count()}")

    # Ensure graph exists for algorithms
    if not db.has_graph('bench'):
        db.create_graph('bench', edge_definitions=[{
            'edge_collection': 'edges',
            'from_vertex_collections': ['nodes'],
            'to_vertex_collections': ['nodes']
        }])

    # Helper to run Pregel and wait for completion
    def run_pregel(algo, max_gss=None, algo_params=None):
        kwargs = {}
        if max_gss is not None:
            kwargs['max_gss'] = max_gss
        if algo_params is not None:
            kwargs['algorithm_params'] = algo_params
        job_id = db.pregel.create_job(
            graph='bench',
            algorithm=algo,
            store=False,
            **kwargs
        )
        import time as t
        while True:
            job = db.pregel.job(job_id)
            if job['state'] in ('done', 'canceled', 'fatal error'):
                _release_pregel_job(db, job_id)
                return job
            t.sleep(0.5)

    if bench_common.dump_only():
        _dump_all(db, run_pregel)
        bench_common.cleanup_docker("arangodb")
        return results

    # --- PageRank ---
    print("\n[ArangoDB] Running PageRank...")
    pr_timer = bench_common.ServerTimer()
    def _run_pagerank():
        job = run_pregel('pagerank', max_gss=PAGERANK_SUPERSTEPS, algo_params={'threshold': 0.0})
        if job['state'] == 'done':
            pr_timer.add(job.get('computation_time'))   # Pregel computation time (supersteps), without the startup / graph loading phase
            return job
        raise RuntimeError(f"PageRank failed: {job['state']}")
    elapsed, _ = bench_common.run_timed_warm("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")
        bench_common.report_server_time("arangodb", "PageRank", pr_timer.median())

    # --- WCC ---
    print("\n[ArangoDB] Running WCC...")
    wcc_timer = bench_common.ServerTimer()
    def _run_wcc():
        job = run_pregel('connectedcomponents')
        if job['state'] == 'done':
            wcc_timer.add(job.get('computation_time'))
            return job
        raise RuntimeError(f"WCC failed: {job['state']}")
    elapsed, _ = bench_common.run_timed_warm("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")
        bench_common.report_server_time("arangodb", "WCC", wcc_timer.median())

    # --- LCC (via AQL triangle counting) ---
    print("\n[ArangoDB] Running LCC...")
    lcc_timer = bench_common.ServerTimer()
    def _run_lcc():
        client_lcc = ArangoClient(hosts='http://localhost:8529', request_timeout=300)
        db_lcc = client_lcc.db('_system', username='root', password='benchmark')
        cursor = db_lcc.aql.execute("""
            FOR v IN nodes
                LET neighbors = (
                    FOR n IN 1..1 ANY v edges
                        OPTIONS {uniqueVertices: 'global'}
                        RETURN n._id
                )
                LET deg = LENGTH(neighbors)
                FILTER deg >= 2
                LET triangles = (
                    FOR i IN 0..deg-2
                        FOR j IN i+1..deg-1
                            LET a = neighbors[i]
                            LET b = neighbors[j]
                            FILTER LENGTH(
                                FOR e IN edges
                                    FILTER (e._from == a AND e._to == b) OR (e._from == b AND e._to == a)
                                    LIMIT 1
                                    RETURN 1
                            ) > 0
                            RETURN 1
                )
                LET tri = LENGTH(triangles)
                LET lcc = tri > 0 ? (2.0 * tri) / (deg * (deg - 1)) : 0
                COLLECT AGGREGATE n = COUNT(1), total = SUM(lcc)
                RETURN {n: n, agg: total}
        """, ttl=300, max_runtime=300)
        row = list(cursor)[0]
        lcc_timer.add(cursor.statistics().get("execution_time"))   # AQL execution time reported by the server
        return row["n"], row["agg"]
    elapsed, summary = bench_common.run_timed_warm("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s  (summary {summary})")
        # the query skips vertices of degree < 2 (their coefficient is 0): n is the vertex count, the sum is over all values
        bench_common.check_summary("arangodb", "LCC", db.collection('nodes').count(), summary[1])
        bench_common.report_server_time("arangodb", "LCC", lcc_timer.median())

    # --- SSSP (Pregel) ---
    print("\n[ArangoDB] Running SSSP from vertex 6...")
    sssp_timer = bench_common.ServerTimer()
    def _run_sssp():
        if bench_common.compute_only():
            cur = _long_db(300).aql.execute(SSSP_COUNT_QUERY, ttl=300, max_runtime=300)
            row = list(cur)[0]
            sssp_timer.add(cur.statistics().get("execution_time"))
            return row["n"], row["agg"]
        rows = list(_long_db(300).aql.execute(SSSP_QUERY, ttl=300, batch_size=100000, max_runtime=300))
        return len(rows), None
    elapsed, summary = bench_common.run_timed_warm("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s  (summary {summary})")
        if bench_common.compute_only():
            bench_common.check_summary("arangodb", "SSSP", *summary)
        bench_common.report_server_time("arangodb", "SSSP", sssp_timer.median())

    # --- CDLP (Label Propagation via Pregel) ---
    print("\n[ArangoDB] Running CDLP...")
    cdlp_timer = bench_common.ServerTimer()
    def _run_cdlp():
        job = run_pregel('labelpropagation', max_gss=10)
        if job['state'] == 'done':
            cdlp_timer.add(job.get('computation_time'))
            return job
        raise RuntimeError(f"CDLP failed: {job['state']}")
    elapsed, _ = bench_common.run_timed_warm("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")
        bench_common.report_server_time("arangodb", "CDLP", cdlp_timer.median())

    # --- BFS (via AQL traversal) ---
    print("\n[ArangoDB] Running BFS from vertex 6...")
    bfs_timer = bench_common.ServerTimer()
    def _run_bfs():
        long_db = ArangoClient(hosts='http://localhost:8529', request_timeout=300).db(
            '_system', username='root', password='benchmark')   # the default client times out after 60 s
        cursor = long_db.aql.execute("""
            FOR v, e, p IN 0..100 OUTBOUND 'nodes/6' GRAPH 'bench'
                OPTIONS {bfs: true, uniqueVertices: 'global'}
                COLLECT depth = LENGTH(p.edges) WITH COUNT INTO cnt
                RETURN {depth: depth, count: cnt}
        """, ttl=300, max_runtime=300)
        rows = list(cursor)   # grouped by depth on the server: a handful of rows, already compute only
        bfs_timer.add(cursor.statistics().get("execution_time"))
        return sum(r['count'] for r in rows), max(r['depth'] for r in rows)
    elapsed, summary = bench_common.run_timed_warm("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s  (summary {summary})")
        bench_common.check_summary("arangodb", "BFS", *summary)
        bench_common.report_server_time("arangodb", "BFS", bfs_timer.median())

    # PageRank, WCC and CDLP are Pregel jobs with store=False: the whole computation runs, nothing is stored or returned (no
    # summary to check; their outputs are validated through the untimed export).
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_server_time"] = dict(bench_common.SERVER_TIMES)
    results["_output"] = bench_common.OUTPUT_MODE
    _dump_all(db, run_pregel)

    # The loaded graph is kept (persisted data directory, reused by the next run; --reset wipes it).
    bench_common.cleanup_docker("arangodb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arangodb")
