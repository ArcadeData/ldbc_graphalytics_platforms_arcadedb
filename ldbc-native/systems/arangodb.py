"""ArangoDB benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


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
        print("  Start with: docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb:latest")
        return {"error": str(e)}

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            if db.has_collection('edges') and db.collection('edges').count() > 0:
                needs_load = False
                print(f"\n[ArangoDB] Data already loaded ({db.collection('edges').count()} edges), skipping import")
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
            for line in f:
                parts = line.strip().split()
                batch.append({
                    "_from": f"nodes/{parts[0]}",
                    "_to": f"nodes/{parts[1]}",
                    "weight": float(parts[2])
                })
                if len(batch) >= batch_size:
                    edges_col.import_bulk(batch, on_duplicate='replace')
                    batch = []
            if batch:
                edges_col.import_bulk(batch, on_duplicate='replace')

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
                return job
            t.sleep(0.5)

    # --- PageRank ---
    print("\n[ArangoDB] Running PageRank...")
    def _run_pagerank():
        job = run_pregel('pagerank', max_gss=10, algo_params={'threshold': 0.0})
        if job['state'] == 'done':
            return job
        raise RuntimeError(f"PageRank failed: {job['state']}")
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[ArangoDB] Running WCC...")
    def _run_wcc():
        job = run_pregel('connectedcomponents')
        if job['state'] == 'done':
            return job
        raise RuntimeError(f"WCC failed: {job['state']}")
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- LCC (via AQL triangle counting) ---
    print("\n[ArangoDB] Running LCC...")
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
                SORT lcc DESC
                LIMIT 10
                RETURN {id: v.vid, lcc: lcc}
        """, ttl=300, max_runtime=300)
        rows = list(cursor)
        for row in rows[:3]:
            print(f"    Top LCC: node={row['id']}, coeff={row['lcc']:.6f}")
        return rows
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- SSSP (Pregel) ---
    print("\n[ArangoDB] Running SSSP from vertex 6...")
    def _run_sssp():
        job = run_pregel('sssp', algo_params={'source': 'nodes/6'})
        if job['state'] == 'done':
            return job
        raise RuntimeError(f"SSSP failed: {job['state']}")
    elapsed, _ = bench_common.run_timed("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s")

    # --- CDLP (Label Propagation via Pregel) ---
    print("\n[ArangoDB] Running CDLP...")
    def _run_cdlp():
        job = run_pregel('labelpropagation', max_gss=10)
        if job['state'] == 'done':
            return job
        raise RuntimeError(f"CDLP failed: {job['state']}")
    elapsed, _ = bench_common.run_timed("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    # --- BFS (via AQL traversal) ---
    print("\n[ArangoDB] Running BFS from vertex 6...")
    def _run_bfs():
        cursor = db.aql.execute("""
            FOR v, e, p IN 0..100 ANY 'nodes/6' GRAPH 'bench'
                OPTIONS {bfs: true, uniqueVertices: 'global'}
                COLLECT depth = LENGTH(p.edges) WITH COUNT INTO cnt
                RETURN {depth: depth, count: cnt}
        """, ttl=300, max_runtime=300)
        rows = list(cursor)
        total_reached = sum(r['count'] for r in rows)
        print(f"  BFS reached {total_reached} nodes")
        return rows
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # Cleanup
    try:
        db.delete_graph('bench', drop_collections=True)
    except Exception:
        pass

    bench_common.cleanup_docker("arangodb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arangodb")
