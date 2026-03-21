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
    start = time.perf_counter()
    try:
        job = run_pregel('pagerank', max_gss=10, algo_params={'threshold': 0.0})
        elapsed = time.perf_counter() - start
        if job['state'] == 'done':
            results["pagerank"] = elapsed
            print(f"  PageRank time: {elapsed:.2f}s")
        else:
            print(f"  PageRank failed: {job['state']}")
            results["pagerank"] = "N/A"
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC ---
    print("\n[ArangoDB] Running WCC...")
    start = time.perf_counter()
    try:
        job = run_pregel('connectedcomponents')
        elapsed = time.perf_counter() - start
        if job['state'] == 'done':
            results["wcc"] = elapsed
            print(f"  WCC time: {elapsed:.2f}s")
        else:
            print(f"  WCC failed: {job['state']}")
            results["wcc"] = "N/A"
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- LCC (via AQL triangle counting) ---
    print("\n[ArangoDB] Running LCC...")
    start = time.perf_counter()
    try:
        client_lcc = ArangoClient(hosts='http://localhost:8529', request_timeout=3600)
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
        """, ttl=3600, max_runtime=3600)
        rows = list(cursor)
        for row in rows[:3]:
            print(f"    Top LCC: node={row['id']}, coeff={row['lcc']:.6f}")
        elapsed = time.perf_counter() - start
        results["lcc"] = elapsed
        print(f"  LCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  LCC failed: {e}")
        results["lcc"] = "N/A"

    # --- SSSP (Pregel) ---
    print("\n[ArangoDB] Running SSSP from vertex 6...")
    start = time.perf_counter()
    try:
        job = run_pregel('sssp', algo_params={'source': 'nodes/6'})
        elapsed = time.perf_counter() - start
        if job['state'] == 'done':
            results["sssp"] = elapsed
            print(f"  SSSP time: {elapsed:.2f}s")
        else:
            print(f"  SSSP failed: {job['state']}")
            results["sssp"] = "N/A"
    except Exception as e:
        print(f"  SSSP failed: {e}")
        results["sssp"] = "N/A"

    # --- CDLP (Label Propagation via Pregel) ---
    print("\n[ArangoDB] Running CDLP...")
    start = time.perf_counter()
    try:
        job = run_pregel('labelpropagation', max_gss=10)
        elapsed = time.perf_counter() - start
        if job['state'] == 'done':
            results["cdlp"] = elapsed
            print(f"  CDLP time: {elapsed:.2f}s")
        else:
            print(f"  CDLP failed: {job['state']}")
            results["cdlp"] = "N/A"
    except Exception as e:
        print(f"  CDLP failed: {e}")
        results["cdlp"] = "N/A"

    # --- BFS (via AQL traversal) ---
    print("\n[ArangoDB] Running BFS from vertex 6...")
    start = time.perf_counter()
    try:
        cursor = db.aql.execute("""
            FOR v, e, p IN 0..100 ANY 'nodes/6' GRAPH 'bench'
                OPTIONS {bfs: true, uniqueVertices: 'global'}
                COLLECT depth = LENGTH(p.edges) WITH COUNT INTO cnt
                RETURN {depth: depth, count: cnt}
        """, ttl=3600, max_runtime=3600)
        rows = list(cursor)
        total_reached = sum(r['count'] for r in rows)
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s (reached {total_reached} nodes)")
    except Exception as e:
        print(f"  BFS failed: {e}")
        results["bfs"] = "N/A"

    # Cleanup
    try:
        db.delete_graph('bench', drop_collections=True)
    except Exception:
        pass

    return results
