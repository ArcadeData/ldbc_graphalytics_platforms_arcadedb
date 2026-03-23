"""Memgraph benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


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

    # --- BFS ---
    print("\n[Memgraph] Running BFS...")
    def _run_bfs():
        cursor.execute("""
            MATCH (a:Node {id: 6})-[e:EDGE *BFS]->(b:Node)
            RETURN b.id, size(e) AS dist
        """)
        rows = cursor.fetchall()
        return rows
    elapsed, result = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s (reached {len(result)} nodes)")

    # --- PageRank ---
    print("\n[Memgraph] Running PageRank...")
    def _run_pagerank():
        cursor.execute("""
            CALL pagerank.get()
            YIELD node, rank
            RETURN node.id AS id, rank
            ORDER BY rank DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
        return rows
    elapsed, result = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[Memgraph] Running WCC...")
    def _run_wcc():
        cursor.execute("""
            CALL weakly_connected_components.get()
            YIELD node, component_id
            RETURN component_id, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Component: id={row[0]}, size={row[1]}")
        return rows
    elapsed, result = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- LCC ---
    print("\n[Memgraph] Running LCC...")
    def _run_lcc():
        cursor.execute("""
            CALL nxalg.clustering()
            YIELD node, clustering
            RETURN node.id AS id, clustering AS coeff
            ORDER BY coeff DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Top LCC: node={row[0]}, coeff={row[1]:.6f}")
        return rows
    elapsed, result = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- SSSP ---
    print("\n[Memgraph] Running SSSP...")
    def _run_sssp():
        cursor.execute("""
            MATCH (a:Node {id: 6})-[e:EDGE *wShortest (e, n | e.weight)]->(b:Node)
            RETURN b.id, size(e) AS hops
        """)
        rows = cursor.fetchall()
        return rows
    elapsed, result = bench_common.run_timed("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s (reached {len(result)} nodes)")

    # --- CDLP ---
    print("\n[Memgraph] Running CDLP...")
    def _run_cdlp():
        cursor.execute("""
            CALL community_detection.get()
            YIELD node, community_id
            RETURN community_id, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Community: id={row[0]}, size={row[1]}")
        return rows
    elapsed, result = bench_common.run_timed("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    conn.close()
    bench_common.cleanup_docker("memgraph")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("memgraph")
