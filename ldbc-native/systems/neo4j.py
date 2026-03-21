"""Neo4j benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


def run_benchmark():
    from neo4j import GraphDatabase
    print("\n" + "=" * 70)
    print("NEO4J BENCHMARK")
    print("=" * 70)

    results = {}

    try:
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "benchmark123"))
        driver.verify_connectivity()
    except Exception as e:
        print(f"  Cannot connect to Neo4j: {e}")
        print("  Start with: docker run -d --name neo4j -p 7474:7474 -p 7688:7687 -e NEO4J_AUTH=neo4j/benchmark123 -e NEO4J_PLUGINS='[\"graph-data-science\"]' neo4j:2026-community")
        return {"error": str(e)}

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            with driver.session() as session:
                r = session.run("MATCH ()-[e]->() RETURN count(e) AS c").single()
                if r and r["c"] > 0:
                    needs_load = False
                    print(f"\n[Neo4j] Data already loaded ({r['c']} edges), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[Neo4j] Loading data...")
        start = time.perf_counter()

        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")

        # Load vertices
        print("  Loading vertices...")
        batch_size = 50000
        with open(VERTEX_FILE) as f:
            batch = []
            for line in f:
                vid = int(line.strip())
                batch.append({"id": vid})
                if len(batch) >= batch_size:
                    with driver.session() as session:
                        session.run("UNWIND $nodes AS n CREATE (:Node {id: n.id})", nodes=batch)
                    batch = []
            if batch:
                with driver.session() as session:
                    session.run("UNWIND $nodes AS n CREATE (:Node {id: n.id})", nodes=batch)

        # Load edges
        print("  Loading edges...")
        with open(EDGE_FILE) as f:
            batch = []
            for line in f:
                parts = line.strip().split()
                batch.append({"src": int(parts[0]), "dst": int(parts[1]),
                              "weight": float(parts[2])})
                if len(batch) >= batch_size:
                    with driver.session() as session:
                        session.run("""
                            UNWIND $edges AS e
                            MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                            CREATE (a)-[:EDGE {weight: e.weight}]->(b)
                        """, edges=batch)
                    batch = []
            if batch:
                with driver.session() as session:
                    session.run("""
                        UNWIND $edges AS e
                        MATCH (a:Node {id: e.src}), (b:Node {id: e.dst})
                        CREATE (a)-[:EDGE {weight: e.weight}]->(b)
                    """, edges=batch)

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

        with driver.session() as session:
            r = session.run("MATCH (n) RETURN count(n) AS c").single()
            print(f"  Vertices: {r['c']}")
            r = session.run("MATCH ()-[e]->() RETURN count(e) AS c").single()
            print(f"  Edges: {r['c']}")

    # Create GDS graph projection
    print("\n[Neo4j] Creating GDS graph projection...")
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('bench', false)")
        except Exception:
            pass
        session.run("""
            CALL gds.graph.project('bench', 'Node',
                {EDGE: {orientation: 'UNDIRECTED', properties: 'weight'}})
        """)

    # --- PageRank ---
    print("\n[Neo4j] Running PageRank...")
    start = time.perf_counter()
    try:
        with driver.session() as session:
            r = session.run("""
                CALL gds.pageRank.stream('bench', {dampingFactor: 0.85, maxIterations: 10})
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).id AS id, score
                ORDER BY score DESC LIMIT 10
            """)
            rows = list(r)
            for row in rows[:3]:
                print(f"    Top PR: node={row['id']}, rank={row['score']:.6f}")
        elapsed = time.perf_counter() - start
        results["pagerank"] = elapsed
        print(f"  PageRank time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC ---
    print("\n[Neo4j] Running WCC...")
    start = time.perf_counter()
    try:
        with driver.session() as session:
            r = session.run("""
                CALL gds.wcc.stream('bench')
                YIELD nodeId, componentId
                RETURN componentId, count(*) AS size
                ORDER BY size DESC LIMIT 10
            """)
            rows = list(r)
            for row in rows[:3]:
                print(f"    Component: id={row['componentId']}, size={row['size']}")
        elapsed = time.perf_counter() - start
        results["wcc"] = elapsed
        print(f"  WCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- BFS ---
    print("\n[Neo4j] Running BFS from vertex 6...")
    start = time.perf_counter()
    try:
        with driver.session() as session:
            # Get internal node ID for source
            src = session.run("MATCH (n:Node {id: 6}) RETURN id(n) AS nid").single()
            src_id = src['nid']
            r = session.run("""
                CALL gds.bfs.stream('bench', {sourceNode: $src})
                YIELD nodeIds
                RETURN size(nodeIds) AS reached
            """, src=src_id)
            row = r.single()
            print(f"  Reached: {row['reached']} nodes")
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  BFS failed: {e}")
        results["bfs"] = "N/A"

    # --- LCC ---
    print("\n[Neo4j] Running LCC...")
    start = time.perf_counter()
    try:
        with driver.session() as session:
            r = session.run("""
                CALL gds.localClusteringCoefficient.stream('bench')
                YIELD nodeId, localClusteringCoefficient
                RETURN gds.util.asNode(nodeId).id AS id, localClusteringCoefficient AS coeff
                ORDER BY coeff DESC LIMIT 10
            """)
            rows = list(r)
            for row in rows[:3]:
                print(f"    Top LCC: node={row['id']}, coeff={row['coeff']:.6f}")
        elapsed = time.perf_counter() - start
        results["lcc"] = elapsed
        print(f"  LCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  LCC failed: {e}")
        results["lcc"] = "N/A"

    # Cleanup
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('bench', false)")
        except Exception:
            pass
    driver.close()
    return results
