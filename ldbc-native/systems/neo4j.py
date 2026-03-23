"""Neo4j benchmark for LDBC Graphalytics."""

import time
import subprocess as _sp

from ._common import VERTEX_FILE, EDGE_FILE, bench_common

NEO4J_CONTAINER = "neo4j-gds"


def _start_neo4j():
    """Start Neo4j Docker container with GDS plugin."""
    _sp.run(["docker", "rm", "-f", NEO4J_CONTAINER], capture_output=True)
    _sp.run([
        "docker", "run", "-d", "--name", NEO4J_CONTAINER,
        "-p", "7688:7687", "-p", "7476:7474",
        "-e", "NEO4J_AUTH=neo4j/benchmark123",
        "-e", 'NEO4J_PLUGINS=["graph-data-science"]',
        "-e", "NEO4J_server_memory_heap_initial__size=8g",
        "-e", "NEO4J_server_memory_heap_max__size=8g",
        "neo4j:2025-community"
    ], check=True)
    print("  Waiting for Neo4j to start...")
    for i in range(60):
        try:
            from neo4j import GraphDatabase
            d = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "benchmark123"))
            d.verify_connectivity()
            d.close()
            print("  Neo4j ready")
            return
        except Exception:
            time.sleep(3)
    raise RuntimeError("Neo4j failed to start")


def run_benchmark():
    from neo4j import GraphDatabase
    print("\n" + "=" * 70)
    print("NEO4J BENCHMARK")
    print("=" * 70)

    results = {}

    # Try connecting, start container if needed
    try:
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "benchmark123"))
        driver.verify_connectivity()
    except Exception:
        print("  Neo4j not running, starting Docker container...")
        _start_neo4j()
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "benchmark123"))
        driver.verify_connectivity()

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
    def _run_pagerank():
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
        return rows
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[Neo4j] Running WCC...")
    def _run_wcc():
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
        return rows
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- BFS ---
    print("\n[Neo4j] Running BFS from vertex 6...")
    def _run_bfs():
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
        return row
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # --- LCC ---
    print("\n[Neo4j] Running LCC...")
    def _run_lcc():
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
        return rows
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # Cleanup
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('bench', false)")
        except Exception:
            pass
    driver.close()
    bench_common.cleanup_docker(NEO4J_CONTAINER)
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker(NEO4J_CONTAINER)
