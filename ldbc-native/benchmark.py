#!/usr/bin/env python3
"""
LDBC Graphalytics Benchmark: ArcadeDB (Docker) vs Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs ArangoDB vs FalkorDB vs HugeGraph
Dataset: datagen-7_5-fb (633K vertices, 34M edges, undirected, weighted)
Algorithms: PageRank, WCC, BFS, LCC, SSSP, CDLP

Usage:
  python3 benchmark.py                     # Run all, skip loading if data exists
  python3 benchmark.py --reset             # Delete all data and reload from scratch
  python3 benchmark.py arcadedb            # Run only ArcadeDB (Docker)
  python3 benchmark.py kuzu duckpgq        # Run only specific systems
  python3 benchmark.py falkordb             # Run only FalkorDB
  python3 benchmark.py hugegraph            # Run only HugeGraph
  python3 benchmark.py --reset memgraph    # Reset and run only Memgraph
"""

import time
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import bench_common
from bench_common import fmt, GRAPHS_DIR

VERTEX_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb", "datagen-7_5-fb.v")
EDGE_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb", "datagen-7_5-fb.e")
SOURCE_VERTEX = 6
PR_DAMPING = 0.85
PR_ITERATIONS = 10
EXPECTED_VERTICES = 633432
EXPECTED_EDGES = 34185747


# =============================================================================
# KUZU BENCHMARK
# =============================================================================
def run_kuzu_benchmark():
    import kuzu
    print("\n" + "=" * 70)
    print("KUZU BENCHMARK")
    print("=" * 70)

    db_path = "/tmp/kuzu_benchmark"
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
    print("\n[Kuzu] Running PageRank...")
    start = time.perf_counter()
    try:
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
        elapsed = time.perf_counter() - start
        results["pagerank"] = elapsed
        print(f"  PageRank time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC (Weakly Connected Components) ---
    print("\n[Kuzu] Running WCC...")
    start = time.perf_counter()
    try:
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
        elapsed = time.perf_counter() - start
        results["wcc"] = elapsed
        print(f"  WCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- LCC (Local Clustering Coefficient) ---
    print("\n[Kuzu] Running LCC...")
    start = time.perf_counter()
    try:
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
        elapsed = time.perf_counter() - start
        results["lcc"] = elapsed
        print(f"  LCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  LCC failed: {e}")
        results["lcc"] = "N/A"

    # --- BFS (shortest path from source) ---
    print("\n[Kuzu] Running BFS/Shortest Path from vertex 6...")
    start = time.perf_counter()
    try:
        r = conn.execute("""
            MATCH (a:Node {id: 6})-[e:Edge* ALL SHORTEST 1..30]->(b:Node)
            RETURN b.id, length(e) LIMIT 50000
        """)
        count = 0
        while r.has_next():
            r.get_next()
            count += 1
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s (reached {count} nodes)")
    except Exception as e:
        print(f"  BFS failed: {e}")
        results["bfs"] = "N/A"

    # Cleanup
    del conn
    del db
    shutil.rmtree(db_path, ignore_errors=True)
    return results


# =============================================================================
# DUCKPGQ BENCHMARK
# =============================================================================
def run_duckpgq_benchmark():
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
    start = time.perf_counter()
    try:
        r = conn.execute("""
            SELECT * FROM pagerank(ldbc, nodes, edges)
            ORDER BY pagerank DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
        elapsed = time.perf_counter() - start
        results["pagerank"] = elapsed
        print(f"  PageRank time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC ---
    print("\n[DuckPGQ] Running WCC...")
    start = time.perf_counter()
    try:
        r = conn.execute("""
            SELECT componentId, count(*) AS size
            FROM weakly_connected_component(ldbc, nodes, edges)
            GROUP BY componentId
            ORDER BY size DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Component: id={row[0]}, size={row[1]}")
        elapsed = time.perf_counter() - start
        results["wcc"] = elapsed
        print(f"  WCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- LCC ---
    print("\n[DuckPGQ] Running LCC...")
    start = time.perf_counter()
    try:
        r = conn.execute("""
            SELECT * FROM local_clustering_coefficient(ldbc, nodes, edges)
            ORDER BY local_clustering_coefficient DESC LIMIT 10
        """).fetchall()
        for row in r[:3]:
            print(f"    Top LCC: node={row[0]}, coeff={row[1]:.6f}")
        elapsed = time.perf_counter() - start
        results["lcc"] = elapsed
        print(f"  LCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  LCC failed: {e}")
        results["lcc"] = "N/A"

    # --- BFS / Shortest Path ---
    print("\n[DuckPGQ] Running Shortest Path from vertex 6...")
    start = time.perf_counter()
    try:
        r = conn.execute("""
            FROM GRAPH_TABLE(ldbc
                MATCH p = ANY SHORTEST (a:nodes WHERE a.id = 6)-[e:edges]->{1,30}(b:nodes)
                COLUMNS (b.id AS dst, path_length(p) AS dist)
            ) LIMIT 50000
        """).fetchall()
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s (reached {len(r)} nodes)")
    except Exception as e:
        print(f"  BFS/SP failed: {e}")
        results["bfs"] = "N/A"

    conn.close()
    os.remove(db_path)
    return results


# =============================================================================
# MEMGRAPH BENCHMARK (requires Docker)
# =============================================================================
def run_memgraph_benchmark():
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
    print("\n[Memgraph] Running BFS from vertex 6...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            MATCH (a:Node {id: 6})-[e:EDGE *BFS]->(b:Node)
            RETURN b.id, size(e) AS dist
        """)
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s (reached {len(rows)} nodes)")
    except Exception as e:
        print(f"  BFS failed: {e}")
        results["bfs"] = "N/A"

    # --- PageRank ---
    print("\n[Memgraph] Running PageRank...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            CALL pagerank.get()
            YIELD node, rank
            RETURN node.id AS id, rank
            ORDER BY rank DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
        elapsed = time.perf_counter() - start
        results["pagerank"] = elapsed
        print(f"  PageRank time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC ---
    print("\n[Memgraph] Running WCC...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            CALL weakly_connected_components.get()
            YIELD node, component_id
            RETURN component_id, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Component: id={row[0]}, size={row[1]}")
        elapsed = time.perf_counter() - start
        results["wcc"] = elapsed
        print(f"  WCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- LCC ---
    print("\n[Memgraph] Running LCC...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            CALL nxalg.clustering()
            YIELD node, clustering
            RETURN node.id AS id, clustering AS coeff
            ORDER BY coeff DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Top LCC: node={row[0]}, coeff={row[1]:.6f}")
        elapsed = time.perf_counter() - start
        results["lcc"] = elapsed
        print(f"  LCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  LCC failed: {e}")
        results["lcc"] = "N/A"

    # --- SSSP ---
    print("\n[Memgraph] Running SSSP from vertex 6...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            MATCH (a:Node {id: 6})-[e:EDGE *wShortest (e, n | e.weight)]->(b:Node)
            RETURN b.id, size(e) AS hops
        """)
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        results["sssp"] = elapsed
        print(f"  SSSP time: {elapsed:.2f}s (reached {len(rows)} nodes)")
    except Exception as e:
        print(f"  SSSP failed: {e}")
        results["sssp"] = "N/A"

    # --- CDLP ---
    print("\n[Memgraph] Running CDLP...")
    start = time.perf_counter()
    try:
        cursor.execute("""
            CALL community_detection.get()
            YIELD node, community_id
            RETURN community_id, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for row in rows[:3]:
            print(f"    Community: id={row[0]}, size={row[1]}")
        elapsed = time.perf_counter() - start
        results["cdlp"] = elapsed
        print(f"  CDLP time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  CDLP failed: {e}")
        results["cdlp"] = "N/A"

    conn.close()
    return results


# =============================================================================
# NEO4J BENCHMARK (requires Docker)
# =============================================================================
def run_neo4j_benchmark():
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


# =============================================================================
# ARANGODB BENCHMARK (requires Docker)
# =============================================================================
def run_arangodb_benchmark():
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


# =============================================================================
# FALKORDB BENCHMARK (requires Docker)
# =============================================================================
FALKORDB_DATA_DIR = "/tmp/falkordb_benchmark"

def run_falkordb_benchmark():
    import redis
    from falkordb import FalkorDB
    print("\n" + "=" * 70)
    print("FALKORDB BENCHMARK")
    print("=" * 70)

    results = {}

    if bench_common.RESET and os.path.isdir(FALKORDB_DATA_DIR):
        print("  [FalkorDB] --reset: removing persisted data...")
        shutil.rmtree(FALKORDB_DATA_DIR)

    os.makedirs(FALKORDB_DATA_DIR, exist_ok=True)

    try:
        fdb = FalkorDB(host='localhost', port=6379)
        g = fdb.select_graph('bench')
    except Exception as e:
        print(f"  Cannot connect to FalkorDB: {e}")
        print(f"  Start with: docker run -d --name falkordb -p 6379:6379 -v {FALKORDB_DATA_DIR}:/var/lib/falkordb/data falkordb/falkordb")
        return {"error": str(e)}

    # Disable query timeout (default 1000ms is too low for large graphs)
    try:
        rc = redis.Redis(host='localhost', port=6379)
        rc.execute_command("GRAPH.CONFIG", "SET", "TIMEOUT", 0)
        print("  Query timeout disabled")
    except Exception:
        pass

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = g.ro_query("MATCH ()-[e]->() RETURN count(e) AS c")
            if r.result_set and r.result_set[0][0] > 0:
                needs_load = False
                print(f"\n[FalkorDB] Data already loaded ({r.result_set[0][0]} edges), skipping import")
        except Exception:
            pass

    if bench_common.RESET:
        # Delete existing graph if present
        try:
            g.delete()
            g = fdb.select_graph('bench')
        except Exception:
            pass

    if needs_load:
        print("\n[FalkorDB] Loading data...")
        start = time.perf_counter()

        # Load vertices in batches
        print("  Loading vertices...")
        batch_size = 5000
        with open(VERTEX_FILE) as f:
            batch = []
            for line in f:
                vid = int(line.strip())
                batch.append(vid)
                if len(batch) >= batch_size:
                    g.query("UNWIND $ids AS id CREATE (:Node {id: id})", {"ids": batch})
                    batch = []
            if batch:
                g.query("UNWIND $ids AS id CREATE (:Node {id: id})", {"ids": batch})

        # Create index on Node.id for edge linking
        try:
            g.query("CREATE INDEX FOR (n:Node) ON (n.id)")
        except Exception:
            pass

        # Load edges in batches
        print("  Loading edges...")
        with open(EDGE_FILE) as f:
            batch = []
            for line in f:
                parts = line.strip().split()
                batch.append([int(parts[0]), int(parts[1]), float(parts[2])])
                if len(batch) >= batch_size:
                    g.query("""
                        UNWIND $edges AS e
                        MATCH (a:Node {id: e[0]}), (b:Node {id: e[1]})
                        CREATE (a)-[:EDGE {weight: e[2]}]->(b)
                    """, {"edges": batch})
                    batch = []
            if batch:
                g.query("""
                    UNWIND $edges AS e
                    MATCH (a:Node {id: e[0]}), (b:Node {id: e[1]})
                    CREATE (a)-[:EDGE {weight: e[2]}]->(b)
                """, {"edges": batch})

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    r = g.ro_query("MATCH (n:Node) RETURN count(n) AS c")
    print(f"  Vertices: {r.result_set[0][0]}")
    r = g.ro_query("MATCH ()-[e:EDGE]->() RETURN count(e) AS c")
    print(f"  Edges: {r.result_set[0][0]}")

    # --- PageRank ---
    print("\n[FalkorDB] Running PageRank...")
    start = time.perf_counter()
    try:
        r = g.ro_query("""
            CALL algo.pageRank('Node', 'EDGE')
            YIELD node, score
            RETURN node.id AS id, score
            ORDER BY score DESC LIMIT 10
        """)
        for row in r.result_set[:3]:
            print(f"    Top PR: node={row[0]}, rank={row[1]:.6f}")
        elapsed = time.perf_counter() - start
        results["pagerank"] = elapsed
        print(f"  PageRank time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  PageRank failed: {e}")
        results["pagerank"] = "N/A"

    # --- WCC (Weakly Connected Components) ---
    print("\n[FalkorDB] Running WCC...")
    start = time.perf_counter()
    try:
        r = g.ro_query("""
            CALL algo.WCC(null)
            YIELD node, componentId
            RETURN componentId, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        for row in r.result_set[:3]:
            print(f"    Component: id={row[0]}, size={row[1]}")
        elapsed = time.perf_counter() - start
        results["wcc"] = elapsed
        print(f"  WCC time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  WCC failed: {e}")
        results["wcc"] = "N/A"

    # --- BFS ---
    print("\n[FalkorDB] Running BFS from vertex 6...")
    start = time.perf_counter()
    try:
        r = g.ro_query("""
            MATCH (src:Node {id: 6})
            CALL algo.BFS(src, 999, 'EDGE')
            YIELD nodes
            RETURN size(nodes) AS reached
        """)
        reached = r.result_set[0][0]
        elapsed = time.perf_counter() - start
        results["bfs"] = elapsed
        print(f"  BFS time: {elapsed:.2f}s (reached {reached} nodes)")
    except Exception as e:
        print(f"  BFS failed: {e}")
        results["bfs"] = "N/A"

    # --- SSSP ---
    # FalkorDB's algo.SSpaths does not support full single-source Dijkstra;
    # it only returns paths to direct neighbors. Not usable for SSSP benchmark.
    print("\n[FalkorDB] SSSP: not supported (algo.SSpaths is pair-oriented, not full SSSP)")
    results["sssp"] = "N/A"

    # --- CDLP (Community Detection via Label Propagation) ---
    print("\n[FalkorDB] Running CDLP...")
    start = time.perf_counter()
    try:
        r = g.ro_query("""
            CALL algo.labelPropagation({
                nodeLabels: ['Node'],
                relationshipTypes: ['EDGE'],
                maxIterations: 10
            })
            YIELD node, communityId
            RETURN communityId, count(*) AS size
            ORDER BY size DESC LIMIT 10
        """)
        for row in r.result_set[:3]:
            print(f"    Community: id={row[0]}, size={row[1]}")
        elapsed = time.perf_counter() - start
        results["cdlp"] = elapsed
        print(f"  CDLP time: {elapsed:.2f}s")
    except Exception as e:
        print(f"  CDLP failed: {e}")
        results["cdlp"] = "N/A"

    # --- LCC (Local Clustering Coefficient) ---
    # FalkorDB has no built-in LCC algorithm. Cypher-based triangle counting
    # is infeasible on 34M edges (would require enumerating all triangles).
    print("\n[FalkorDB] LCC: not supported (no built-in algorithm, Cypher too slow)")
    results["lcc"] = "N/A"

    return results


# =============================================================================
# HUGEGRAPH BENCHMARK (requires Docker: hugegraph/vermeer for OLAP)
# =============================================================================
def run_hugegraph_benchmark():
    """
    HugeGraph benchmark using Vermeer (the HugeGraph-Computer Go engine).
    Vermeer loads data directly from CSV files and runs all OLAP algorithms.

    Setup (3 containers on a shared Docker network):
      docker network create hugegraph-net
      docker run -d --name vermeer-master --network hugegraph-net \\
        -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
      docker run -d --name vermeer-worker --network hugegraph-net \\
        -p 6788:6788 -p 6789:6789 \\
        -v "$(cd ../datasets && pwd)":/data/graphs:ro \\
        hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
    Then assign worker to the common pool:
      curl -X POST http://localhost:6688/api/v1/admin/workers/group/\\$/$(
        curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
    """
    import requests
    import json as jsonlib
    print("\n" + "=" * 70)
    print("HUGEGRAPH (VERMEER) BENCHMARK")
    print("=" * 70)

    results = {}
    vermeer = "http://localhost:6688/api/v1"

    # Check Vermeer connectivity
    try:
        r = requests.get(f"{vermeer}/workers", timeout=5)
        workers = r.json().get("workers", [])
        if not workers:
            raise Exception("No workers registered")
        worker_ip = workers[0]["ip_addr"]
        worker_name = workers[0]["name"]
        print(f"  Vermeer master: OK ({len(workers)} worker(s))")
    except Exception as e:
        print(f"  Cannot connect to Vermeer: {e}")
        print("  See docstring for setup instructions")
        return {"error": str(e)}

    # Ensure worker is in the common "$" pool (required for task scheduling)
    if workers[0].get("group") != "$":
        requests.post(f"{vermeer}/admin/workers/group/$/{worker_name}")

    # Check if graph already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = requests.get(f"{vermeer}/graphs")
            for g in r.json().get("graphs", []):
                if g["name"] == "bench" and g["state"] == "loaded":
                    needs_load = False
                    print("\n[HugeGraph] Graph already loaded in Vermeer, skipping import")
                    break
        except Exception:
            pass

    if needs_load:
        # Delete existing graph if present
        try:
            requests.delete(f"{vermeer}/graphs/bench")
        except Exception:
            pass

        print("\n[HugeGraph] Loading data into Vermeer...")
        start = time.perf_counter()

        r = requests.post(f"{vermeer}/tasks/create/sync", json={
            "task_type": "load",
            "graph": "bench",
            "params": {
                "load.type": "local",
                "load.parallel": "50",
                "load.delimiter": " ",
                "load.vertex_files": jsonlib.dumps(
                    {worker_ip: VERTEX_FILE.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.edge_files": jsonlib.dumps(
                    {worker_ip: EDGE_FILE.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.use_property": "1",
                "load.vertex_backend": "mem"
            }
        }, timeout=600)

        if r.status_code != 200 or r.json().get("task", {}).get("state") != "loaded":
            print(f"  Load failed: {r.text[:300]}")
            return {"error": "Load failed"}

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Helper to run a Vermeer compute task
    def run_algo(name, display_name, params):
        print(f"\n[HugeGraph] Running {display_name}...")
        start = time.perf_counter()
        try:
            algo_params = {"compute.algorithm": name}
            algo_params.update(params)
            r = requests.post(f"{vermeer}/tasks/create/sync", json={
                "task_type": "compute",
                "graph": "bench",
                "params": algo_params
            }, timeout=600)
            elapsed = time.perf_counter() - start
            if r.status_code == 200 and r.json().get("task", {}).get("state") == "complete":
                results[display_name] = elapsed
                print(f"  {display_name} time: {elapsed:.2f}s")
            else:
                print(f"  {display_name} failed: {r.text[:200]}")
                results[display_name] = "N/A"
        except Exception as e:
            print(f"  {display_name} failed: {e}")
            results[display_name] = "N/A"

    # --- PageRank ---
    run_algo("pagerank", "pagerank",
             {"pagerank.damping": "0.85", "pagerank.diff_threshold": "0.00001"})

    # --- WCC ---
    run_algo("wcc", "wcc", {})

    # --- BFS (SSSP unweighted = hop-count BFS) ---
    run_algo("sssp", "bfs", {"sssp.source": str(SOURCE_VERTEX)})

    # --- LCC (Clustering Coefficient) ---
    run_algo("clustering_coefficient", "lcc", {})

    # --- SSSP (weighted — Vermeer's sssp is unweighted/hop-count only) ---
    # Vermeer's built-in SSSP computes unweighted shortest paths (hop count).
    # There is no weighted Dijkstra variant available.
    print("\n[HugeGraph] SSSP: not supported (Vermeer sssp is unweighted only)")
    results["sssp"] = "N/A"

    # --- CDLP (Label Propagation) ---
    run_algo("lpa", "cdlp", {})

    return results


# =============================================================================
# ARCADEDB DOCKER BENCHMARK (requires Docker)
# =============================================================================
def run_arcadedb_docker_benchmark():
    """
    ArcadeDB benchmark via Docker (HTTP API) — same network overhead as
    Neo4j/Memgraph/FalkorDB/HugeGraph benchmarks for fair comparison.

    Setup:
      docker run -d --name arcadedb -p 2480:2480 -p 2424:2424 \
        -e JAVA_OPTS="-Darcadedb.server.rootPassword=benchmark -Xms16g -Xmx16g --add-modules jdk.incubator.vector" \
        -v "$(cd ../datasets && pwd)":/data/graphs:ro \
        -v /tmp/arcadedb-docker-data:/home/arcadedb/databases \
        arcadedata/arcadedb:latest
    """
    import requests
    import subprocess as _sp
    print("\n" + "=" * 70)
    print("ARCADEDB (DOCKER) BENCHMARK")
    print("=" * 70)

    results = {}
    base = "http://localhost:2480/api/v1"
    auth = ("root", "benchmark")
    db = "bench"

    def cmd(command, language="sql", timeout=600, params=None):
        body = {
            "language": language,
            "command": command,
            "limit": -1,
            "serializer": "record"
        }
        if params is not None:
            body["params"] = params
        r = requests.post(f"{base}/command/{db}", json=body,
                          auth=auth, timeout=timeout)
        return r

    # --- Phase 1: Load data via embedded Java (fast GraphBatchImporter) ---
    # The database is created by the Java loader and then mounted into Docker.
    # This separates "load" (embedded, ~160s) from "compute" (Docker, HTTP API).
    db_path = "/tmp/arcadedb-docker-data/bench"
    needs_load = not os.path.isdir(db_path) or bench_common.RESET

    if needs_load:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        os.makedirs("/tmp/arcadedb-docker-data", exist_ok=True)

        print("\n[ArcadeDB] Loading data via embedded Java (GraphBatchImporter)...")
        start = time.perf_counter()

        ldbc_jar = os.path.join(os.path.dirname(os.path.abspath(__file__)),
            "..", "target",
            "graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar")
        bench_dir = os.path.dirname(os.path.abspath(__file__))

        # Compile and run the Java loader (ArcadeDBEmbeddedBenchmark writes to DB_PATH)
        # We patch DB_PATH via a tiny wrapper that just loads and exits
        loader_src = os.path.join(bench_dir, "ArcadeDBEmbeddedLoader.java")
        with open(loader_src, "w") as f:
            f.write("""
import com.arcadedb.database.*;
import com.arcadedb.graph.*;
import com.arcadedb.schema.*;
import java.io.*;
import java.util.*;

public class ArcadeDBEmbeddedLoader {
    public static void main(String[] args) throws Exception {
        String dbPath = args[0];
        String vertexFile = args[1];
        String edgeFile = args[2];

        Database db = new DatabaseFactory(dbPath).create();
        db.begin();
        db.getSchema().createVertexType("Vertex", 8);
        db.getSchema().createEdgeType("EDGE", 8);
        db.getSchema().getType("Vertex").createProperty("VID", Type.LONG);
        db.getSchema().getType("Vertex").createTypeIndex(Schema.INDEX_TYPE.HASH, true, "VID");
        db.getSchema().getType("EDGE").createProperty("WEIGHT", Type.DOUBLE);
        db.commit();

        Map<Long, RID> vidToRid = new HashMap<>(700_000);
        db.begin();
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(vertexFile), 1 << 20)) {
            String line;
            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.trim());
                MutableVertex v = db.newVertex("Vertex");
                v.set("VID", vid);
                v.save();
                vidToRid.put(vid, v.getIdentity());
                if (++count % 10_000 == 0) { db.commit(); db.begin(); }
            }
        }
        db.commit();
        System.out.println("  Vertices: " + count);

        GraphBatchImporter importer = GraphBatchImporter.builder(db)
            .withBatchSize(100_000).withLightEdges(false).withWAL(false).build();
        int edgeCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(edgeFile), 1 << 20)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                RID srcRid = vidToRid.get(Long.parseLong(parts[0]));
                RID dstRid = vidToRid.get(Long.parseLong(parts[1]));
                if (srcRid != null && dstRid != null)
                    importer.newEdge(srcRid, "EDGE", dstRid, "WEIGHT", Double.parseDouble(parts[2]));
                edgeCount++;
            }
        }
        importer.close();
        System.out.println("  Edges: " + edgeCount);
        db.close();
        System.out.println("  Database ready at: " + dbPath);
    }
}
""")

        # Compile
        _sp.run([
            "javac", "--add-modules", "jdk.incubator.vector",
            "-cp", ldbc_jar, loader_src
        ], cwd=bench_dir, check=True)

        # Run
        proc = _sp.run([
            "java", "--add-modules", "jdk.incubator.vector",
            "-Xms8g", "-Xmx8g",
            "-cp", f".:{ldbc_jar}",
            "ArcadeDBEmbeddedLoader", db_path, VERTEX_FILE, EDGE_FILE
        ], cwd=bench_dir, capture_output=True, text=True)
        print(proc.stdout)
        if proc.returncode != 0:
            print(f"  Loader failed: {proc.stderr[-500:]}")
            return {"error": "Java loader failed"}

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
    else:
        print("\n[ArcadeDB] Database already exists at " + db_path + ", skipping load")

    # --- Phase 2: Start Docker server on the pre-loaded database ---
    print("\n[ArcadeDB] Starting Docker server...")
    _sp.run(["docker", "rm", "-f", "arcadedb"], capture_output=True)
    _sp.run([
        "docker", "run", "-d", "--name", "arcadedb",
        "-p", "2480:2480", "-p", "2424:2424",
        "-e", "ARCADEDB_OPTS_MEMORY=-Xms16g -Xmx16g",
        "-e", "JAVA_OPTS=-Darcadedb.server.rootPassword=benchmark",
        "-v", "/tmp/arcadedb-docker-data:/home/arcadedb/databases",
        "-v", "/tmp/arcadedb-docker-log:/home/arcadedb/log",
        "arcadedata/arcadedb:latest"
    ], check=True)

    # Wait for server + GAV auto-restore (CSR build takes ~60-90s)
    print("  Waiting for server and GAV (CSR) build...")
    for i in range(120):
        try:
            r = requests.get(f"{base}/ready", timeout=2)
            if r.status_code == 204:
                print("  ArcadeDB Docker server: OK")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print("  ArcadeDB Docker server failed to start")
        return {"error": "Docker server timeout"}

    # GAV is auto-restored on database open (persisted definition from the Java loader).
    # Wait for the async CSR build to finish before running algorithms.
    print("\n[ArcadeDB] Waiting for Graph Analytical View (CSR) to be ready...")
    for i in range(120):
        try:
            r = cmd("SELECT FROM schema:graphAnalyticalViews WHERE name = 'benchmark'")
            if r.status_code == 200:
                result = r.json().get("result", {})
                records = result.get("records", []) if isinstance(result, dict) else result
                if records and len(records) > 0:
                    print("  GAV ready")
                    break
        except Exception:
            pass
        time.sleep(2)
    else:
        # GAV not found — create it (first run or --reset)
        print("  No GAV found, creating...")
        cmd("CREATE GRAPH ANALYTICAL VIEW benchmark VERTEX TYPES (Vertex) EDGE TYPES (EDGE) EDGE PROPERTIES (WEIGHT)")
        start = time.perf_counter()
        r = cmd("REBUILD GRAPH ANALYTICAL VIEW benchmark", timeout=600)
        gav_time = time.perf_counter() - start
        if r.status_code != 200:
            print(f"  GAV build failed: {r.text[:300]}")
            return {"error": "GAV build failed"}
        print(f"  GAV build: {gav_time:.2f}s")

    # Give the async CSR build time to complete
    time.sleep(5)

    # Helper to run an algorithm via OpenCypher
    def run_algo(name, cypher_cmd, timeout=600):
        print(f"\n[ArcadeDB] Running {name}...")
        start = time.perf_counter()
        try:
            r = cmd(cypher_cmd, language="opencypher", timeout=timeout)
            elapsed = time.perf_counter() - start
            if r.status_code == 200:
                results[name] = elapsed
                print(f"  {name} time: {elapsed:.2f}s")
            else:
                print(f"  {name} failed: {r.text[:200]}")
                results[name] = "N/A"
        except Exception as e:
            print(f"  {name} failed: {e}")
            results[name] = "N/A"

    # All queries return count(*) to avoid streaming 633K rows over HTTP.
    # The algorithm runs server-side; we only measure compute time + minimal transfer.

    # --- PageRank ---
    run_algo("pagerank",
             "CALL algo.pagerank() YIELD score RETURN count(*) AS cnt")

    # --- WCC ---
    run_algo("wcc",
             "CALL algo.wcc() YIELD componentId RETURN count(*) AS cnt")

    # --- BFS ---
    run_algo("bfs",
             "MATCH (s:Vertex {VID: 6}) CALL algo.bfs(s) YIELD node RETURN count(*) AS cnt")

    # --- LCC ---
    run_algo("lcc",
             "CALL algo.localClusteringCoefficient() YIELD localClusteringCoefficient RETURN count(*) AS cnt",
             timeout=1200)

    # --- SSSP (Dijkstra single-source) ---
    run_algo("sssp",
             "MATCH (s:Vertex {VID: 6}) CALL algo.dijkstra.singleSource(s, 'EDGE', 'WEIGHT') YIELD distance RETURN count(*) AS cnt")

    # --- CDLP (Label Propagation) ---
    run_algo("cdlp",
             "CALL algo.labelPropagation({maxIterations: 10}) YIELD communityId RETURN count(*) AS cnt")

    return results


# =============================================================================
# SUMMARY
# =============================================================================
def print_summary(all_results):
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY  -  datagen-7_5-fb (633K vertices, 34M edges)")
    print("=" * 70)

    algos = ["load", "pagerank", "wcc", "lcc", "bfs", "sssp", "cdlp"]
    systems = list(all_results.keys())

    header = f"{'Algorithm':<15}"
    for sys_name in systems:
        header += f"{sys_name:>15}"
    print(header)
    print("-" * len(header))

    for algo in algos:
        row = f"{algo.upper():<15}"
        for sys_name in systems:
            val = all_results[sys_name].get(algo, "N/A")
            row += fmt(val)
        print(row)
    print()


# =============================================================================
# MAIN
# =============================================================================
AVAILABLE_SYSTEMS = {
    "arcadedb": ("ArcadeDB-Docker", run_arcadedb_docker_benchmark),
    "kuzu": ("Kuzu", run_kuzu_benchmark),
    "duckpgq": ("DuckPGQ", run_duckpgq_benchmark),
    "memgraph": ("Memgraph", run_memgraph_benchmark),
    "neo4j": ("Neo4j", run_neo4j_benchmark),
    "arangodb": ("ArangoDB", run_arangodb_benchmark),
    "falkordb": ("FalkorDB", run_falkordb_benchmark),
    "hugegraph": ("HugeGraph", run_hugegraph_benchmark),
}

GRAPHALYTICS_METRICS = ["load", "pagerank", "wcc", "lcc", "bfs", "sssp", "cdlp"]

if __name__ == "__main__":
    bench_common.run_benchmarks(
        description="LDBC Graphalytics multi-vendor benchmark",
        available_systems=AVAILABLE_SYSTEMS,
        summary_title="datagen-7_5-fb (633K vertices, 34M edges)",
        metrics=GRAPHALYTICS_METRICS,
    )
