#!/usr/bin/env python3
"""
LDBC Graphalytics Benchmark: Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs ArangoDB vs FalkorDB
Dataset: datagen-7_5-fb (633K vertices, 34M edges, undirected, weighted)
Algorithms: PageRank, WCC, BFS, LCC, SSSP, CDLP

Usage:
  python3 benchmark.py                     # Run all, skip loading if data exists
  python3 benchmark.py --reset             # Delete all data and reload from scratch
  python3 benchmark.py kuzu duckpgq        # Run only specific systems
  python3 benchmark.py falkordb             # Run only FalkorDB
  python3 benchmark.py --reset memgraph    # Reset and run only Memgraph
"""

import time
import os
import sys
import shutil
import argparse

GRAPHS_DIR = "/Users/luca/graphs"
VERTEX_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb.v")
EDGE_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb.e")
SOURCE_VERTEX = 6
PR_DAMPING = 0.85
PR_ITERATIONS = 10
EXPECTED_VERTICES = 633432
EXPECTED_EDGES = 34185747

# Global flag set by --reset
RESET = False


def fmt(val):
    if isinstance(val, (int, float)):
        return f"{val:>14.2f}s"
    return f"{str(val):>15}"


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

    if RESET:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        elif os.path.exists(db_path):
            os.remove(db_path)

    # Check if data already loaded
    needs_load = True
    if os.path.exists(db_path) and not RESET:
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
    if not RESET:
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
    if not RESET:
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
    if not RESET:
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

    if RESET and os.path.isdir(FALKORDB_DATA_DIR):
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
    if not RESET:
        try:
            r = g.ro_query("MATCH ()-[e]->() RETURN count(e) AS c")
            if r.result_set and r.result_set[0][0] > 0:
                needs_load = False
                print(f"\n[FalkorDB] Data already loaded ({r.result_set[0][0]} edges), skipping import")
        except Exception:
            pass

    if RESET:
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
    "kuzu": ("Kuzu", run_kuzu_benchmark),
    "duckpgq": ("DuckPGQ", run_duckpgq_benchmark),
    "memgraph": ("Memgraph", run_memgraph_benchmark),
    "neo4j": ("Neo4j", run_neo4j_benchmark),
    "arangodb": ("ArangoDB", run_arangodb_benchmark),
    "falkordb": ("FalkorDB", run_falkordb_benchmark),
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LDBC Graphalytics multi-vendor benchmark")
    parser.add_argument("--reset", action="store_true",
                        help="Delete all data and reload from scratch")
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). Choices: {', '.join(AVAILABLE_SYSTEMS.keys())}")
    args = parser.parse_args()

    RESET = args.reset

    systems_to_run = args.systems if args.systems else list(AVAILABLE_SYSTEMS.keys())

    all_results = {}
    for key in systems_to_run:
        key = key.lower()
        if key not in AVAILABLE_SYSTEMS:
            print(f"Unknown system: {key}. Available: {', '.join(AVAILABLE_SYSTEMS.keys())}")
            continue
        name, func = AVAILABLE_SYSTEMS[key]
        try:
            r = func()
            if isinstance(r, dict) and "error" not in r:
                all_results[name] = r
        except Exception as e:
            print(f"\n{name} failed: {e}")
            import traceback; traceback.print_exc()

    if all_results:
        print_summary(all_results)
