"""Neo4j benchmark for LDBC Graphalytics."""

import time
import subprocess as _sp

from ._common import VERTEX_FILE, EDGE_FILE, bench_common

NEO4J_CONTAINER = "neo4j-gds"


def _start_neo4j():
    """Start Neo4j Docker container with GDS plugin (unless the orchestrator already did)."""
    import bench_containers
    if not bench_containers.container_running(NEO4J_CONTAINER):
        _start_neo4j_container()
    _wait_for_neo4j()


def _start_neo4j_container():
    _sp.run(["docker", "rm", "-f", NEO4J_CONTAINER], capture_output=True)
    _sp.run([
        "docker", "run", "-d", "--name", NEO4J_CONTAINER,
        "-p", "7688:7687", "-p", "7476:7474",
        "-e", "NEO4J_AUTH=neo4j/benchmark123",
        "-e", 'NEO4J_PLUGINS=["graph-data-science"]',
        "-e", "NEO4J_server_memory_heap_initial__size=12g",
        "-e", "NEO4J_server_memory_heap_max__size=12g",
        "neo4j:2026.09.0-community"
    ], check=True)


def _wait_for_neo4j():
    print("  Waiting for Neo4j to start...")
    for i in range(400):  # a large persisted store can take many minutes to recover
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


# Graphalytics PageRank: every vertex starts at 1/N and the update is r' = (1-d)/N + d*P*r, 10 iterations. GDS starts
# every vertex at 1-d, does not normalise, and counts the initialisation as the first iteration (maxIterations=10
# returns the state after 9 updates, verified to 1e-14 against a simulation), so its scores G differ from the
# reference by 0.85 * A^10 * 1 (A = d*P). The update is linear, so G(t) - G(t-1) = 0.15 * A^t * 1 and the reference
# (scaled by N) is  G10 + (0.85/0.15) * (G10 - G9)  =  (20*S11 - 17*S10) / 3  with S10, S11 the GDS scores for
# maxIterations 10 and 11. Checked on this graph: max relative error 3e-14 against the reference outputs.
PAGERANK_QUERY = ("CALL gds.pageRank.stream('bench', {{dampingFactor: 0.85, maxIterations: {it}, tolerance: 0.0}}) "
                  "YIELD nodeId, score RETURN gds.util.asNode(nodeId).id AS id, score")


PAGERANK_STATS_QUERY = ("CALL gds.pageRank.stats('bench', {{dampingFactor: 0.85, maxIterations: {it}, tolerance: 0.0}}) "
                        "YIELD ranIterations RETURN ranIterations")


def _pagerank_compute(driver):
    """The two GDS PageRank executions the correction needs (10 and 11 iterations), compute only: `stats` mode runs
    the algorithm without shipping the 633K scores to the client (shipping them is what the validated export does)."""
    ran = []
    with driver.session() as session:
        for it in (10, 11):
            ran.append(session.run(PAGERANK_STATS_QUERY.format(it=it)).single()["ranIterations"])
    return ran


def _graphalytics_pagerank(driver):
    """{vertex id: rank} equal to the Graphalytics reference PageRank (two GDS runs plus a linear correction)."""
    with driver.session() as session:
        s10 = {r["id"]: r["score"] for r in session.run(PAGERANK_QUERY.format(it=10))}
        s11 = {r["id"]: r["score"] for r in session.run(PAGERANK_QUERY.format(it=11))}
    n = len(s10)
    return {i: (20.0 * s11[i] - 17.0 * s10[i]) / 3.0 / n for i in s10}


def _dump_all(driver):
    """Full per-vertex outputs of the exact calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q, **params):
        with driver.session() as session:
            for r in session.run(q, **params):
                yield r
    bench_common.dump_safely("neo4j", "PR", lambda: bench_common.dump_rows(
        "neo4j", "PR", _graphalytics_pagerank(driver).items()))
    bench_common.dump_safely("neo4j", "WCC", lambda: bench_common.dump_rows("neo4j", "WCC", (
        (r["id"], r["componentId"]) for r in rows("""
            CALL gds.wcc.stream('bench') YIELD nodeId, componentId
            RETURN gds.util.asNode(nodeId).id AS id, componentId"""))))
    bench_common.dump_safely("neo4j", "LCC", lambda: bench_common.dump_rows("neo4j", "LCC", (
        (r["id"], float(r["coeff"])) for r in rows("""
            CALL gds.localClusteringCoefficient.stream('bench')
            YIELD nodeId, localClusteringCoefficient
            RETURN gds.util.asNode(nodeId).id AS id, localClusteringCoefficient AS coeff"""))))
    def bfs():
        with driver.session() as session:
            src = session.run("MATCH (n:Node {id: 6}) RETURN id(n) AS nid").single()["nid"]
        # gds.bfs.stream returns the visited node ids only (no distances): reachability check
        bench_common.dump_rows("neo4j", "BFSREACH", ((r["id"], 1) for r in rows("""
            CALL gds.bfs.stream('bench', {sourceNode: $src}) YIELD nodeIds
            UNWIND nodeIds AS nid RETURN gds.util.asNode(nid).id AS id""", src=src)))
    bench_common.dump_safely("neo4j", "BFSREACH", bfs)


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

    if bench_common.dump_only():
        _dump_all(driver)
        driver.close()
        bench_common.cleanup_docker(NEO4J_CONTAINER)
        return results

    # --- PageRank ---
    print("\n[Neo4j] Running PageRank...")
    def _run_pagerank():
        ran = _pagerank_compute(driver)
        print(f"  PageRank: GDS ran {ran[0]} and {ran[1]} iterations (the validated export streams both score sets)")
        return ran
    elapsed, _ = bench_common.run_timed_warm("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")
        def _pr_server():   # GDS computeMillis of the two runs the PageRank correction needs (10 and 11 iterations)
            total = 0.0
            with driver.session() as session:
                for it in (10, 11):
                    total += session.run(PAGERANK_STATS_QUERY.format(it=it).replace("RETURN ranIterations", "RETURN computeMillis AS ms")
                                         .replace("YIELD ranIterations", "YIELD computeMillis")).single()["ms"]
            return total / 1000.0
        bench_common.measure_server_time("neo4j", "PageRank", _pr_server)

    # --- WCC ---  (compute only: the stream is reduced to a summary on the server; no asNode lookup, nothing shipped)
    print("\n[Neo4j] Running WCC...")
    def _run_wcc():
        with driver.session() as session:
            row = session.run("""
                CALL gds.wcc.stream('bench')
                YIELD nodeId, componentId
                RETURN count(*) AS n, max(componentId) AS agg
            """).single()
        return row["n"], row["agg"]
    elapsed, summary = bench_common.run_timed_warm("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s  (summary {summary})")
        with driver.session() as session:   # untimed verification: the number of distinct components
            row = session.run("CALL gds.wcc.stream('bench') YIELD nodeId, componentId RETURN count(*) AS n, count(DISTINCT componentId) AS agg").single()
        bench_common.check_summary("neo4j", "WCC", row["n"], row["agg"])
        def _wcc_server():
            with driver.session() as session:
                return session.run("CALL gds.wcc.stats('bench') YIELD computeMillis RETURN computeMillis AS ms").single()["ms"] / 1000.0
        bench_common.measure_server_time("neo4j", "WCC", _wcc_server)

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
        return row['reached'], None
    elapsed, summary = bench_common.run_timed_warm("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s  (summary {summary})")
        bench_common.check_summary("neo4j", "BFS", summary[0], None)

    # --- LCC ---
    print("\n[Neo4j] Running LCC...")
    def _run_lcc():
        with driver.session() as session:
            row = session.run("""
                CALL gds.localClusteringCoefficient.stream('bench')
                YIELD nodeId, localClusteringCoefficient
                RETURN count(*) AS n, sum(localClusteringCoefficient) AS agg
            """).single()
        return row["n"], row["agg"]
    elapsed, summary = bench_common.run_timed_warm("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s  (summary {summary})")
        bench_common.check_summary("neo4j", "LCC", *summary)
        def _lcc_server():
            with driver.session() as session:
                return session.run("CALL gds.localClusteringCoefficient.stats('bench') YIELD computeMillis RETURN computeMillis AS ms").single()["ms"] / 1000.0
        bench_common.measure_server_time("neo4j", "LCC", _lcc_server)

    results["_server_time"] = dict(bench_common.SERVER_TIMES)   # BFS: GDS reports no compute time for stream mode
    _dump_all(driver)  # before the projection is dropped

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
