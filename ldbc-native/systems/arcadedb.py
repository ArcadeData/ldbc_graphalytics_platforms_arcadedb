"""ArcadeDB (Docker) benchmark for LDBC Graphalytics."""

import time
import os
import shutil

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


# The timed calls are the exact full-output queries that _dump_all exports and the validator checks (like every other vendor):
# one row per vertex, no count(*), so the timing includes moving the whole result to the client.
PR_Q = ("CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 10, tolerance: 0.0, direction: 'BOTH'}) "
        "YIELD node, score RETURN node.VID AS id, score")
WCC_Q = "CALL algo.wcc() YIELD node, componentId RETURN node.VID AS id, componentId"
LCC_Q = ("CALL algo.localClusteringCoefficient() YIELD node, localClusteringCoefficient "
         "RETURN node.VID AS id, localClusteringCoefficient AS lcc")
BFS_Q = "MATCH (s:Vertex {VID: 6}) CALL algo.bfs(s) YIELD node, depth RETURN node.VID AS id, depth"
SSSP_Q = ("MATCH (s:Vertex {VID: 6}) CALL algo.dijkstra.singleSource(s, 'EDGE', 'WEIGHT', 'BOTH') "
          "YIELD node, cost RETURN node.VID AS id, cost")
CDLP_Q = ("CALL algo.labelPropagation({maxIterations: 10, tieBreakProperty: 'VID'}) YIELD node, communityId "
          "RETURN node.VID AS id, communityId")


# Compute-only variants (GRAPHALYTICS_OUTPUT=count, the default): the same procedure call, but the server reduces the output to a
# summary that depends on every value, so only one row travels. check_summary() compares it with the reference output.
# The timed aggregate is cheap (sum / max): a DISTINCT over millions of rows costs the Cypher pipeline more than shipping the output.
# WCC and CDLP are checked with the number of distinct labels in one extra UNTIMED call (DISTINCT_AGG) after the timed runs.
COUNT_AGG = {"pagerank": ("score", "sum(score)"), "wcc": ("componentId", "max(componentId)"), "bfs": ("depth", "max(depth)"),
             "lcc": ("localClusteringCoefficient", "sum(localClusteringCoefficient)"), "sssp": ("cost", "max(cost)"),
             "cdlp": ("communityId", "max(communityId)")}
DISTINCT_AGG = {"wcc": "componentId", "cdlp": "communityId"}


def _distinct_query(name, full):
    col = DISTINCT_AGG[name]
    head = full[:full.rindex(" RETURN ")]
    return f"{head} WITH node, {col} RETURN count(*) AS n, count(DISTINCT {col}) AS agg"


def _count_query(name, full):
    """The WITH clause is required: an aggregate directly after CALL ... YIELD returns 0 / null for sum, max and count(DISTINCT)
    on 26.11.1-SNAPSHOT (only count(*) is right), see DECISIONS-2026-10-07.md."""
    col, agg = COUNT_AGG[name]
    head = full[:full.rindex(" RETURN ")]
    return f"{head} WITH node, {col} RETURN count(*) AS n, {agg} AS agg"


def _dump_all(cmd):
    """Full per-vertex outputs of the exact procedure calls the benchmark times (see bench_common.dump_*)."""
    if not bench_common.dump_enabled():
        return
    def rows(q, *cols):
        r = cmd(q, language="opencypher", timeout=900)
        if r.status_code != 200:
            raise RuntimeError(r.text[:200])
        for rec in r.json()["result"]:
            yield tuple(rec[c] for c in cols)
    def all_ids():
        return [r[0] for r in rows("MATCH (v:Vertex) RETURN v.VID AS id", "id")]
    bench_common.dump_safely("arcadedb", "PR", lambda: bench_common.dump_rows("arcadedb", "PR", (
        (i, float(s)) for i, s in rows(
PR_Q, "id", "score"))))
    bench_common.dump_safely("arcadedb", "WCC", lambda: bench_common.dump_rows("arcadedb", "WCC", rows(
        WCC_Q, "id", "componentId")))
    bench_common.dump_safely("arcadedb", "LCC", lambda: bench_common.dump_rows("arcadedb", "LCC", (
        (i, float(c)) for i, c in rows(
LCC_Q, "id", "lcc"))))
    def bfs():
        reached = {i: d for i, d in rows(
BFS_Q, "id", "depth")}
        bench_common.dump_bfs("arcadedb", reached, all_ids(), 6)
    bench_common.dump_safely("arcadedb", "BFS", bfs)
    def sssp():
        dist = {i: float(c) for i, c in rows(
SSSP_Q, "id", "cost")}
        dist[6] = 0.0
        bench_common.dump_rows("arcadedb", "SSSP", ((i, dist.get(i, "infinity")) for i in all_ids()))
    if "sssp" not in bench_common.GRAPHALYTICS_SKIP:
        bench_common.dump_safely("arcadedb", "SSSP", sssp)
    def cdlp():
        # communityId is the dense index of the vertex whose label was adopted, and the procedure emits its rows in dense
        # order, so the label of vertex i is the id found in row communityId.
        got = list(rows(
CDLP_Q, "id", "communityId"))
        ids = [i for i, _ in got]
        bench_common.dump_rows("arcadedb", "CDLP", ((i, ids[c]) for i, c in got))
    bench_common.dump_safely("arcadedb", "CDLP", cdlp)


def run_benchmark():
    """
    ArcadeDB benchmark via Docker. The timed algorithm calls and the exports go over the HTTP API; the setup (GAV create/rebuild) stays on HTTP.

    Setup:
      docker run -d --name arcadedb -p 2480:2480 -p 2424:2424 \
        -e JAVA_OPTS="-Darcadedb.server.rootPassword=benchmark -Xms12g -Xmx12g --add-modules jdk.incubator.vector \
                      " \
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
    # Host ports are configurable so that the benchmark can run next to another ArcadeDB on the default ports
    http_port = os.environ.get("ARCADEDB_BENCH_HTTP_PORT", "2480")
    binary_port = os.environ.get("ARCADEDB_BENCH_BINARY_PORT", "2424")
    base = f"http://localhost:{http_port}/api/v1"
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
    data_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "databases")
    log_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "log")
    db_path = os.path.join(data_root, "bench")
    needs_load = not os.path.isdir(db_path) or bench_common.RESET

    if needs_load:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        os.makedirs(data_root, exist_ok=True)

        print("\n[ArcadeDB] Loading data via embedded Java (GraphBatchImporter)...")
        start = time.perf_counter()

        ldbc_jar = os.path.join(os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "target",
            "graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar")
        bench_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

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

        GraphBatch importer = db.batch()
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

        import bench_java
        bench_java.assert_not_graalvm("java")  # Temurin 25 only
        # Compile
        _sp.run([
            "javac", "--add-modules", "jdk.incubator.vector",
            "-cp", ldbc_jar, loader_src
        ], cwd=bench_dir, check=True)

        # Run
        proc = _sp.run([
            "java", "--add-modules", "jdk.incubator.vector",
            "-Xms12g", "-Xmx12g", *os.environ.get("ARCADEDB_JVM_FLAGS", "").split(),
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
        "-p", f"{http_port}:2480", "-p", f"{binary_port}:2424",
        "-e", "ARCADEDB_OPTS_MEMORY=-Xms12g -Xmx12g",
        "-e", "JAVA_OPTS=--add-modules jdk.incubator.vector -Darcadedb.server.rootPassword=benchmark "
                 "-Darcadedb.server.httpQueryMaxResultRows=5000000",   # full per-vertex output of graph500-22 (2.4M rows)
        "-v", f"{data_root}:/home/arcadedb/databases",
        "-v", f"{log_root}:/home/arcadedb/log",
        os.environ.get("ARCADEDB_IMAGE", "arcadedata/arcadedb:26.11.1-SNAPSHOT")
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

    # Helper to run an algorithm via OpenCypher. GRAPHALYTICS_OUTPUT=count (default): compute only, the call returns one summary row;
    # =full: the call returns the full per-vertex output and the whole JSON result is parsed inside the timed call.
    def run_algo(name, full_query, timeout=300):
        print(f"\n[ArcadeDB] Running {name}...")
        query = _count_query(name, full_query) if bench_common.compute_only() else full_query
        def _run():
            r = cmd(query, language="opencypher", timeout=timeout)
            if r.status_code != 200:
                raise RuntimeError(r.text[:200])
            rows = r.json()["result"]
            return (rows[0]["n"], rows[0]["agg"]) if bench_common.compute_only() else (len(rows), None)
        elapsed, summary = bench_common.run_timed_warm(name, _run, timeout=timeout)
        results[name] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {name} time: {elapsed:.2f}s  (summary {summary})")
            if bench_common.compute_only():
                n, agg = summary
                if name in DISTINCT_AGG:   # untimed verification: the number of distinct labels
                    r = cmd(_distinct_query(name, full_query), language="opencypher", timeout=timeout)
                    row = r.json()["result"][0]
                    n, agg = row["n"], row["agg"]
                # algo.bfs and the Dijkstra procedure do not emit the source itself (the export adds it with distance 0)
                bench_common.check_summary("arcadedb", name, n + 1 if name in ("bfs", "sssp") else n, agg)

        # server-reported compute time: the cost of the CALL step in PROFILE (the algorithm, without the summary aggregate or the HTTP round trip)
        def _profile():
            r = cmd("PROFILE " + _count_query(name, full_query), language="opencypher", timeout=timeout)
            steps = r.json()["explainPlan"]["steps"]
            return sum(st["cost"] for st in steps if st["name"] == "CallStep") / 1e9
        if isinstance(elapsed, (int, float)):
            bench_common.measure_server_time("arcadedb", name, _profile)

    # Graphalytics PageRank: undirected (BOTH), exactly 10 iterations, no early stop. The procedure's
    # defaults (direction OUT, 20 iterations, tolerance 1e-4) compute a different, directed PageRank.
    run_algo("pagerank", PR_Q)
    run_algo("wcc", WCC_Q)
    run_algo("bfs", BFS_Q)
    run_algo("lcc", LCC_Q, timeout=300)
    # The SSSP procedure defaults to direction OUT; the undirected Graphalytics SSSP needs BOTH (in SSSP_Q).
    run_algo("sssp", SSSP_Q)
    run_algo("cdlp", CDLP_Q)
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_server_time"] = dict(bench_common.SERVER_TIMES)
    results["_output"] = bench_common.OUTPUT_MODE

    _dump_all(cmd)
    bench_common.cleanup_docker("arcadedb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arcadedb")
