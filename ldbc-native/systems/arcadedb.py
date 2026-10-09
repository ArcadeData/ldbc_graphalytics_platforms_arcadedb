"""ArcadeDB (Docker) benchmark for LDBC Graphalytics."""

import time

import bench_containers
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


def _dump_all(cmd, vendor):
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
    bench_common.dump_safely(vendor, "PR", lambda: bench_common.dump_rows(vendor, "PR", (
        (i, float(s)) for i, s in rows(
PR_Q, "id", "score"))))
    bench_common.dump_safely(vendor, "WCC", lambda: bench_common.dump_rows(vendor, "WCC", rows(
        WCC_Q, "id", "componentId")))
    bench_common.dump_safely(vendor, "LCC", lambda: bench_common.dump_rows(vendor, "LCC", (
        (i, float(c)) for i, c in rows(
LCC_Q, "id", "lcc"))))
    def bfs():
        reached = {i: d for i, d in rows(
BFS_Q, "id", "depth")}
        bench_common.dump_bfs(vendor, reached, all_ids(), 6)
    bench_common.dump_safely(vendor, "BFS", bfs)
    def sssp():
        dist = {i: float(c) for i, c in rows(
SSSP_Q, "id", "cost")}
        dist[6] = 0.0
        bench_common.dump_rows(vendor, "SSSP", ((i, dist.get(i, "infinity")) for i in all_ids()))
    if "sssp" not in bench_common.GRAPHALYTICS_SKIP:
        bench_common.dump_safely(vendor, "SSSP", sssp)
    def cdlp():
        # communityId is the dense index of the vertex whose label was adopted, and the procedure emits its rows in dense
        # order, so the label of vertex i is the id found in row communityId.
        got = list(rows(
CDLP_Q, "id", "communityId"))
        ids = [i for i, _ in got]
        bench_common.dump_rows(vendor, "CDLP", ((i, ids[c]) for i, c in got))
    bench_common.dump_safely(vendor, "CDLP", cdlp)


def _server_build(container):
    """Version, commit and runtime the server prints at startup ("ArcadeDB Server v... (build <sha>/...)", "Running on ... - <VM>").
    The image tags move and the native image is built from another commit than the JVM tag, so every result carries its build."""
    import re
    import subprocess
    out = subprocess.run(["docker", "logs", container], capture_output=True, text=True)
    text = out.stdout + out.stderr
    m = re.search(r"ArcadeDB Server v(\S+) \(build (\w+)/", text)
    r = re.search(r"Running on (.+)", text)
    return {"version": m.group(1) if m else None, "commit": m.group(2)[:10] if m else None,
            "runtime": r.group(1).strip() if r else None}


def _port_in_use(port):
    import socket
    for host in ("127.0.0.1", "::1"):
        try:
            with socket.create_connection((host, int(port)), timeout=0.5):
                return True
        except OSError:
            continue
    return False


def run_benchmark():
    """The JVM image (arcadedb_image()): see _run_variant."""
    return _run_variant(native=False)


def run_benchmark_native():
    """The GraalVM native-image build (arcadedb_native_image()): see _run_variant."""
    return _run_variant(native=True)


def _run_variant(native):
    """
    ArcadeDB benchmark via Docker. The timed algorithm calls and the exports go over the HTTP API; the setup (GAV create/rebuild) stays on HTTP.

    Two variants of the same server, same loader, same 12 GB heap, same queries (HTTP, OpenCypher):
      JVM    (vendor "arcadedb"):        container arcadedb,        JAVA_OPTS / ARCADEDB_OPTS_MEMORY environment
      native (vendor "arcadedb-native"): container arcadedb-native, distroless image without JVM and shell: the entrypoint is the native
                                         binary, so heap and system properties are command-line arguments (no JAVA_OPTS; the server still logs
                                         "Graph-OLAP SIMD vector ops enabled" in this image, so the Vector API is available there too)
    Each variant has its own database directory (the loader runs once per variant), so neither opens files the other one touched.

    Setup (JVM):
      docker run -d --name arcadedb -p 2480:2480 -p 2424:2424 \
        -e JAVA_OPTS="-Darcadedb.server.rootPassword=benchmark -Xms12g -Xmx12g --add-modules jdk.incubator.vector \
                      " \
        -v "$(cd ../datasets && pwd)":/data/graphs:ro \
        -v /tmp/arcadedb-docker-data:/home/arcadedb/databases \
        arcadedata/arcadedb:latest
    Setup (native):
      docker run -d --name arcadedb-native -p 2480:2480 -v <databases>:/home/arcadedb/databases arcadedata/arcadedb:latest-native \
        -Xms12g -Xmx12g -Darcadedb.server.rootPassword=benchmark
    """
    import requests
    import subprocess as _sp
    vendor = "arcadedb-native" if native else "arcadedb"
    container = vendor
    image = bench_containers.arcadedb_native_image() if native else bench_containers.arcadedb_image()
    print("\n" + "=" * 70)
    print("ARCADEDB (DOCKER, NATIVE IMAGE) BENCHMARK" if native else "ARCADEDB (DOCKER) BENCHMARK")
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
    data_root = bench_common.embedded_db_path("graphalytics", vendor, "databases")
    log_root = bench_common.embedded_db_path("graphalytics", vendor, "log")
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
    print(f"\n[ArcadeDB] Starting Docker server ({image})...")
    _sp.run(["docker", "rm", "-f", container], capture_output=True)
    # A foreign server on the port (another ArcadeDB, a test run) would answer /ready and reject our login, or take our requests: refuse to start.
    for _ in range(20):
        if not _port_in_use(http_port):
            break
        time.sleep(0.5)
    else:
        return {"error": f"host port {http_port} is already in use by another process (set ARCADEDB_BENCH_HTTP_PORT to a free port)"}
    settings = ["-Darcadedb.server.rootPassword=benchmark",
                "-Darcadedb.server.httpQueryMaxResultRows=5000000"]   # full per-vertex output of graph500-22 (2.4M rows)
    if native:
        # no JVM, no shell, no JAVA_OPTS: the entrypoint is the native binary and takes the heap size and the properties as arguments
        # (no --add-modules: the Vector ops are enabled anyway, see the startup log); the binary port is not published, nothing uses it
        run_cmd = ["docker", "run", "-d", "--name", container, "-p", f"{http_port}:2480",
                   "-v", f"{data_root}:/home/arcadedb/databases", "-v", f"{log_root}:/home/arcadedb/log",
                   image, "-Xms12g", "-Xmx12g", *settings]
    else:
        run_cmd = ["docker", "run", "-d", "--name", container,
                   "-p", f"{http_port}:2480", "-p", f"{binary_port}:2424",
                   "-e", "ARCADEDB_OPTS_MEMORY=-Xms12g -Xmx12g",
                   "-e", "JAVA_OPTS=--add-modules jdk.incubator.vector " + " ".join(settings),
                   "-v", f"{data_root}:/home/arcadedb/databases",
                   "-v", f"{log_root}:/home/arcadedb/log",
                   image]
    started = time.perf_counter()
    _sp.run(run_cmd, check=True)

    # Wait for server + GAV auto-restore (CSR build takes ~60-90s). Start-up time = docker run until /ready answers (polled every 0.1 s).
    print("  Waiting for server and GAV (CSR) build...")
    for i in range(1200):
        try:
            r = requests.get(f"{base}/ready", timeout=2)
            if r.status_code == 204:
                results["_startup"] = round(time.perf_counter() - started, 2)
                print(f"  ArcadeDB Docker server: OK (docker run -> /ready {results['_startup']}s)")
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        print("  ArcadeDB Docker server failed to start")
        return {"error": "Docker server timeout"}
    results["_engine"] = _server_build(container)
    print(f"  [engine] {vendor}: {results['_engine']}", flush=True)
    # /ready answers before the login works (and a foreign server answers it too): wait for an authenticated query on our database.
    last = None
    for _ in range(120):
        try:
            r = cmd("SELECT 1 AS ok", timeout=10)
            if r.status_code == 200:
                break
            last = f"HTTP {r.status_code} {r.text[:120]}"
        except Exception as e:  # noqa: BLE001
            last = f"{type(e).__name__}: {str(e)[:100]}"
        time.sleep(0.5)
    else:
        print(f"  login probe failed: {last}")
        return {"error": f"the server on port {http_port} does not accept root/benchmark ({last}); is another server using the port?"}

    # GAV is auto-restored on database open (persisted definition from the Java loader).
    # Wait for the async CSR build to finish before running algorithms. A database that was just loaded has no GAV yet: do not wait for one.
    print("\n[ArcadeDB] Waiting for Graph Analytical View (CSR) to be ready...")
    # A restored view is rebuilt in the background: it is listed at once, but until its status is READY the algorithms run unaccelerated
    # (PageRank on graph500-22-w then exceeds the 5-minute limit) and compete with the builder. Wait for READY, not for the row.
    gav_found = False
    gav_status = None
    gav_wait_start = time.perf_counter()
    for i in range(0 if needs_load else 450):
        try:
            r = cmd("SELECT FROM schema:graphAnalyticalViews WHERE name = 'benchmark'", timeout=30)
            if r.status_code == 200:
                result = r.json().get("result", {})
                records = result.get("records", []) if isinstance(result, dict) else result
                if records and len(records) > 0:
                    rec = records[0]
                    gav_status = rec.get("status") if isinstance(rec, dict) else None
                    if gav_status in (None, "READY"):   # None: an engine without the status column
                        waited = time.perf_counter() - gav_wait_start
                        print(f"  GAV ready (status {gav_status}, {waited:.0f}s after the server answered)")
                        results["_gav_restore"] = round(waited, 1)
                        gav_found = True
                        break
                    if i % 5 == 0:
                        print(f"  GAV status {gav_status}, waiting for READY ({time.perf_counter() - gav_wait_start:.0f}s)", flush=True)
        except Exception:
            pass
        time.sleep(2)
    if not gav_found and gav_status is not None:
        return {"error": f"the Graph Analytical View is still {gav_status} after 900 s; not measuring an unaccelerated engine"}
    if not gav_found:
        # GAV not found — create it (first run or --reset)
        print("  No GAV found, creating...")
        cmd("CREATE GRAPH ANALYTICAL VIEW benchmark VERTEX TYPES (Vertex) EDGE TYPES (EDGE) EDGE PROPERTIES (WEIGHT)")
        start = time.perf_counter()
        r = cmd("REBUILD GRAPH ANALYTICAL VIEW benchmark", timeout=600)
        gav_time = time.perf_counter() - start
        if r.status_code != 200:
            print(f"  GAV build failed: {r.text[:300]}")
            return {"error": "GAV build failed"}
        results["_gav_build"] = round(gav_time, 2)
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
                bench_common.check_summary(vendor, name, n + 1 if name in ("bfs", "sssp") else n, agg)

        # server-reported compute time: the cost of the CALL step in PROFILE (the algorithm, without the summary aggregate or the HTTP round trip)
        def _profile():
            r = cmd("PROFILE " + _count_query(name, full_query), language="opencypher", timeout=timeout)
            steps = r.json()["explainPlan"]["steps"]
            return sum(st["cost"] for st in steps if st["name"] == "CallStep") / 1e9
        if isinstance(elapsed, (int, float)):
            bench_common.measure_server_time(vendor, name, _profile)

    # Graphalytics PageRank: undirected (BOTH), exactly 10 iterations, no early stop. The procedure's
    # defaults (direction OUT, 20 iterations, tolerance 1e-4) compute a different, directed PageRank.
    run_algo("pagerank", PR_Q)
    run_algo("wcc", WCC_Q)
    run_algo("bfs", BFS_Q)
    run_algo("lcc", LCC_Q, timeout=300)
    # The SSSP procedure defaults to direction OUT; the undirected Graphalytics SSSP needs BOTH (in SSSP_Q).
    run_algo("sssp", SSSP_Q)
    run_algo("cdlp", CDLP_Q)
    bench_common.record_image(results, image)
    results["_summary"] = dict(bench_common.SUMMARY_CHECKS)
    results["_server_time"] = dict(bench_common.SERVER_TIMES)
    results["_output"] = bench_common.OUTPUT_MODE

    _dump_all(cmd, vendor)
    bench_common.cleanup_docker(container)
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arcadedb")
run_benchmark_native._cleanup = lambda: bench_common.cleanup_docker("arcadedb-native")
