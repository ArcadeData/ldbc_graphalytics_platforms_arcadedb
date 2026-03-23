"""ArcadeDB (Docker) benchmark for LDBC Graphalytics."""

import time
import os
import shutil

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


def run_benchmark():
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

        GraphBatch importer = GraphBatch.builder(db)
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
            "-Xms12g", "-Xmx12g",
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
        "-e", "JAVA_OPTS=-Xms12g -Xmx12g --add-modules jdk.incubator.vector -Darcadedb.server.rootPassword=benchmark",
        "-v", "/tmp/arcadedb-docker-data:/home/arcadedb/databases",
        "-v", "/tmp/arcadedb-docker-log:/home/arcadedb/log",
        "arcadedata/arcadedb:26.4.1-SNAPSHOT"
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
    def run_algo(name, cypher_cmd, timeout=300):
        print(f"\n[ArcadeDB] Running {name}...")
        def _run():
            r = cmd(cypher_cmd, language="opencypher", timeout=timeout)
            if r.status_code != 200:
                raise RuntimeError(r.text[:200])
            return r
        elapsed, _ = bench_common.run_timed(name, _run, timeout=timeout)
        results[name] = elapsed
        if isinstance(elapsed, (int, float)):
            print(f"  {name} time: {elapsed:.2f}s")

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
             timeout=300)

    # --- SSSP (Dijkstra single-source) ---
    run_algo("sssp",
             "MATCH (s:Vertex {VID: 6}) CALL algo.dijkstra.singleSource(s, 'EDGE', 'WEIGHT') YIELD distance RETURN count(*) AS cnt")

    # --- CDLP (Label Propagation) ---
    run_algo("cdlp",
             "CALL algo.labelPropagation({maxIterations: 10}) YIELD communityId RETURN count(*) AS cnt")

    bench_common.cleanup_docker("arcadedb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arcadedb")
