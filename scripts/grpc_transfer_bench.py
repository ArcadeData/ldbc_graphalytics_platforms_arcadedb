"""Time many-row calls over HTTP, Bolt (one PULL) and gRPC streaming (three batch sizes) on one ArcadeDB container.

  pip install grpcio grpcio-tools neo4j requests
  (generate the stubs from arcadedb/grpc/src/main/proto/arcadedb-server.proto into $GRPC_STUBS_DIR, see the comment below)
  ARCADEDB_IMAGE=arcadedata/arcadedb:<tag> python scripts/grpc_transfer_bench.py out.json

Starts the Graphalytics container with the Bolt and gRPC plugins
(-Darcadedb.server.plugins=Bolt:com.arcadedb.bolt.BoltProtocolPlugin,GRPC:com.arcadedb.server.grpc.GrpcServerPlugin;
ports 7687 and 50051; gRPC authenticates with the call metadata x-arcade-user / x-arcade-password). Warm-up + median of 3. Run on AC for
publishable numbers; one container at a time.
"""
import os, sys, time, json, statistics, subprocess
R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
# Python stubs of arcadedb-server.proto: python -m grpc_tools.protoc -I<dir> --python_out=<out> --grpc_python_out=<out> arcadedb-server.proto
STUBS = os.environ.get("GRPC_STUBS_DIR", "grpcgen")
sys.path.insert(0, R + "/shared"); sys.path.insert(0, R + "/scripts"); sys.path.insert(0, STUBS)
import requests, bench_bolt, bench_common, bolt_transfer_bench as tb
import grpc, arcadedb_server_pb2 as pb, arcadedb_server_pb2_grpc as pbg
OUT = sys.argv[1]
data_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "databases"); log_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "log")
subprocess.run(["docker", "rm", "-f", "arcadedb"], capture_output=True)
subprocess.run(["docker", "run", "-d", "--name", "arcadedb", "-p", "2480:2480", "-p", "7687:7687", "-p", "50051:50051",
    "-e", "ARCADEDB_OPTS_MEMORY=-Xms12g -Xmx12g",
    "-e", "JAVA_OPTS=--add-modules jdk.incubator.vector -Darcadedb.server.rootPassword=benchmark -Darcadedb.server.httpQueryMaxResultRows=5000000 "
          "-Darcadedb.server.plugins=Bolt:com.arcadedb.bolt.BoltProtocolPlugin,GRPC:com.arcadedb.server.grpc.GrpcServerPlugin",
    "-v", f"{data_root}:/home/arcadedb/databases", "-v", f"{log_root}:/home/arcadedb/log", os.environ["ARCADEDB_IMAGE"]], check=True, capture_output=True)
base = "http://localhost:2480/api/v1"; auth = ("root", "benchmark"); MD = (("x-arcade-user", "root"), ("x-arcade-password", "benchmark"))
CALLS = [("vertex ids (633K x 1)", "MATCH (v:Vertex) RETURN v.VID AS id", ("id",)),
         ("PageRank scores (633K x 2)", "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 10, tolerance: 0.0, direction: 'BOTH'}) YIELD node, score RETURN node.VID AS id, score", ("id", "score")),
         ("edge sample (2M x 2)", "MATCH (a:Vertex)-[:EDGE]->(b:Vertex) RETURN a.VID AS src, b.VID AS dst LIMIT 2000000", ("src", "dst"))]
def val(v):
    k = v.WhichOneof("kind"); return getattr(v, k)
res = {}
try:
    tb.wait_ready(requests, base)
    ch = grpc.insecure_channel("localhost:50051", options=[("grpc.max_receive_message_length", 256 * 1024 * 1024)])
    stub = pbg.ArcadeDbServiceStub(ch)
    bolt = bench_bolt.ArcadeBolt("bench", fetch_size=-1)
    def http(q, cols):
        r = requests.post(f"{base}/command/bench", auth=auth, timeout=900, json={"language": "opencypher", "command": q, "limit": -1, "serializer": "record"})
        return len(r.json()["result"])
    def bolt_run(q, cols): return sum(1 for _ in bolt.rows(q, *cols))
    def grpc_run(batch, read):
        def f(q, cols):
            n = 0
            for b in stub.StreamQuery(pb.StreamQueryRequest(database="bench", query=q, language="opencypher", batch_size=batch), metadata=MD):
                if read:
                    for rec in b.records:
                        p = rec.properties
                        for c in cols: val(p[c])
                n += len(b.records)
            return n
        return f
    variants = [("http", http), ("bolt (fetch all)", bolt_run), ("grpc batch 1000 +read", grpc_run(1000, True)), ("grpc batch 10000 +read", grpc_run(10000, True)),
                ("grpc batch 100000 +read", grpc_run(100000, True)), ("grpc batch 10000 count only", grpc_run(10000, False))]
    for name, q, cols in CALLS:
        print(f"== {name}", flush=True)
        for vname, fn in variants:
            try:
                fn(q, cols); ts = []; n = None
                for _ in range(3):
                    t0 = time.perf_counter(); n = fn(q, cols); ts.append(time.perf_counter() - t0)
                res[f"{name}|{vname}"] = {"median_s": round(statistics.median(ts), 3), "rows": n, "runs": [round(t, 3) for t in ts]}
                print(f"  {vname:30} {statistics.median(ts):7.3f}s  rows={n:,}  {n / statistics.median(ts):12,.0f} rows/s", flush=True)
            except Exception as e:
                print(f"  {vname:30} FAILED {type(e).__name__}: {str(e)[:150]}", flush=True)
    bolt.close()
finally:
    subprocess.run(["docker", "rm", "-f", "arcadedb"], capture_output=True)
    json.dump(res, open(OUT, "w"), indent=1)
