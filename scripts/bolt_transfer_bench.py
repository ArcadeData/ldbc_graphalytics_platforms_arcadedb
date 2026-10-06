#!/usr/bin/env python3
"""Time ArcadeDB calls that return many rows, over Bolt and over the HTTP API (the published benchmarks return one row).

  .venv/bin/python scripts/bolt_transfer_bench.py                    # image arcadedata/arcadedb:26.11.1-SNAPSHOT
  ARCADEDB_IMAGE=arcadedata/arcadedb:<tag> .venv/bin/python scripts/bolt_transfer_bench.py --out result.json

Starts the Graphalytics container on the cached `datagen-7_5-fb` database (like the Docker driver), waits for the Graph Analytical
View, then times each call over both protocols: one warm-up, then the median of --reps timed runs, the rows consumed on the client
(Bolt records decoded, HTTP JSON parsed). Row counts must agree between the two protocols or the call is flagged. The point is the
cost of moving a result, so compare the same call across images (before and after a Bolt change) and across the two protocols.
Run on AC power; the container is removed at the end and the data is kept. Never run it next to another benchmark.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "shared"))

import bench_bolt  # noqa: E402
import bench_common  # noqa: E402
import bench_state  # noqa: E402

CALLS = [
    ("vertex ids (633K rows, 1 column)", "MATCH (v:Vertex) RETURN v.VID AS id", ("id",)),
    ("PageRank scores (633K rows, 2 columns)",
     "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 10, tolerance: 0.0, direction: 'BOTH'}) "
     "YIELD node, score RETURN node.VID AS id, score", ("id", "score")),
    ("edge sample (2M rows, 2 columns)",
     "MATCH (a:Vertex)-[:EDGE]->(b:Vertex) RETURN a.VID AS src, b.VID AS dst LIMIT 2000000", ("src", "dst")),
]


def start_container(image, http_port, bolt_port):
    data_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "databases")
    log_root = bench_common.embedded_db_path("graphalytics", "arcadedb", "log")
    if not os.path.isdir(os.path.join(data_root, "bench")):
        raise SystemExit(f"No Graphalytics database at {data_root}/bench: run the Docker Graphalytics benchmark once first")
    subprocess.run(["docker", "rm", "-f", "arcadedb"], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", "arcadedb", "-p", f"{http_port}:2480", "-p", f"{bolt_port}:7687",
                    "-e", "ARCADEDB_OPTS_MEMORY=-Xms12g -Xmx12g",
                    "-e", "JAVA_OPTS=--add-modules jdk.incubator.vector -Darcadedb.server.rootPassword=benchmark "
                          "-Darcadedb.server.httpQueryMaxResultRows=5000000 " + bench_bolt.BOLT_PLUGIN_OPT,
                    "-v", f"{data_root}:/home/arcadedb/databases", "-v", f"{log_root}:/home/arcadedb/log", image],
                   check=True, capture_output=True)


def wait_ready(requests, base):
    for _ in range(180):
        try:
            if requests.get(f"{base}/ready", timeout=2).status_code == 204:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    else:
        raise SystemExit("server did not become ready")
    for _ in range(120):   # the persisted view is restored asynchronously on open
        try:
            r = requests.post(f"{base}/command/bench", auth=("root", "benchmark"), timeout=30, json={
                "language": "sql", "command": "SELECT FROM schema:graphAnalyticalViews WHERE name = 'benchmark'"})
            if r.status_code == 200 and r.json().get("result"):
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2)
    time.sleep(10)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--out", help="write the results as JSON")
    ap.add_argument("--allow-battery", action="store_true")
    args = ap.parse_args()
    if bench_state.power_source() == "battery" and not args.allow_battery:
        raise SystemExit("On battery power: the timings would be throttled (--allow-battery to run anyway)")
    import requests
    image = os.environ.get("ARCADEDB_IMAGE", "arcadedata/arcadedb:26.11.1-SNAPSHOT")
    http_port, bolt_port = "2480", bench_bolt.bolt_port()
    base = f"http://localhost:{http_port}/api/v1"
    print(f"image {image}")
    start_container(image, http_port, bolt_port)
    results = {"image": image, "calls": {}}
    try:
        wait_ready(requests, base)
        bolt = bench_bolt.ArcadeBolt("bench")

        def over_http(query, cols):
            r = requests.post(f"{base}/command/bench", auth=("root", "benchmark"), timeout=600,
                              json={"language": "opencypher", "command": query, "limit": -1, "serializer": "record"})
            if r.status_code != 200:
                raise RuntimeError(r.text[:200])
            return len(r.json()["result"])

        def over_bolt(query, cols):
            return sum(1 for _ in bolt.rows(query, *cols))

        for name, query, cols in CALLS:
            entry = {}
            for proto, fn in (("bolt", over_bolt), ("http", over_http)):
                fn(query, cols)                                  # warm-up, not reported
                times, rows = [], None
                for _ in range(args.reps):
                    t0 = time.perf_counter()
                    rows = fn(query, cols)
                    times.append(time.perf_counter() - t0)
                entry[proto] = {"median_s": round(statistics.median(times), 3), "runs_s": [round(t, 3) for t in times], "rows": rows}
            entry["rows_agree"] = entry["bolt"]["rows"] == entry["http"]["rows"]
            results["calls"][name] = entry
            print(f"{name}: bolt {entry['bolt']['median_s']}s ({entry['bolt']['rows']:,} rows)  "
                  f"http {entry['http']['median_s']}s ({entry['http']['rows']:,} rows)"
                  + ("" if entry["rows_agree"] else "  ROW COUNTS DIFFER"), flush=True)
        bolt.close()
    finally:
        subprocess.run(["docker", "rm", "-f", "arcadedb"], capture_output=True)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=1)
    sys.exit(0 if all(c["rows_agree"] for c in results["calls"].values()) else 1)


if __name__ == "__main__":
    main()
