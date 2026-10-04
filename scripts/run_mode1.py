#!/usr/bin/env python3
"""Run the official LDBC Graphalytics framework (Mode 1) for OLAP and OLTP, unattended.

Prepares one config directory per mode inside an extracted distribution, runs the
framework under hard limits (SIGTERM, then SIGKILL on a hang) and extracts the
per-algorithm processing times.

  python3 scripts/run_mode1.py --dist graphalytics-1.10.0-arcadedb-0.1-SNAPSHOT

Runs on Eclipse Temurin 25 with -XX:+UseCompactObjectHeaders (found automatically; GraalVM and
any other JDK are refused).

The runner heap is 12 GB: the executor gets 4 GB and the framework starts the
runner with 3x that (the benchmark.runner.max-memory option rejects plain numbers).
"""

import argparse
import glob
import json
import os
import shutil
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "shared"))

import bench_isolation  # noqa: E402
import bench_java  # noqa: E402

DEFAULT_GRAPH = "datagen-7_5-fb"
DEFAULT_ALGORITHMS = "BFS, WCC, PR, CDLP, LCC, SSSP"


def _set_prop(path, key, value):
    with open(path) as f:
        lines = f.read().split("\n")
    out, seen = [], False
    for line in lines:
        if line.split("=")[0].strip() == key:
            out.append(f"{key} = {value}")
            seen = True
        else:
            out.append(line)
    if not seen:
        out.append(f"{key} = {value}")
    with open(path, "w") as f:
        f.write("\n".join(out))


def prepare_configs(dist, datasets, graph=DEFAULT_GRAPH, algorithms=DEFAULT_ALGORITHMS):
    datasets = os.path.abspath(datasets)
    for mode in ("olap", "oltp"):
        cfg = os.path.join(dist, f"config-{mode}")
        if os.path.isdir(cfg):
            shutil.rmtree(cfg)
        shutil.copytree(os.path.join(dist, "config-template"), cfg)
        bp = os.path.join(cfg, "benchmark.properties")
        _set_prop(bp, "graphs.root-directory", datasets)
        # expected outputs live in <datasets>/<graph>/<graph>-<ALGO>
        _set_prop(bp, "graphs.validation-directory", os.path.join(datasets, graph))
        gp = os.path.join(cfg, "graphs", f"{graph}.properties")
        _set_prop(gp, f"graph.{graph}.vertex-file", f"{graph}/{graph}.v")
        _set_prop(gp, f"graph.{graph}.edge-file", f"{graph}/{graph}.e")
        cp = os.path.join(cfg, "benchmarks", "custom.properties")
        _set_prop(cp, "benchmark.custom.graphs", graph)
        _set_prop(cp, "benchmark.custom.algorithms", algorithms)
        _set_prop(os.path.join(cfg, "platform.properties"), "platform.olap",
                  "true" if mode == "olap" else "false")


def extract(report_dir):
    """{algorithm: processing_time_seconds} from a framework report directory."""
    found = glob.glob(os.path.join(report_dir, "*", "json", "results.json")) or \
        glob.glob(os.path.join(report_dir, "json", "results.json"))
    if not found:
        return {}
    res = json.load(open(found[0]))["result"]
    jobs, out = res["jobs"], {}
    for rid, run in res["runs"].items():
        algo = next(j["algorithm"] for j in jobs.values() if rid in j["runs"])
        out[algo] = {"processing_time": float(run["processing_time"]),
                     "success": bool(run.get("success", True))}
    return out


def run_mode(dist, mode, java_home, jvm_flags, out_dir, total_timeout, idle_timeout, log=print):
    for d in ("report", "output"):
        shutil.rmtree(os.path.join(dist, d), ignore_errors=True)
    link = os.path.join(dist, "config")
    if os.path.islink(link) or os.path.exists(link):
        os.remove(link) if os.path.islink(link) else shutil.rmtree(link)
    os.symlink(f"config-{mode}", link)  # run-benchmark.sh needs no --config (and no greadlink)
    env = dict(os.environ)
    if java_home:
        env["JAVA_HOME"] = java_home
        env["PATH"] = os.path.join(java_home, "bin") + os.pathsep + env["PATH"]
    env["GRAPHALYTICS_HEAP_OPTS"] = "-Xms4g -Xmx4g"
    env["EXTRA_JVM"] = jvm_flags or ""
    os.makedirs(out_dir, exist_ok=True)
    log_file = os.path.join(out_dir, f"mode1-{mode}.log")
    for port in (8011, 8012):  # fixed framework ports: a leftover runner would break the run
        if _port_in_use(port):
            raise SystemExit(f"Port {port} is in use (a previous Graphalytics run still alive?); "
                             f"stop it first")
    outcome = bench_isolation.run_child(
        ["bash", "bin/sh/run-benchmark.sh"], env=env, cwd=dist, total_timeout=total_timeout,
        idle_timeout=idle_timeout, log_file=log_file, log=log)
    saved = os.path.join(out_dir, f"mode1-{mode}-report")
    shutil.rmtree(saved, ignore_errors=True)
    if os.path.isdir(os.path.join(dist, "report")):
        shutil.copytree(os.path.join(dist, "report"), saved)
    return outcome, extract(saved)


def _port_in_use(port):
    import socket
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dist", required=True, help="extracted graphalytics-*-arcadedb-* directory")
    ap.add_argument("--datasets", default=os.path.join(ROOT, "datasets"))
    ap.add_argument("--java-home", help="Temurin 25 home (default: found automatically)")
    ap.add_argument("--jvm-flags", default=bench_java.JVM_FLAGS)
    ap.add_argument("--modes", default="olap,oltp")
    ap.add_argument("--graph", default=DEFAULT_GRAPH, help="dataset under datasets/ (default datagen-7_5-fb)")
    ap.add_argument("--algorithms", default=DEFAULT_ALGORITHMS, help='for example "BFS, WCC, PR, CDLP, LCC" (graph500-22 has no SSSP)')
    ap.add_argument("--out-dir", default=os.path.join(ROOT, "runs-mode1"))
    ap.add_argument("--total-timeout", type=int, default=3600, help="seconds per mode (default 3600)")
    ap.add_argument("--idle-timeout", type=int, default=900, help="seconds without output (default 900)")
    args = ap.parse_args()
    dist = os.path.abspath(args.dist)
    args.java_home = bench_java.require_temurin25(args.java_home)  # Temurin 25 only, never GraalVM
    prepare_configs(dist, args.datasets, args.graph, args.algorithms)
    summary = {}
    for mode in args.modes.split(","):
        outcome, times = run_mode(dist, mode, args.java_home, args.jvm_flags, args.out_dir,
                                  args.total_timeout, args.idle_timeout)
        summary[mode] = {"outcome": repr(outcome), "algorithms": times}
        print(f"\nMode 1 {mode}: {outcome}")
        for a, v in sorted(times.items()):
            print(f"  {a:6} {v['processing_time']:9.2f}s {'ok' if v['success'] else 'FAILED'}")
    out = os.path.join(args.out_dir, "mode1-summary.json")
    bench_isolation.atomic_write_json(out, summary)
    print(f"Summary written to {out}")


if __name__ == "__main__":
    main()
