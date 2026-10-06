#!/usr/bin/env python3
"""Reproducer / regression check for ArcadeDB #8660 (quadratic free-space scan on bulk updates).

Clones the loaded embedded Graphalytics database (APFS clone, the shared one is never touched), runs
ldbc-native/BulkUpdateRepro.java on the clone and checks that no bulk UPDATE is slow. Two property orders are tried
because the regression depended on what had already been written to the vertices:

  python3 scripts/bulk_update_repro.py                      # both orders, limit 15 s per UPDATE
  python3 scripts/bulk_update_repro.py --limit 5 --orders BFS,WCC,PR
  python3 scripts/bulk_update_repro.py --json out.json      # machine readable (for the weekly report)

Exit code 0 when every UPDATE stays under --limit, 1 otherwise (or when a run fails or times out).
The limit is a guard against the quadratic (65-260 s on the bad engine, a few seconds on a good one), not a benchmark:
calibrate it from the first runs on AC power. Runs on Temurin 25 with -Xms12g -Xmx12g like every ArcadeDB benchmark.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "shared"))

import bench_isolation  # noqa: E402
import bench_java  # noqa: E402
import bench_state  # noqa: E402

JAR = os.path.join(ROOT, "target", "graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar")
SRC = os.path.join(ROOT, "ldbc-native", "BulkUpdateRepro.java")
DEFAULT_ORDERS = ["BFS,WCC,PR,CDLP,LCC,SSSP", "SSSP,LCC,CDLP,PR,WCC,BFS"]


def compile_repro(java_home):
    out = bench_state.state_path("build", create=True)
    cls = os.path.join(out, "BulkUpdateRepro.class")
    if not (os.path.exists(cls) and os.path.getmtime(cls) > os.path.getmtime(SRC)):
        r = subprocess.run([os.path.join(java_home, "bin", "javac"), "-proc:none", "-cp", JAR, "-d", out, SRC],
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"javac failed: {r.stderr[-400:]}")
    return out


def clone(src, dst):
    shutil.rmtree(dst, ignore_errors=True)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if subprocess.run(["cp", "-cR", src, dst], capture_output=True).returncode != 0:  # not APFS: plain copy
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)


def run_order(args, build, order, tag):
    work = bench_state.state_path("tmp", f"bulk-update-repro-{tag}")
    clone(args.db, work)
    try:
        cmd = [os.path.join(args.java_home, "bin", "java"), "--add-modules", "jdk.incubator.vector", "-Xms12g", "-Xmx12g",
               *args.jvm_flags.split(), f"-Ddb.path={work}", f"-Dorder={order}",
               "-cp", f"{build}{os.pathsep}{JAR}", "BulkUpdateRepro"]
        log_file = os.path.join(args.out_dir, f"bulk-update-{tag}.log")
        os.makedirs(args.out_dir, exist_ok=True)
        open(log_file, "w").close()
        outcome = bench_isolation.run_child(cmd, total_timeout=args.timeout, idle_timeout=args.timeout,
                                            log_file=log_file, log=print)
        text = open(log_file, errors="replace").read()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    updates = {m.group(1): float(m.group(2)) for m in re.finditer(r"^UPDATE (\w+) ([\d.]+)s", text, re.M)}
    total = re.search(r"^TOTAL ([\d.]+)s", text, re.M)
    return {"order": order, "updates": updates, "total": float(total.group(1)) if total else None,
            "outcome": repr(outcome), "complete": bool(total) and outcome.ok}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=bench_state.state_path("data", "graphalytics-arcadedb-java", "db"),
                    help="loaded embedded database (default: the one the weekly Java benchmark reuses)")
    ap.add_argument("--orders", nargs="*", default=DEFAULT_ORDERS, help="comma separated algorithm orders")
    ap.add_argument("--limit", type=float, default=15.0, help="max seconds for one bulk UPDATE (default 15)")
    ap.add_argument("--timeout", type=int, default=900, help="seconds per order before the run is killed (default 900)")
    ap.add_argument("--java-home", help="Temurin 25 home (default: found automatically)")
    ap.add_argument("--jvm-flags", default=bench_java.JVM_FLAGS)
    ap.add_argument("--out-dir", default=os.path.join(ROOT, "runs-bulk-update"))
    ap.add_argument("--json", help="write the results here")
    args = ap.parse_args()
    args.java_home = bench_java.require_temurin25(args.java_home)
    if not os.path.isdir(args.db):
        raise SystemExit(f"No loaded database at {args.db}; run `python3 weekend.py --only java-m2` once first")
    if not os.path.exists(JAR):
        raise SystemExit(f"{JAR} missing; run `mvn package -DskipTests`")
    build = compile_repro(args.java_home)

    runs = [run_order(args, build, order, f"order{i}") for i, order in enumerate(args.orders, 1)]
    bad = False
    for r in runs:
        slow = {a: t for a, t in r["updates"].items() if t > args.limit}
        r["slow"] = slow
        r["ok"] = r["complete"] and not slow
        bad |= not r["ok"]
        cells = "  ".join(f"{a} {t:.2f}s" for a, t in r["updates"].items())
        verdict = "OK" if r["ok"] else ("SLOW " + ", ".join(slow) if slow else f"INCOMPLETE {r['outcome']}")
        print(f"{r['order']:34} {cells}  total {r['total']}  -> {verdict}")
    if args.json:
        bench_isolation.atomic_write_json(args.json, {"limit": args.limit, "runs": runs})
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
