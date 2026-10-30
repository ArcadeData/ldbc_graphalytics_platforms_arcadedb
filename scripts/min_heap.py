#!/usr/bin/env python3
"""Minimum-heap experiment: the smallest JVM heap at which ArcadeDB Docker and Neo4j still run every Graphalytics algorithm.

    python3 scripts/min_heap.py --out weekly-results/<dir> [--dataset graph500-22-w] [--vendors arcadedb neo4j]
                                [--heaps 12g 4g 2g 1536m 1g 768m]

For each vendor the heaps are tried from the largest down (BENCH_JVM_HEAP, with BENCH_JVM_PROBE=1 for the live heap and footprint;
for Memgraph, which has no heap, the same sizes are its --memory-limit; 'none' = the vendor default);
the first heap is the baseline. A heap passes when every operation that was valid at the baseline is again a number, validates
against the LDBC reference outputs, and the container was not killed by the out-of-memory killer. The descent of a vendor stops at
its first failing heap. The container is not capped: with a small heap its working set is what the engine really uses, so the
container peak at the smallest passing heap is the RAM the workload needs. Each engine keeps its default page cache (ArcadeDB: a
quarter of the heap, on the heap; Neo4j: the image's 512 MiB, off the heap).

ARCADEDB_NO_GAV=1 runs ArcadeDB without its Graph Analytical View (use its own --out); MIN_HEAP_VENDOR_TIMEOUT raises the 3600 s
limit per run for slow configurations. Writes <out>/<vendor>-<heap>.json and .log per run and <out>/min-heap.json / min-heap.md with the summary. One vendor at a time.
"""

import argparse
import json
import os
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
NATIVE = os.path.join(ROOT, "ldbc-native")
PYTHON = os.path.join(ROOT, ".venv", "bin", "python")
DEFAULT_HEAPS = ["12g", "4g", "2g", "1536m", "1g", "768m"]


def to_mib(size):
    """'12g' / '1536m' -> MiB."""
    size = size.strip().lower()
    return int(float(size[:-1]) * 1024) if size.endswith("g") else int(size.rstrip("m"))


def memory_env(vendor, heap):
    """The memory setting of one rung: the JVM heap (ArcadeDB, Neo4j) or Memgraph's --memory-limit; 'none' = the vendor default."""
    if heap == "none":
        return {}
    if vendor == "memgraph":
        return {"BENCH_MEMGRAPH_MEMORY_LIMIT": str(to_mib(heap))}
    return {"BENCH_JVM_HEAP": heap}


def run_one(vendor, heap, dataset, out_dir, extra_env):
    name = f"{vendor}-{heap}"
    result_file = os.path.join(out_dir, f"{name}.json")
    log_file = os.path.join(out_dir, f"{name}.log")
    env = dict(os.environ, BENCH_JVM_PROBE="1", GRAPHALYTICS_DATASET=dataset, **memory_env(vendor, heap), **extra_env)
    if dataset.startswith("graph500"):
        env["GRAPHALYTICS_SKIP"] = "sssp"
    with open(log_file, "w") as log:
        subprocess.run([PYTHON, "benchmark.py", vendor, "--out", result_file,
                        "--vendor-timeout", os.environ.get("MIN_HEAP_VENDOR_TIMEOUT", "3600")], cwd=NATIVE, env=env,
                       stdout=log, stderr=subprocess.STDOUT)
    try:
        data = json.load(open(result_file))
        result = data["vendors"][vendor].get("result") or {}
    except (OSError, ValueError, KeyError):
        result = {}
    oom = "oom=true" in open(log_file, errors="replace").read()
    return result, oom


SUMMARY_KEYS = {"pagerank": "PR", "wcc": "WCC", "bfs": "BFS", "lcc": "LCC", "sssp": "SSSP", "cdlp": "CDLP"}


def summarize(result, oom, expected, compute_only=False):
    """(passed, failures, row) for one run; expected = operations valid at the baseline (None for the baseline itself).
    compute_only: the full outputs are not exported (BENCH_NO_VALIDATE=1), so an operation counts as valid when its summary
    check (vertex count and an aggregate of the output) is ok."""
    validation = result.get("_validation") or {}
    if compute_only:
        summary = result.get("_summary") or {}
        validation = {op: {"verdict": "valid" if summary.get(key) == "ok" else f"summary {summary.get(key, 'missing')}"}
                      for op, key in SUMMARY_KEYS.items() if key in summary}
    valid = {op for op, v in validation.items() if v.get("verdict") == "valid"}
    ops = expected if expected is not None else valid
    failures = []
    for op in sorted(ops):
        value = result.get(op)
        if not isinstance(value, (int, float)):
            failures.append(f"{op}: {value if value is not None else 'missing'}")
        elif op not in valid:
            failures.append(f"{op}: {validation.get(op, {}).get('verdict', 'not validated')}")
    if oom:
        failures.append("container OOM-killed")
    if not ops:
        failures.append(f"no valid operation ({result.get('error') or result.get('_status') or 'no result'})")
    memory = result.get("_memory") or {}
    jvm = memory.get("jvm") or {}
    row = {
        "container_peak_mb": memory.get("peak_overall_mb"),
        "container_before_mb": memory.get("resident_before_mb"),
        "live_heap_max_mb": max((p.get("live_heap_mb", 0) for p in jvm.values()), default=None),
        "container_footprint_max_mb": max((p.get("container_footprint_mb", 0) for p in jvm.values()), default=None),
        "compute_s": result.get("_server_time") or {},
        "wall_s": {op: result.get(op) for op in sorted(ops)},
        "failures": failures,
    }
    return not failures, failures, row, valid


def to_md(dataset, summary):
    lines = [f"# Minimum JVM heap, `{dataset}`", "",
             "Container peak = `docker stats` working set while the vendor ran (not capped). Live heap / footprint = the JVM probe "
             "(maximum over the probes). Compute = engine-reported seconds.", ""]
    for vendor, runs in summary.items():
        lines += [f"## {vendor}", "", "| Heap | Result | Container peak (GiB) | Live heap (GiB) | Container footprint (GiB) | Compute (s) |",
                  "|---|---|---:|---:|---:|---|"]
        gib = lambda mb: f"{mb / 1024:.2f}" if mb else "n/a"  # noqa: E731
        for heap, r in runs.items():
            compute = ", ".join(f"{k} {v:.3g}" for k, v in r["compute_s"].items())
            verdict = "pass" if r["passed"] else "FAIL: " + "; ".join(r["failures"])
            lines.append(f"| {heap} | {verdict} | {gib(r['container_peak_mb'])} | {gib(r['live_heap_max_mb'])} | "
                         f"{gib(r['container_footprint_max_mb'])} | {compute} |")
        lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", required=True)
    ap.add_argument("--dataset", default="datagen-7_5-fb")
    ap.add_argument("--vendors", nargs="+", default=["arcadedb", "neo4j"])
    ap.add_argument("--heaps", nargs="+", default=DEFAULT_HEAPS)
    ap.add_argument("--expect", help="operations that must pass, comma separated (default: those valid at the first heap)")
    ap.add_argument("--compute-only", action="store_true",
                    help="do not export and validate the full outputs (BENCH_NO_VALIDATE=1); pass on the summary checks")
    args = ap.parse_args()
    if args.compute_only:
        os.environ["BENCH_NO_VALIDATE"] = "1"
    args.out = os.path.abspath(args.out)   # benchmark.py runs in ldbc-native/
    os.makedirs(args.out, exist_ok=True)
    summary_file = os.path.join(args.out, "min-heap.json")
    summary = json.load(open(summary_file)) if os.path.exists(summary_file) else {}
    for vendor in args.vendors:
        runs = summary.setdefault(vendor, {})
        expected = {op.strip() for op in args.expect.split(",")} if args.expect else None
        for heap in args.heaps:
            print(f"[min-heap] {vendor} heap {heap}: running", flush=True)
            result, oom = run_one(vendor, heap, args.dataset, args.out, {})
            passed, failures, row, valid = summarize(result, oom, expected, args.compute_only)
            if expected is None:
                expected = valid
            runs[heap] = {"passed": passed, **row}
            json.dump(summary, open(summary_file, "w"), indent=2)
            open(os.path.join(args.out, "min-heap.md"), "w").write(to_md(args.dataset, summary))
            peak = row["container_peak_mb"]
            peak_text = f"container peak {peak / 1024:.2f} GiB" if peak else "no memory data"
            reason = "" if passed else " (" + "; ".join(failures) + ")"
            print(f"[min-heap] {vendor} heap {heap}: {'PASS' if passed else 'FAIL'}{reason}, {peak_text}", flush=True)
            if not passed:
                break
    print(f"[min-heap] done: {os.path.join(args.out, 'min-heap.md')}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
