#!/usr/bin/env python3
"""ArcadeDB native-image server vs JVM server: the same Graphalytics workload (HTTP, OpenCypher, compute only, outputs validated).

  python3 scripts/native_vs_jvm.py --jvm-image arcadedb-bench:af4ce03048-jvm
  python3 scripts/native_vs_jvm.py --jvm-image <image> --dataset graph500-22-w --runs 3 --reference-image arcadedata/arcadedb:26.11.1-SNAPSHOT

A comparison is only meaningful when both images contain the SAME engine commit: the native image (arcadedata/arcadedb:latest-native) is published
on its own schedule, so the JVM tag is usually a different commit. Build a JVM image from the commit the native image prints at startup
(`ArcadeDB Server v... (build <sha>/...)`): clone the engine, `git checkout <sha>`, `mvn package -DskipTests -pl package -am`, then
`docker build -f package/src/main/docker/Dockerfile package/target/arcadedb-<version>.dir` (the same Dockerfile as the published image).
`--reference-image` adds the current JVM tag as a third column (a different commit, shown only as context).

Order: the native and the same-commit JVM runs are interleaved (native 1, jvm 1, native 2, jvm 2, ...) so that machine drift hits both alike;
the reference runs come last (it opens the JVM variant's database with a newer engine, which must not happen before the matched runs).
Each run is a separate process and container start with its own warm-up and median of 3 (the usual harness protocol); the table shows
the median over the runs. Every run validates the full per-vertex outputs against the official reference outputs.
Progress is written as a queue plan for scripts/queue_status.py.
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "shared"))
import bench_isolation  # noqa: E402
import bench_java  # noqa: E402
import bench_state  # noqa: E402

ALGOS = ["PR", "WCC", "BFS", "LCC", "SSSP", "CDLP"]
WALL = {"PR": "pagerank", "WCC": "wcc", "BFS": "bfs", "LCC": "lcc", "SSSP": "sssp", "CDLP": "cdlp"}
PLAN = os.path.expanduser("~/.cache/ldbc-graph-bench/queue-plan.json")


def image_info(image):
    out = subprocess.run(["docker", "image", "inspect", image, "--format", "{{.Id}} {{.Size}} {{.Created}}"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"image {image} not found locally (docker pull / docker build it first)")
    iid, size, created = out.stdout.split()
    return {"id": iid.replace("sha256:", "")[:12], "size_mb": round(int(size) / 1e6), "created": created[:19]}


# Processes that mean "somebody else is building or testing on this machine": the measurements need an idle Mac (CPU, memory and the
# ports 2480-2499 that ArcadeDB test servers use). A run during which one of them shows up is discarded and repeated.
COMPETING = re.compile(r"surefire|failsafe|plexus-classworlds|gradlew|GradleDaemon|byte-buddy-agent")


def competing_processes():
    out = subprocess.run(["ps", "-Ao", "pid,command"], capture_output=True, text=True).stdout
    return sorted({line.split(None, 1)[0] for line in out.splitlines()[1:]
                   if COMPETING.search(line) and "native_vs_jvm" not in line and "benchmark.py" not in line})


def cpu_idle_percent():
    try:
        out = subprocess.run(["top", "-l", "2", "-s", "1", "-n", "0"], capture_output=True, text=True, timeout=30).stdout
    except Exception:  # noqa: BLE001
        return None
    found = re.findall(r"CPU usage: [\d.]+% user, [\d.]+% sys, ([\d.]+)% idle", out)
    return float(found[-1]) if found else None


def wait_quiet(max_wait_s):
    """Block until no competing process runs and the CPU is >= 90% idle; False when that does not happen within max_wait_s."""
    start = time.monotonic()
    while True:
        busy, idle = competing_processes(), cpu_idle_percent()
        if not busy and (idle is None or idle >= 90):
            return True
        if time.monotonic() - start > max_wait_s:
            return False
        print(f"  [{time.strftime('%H:%M:%S')}] waiting for a quiet machine: competing pids {busy[:5]}, cpu idle {idle}%", flush=True)
        time.sleep(30)


class ContentionWatch(threading.Thread):
    """Notes every competing process that appears while a run is going."""

    def __init__(self):
        super().__init__(daemon=True)
        self.seen = set()
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.wait(5):
            self.seen |= set(competing_processes())

    def finish(self):
        self._stop_event.set()
        self.join(10)
        return sorted(self.seen)


def run_problem(path, skipped):
    """None when the run is complete and correct, otherwise why it is not usable as a measurement."""
    try:
        d = json.load(open(path))
    except (OSError, ValueError):
        return "no result file"
    for v in d.get("vendors", {}).values():
        r = v.get("result")
        if not isinstance(r, dict):
            return str(v.get("status") or "no result")[:120]
        for a in (a for a in ALGOS if WALL[a] not in skipped):
            if not isinstance(r.get(WALL[a]), (int, float)):
                return f"{WALL[a]} has no timing ({r.get(WALL[a])})"
            if a not in (r.get("_server_time") or {}):
                return f"{a} has no engine-reported time"
            if (r.get("_summary") or {}).get(a) != "ok":
                return f"{a} summary check: {(r.get('_summary') or {}).get(a)}"
            if ((r.get("_validation") or {}).get(WALL[a]) or {}).get("verdict") not in ("valid", "equivalent"):
                return f"{a} output not validated as correct"
        return None
    return "no vendor in the result"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jvm-image", required=True, help="JVM image built from the same engine commit as the native image")
    ap.add_argument("--native-image", default="arcadedata/arcadedb:latest-native")
    ap.add_argument("--reference-image", help="optional: the current JVM tag (another commit), run last")
    ap.add_argument("--dataset", default="datagen-7_5-fb")
    ap.add_argument("--runs", type=int, default=3, help="separate process runs per variant (default 3)")
    ap.add_argument("--reset", action="store_true", help="reload both databases first (identical start state; recommended)")
    ap.add_argument("--out-dir")
    ap.add_argument("--step-minutes", default="12,4", help="expected minutes of the first run (with load) and of each later run, for the queue picture")
    ap.add_argument("--java-home", help="Temurin 25 for the Java loader (found automatically)")
    ap.add_argument("--allow-battery", action="store_true")
    ap.add_argument("--http-port", type=int, default=12480,
                    help="host port of the server (default 12480: ArcadeDB test servers of other sessions use 2480-2499)")
    ap.add_argument("--skip", help="algorithms the dataset does not define, comma separated (default: sssp for graph500-*, else none)")
    ap.add_argument("--max-wait", type=int, default=180, help="minutes to wait for a quiet machine before a run (default 180)")
    ap.add_argument("--attempts", type=int, default=3, help="tries per run: a contended or incomplete run is discarded and repeated")
    ap.add_argument("--run-timeout", type=int, default=3600)
    ap.add_argument("--idle-timeout", type=int, default=900)
    args = ap.parse_args()

    if bench_state.power_source() == "battery" and not args.allow_battery:
        sys.exit("On battery power: the CPU is throttled and the timings are not comparable (plug in, or --allow-battery).")
    home = bench_java.require_temurin25(args.java_home)
    env = bench_java.env_for(home)
    env["GRAPHALYTICS_DATASET"] = args.dataset
    env["ARCADEDB_NATIVE_IMAGE"] = args.native_image
    env["ARCADEDB_BENCH_HTTP_PORT"] = str(args.http_port)
    env["ARCADEDB_BENCH_BINARY_PORT"] = str(args.http_port - 56)
    skip = args.skip if args.skip is not None else ("sssp" if args.dataset.startswith("graph500") else "")
    skipped = {x.strip().lower() for x in skip.split(",") if x.strip()}
    if skip:
        env["GRAPHALYTICS_SKIP"] = skip

    variants = [("native", "arcadedb-native", args.native_image), ("jvm", "arcadedb", args.jvm_image)]
    images = {label: image_info(image) for label, _, image in variants}
    schedule = []                                   # (label, vendor, image, run number)
    for i in range(1, args.runs + 1):
        schedule += [(label, vendor, image, i) for label, vendor, image in variants]
    if args.reference_image:
        images["jvm-current-tag"] = image_info(args.reference_image)
        schedule += [("jvm-current-tag", "arcadedb", args.reference_image, i) for i in range(1, args.runs + 1)]

    out_dir = os.path.abspath(args.out_dir or os.path.join(
        ROOT, "weekly-results", time.strftime("%Y%m%d-%H%M") + f"-native-vs-jvm-{args.dataset}"))
    os.makedirs(out_dir, exist_ok=True)
    progress = os.path.join(out_dir, "progress")
    open(progress, "a").close()
    first_min, next_min = (int(x) for x in args.step_minutes.split(","))
    steps = [{"name": f"{label}-{n}", "log": os.path.join(out_dir, f"{label}-{n}.log"), "minutes": next_min if n > 1 else first_min,
              "what": f"{vendor} {image.split('/')[-1]}"} for label, vendor, image, n in schedule]
    os.makedirs(os.path.dirname(PLAN), exist_ok=True)
    with open(PLAN, "w") as f:
        json.dump({"title": f"native vs JVM, {args.dataset}", "progress": progress, "steps": steps,
                   "after": [{"name": "report", "minutes": 5, "what": "native-vs-jvm.md"}]}, f, indent=1)
    print(f"Plan: {len(schedule)} runs, results in {out_dir}; picture: python3 scripts/queue_status.py", flush=True)

    first = {}
    discarded = []
    for label, vendor, image, n in schedule:
        name = f"{label}-{n}"
        run_env = dict(env, ARCADEDB_IMAGE=image) if vendor == "arcadedb" else env
        for attempt in range(1, args.attempts + 1):
            if not wait_quiet(args.max_wait * 60):
                sys.exit(f"{name}: the machine did not become quiet within {args.max_wait} minutes; stopping (finished runs are kept)")
            cmd = [sys.executable, "-u", os.path.join(ROOT, "ldbc-native", "benchmark.py"),
                   "--out", os.path.join(out_dir, f"{name}.json"),
                   "--vendor-timeout", str(args.run_timeout), "--idle-timeout", str(args.idle_timeout)]
            # --reset only on the first attempt of the first run of a database; the reference reuses the JVM variant's database
            if args.reset and vendor not in first and label != "jvm-current-tag":
                cmd.append("--reset")
            first[vendor] = True
            cmd.append(vendor)
            swap = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True).stdout.strip()
            print(f"[{time.strftime('%H:%M:%S')}] {name} (attempt {attempt}): {vendor} on {image} | swap: {swap}", flush=True)
            watch = ContentionWatch()
            watch.start()
            bench_isolation.run_child(cmd, env=run_env, total_timeout=args.run_timeout + 600,
                                      idle_timeout=args.idle_timeout + 300, log_file=os.path.join(out_dir, f"{name}.log"))
            contended = watch.finish()
            problem = run_problem(os.path.join(out_dir, f"{name}.json"), skipped)
            if not problem and not contended:
                break
            reason = "contended" if contended else "incomplete"
            note = f"{name} attempt {attempt}: {reason}: " + (f"competing pids {contended}" if contended else problem)
            if contended and problem:
                note += f" (also: {problem})"
            print(f"  DISCARDED {note}", flush=True)
            discarded.append(note)
            for ext in ("log", "json"):
                src = os.path.join(out_dir, f"{name}.{ext}")
                if os.path.exists(src):
                    os.replace(src, os.path.join(out_dir, f"{name}.failed-{reason}-{attempt}.{ext}"))
        else:
            with open(os.path.join(out_dir, "discarded.json"), "w") as f:
                json.dump(discarded, f, indent=1)
            sys.exit(f"{name}: no usable run after {args.attempts} attempts ({discarded[-1]}); stopping")
        with open(progress, "a") as f:
            f.write(f"{name} done {time.strftime('%H:%M')}\n")
    with open(os.path.join(out_dir, "discarded.json"), "w") as f:
        json.dump(discarded, f, indent=1)

    report(out_dir, args, images, schedule, skipped, discarded)


def load_runs(out_dir, label, n_runs):
    runs = []
    for n in range(1, n_runs + 1):
        path = os.path.join(out_dir, f"{label}-{n}.json")
        try:
            d = json.load(open(path))
        except (OSError, ValueError):
            continue
        for v in d.get("vendors", {}).values():
            if isinstance(v.get("result"), dict):
                runs.append(v["result"])
    return runs


def med(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return statistics.median(values) if values else None


def cell(v, digits=3):
    return "-" if v is None else f"{v:.{digits}f}"


def ratio(a, b):
    return "-" if not a or not b else f"{a / b:.2f}x"


def report(out_dir, args, images, schedule, skipped=frozenset(), discarded=()):
    labels = list(dict.fromkeys(label for label, *_ in schedule))
    data = {label: load_runs(out_dir, label, args.runs) for label in labels}
    L = [f"# ArcadeDB native image vs JVM image, {args.dataset}", "",
         f"Median over {args.runs} separate runs (each: warm-up call, then the median of 3 timed calls). HTTP, OpenCypher, compute only; "
         f"outputs validated against the official reference. AC power, 12 GB heap for both.", ""]
    L += ["| variant | image | id | size MB | engine commit | runtime |", "|---|---|---|---|---|---|"]
    for label in labels:
        eng = (data[label][0].get("_engine") if data[label] else None) or {}
        im = images[label]
        L.append(f"| {label} | `{[i for l, _, i, _ in schedule if l == label][0]}` | {im['id']} | {im['size_mb']} | "
                 f"{eng.get('version')} `{eng.get('commit')}` | {eng.get('runtime')} |")
    L.append("")

    def table(title, getter, unit="s"):
        L.append(f"**{title}**")
        L.append("")
        L.append("| algorithm | " + " | ".join(labels) + " | native / jvm |")
        L.append("|---|" + "---|" * (len(labels) + 1))
        for a in (a for a in ALGOS if WALL[a] not in skipped):
            vals = {label: med([getter(r, a) for r in data[label]]) for label in labels}
            L.append(f"| {a} | " + " | ".join(cell(vals[label]) for label in labels) + f" | {ratio(vals.get('native'), vals.get('jvm'))} |")
        L.append("")

    table("Engine-reported compute time, seconds (the headline of the other tables)", lambda r, a: (r.get("_server_time") or {}).get(a))
    table("Wall-clock of the call incl. HTTP round trip and summary aggregate, seconds", lambda r, a: r.get(WALL[a]))

    L += ["**Start-up, build and memory**", "", "| metric | " + " | ".join(labels) + " | native / jvm |",
          "|---|" + "---|" * (len(labels) + 1)]

    def row(name, getter, digits=2):
        vals = {label: med([getter(r) for r in data[label]]) for label in labels}
        L.append(f"| {name} | " + " | ".join(cell(vals[label], digits) for label in labels) + f" | {ratio(vals.get('native'), vals.get('jvm'))} |")
    row("docker run -> /ready (s)", lambda r: r.get("_startup"))
    row("GAV (CSR) build, REBUILD (s), first run only", lambda r: r.get("_gav_build"))
    row("container memory before the first call (MiB)", lambda r: (r.get("_memory") or {}).get("resident_before_mb"), 0)
    row("container memory peak (MiB)", lambda r: (r.get("_memory") or {}).get("peak_mb"), 0)
    L.append("")

    L += ["**Correctness**", "", "| variant | runs | summary check | validated outputs |", "|---|---|---|---|"]
    for label in labels:
        runs = data[label]
        bad_s = sorted({a for r in runs for a, v in (r.get("_summary") or {}).items() if v != "ok"})
        bad_v = sorted({a for r in runs for a, v in (r.get("_validation") or {}).items() if v.get("verdict") != "valid"})
        n_v = sum(len(r.get("_validation") or {}) for r in runs)
        L.append(f"| {label} | {len(runs)} | {'all ok' if not bad_s else 'NOT ok: ' + ', '.join(bad_s)} | "
                 f"{'all valid (' + str(n_v) + ' outputs)' if not bad_v and n_v else 'INVALID or missing: ' + ', '.join(bad_v or ['none exported'])} |")
    L.append("")
    if skipped:
        L += [f"Not run (the dataset defines no reference output): {', '.join(sorted(skipped))}.", ""]
    L += ["**Discarded attempts** (contended by a competing process, or incomplete; repeated on a quiet machine)", ""]
    L += [f"- {d}" for d in discarded] or ["- none"]
    L.append("")
    text = "\n".join(L)
    with open(os.path.join(out_dir, "native-vs-jvm.md"), "w") as f:
        f.write(text)
    with open(os.path.join(out_dir, "native-vs-jvm.json"), "w") as f:
        json.dump({"images": images, "runs": data}, f, indent=1)
    print("\n" + text)


if __name__ == "__main__":
    main()
