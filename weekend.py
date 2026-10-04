#!/usr/bin/env python3
"""Weekly unattended benchmark run: every suite, every vendor, hard limits, data loaded once.

  python3 weekend.py                                                       # everything (Temurin 25, compact headers)
  python3 weekend.py --only py-m2,py-lsqb                                  # only the multi-vendor suites
  python3 weekend.py --mode1-dist graphalytics-1.10.0-arcadedb-0.1-SNAPSHOT  # also run Mode 1
  python3 weekend.py --reset                                               # reload every database

Steps (in order):
  preflight  Docker memory, disk, datasets, JAR, stray vendor containers
  java-m2    ArcadeDB embedded, Graphalytics (reps runs; load once, then reuse)
  java-lsqb  ArcadeDB embedded, LSQB OLAP (GAV) and OLTP (no GAV) (reps runs)
  py-m2      Graphalytics, all vendors (ldbc-native/benchmark.py)
  py-lsqb    LSQB, all vendors (lsqb/lsqb_benchmark.py)
  mode1      Official framework (only with --mode1-dist)

Every child process runs in its own process group with a total-time and a no-output
limit; on a hang it gets SIGTERM and, after the grace period, SIGKILL.

JVM rule: the benchmarks run on Eclipse Temurin 25 with -XX:+UseCompactObjectHeaders and nothing
else. The JDK is found automatically (or set --java-home / LDBC_JAVA_HOME); GraalVM and any other JDK
are refused. A failed or
killed step never stops the following ones. Databases live under ~/.cache/ldbc-graph-bench
(override with LDBC_BENCH_STATE); a completed load is reused until --reset, a new image
version or a changed dataset.
"""

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "shared"))

import bench_common  # noqa: E402
import bench_containers  # noqa: E402
import bench_java  # noqa: E402
import bench_isolation  # noqa: E402
import bench_state  # noqa: E402

JAR = os.path.join(ROOT, "target", "graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar")
STEPS = ["preflight", "java-m2", "java-lsqb", "py-m2", "py-lsqb", "mode1"]
M2_KEYS = ["LOAD", "PR", "WCC", "BFS", "LCC", "SSSP", "CDLP"]
LSQB_KEYS = ["LOAD"] + [f"Q{i}" for i in range(1, 10)]
STRAY = ["arcadedb", "arcadedb-lsqb", "neo4j-gds", "neo4j-lsqb", "memgraph", "memgraph-lsqb",
         "arangodb", "falkordb", "falkordb-lsqb", "postgres-lsqb", "vermeer-master", "vermeer-worker"]


def log(msg=""):
    print(msg, flush=True)


def docker_memory_gb():
    try:
        r = subprocess.run(["docker", "info", "--format", "{{.MemTotal}}"], capture_output=True,
                           text=True, timeout=30)
        return int(r.stdout.strip()) / 2**30
    except Exception:
        return None


# ------------------------------------------------------------------ steps

def preflight(args, report):
    problems, notes = [], []
    power = bench_state.power_source()
    report["power_source"] = power
    if power == "battery" and not args.allow_battery:
        problems.append("running on battery power: macOS throttles the CPU, so the timings would not be comparable "
                        "with earlier runs; plug in the charger (or pass --allow-battery to accept unreliable numbers)")
    mem = docker_memory_gb()
    if mem is None:
        problems.append("Docker is not running")
    elif mem < args.min_docker_gb:
        problems.append(f"Docker Desktop has {mem:.0f} GB, need at least {args.min_docker_gb} GB "
                        f"(12 GB JVM heaps plus headroom)")
    root = bench_state.state_path(create=True)
    free = shutil.disk_usage(root).free / 2**30
    if free < args.min_disk_gb:
        problems.append(f"only {free:.0f} GB free under {root}, need {args.min_disk_gb} GB")
    for needed in (os.path.join(ROOT, "datasets", "datagen-7_5-fb", "datagen-7_5-fb.e"),
                   os.path.join(ROOT, "datasets", "social-network-sf1-merged-fk"),
                   os.path.join(ROOT, "datasets", "social-network-sf1-projected-fk")):
        if not os.path.exists(needed):
            problems.append(f"missing dataset {needed} (python3 datasets.py download ...)")
    if not os.path.exists(JAR):
        problems.append(f"missing {JAR} (mvn package -DskipTests)")
    for name in STRAY:
        r = subprocess.run(["docker", "ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}"],
                           capture_output=True, text=True)
        if name in r.stdout.split():
            notes.append(f"stray container {name} removed (would skew the results)")
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    report["preflight"] = {"problems": problems, "notes": notes,
                           "docker_gb": mem, "free_gb": round(free)}
    for n in notes:
        log(f"  note: {n}")
    for p in problems:
        log(f"  PROBLEM: {p}")
    return not problems


def java_bin(args):
    return os.path.join(args.java_home, "bin", "java") if args.java_home else "java"


def javac_bin(args):
    return os.path.join(args.java_home, "bin", "javac") if args.java_home else "javac"


def compile_java(args):
    out = bench_state.state_path("build", create=True)
    for src in ("ldbc-native/ArcadeDBEmbeddedBenchmark.java", "lsqb/ArcadeDBEmbeddedLSQB.java"):
        path = os.path.join(ROOT, src)
        cls = os.path.join(out, os.path.basename(src).replace(".java", ".class"))
        if os.path.exists(cls) and os.path.getmtime(cls) > os.path.getmtime(path) \
                and os.path.getmtime(cls) > os.path.getmtime(JAR):
            continue
        r = subprocess.run([javac_bin(args), "-proc:none", "--add-modules", "jdk.incubator.vector",
                            "-cp", JAR, "-d", out, path], capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"javac failed for {src}: {r.stderr[-400:]}")
    return out


def parse_table(text, keys):
    """Summary lines like `PR    0.19s` and `Q4    0.12s` from the Java benchmarks."""
    out = {}
    for k in keys:
        m = re.search(rf"^{k}\s+([\d.]+)s", text, re.M)
        if m:
            out[k] = float(m.group(1))
    return out


def run_java(args, name, main_class, cwd, props, extra, out_dir):
    cmd = [java_bin(args), "--add-modules", "jdk.incubator.vector", "-Xms12g", "-Xmx12g"]
    cmd += args.jvm_flags.split() + [f"-D{k}={v}" for k, v in props.items()]
    cmd += ["-cp", f"{bench_state.state_path('build')}{os.pathsep}{JAR}", main_class, *extra]
    log_file = os.path.join(out_dir, f"{name}.log")
    outcome = bench_isolation.run_child(cmd, cwd=cwd, total_timeout=args.java_timeout,
                                        idle_timeout=args.java_idle, log_file=log_file, log=log)
    text = open(log_file, errors="replace").read() if os.path.exists(log_file) else ""
    return outcome, text


def step_java(args, report, out_dir, suite):
    compile_java(args)
    results = {}
    if suite == "m2":
        variants = {"m2": ("ArcadeDBEmbeddedBenchmark", os.path.join(ROOT, "ldbc-native"),
                           {"db.path": bench_state.state_path("data", "graphalytics-arcadedb-java", "db", create=False)},
                           M2_KEYS)}
    else:
        variants = {
            "lsqb-olap": ("ArcadeDBEmbeddedLSQB", os.path.join(ROOT, "lsqb"),
                          {"db.path": bench_state.state_path("data", "lsqb-arcadedb-java-olap", "db")}, LSQB_KEYS),
            "lsqb-oltp": ("ArcadeDBEmbeddedLSQB", os.path.join(ROOT, "lsqb"),
                          {"db.path": bench_state.state_path("data", "lsqb-arcadedb-java-oltp", "db")}, LSQB_KEYS)}
    for vname, (cls, cwd, props, keys) in variants.items():
        os.makedirs(os.path.dirname(props["db.path"]), exist_ok=True)
        runs, statuses, logs = [], [], []
        extra_flags = ["--no-gav"] if vname == "lsqb-oltp" else []
        if vname == "m2":
            props["dump.dir"] = os.path.join(out_dir, "outputs-java")
        for rep in range(1, args.reps + 1):
            flags = list(extra_flags) + (["--reset"] if (args.reset and rep == 1) else [])
            outcome, text = run_java(args, f"java-{vname}-{rep}", cls, cwd, props, flags, out_dir)
            table = parse_table(text, keys)
            if len(table) < len(keys) - (0 if vname == "m2" else 0) and not args.reset and rep == 1:
                # reuse path failed (stale or incompatible data): one retry with a fresh load
                log(f"  {vname}: reuse run incomplete ({outcome}); retrying with --reset")
                outcome, text = run_java(args, f"java-{vname}-{rep}-reset", cls, cwd, props,
                                         flags + ["--reset"], out_dir)
                table = parse_table(text, keys)
            statuses.append(repr(outcome))
            logs.append(text)
            if table:
                runs.append(table)
        med = {}
        for k in keys:
            vals = [r[k] for r in runs if k in r]
            if vals:
                med[k] = round(statistics.median(vals), 3)
        results[vname] = {"median": med, "runs": runs, "outcomes": statuses}
        # Correctness: Graphalytics outputs against the LDBC reference outputs, LSQB counts against the
        # official expected counts. Timings are only reported next to this verdict.
        try:
            if vname == "m2":
                sys.path.insert(0, os.path.join(ROOT, "scripts"))
                import validate_outputs
                results[vname]["validation"] = validate_outputs.validate_vendor(
                    props["dump.dir"], "arcadedb-embedded", "datagen-7_5-fb", os.path.join(ROOT, "datasets"))
            elif logs:
                results[vname]["validation"] = bench_common.validate_lsqb_counts(logs[-1])
        except Exception as e:  # noqa: BLE001
            results[vname]["validation"] = {"error": str(e)}
    report[f"java-{suite}"] = results


def step_python(args, report, out_dir, suite):
    script = os.path.join(ROOT, "ldbc-native" if suite == "m2" else "lsqb",
                          "benchmark.py" if suite == "m2" else "lsqb_benchmark.py")
    out = os.path.join(out_dir, f"py-{suite}.json")
    cmd = [sys.executable, "-u", script, "--out", out,
           "--vendor-timeout", str(args.vendor_timeout), "--idle-timeout", str(args.idle_timeout)]
    if args.reset:
        cmd.append("--reset")
    # JVM engines (ArcadeDB Docker, Neo4j) are measured warm on LSQB: 1 untimed run, then the
    # median of 3 (queries slower than 30 s are reported from a single run).
    os.environ.setdefault("LSQB_WARMUP", "1")
    os.environ.setdefault("LSQB_REPS", "3")
    if suite == "m2":
        cmd += ["neo4j", "memgraph", "arangodb", "falkordb", "hugegraph",
                "arcadedb", "kuzu", "ladybug", "duckpgq"]
    log_file = os.path.join(out_dir, f"py-{suite}.log")
    outcome = bench_isolation.run_child(cmd, total_timeout=args.suite_timeout, idle_timeout=args.suite_idle,
                                        log_file=log_file, log=log)
    report[f"py-{suite}"] = {"outcome": repr(outcome), "results": bench_isolation.read_json(out)}


def step_mode1(args, report, out_dir):
    cmd = [sys.executable, os.path.join(ROOT, "scripts", "run_mode1.py"), "--dist", args.mode1_dist,
           "--out-dir", os.path.join(out_dir, "mode1"), "--jvm-flags", args.jvm_flags]
    if args.java_home:
        cmd += ["--java-home", args.java_home]
    outcome = bench_isolation.run_child(cmd, total_timeout=2 * 3600 + 600, idle_timeout=1200,
                                        log_file=os.path.join(out_dir, "mode1.log"), log=log)
    report["mode1"] = {"outcome": repr(outcome),
                       "summary": bench_isolation.read_json(os.path.join(out_dir, "mode1", "mode1-summary.json"))}


# ----------------------------------------------------------------- report

def _mark(value, verdict):
    cell = f"{value:.2f}" if isinstance(value, (int, float)) else str(value)
    if verdict is None:
        return cell
    return {"invalid": f"{cell} **INVALID**", "equivalent": f"{cell} (labels renamed)"}.get(verdict["verdict"], cell)


def md_report(report):
    L = [f"# Weekly benchmark {report['started']}", "",
         f"JVM: `{report['java']}` flags `{report['jvm_flags'] or '(none)'}`; ArcadeDB JAR build "
         f"`{report['jar_mtime']}`", ""]
    if report.get("preflight", {}).get("problems"):
        L += ["**Preflight problems:** " + "; ".join(report["preflight"]["problems"]), ""]
    for key, title, keys in (("java-m2", "ArcadeDB embedded, Graphalytics (median of runs, s)", M2_KEYS),
                             ("java-lsqb", "ArcadeDB embedded, LSQB (median of runs, s)", LSQB_KEYS)):
        if key in report:
            L += [f"## {title}", "", "| variant | " + " | ".join(keys) + " |",
                  "|---|" + "---|" * len(keys)]
            for v, d in report[key].items():
                val = d.get("validation") or {}
                names = {"PR": "pagerank", "WCC": "wcc", "BFS": "bfs", "LCC": "lcc", "SSSP": "sssp", "CDLP": "cdlp"}
                cells = []
                for k in keys:
                    cell = str(d["median"].get(k, "N/A"))
                    verdict = val.get(names.get(k, k.lower()))
                    if verdict and verdict.get("verdict") == "invalid":
                        cell += " **INVALID**"
                    elif verdict and verdict.get("verdict") == "equivalent":
                        cell += " (labels renamed)"
                    cells.append(cell)
                L.append(f"| {v} | " + " | ".join(cells) + " |")
            L.append("")
    for key, title, keys in (("py-m2", "Graphalytics, all vendors (s)", ["load", "pagerank", "wcc", "lcc", "bfs", "sssp", "cdlp"]),
                             ("py-lsqb", "LSQB, all vendors (s)", ["load"] + [f"q{i}" for i in range(1, 10)])):
        data = (report.get(key) or {}).get("results")
        if not data:
            if key in report:
                L += [f"## {title}", "", f"No results ({report[key]['outcome']})", ""]
            continue
        L += [f"## {title}", "", "| vendor | status | " + " | ".join(keys) + " |",
              "|---|---|" + "---|" * len(keys)]
        for k, v in data["vendors"].items():
            r = v.get("result") or {}
            cells = []
            verdicts = r.get("_validation") or {}
            for m in keys:
                x = r.get(m, "N/A")
                cell = _mark(x, verdicts.get(m))
                if (isinstance(x, (int, float)) and m not in verdicts and m != "load"
                        and r.get("_validation") is not None and m in bench_common.ALGORITHM_METRICS):
                    cell += " (unchecked)"
                cells.append(cell)
            L.append(f"| {v['name']} | {v['status']} | " + " | ".join(cells) + " |")
        L.append("")
    if "mode1" in report and report["mode1"].get("summary"):
        L += ["## Mode 1 (official framework), processing time (s)", ""]
        for mode, d in report["mode1"]["summary"].items():
            L.append(f"- {mode}: " + ", ".join(f"{a} {v['processing_time']:.2f}" for a, v in sorted(d["algorithms"].items())))
        L.append("")
    L += ["Cells marked **INVALID** failed the correctness check (LDBC reference outputs for Graphalytics, official "
          "expected counts for LSQB); \"unchecked\" means no output was available to check; \"labels renamed\" means "
          "identical communities under different label names.", ""]
    L += ["## Step timings", ""] + [f"- {k}: {v:.0f}s" for k, v in report["step_seconds"].items()]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", help=f"comma separated subset of: {', '.join(STEPS)}")
    ap.add_argument("--skip", default="", help="comma separated steps to skip")
    ap.add_argument("--java-home", help="Temurin 25 home (default: found automatically); other JDKs are refused")
    ap.add_argument("--jvm-flags", default=bench_java.JVM_FLAGS, help="JVM flags for the ArcadeDB runs (default: compact object headers)")
    ap.add_argument("--reps", type=int, default=3, help="repetitions of the ArcadeDB Java benchmarks (default 3)")
    ap.add_argument("--reset", action="store_true", help="reload every database from scratch")
    ap.add_argument("--mode1-dist", help="extracted Graphalytics distribution; enables the mode1 step")
    ap.add_argument("--out-dir", help="results directory (default weekly-results/<timestamp>)")
    ap.add_argument("--vendor-timeout", type=int, default=3600)
    ap.add_argument("--idle-timeout", type=int, default=600)
    ap.add_argument("--suite-timeout", type=int, default=8 * 3600, help="total seconds for one multi-vendor suite")
    ap.add_argument("--suite-idle", type=int, default=1800, help="orchestrator silence limit (vendors have their own)")
    ap.add_argument("--java-timeout", type=int, default=1800)
    ap.add_argument("--java-idle", type=int, default=900)
    ap.add_argument("--allow-battery", action="store_true",
                    help="run even on battery power (timings are throttled and not comparable)")
    ap.add_argument("--dry-run", action="store_true",
                    help="run preflight and compile, then print every command and limit of the plan without starting "
                         "any benchmark (the report is still written)")
    ap.add_argument("--min-docker-gb", type=int, default=24)
    ap.add_argument("--min-disk-gb", type=int, default=30)
    args = ap.parse_args()
    args.java_home = bench_java.require_temurin25(args.java_home)
    os.environ.update({k: v for k, v in bench_java.env_for(args.java_home).items()
                       if k in ("JAVA_HOME", "PATH", "ARCADEDB_JVM_FLAGS")})
    if args.jvm_flags != bench_java.JVM_FLAGS:
        os.environ["ARCADEDB_JVM_FLAGS"] = args.jvm_flags

    steps = [s for s in (args.only.split(",") if args.only else STEPS) if s not in args.skip.split(",")]
    if "mode1" in steps and not args.mode1_dist:
        steps.remove("mode1")
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.abspath(args.out_dir or os.path.join(ROOT, "weekly-results", stamp))
    os.makedirs(out_dir, exist_ok=True)
    java = subprocess.run([java_bin(args), "-version"], capture_output=True, text=True).stderr.splitlines()
    report = {"started": bench_state.now_iso(), "java": java[0] if java else "?", "jvm_flags": args.jvm_flags,
              "jar_mtime": time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(JAR))) if os.path.exists(JAR) else "missing",
              "step_seconds": {}}
    log(f"Weekly run {stamp}: steps {', '.join(steps)}; results in {out_dir}")

    if args.dry_run:

        def plan_only(cmd, **kw):
            log(f"  [dry-run] would run: {' '.join(str(c) for c in cmd)}")
            log(f"  [dry-run]   cwd={kw.get('cwd') or ROOT} total_timeout={kw.get('total_timeout')}s "
                f"idle_timeout={kw.get('idle_timeout')}s log={kw.get('log_file')}")
            return bench_isolation.Outcome()

        bench_isolation.run_child = plan_only
        report["dry_run"] = True

    dispatch = {
        "java-m2": lambda: step_java(args, report, out_dir, "m2"),
        "java-lsqb": lambda: step_java(args, report, out_dir, "lsqb"),
        "py-m2": lambda: step_python(args, report, out_dir, "m2"),
        "py-lsqb": lambda: step_python(args, report, out_dir, "lsqb"),
        "mode1": lambda: step_mode1(args, report, out_dir),
    }
    for step in steps:
        t0 = time.monotonic()
        log(f"\n=========== {step} ===========")
        try:
            if step == "preflight":
                if not preflight(args, report):
                    log("Preflight failed; fix the problems above (nothing was run).")
                    break
            else:
                dispatch[step]()
        except Exception as e:  # noqa: BLE001 - one broken step must not stop the weekend
            log(f"  step {step} failed: {type(e).__name__}: {e}")
            report.setdefault("errors", {})[step] = f"{type(e).__name__}: {e}"
        report["step_seconds"][step] = time.monotonic() - t0
        bench_isolation.atomic_write_json(os.path.join(out_dir, "weekly.json"), report)

    report["finished"] = bench_state.now_iso()
    bench_isolation.atomic_write_json(os.path.join(out_dir, "weekly.json"), report)
    with open(os.path.join(out_dir, "weekly.md"), "w") as f:
        f.write(md_report(report))
    log(f"\nDone. Report: {os.path.join(out_dir, 'weekly.md')}")


if __name__ == "__main__":
    main()
