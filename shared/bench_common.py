#!/usr/bin/env python3
"""
Shared infrastructure for LDBC benchmarks (Graphalytics and LSQB).

Each vendor runs in its own child process group (see bench_isolation.py) with a
total-time limit and a no-output limit; a hung vendor is killed (SIGTERM, then
SIGKILL) and the suite continues. Docker vendors keep their data between runs
(see bench_containers.py), so the graph is loaded once and reused.
"""

import argparse
import os
import signal
import subprocess
import sys
import time

import bench_containers
import bench_isolation
import bench_state

GRAPHS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'datasets')

# Global flag set by --reset
RESET = False

# Graphalytics dataset (datasets/<name>/<name>.v|.e). Non-default datasets get their own persisted
# data directories and load markers so that data of one dataset is never reused for another.
DEFAULT_DATASET = "datagen-7_5-fb"
GRAPHALYTICS_DATASET = os.environ.get("GRAPHALYTICS_DATASET", DEFAULT_DATASET)


def graphalytics_suite():
    if GRAPHALYTICS_DATASET == DEFAULT_DATASET:
        return "graphalytics"
    return f"graphalytics-{GRAPHALYTICS_DATASET}"

# 5-minute timeout per algorithm/query (in-process first line of defence)
QUERY_TIMEOUT = 300

# Hard limits enforced by the parent process (defaults, overridable by flags)
VENDOR_TOTAL_TIMEOUT = 3600   # 1 h for setup + load + all operations of one vendor
VENDOR_IDLE_TIMEOUT = 600     # 10 min without any output = hung (2x QUERY_TIMEOUT)
KILL_GRACE = 10               # seconds between SIGTERM and SIGKILL


class QueryTimeout(Exception):
    pass


def _alarm_handler(signum, frame):
    raise QueryTimeout("timeout")


# ------------------------------------------------------------ partial results

def _metric_key(name):
    return str(name).strip().lower().replace(" ", "")


def _flush_partial(partial):
    path = os.environ.get(bench_isolation.PARTIAL_ENV)
    if path:
        try:
            bench_isolation.atomic_write_json(path, partial)
        except Exception:
            pass


_PARTIAL = {"done": {}, "inflight": None}


def run_timed(name, func, timeout=QUERY_TIMEOUT):
    """Run func() with a wall-clock timeout. Returns elapsed seconds or 'timeout'.

    Every operation is also recorded in the partial-results file (when the
    orchestrator set one), so that a vendor killed later still reports the
    operations that completed, and the operation in flight is known.
    """
    key = _metric_key(name)
    print(f"  Running {name}...")
    _PARTIAL["inflight"] = key
    _flush_partial(_PARTIAL)
    old = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(timeout)
    start = time.perf_counter()
    try:
        result = func()
        elapsed = time.perf_counter() - start
        signal.alarm(0)
        _PARTIAL["done"][key] = elapsed
        return elapsed, result
    except QueryTimeout:
        print(f"  {name}: TIMEOUT ({timeout}s)")
        _PARTIAL["done"][key] = "timeout"
        return "timeout", None
    except Exception as e:
        signal.alarm(0)
        print(f"  {name} failed: {e}")
        _PARTIAL["done"][key] = "N/A"
        return "N/A", None
    finally:
        signal.signal(signal.SIGALRM, old)
        signal.alarm(0)
        _PARTIAL["inflight"] = None
        _flush_partial(_PARTIAL)


# ---- per-vertex output dumps (validation against the LDBC reference outputs) ----
# GRAPHALYTICS_DUMP_DIR=<dir> makes the Graphalytics drivers write, after their timed runs, one file
# <dir>/<vendor>-<ALGO>.out per algorithm with one "<vertex id> <value>" line for every vertex
# (scripts/validate_outputs.py compares them with the official reference outputs).
BFS_UNREACHABLE = 9223372036854775807
ALGORITHM_METRICS = {"pagerank", "wcc", "lcc", "bfs", "sssp", "cdlp",
                     "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9"}


def dump_dir():
    return os.environ.get("GRAPHALYTICS_DUMP_DIR")


def dump_enabled():
    return bool(dump_dir())


def dump_only():
    """GRAPHALYTICS_DUMP_ONLY=1: export the outputs right after the load and skip the timed runs
    (validation runs; avoids waiting for, or being killed during, slow timed algorithms)."""
    return dump_enabled() and os.environ.get("GRAPHALYTICS_DUMP_ONLY") == "1"


def dump_rows(vendor, algo, rows):
    """rows: iterable of (vertex id, value) pairs."""
    os.makedirs(dump_dir(), exist_ok=True)
    path = os.path.join(dump_dir(), f"{vendor}-{algo.upper()}.out")
    with open(path + ".tmp", "w") as f:
        for vid, val in rows:
            f.write(f"{vid} {val!r}\n" if isinstance(val, float) else f"{vid} {val}\n")
    os.replace(path + ".tmp", path)
    print(f"  [dump] {algo.upper()} -> {path}")


def dump_bfs(vendor, reached, all_ids, source):
    """BFS output: distance for reached vertices, the unreachable sentinel for the rest."""
    reached = dict(reached)
    reached[source] = 0
    dump_rows(vendor, "BFS", ((v, reached.get(v, BFS_UNREACHABLE)) for v in all_ids))


def dump_safely(vendor, algo, fn):
    """Run an export; a failing export must never break the benchmark."""
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        print(f"  [dump] {vendor} {algo}: export failed: {str(e)[:200]}")


def measure_repeated(fn, slow=30.0):
    """Time fn() with the optional warm-up/repetition protocol; returns (seconds, fn's result).

    LSQB_WARMUP (default 0) untimed runs followed by LSQB_REPS (default 1) timed runs, median
    reported. The defaults keep the historical single cold run. JVM engines (ArcadeDB Docker,
    Neo4j) need warm-up to be measured the way a production server runs; the weekly driver sets
    LSQB_WARMUP=1 LSQB_REPS=3 for them. A query whose first run exceeds `slow` seconds is
    reported from that single run (warm-up and repetitions would only multiply the time).
    """
    import statistics
    warmup = int(os.environ.get("LSQB_WARMUP", "0"))
    reps = max(1, int(os.environ.get("LSQB_REPS", "1")))
    t0 = time.perf_counter()
    out = fn()
    first = time.perf_counter() - t0
    if (warmup == 0 and reps == 1) or first > slow:
        return first, out
    times = [] if warmup > 0 else [first]
    for _ in range(max(0, warmup - 1)):
        fn()
    while len(times) < reps:
        t0 = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), out


def embedded_db_path(suite, key, name):
    """Persistent location of an embedded database (survives reboots, wiped by --reset)."""
    if suite == "graphalytics":
        suite = graphalytics_suite()
    d = bench_state.state_path("data", f"{suite}-{key}-embedded", create=True)
    return os.path.join(d, name)


def fmt(val):
    if isinstance(val, (int, float)):
        return f"{val:>14.2f}s"
    return f"{str(val):>15}"


def print_summary(title, metrics, all_results):
    """Print a summary table.

    Args:
        title: Banner text (e.g. dataset description).
        metrics: Ordered list of metric keys (e.g. ["load", "pagerank", ...]).
        all_results: {system_name: {metric: value_or_N/A}}.
    """
    print("\n" + "=" * 70)
    print(f"BENCHMARK SUMMARY  -  {title}")
    print("=" * 70)

    systems = list(all_results.keys())

    header = f"{'Metric':<15}"
    for sys_name in systems:
        header += f"{sys_name:>15}"
    print(header)
    print("-" * len(header))

    for m in metrics:
        row = f"{m.upper():<15}"
        for sys_name in systems:
            val = all_results[sys_name].get(m, "N/A")
            verdict = (all_results[sys_name].get("_validation") or {}).get(m, {}).get("verdict")
            if verdict == "invalid" and isinstance(val, (int, float)):
                val = f"{val:.2f}s INVALID"
            elif (verdict is None and m in ALGORITHM_METRICS and isinstance(val, (int, float))
                  and all_results[sys_name].get("_validation") is not None):
                val = f"{val:.2f}s unchecked"
            row += fmt(val)
        print(row)
    print()
    for sys_name in systems:
        verdicts = all_results[sys_name].get("_validation")
        if verdicts:
            valid = [m for m, v in verdicts.items() if v["verdict"] == "valid"]
            equivalent = [m for m, v in verdicts.items() if v["verdict"] == "equivalent"]
            invalid = [f"{m} ({v['pct']}% match)" for m, v in verdicts.items() if v["verdict"] == "invalid"]
            print(f"  Correctness {sys_name}: valid: {', '.join(valid) or 'none'}; "
                  f"same result, other label names: {', '.join(equivalent) or 'none'}; "
                  f"INVALID: {', '.join(invalid) or 'none'}")
        cached = all_results[sys_name].get("_load_cached_from")
        if cached:
            print(f"  Note: {sys_name} LOAD is the time of the original load ({cached}); "
                  f"the data was reused in this run.")
        if all_results[sys_name].get("_on_battery"):
            print(f"  Note: {sys_name} was measured on battery power: timings are NOT reliable, rerun on AC.")
        status = all_results[sys_name].get("_status")
        if status and status != "ok":
            print(f"  Note: {sys_name}: {status}")


# ------------------------------------------------------------ orchestration

def merge_partial(result, partial, metrics, outcome):
    """Build the result dict of a vendor that was killed from its partial data."""
    merged = {}
    done = (partial or {}).get("done", {})
    inflight = (partial or {}).get("inflight")
    for m in metrics:
        if m in done:
            merged[m] = done[m]
    if result and "error" not in result:
        for k, v in result.items():
            merged.setdefault(k, v)
    if inflight and inflight not in merged:
        merged[inflight] = "timeout"
    elif not done and "load" not in merged:
        merged["load"] = "timeout"  # killed before the first timed operation: the load hung
    for m in metrics:
        merged.setdefault(m, "N/A")
    return merged


def _apply_load_cache(suite, key, version_tag, dataset_sig, res):
    """Record the load time after a fresh load; fill it in when data was reused."""
    load = res.get("load")
    marker = bench_state.read_marker(suite, key)
    valid = (marker and marker.get("dataset") == dataset_sig
             and marker.get("version") == version_tag)
    if isinstance(load, (int, float)) and load > 0.05:
        bench_state.write_marker(suite, key, {
            "load_time": load, "dataset": dataset_sig, "version": version_tag,
            "loaded_at": bench_state.now_iso()})
    elif valid and marker.get("load_time") and (load in (None, "N/A", 0, 0.0) or isinstance(load, (int, float)) and load <= 0.05):
        res["load"] = marker["load_time"]
        res["_load_cached_from"] = marker.get("loaded_at", "earlier run")


def _version_tag(suite, key, spec):
    if spec is not None:
        try:
            return f"{spec.image}@{bench_containers.image_id(spec.image)}"
        except Exception:
            return spec.image
    return "embedded"


# Official LSQB expected counts for SF1 (https://github.com/ldbc/lsqb expected-output)
LSQB_EXPECTED_SF1 = {"q1": 221636419, "q2": 1085627, "q3": 753570, "q4": 14836038, "q5": 13824510,
                     "q6": 1668134320, "q7": 26190133, "q8": 6907213, "q9": 1596153418}


def validate_lsqb_counts(log_text, scale_factor="1"):
    """Verdict per query from the "Qn time: Xs (count=N)" lines of a vendor's log; None if no reference."""
    import re
    if str(scale_factor) != "1":
        return None
    verdicts = {}
    for m in re.finditer(r"Q(\d) time: [\d.]+s\s+\(count=(\d+)\)", log_text):
        q = f"q{m.group(1)}"
        ok = int(m.group(2)) == LSQB_EXPECTED_SF1[q]
        verdicts[q] = {"verdict": "valid" if ok else "invalid", "algo": q.upper(),
                       "pct": 100.0 if ok else 0.0, "count": int(m.group(2)),
                       "expected": LSQB_EXPECTED_SF1[q]}
    return verdicts


def _validate_dumps(dump_directory, vendor):
    """Verdict per metric of the outputs a vendor dumped, against the official LDBC reference outputs."""
    import importlib.util
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts", "validate_outputs.py")
    spec = importlib.util.spec_from_file_location("validate_outputs", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    verdicts = mod.validate_vendor(dump_directory, vendor, GRAPHALYTICS_DATASET,
                                   os.path.abspath(GRAPHS_DIR))
    print(f"  Correctness check of {vendor} against the LDBC reference outputs:")
    for metric, v in verdicts.items():
        print(f"    {metric:9} {v['verdict'].upper():8} {v['pct']:6.2f}% match ({v['algo']})")
    return verdicts


def run_vendor_isolated(suite, key, name, script, passthrough, metrics, dataset_sig,
                        total_timeout, idle_timeout, kill_grace, reset, run_dir,
                        cleanup=None, hugegraph_datasets=None):
    """Run one vendor in a child process group. Returns (result_dict | None, status)."""
    spec = bench_containers.spec_for(suite, key)
    version_tag = _version_tag(suite, key, spec) if spec is not None else "embedded"
    if reset:
        bench_state.reset_vendor_data(suite, key)
    result_file = os.path.join(run_dir, f"{suite}-{key}.result.json")
    partial_file = os.path.join(run_dir, f"{suite}-{key}.partial.json")
    log_file = os.path.join(run_dir, f"{suite}-{key}.log")
    for f in (result_file, partial_file):
        try:
            os.remove(f)
        except OSError:
            pass
    env = dict(os.environ)
    env[bench_isolation.RESULT_ENV] = result_file
    env[bench_isolation.PARTIAL_ENV] = partial_file
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [sys.executable, "-u", script, *passthrough, "--child", key]
    validate_outputs = suite.startswith("graphalytics") and os.environ.get("BENCH_NO_VALIDATE") != "1"
    dump_directory = os.environ.get("GRAPHALYTICS_DUMP_DIR") or os.path.join(run_dir, "outputs")
    if validate_outputs:
        env["GRAPHALYTICS_DUMP_DIR"] = dump_directory

    status = "ok"
    result = None
    outcome = None
    on_battery = bench_state.power_source() == "battery"
    if on_battery:
        print(f"  WARNING: running on battery power; the CPU is throttled, so the timings of {name} are not reliable")
    prev = bench_state.read_marker(suite, key)
    had_good_load = bool(prev and prev.get("dataset") == dataset_sig
                         and prev.get("version") == version_tag and not reset)
    try:
        if spec is not None:
            bench_containers.ensure_running(suite, key, spec)
        if key == "hugegraph" and hugegraph_datasets:
            bench_containers.hugegraph_start(hugegraph_datasets)
        for attempt in (1, 2):
            outcome = bench_isolation.run_child(
                cmd, env=env, total_timeout=total_timeout, idle_timeout=idle_timeout,
                grace=kill_grace, result_file=result_file, log_file=log_file)
            result = bench_isolation.read_json(result_file)
            refused = (spec is not None and attempt == 1 and isinstance(result, dict)
                       and "connect" in str(result.get("error", "")).lower() and outcome.elapsed < 180)
            if not refused:
                break
            print(f"  {name}: container not ready yet ({result.get('error')}); waiting 45s and retrying once")
            time.sleep(45)
            try:
                os.remove(result_file)
            except OSError:
                pass
        partial = bench_isolation.read_json(partial_file, {})
        if outcome.reason in ("total", "idle"):
            status = (f"killed after {outcome.elapsed:.0f}s ({outcome.reason} limit, "
                      f"{outcome.kill_mode}); partial results kept")
            result = merge_partial(result, partial, metrics, outcome)
        elif result is None:
            status = f"child exited with code {outcome.returncode} without a result"
            if partial and partial.get("done"):
                result = merge_partial(None, partial, metrics, outcome)
        elif "error" in result:
            status = f"failed: {result['error']}"
            if partial and partial.get("done"):
                result = merge_partial(None, partial, metrics, outcome)
            else:
                result = None
    except Exception as e:  # noqa: BLE001
        status = f"orchestration error: {type(e).__name__}: {e}"
        print(f"\n{name}: {status}")
    finally:
        try:
            if key == "hugegraph" and hugegraph_datasets:
                bench_containers.hugegraph_stop()
            if spec is not None:
                bench_containers.stop(spec)
        except Exception as e:  # noqa: BLE001
            print(f"  Cleanup warning for {name}: {e}")
        if cleanup:
            try:
                cleanup()
            except Exception:
                pass
    # A run that ended badly before any load ever completed may have left half
    # loaded data behind; wipe it so the next run reloads instead of trusting it.
    load_done = isinstance(result, dict) and isinstance(result.get("load"), (int, float))
    if status != "ok" and not (had_good_load or load_done):
        bench_state.reset_vendor_data(suite, key)
        print(f"  Persisted data of {name} wiped (the run failed before the load completed)")
    if result is not None and "error" not in result and (on_battery or bench_state.power_source() == "battery"):
        result["_on_battery"] = True
    if validate_outputs and result is not None and "error" not in result:
        try:
            result["_validation"] = _validate_dumps(dump_directory, key)
        except Exception as e:  # noqa: BLE001
            print(f"  Validation of {name} could not run: {e}")
    if suite == "lsqb" and result is not None and "error" not in result and os.path.exists(log_file):
        sf = passthrough[passthrough.index("--sf") + 1] if "--sf" in passthrough else "1"
        verdicts = validate_lsqb_counts(open(log_file, errors="replace").read(), sf)
        if verdicts is not None:
            result["_validation"] = verdicts
            bad = [q for q, v in verdicts.items() if v["verdict"] == "invalid"]
            print(f"  Correctness check of {name}: {len(verdicts) - len(bad)} of {len(verdicts)} query counts match "
                  f"the official expected counts" + (f"; WRONG: {', '.join(bad)}" if bad else ""))
    if result is not None and "error" not in result:
        _apply_load_cache(suite, key, version_tag, dataset_sig, result)
        result["_status"] = status
        result["_version"] = version_tag
        result["_elapsed"] = round(outcome.elapsed, 1) if outcome else None
    return result, status


def run_benchmarks(description, available_systems, summary_title, metrics,
                   default_exclude=None, suite="graphalytics", dataset_paths=None,
                   extra_args=None, post_args=None, hugegraph_datasets=None):
    """Parse CLI args and execute the benchmark loop.

    Args:
        description: argparse description string.
        available_systems: {key: (display_name, callable)}.
        summary_title: String (or callable returning one) for print_summary.
        metrics: Ordered list of metric keys for the summary table.
        default_exclude: Optional set of system keys excluded from default runs.
                         These systems are still available when explicitly named.
        suite: "graphalytics" or "lsqb" (names state directories and markers).
        dataset_paths: Optional callable returning the dataset files/dirs used
                       to fingerprint the loaded data.
        extra_args: Optional callable(parser) adding suite specific options.
        post_args: Optional callable(args) applying them (also runs in the child).
        hugegraph_datasets: Directory mounted into the Vermeer worker.
    """
    global RESET

    try:
        sys.stdout.reconfigure(line_buffering=True)  # keep parent and child output in order
    except Exception:
        pass
    default_exclude = default_exclude or set()

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--reset", action="store_true",
                        help="Delete the selected vendors' persisted data and reload from scratch")
    parser.add_argument("--no-isolate", action="store_true",
                        help="Run vendors in this process (debugging; no hard timeouts)")
    parser.add_argument("--vendor-timeout", type=int, default=VENDOR_TOTAL_TIMEOUT,
                        help=f"Total seconds allowed per vendor (default {VENDOR_TOTAL_TIMEOUT})")
    parser.add_argument("--idle-timeout", type=int, default=VENDOR_IDLE_TIMEOUT,
                        help=f"Seconds without output before a vendor is killed (default {VENDOR_IDLE_TIMEOUT})")
    parser.add_argument("--kill-grace", type=int, default=KILL_GRACE,
                        help=f"Seconds between SIGTERM and SIGKILL (default {KILL_GRACE})")
    parser.add_argument("--out", help="Write all results as JSON to this file")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). "
                             f"Choices: {', '.join(available_systems.keys())}")
    if extra_args:
        extra_args(parser)
    args = parser.parse_args()

    RESET = args.reset
    if post_args:
        post_args(args)

    # ---- child mode: run exactly one vendor and write its result file
    if args.child:
        key = args.child.lower()
        name, func = available_systems[key]
        bench_isolation.child_main(func)
        return

    if args.systems:
        systems_to_run = [s.lower() for s in args.systems]
    else:
        systems_to_run = [k for k in available_systems if k not in default_exclude]

    dataset_sig = bench_state.dataset_signature(dataset_paths() if dataset_paths else [])
    run_dir = bench_state.state_path("runs", f"{suite}-{time.strftime('%Y%m%d-%H%M%S')}", create=True)
    script = os.path.abspath(sys.argv[0])
    passthrough = [a for a in sys.argv[1:] if a not in systems_to_run and a != "--child"]
    # Drop options consumed here; keep suite options such as --sf and --reset for the child.
    skip_next = False
    cleaned = []
    for a in passthrough:
        if skip_next:
            skip_next = False
            continue
        if a in ("--vendor-timeout", "--idle-timeout", "--kill-grace", "--out"):
            skip_next = True
            continue
        if a.startswith(("--vendor-timeout=", "--idle-timeout=", "--kill-grace=", "--out=")):
            continue
        if a in ("--no-isolate",):
            continue
        cleaned.append(a)
    passthrough = cleaned

    all_results = {}
    report = {"suite": suite, "started": bench_state.now_iso(), "dataset": dataset_sig,
              "power_source": bench_state.power_source(), "vendors": {}}
    for key in systems_to_run:
        if key not in available_systems:
            print(f"Unknown system: {key}. "
                  f"Available: {', '.join(available_systems.keys())}")
            continue
        name, func = available_systems[key]
        cleanup = getattr(func, '_cleanup', None)
        if args.no_isolate:
            r, status = None, "ok"
            try:
                r = func()
            except Exception as e:  # noqa: BLE001
                status = f"failed: {e}"
                print(f"\n{name} failed: {e}")
                import traceback
                traceback.print_exc()
            finally:
                if cleanup:
                    try:
                        cleanup()
                    except Exception:
                        pass
        else:
            print(f"\n######## {name} ({suite}) ########")
            r, status = run_vendor_isolated(
                suite, key, name, script, passthrough, metrics, dataset_sig,
                args.vendor_timeout, args.idle_timeout, args.kill_grace, args.reset,
                run_dir, cleanup=cleanup, hugegraph_datasets=hugegraph_datasets)
        report["vendors"][key] = {"name": name, "status": status, "result": r}
        if isinstance(r, dict) and "error" not in r:
            all_results[name] = r

    if all_results:
        print_summary(summary_title() if callable(summary_title) else summary_title,
                      metrics, all_results)
    failures = {k: v["status"] for k, v in report["vendors"].items() if v["status"] != "ok"}
    if failures:
        print("Vendor issues:")
        for k, s in failures.items():
            print(f"  {k}: {s}")
    report["finished"] = bench_state.now_iso()
    out = args.out or os.path.join(run_dir, "results.json")
    try:
        bench_isolation.atomic_write_json(out, report)
        print(f"Results written to {out}")
    except Exception as e:  # noqa: BLE001
        print(f"Could not write results file: {e}")


def cleanup_docker(*container_names):
    """Kill and remove Docker containers."""
    for name in container_names:
        subprocess.run(["docker", "rm", "-f", name],
                       capture_output=True, timeout=30)
    print(f"  Cleanup: removed Docker containers {', '.join(container_names)}")
