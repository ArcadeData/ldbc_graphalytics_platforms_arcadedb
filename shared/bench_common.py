#!/usr/bin/env python3
"""
Shared infrastructure for LDBC benchmarks (Graphalytics and LSQB).
"""

import time
import os
import argparse
import signal
import subprocess

GRAPHS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'datasets')

# Global flag set by --reset
RESET = False

# 5-minute timeout per algorithm/query
QUERY_TIMEOUT = 300


class QueryTimeout(Exception):
    pass


def _alarm_handler(signum, frame):
    raise QueryTimeout("timeout")


def run_timed(name, func, timeout=QUERY_TIMEOUT):
    """Run func() with a wall-clock timeout. Returns elapsed seconds or 'timeout'."""
    print(f"  Running {name}...")
    old = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(timeout)
    start = time.perf_counter()
    try:
        result = func()
        elapsed = time.perf_counter() - start
        signal.alarm(0)
        return elapsed, result
    except QueryTimeout:
        print(f"  {name}: TIMEOUT ({timeout}s)")
        return "timeout", None
    except Exception as e:
        signal.alarm(0)
        print(f"  {name} failed: {e}")
        return "N/A", None
    finally:
        signal.signal(signal.SIGALRM, old)
        signal.alarm(0)


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
            row += fmt(val)
        print(row)
    print()


def run_benchmarks(description, available_systems, summary_title, metrics,
                    default_exclude=None):
    """Parse CLI args and execute the benchmark loop.

    Args:
        description: argparse description string.
        available_systems: {key: (display_name, callable)}.
        summary_title: Passed to print_summary.
        metrics: Ordered list of metric keys for the summary table.
        default_exclude: Optional set of system keys excluded from default runs.
                         These systems are still available when explicitly named.
    """
    global RESET

    default_exclude = default_exclude or set()

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--reset", action="store_true",
                        help="Delete all data and reload from scratch")
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). "
                             f"Choices: {', '.join(available_systems.keys())}")
    args = parser.parse_args()

    RESET = args.reset

    if args.systems:
        systems_to_run = args.systems
    else:
        systems_to_run = [k for k in available_systems if k not in default_exclude]

    all_results = {}
    for key in systems_to_run:
        key = key.lower()
        if key not in available_systems:
            print(f"Unknown system: {key}. "
                  f"Available: {', '.join(available_systems.keys())}")
            continue
        name, func = available_systems[key]
        try:
            r = func()
            if isinstance(r, dict) and "error" not in r:
                all_results[name] = r
        except Exception as e:
            print(f"\n{name} failed: {e}")
            import traceback; traceback.print_exc()
        finally:
            # Cleanup: get cleanup info from the system module
            cleanup = getattr(func, '_cleanup', None)
            if cleanup:
                try:
                    cleanup()
                except Exception:
                    pass

    if all_results:
        print_summary(summary_title, metrics, all_results)


def cleanup_docker(*container_names):
    """Kill and remove Docker containers."""
    for name in container_names:
        subprocess.run(["docker", "rm", "-f", name],
                       capture_output=True, timeout=30)
    print(f"  Cleanup: removed Docker containers {', '.join(container_names)}")
