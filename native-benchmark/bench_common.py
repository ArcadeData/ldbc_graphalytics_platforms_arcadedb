#!/usr/bin/env python3
"""
Shared infrastructure for LDBC benchmarks (Graphalytics and LSQB).
"""

import time
import os
import argparse

GRAPHS_DIR = "/Users/luca/graphs"

# Global flag set by --reset
RESET = False


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


def run_benchmarks(description, available_systems, summary_title, metrics):
    """Parse CLI args and execute the benchmark loop.

    Args:
        description: argparse description string.
        available_systems: {key: (display_name, callable)}.
        summary_title: Passed to print_summary.
        metrics: Ordered list of metric keys for the summary table.
    """
    global RESET

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--reset", action="store_true",
                        help="Delete all data and reload from scratch")
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). "
                             f"Choices: {', '.join(available_systems.keys())}")
    args = parser.parse_args()

    RESET = args.reset

    systems_to_run = args.systems if args.systems else list(available_systems.keys())

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

    if all_results:
        print_summary(summary_title, metrics, all_results)
