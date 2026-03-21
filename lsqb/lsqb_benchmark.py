#!/usr/bin/env python3
"""
LSQB (Labelled Subgraph Query Benchmark):
  Kuzu vs DuckDB vs Neo4j vs ArcadeDB vs Memgraph vs PostgreSQL vs SurrealDB vs Dgraph

Dataset: LDBC SNB social-network-sf1 (or configurable via --sf)
Queries: 9 subgraph pattern matching queries (Q1-Q9)

Setup:
  # Download dataset (SF1, ~50MB)
  curl -L -o ../datasets/lsqb-sf1-projected.tar.zst \\
    https://datasets.ldbcouncil.org/lsqb/social-network-sf1-projected-fk.tar.zst
  curl -L -o ../datasets/lsqb-sf1-merged.tar.zst \\
    https://datasets.ldbcouncil.org/lsqb/social-network-sf1-merged-fk.tar.zst
  cd ../datasets && tar --use-compress-program=unzstd -xf lsqb-sf1-projected.tar.zst
  cd ../datasets && tar --use-compress-program=unzstd -xf lsqb-sf1-merged.tar.zst

Usage:
  python3 lsqb_benchmark.py                   # Run all systems
  python3 lsqb_benchmark.py --reset           # Delete all data and reload
  python3 lsqb_benchmark.py kuzu              # Run only Kuzu
  python3 lsqb_benchmark.py surrealdb         # Run only SurrealDB
  python3 lsqb_benchmark.py dgraph            # Run only Dgraph
  python3 lsqb_benchmark.py --sf 3 neo4j      # Use SF3 dataset, Neo4j only
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import bench_common

from systems import AVAILABLE_SYSTEMS, DEFAULT_EXCLUDE
from systems._common import LSQB_METRICS
import systems._common as _common


if __name__ == "__main__":
    import argparse as _ap

    # Custom arg parsing to handle --sf before delegating to bench_common
    parser = _ap.ArgumentParser(description="LSQB multi-vendor benchmark")
    parser.add_argument("--sf", default="1",
                        help="LDBC SNB scale factor (default: 1)")
    parser.add_argument("--reset", action="store_true",
                        help="Delete all data and reload from scratch")
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). "
                             f"Choices: {', '.join(AVAILABLE_SYSTEMS.keys())}")
    args = parser.parse_args()

    _common.SF = args.sf
    bench_common.RESET = args.reset

    if args.systems:
        systems_to_run = args.systems
    else:
        systems_to_run = [k for k in AVAILABLE_SYSTEMS if k not in DEFAULT_EXCLUDE]

    all_results = {}
    for key in systems_to_run:
        key = key.lower()
        if key not in AVAILABLE_SYSTEMS:
            print(f"Unknown system: {key}. "
                  f"Available: {', '.join(AVAILABLE_SYSTEMS.keys())}")
            continue
        name, func = AVAILABLE_SYSTEMS[key]
        try:
            r = func()
            if isinstance(r, dict) and "error" not in r:
                all_results[name] = r
        except Exception as e:
            print(f"\n{name} failed: {e}")
            import traceback; traceback.print_exc()

    if all_results:
        bench_common.print_summary(
            f"LSQB SF{_common.SF} (subgraph pattern matching)",
            LSQB_METRICS, all_results)
