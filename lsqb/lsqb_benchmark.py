#!/usr/bin/env python3
"""
LSQB (Labelled Subgraph Query Benchmark):
  Kuzu vs DuckDB vs Neo4j vs ArcadeDB vs Memgraph vs PostgreSQL vs FalkorDB vs SurrealDB vs Dgraph

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


def _add_args(parser):
    parser.add_argument("--sf", default="1",
                        help="LDBC SNB scale factor (default: 1)")


def _apply_args(args):
    _common.SF = args.sf


if __name__ == "__main__":
    bench_common.run_benchmarks(
        description="LSQB multi-vendor benchmark",
        available_systems=AVAILABLE_SYSTEMS,
        summary_title=lambda: f"LSQB SF{_common.SF} (subgraph pattern matching)",
        metrics=LSQB_METRICS,
        default_exclude=DEFAULT_EXCLUDE,
        suite="lsqb",
        dataset_paths=lambda: [_common.data_dir_projected(), _common.data_dir_merged()],
        extra_args=_add_args,
        post_args=_apply_args,
    )
