#!/usr/bin/env python3
"""
LDBC Graphalytics Benchmark: ArcadeDB (Docker) vs Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs ArangoDB vs FalkorDB vs HugeGraph vs SurrealDB vs Dgraph
Dataset: datagen-7_5-fb (633K vertices, 34M edges, undirected, weighted)
Algorithms: PageRank, WCC, BFS, LCC, SSSP, CDLP

Usage:
  python3 benchmark.py                     # Run all, skip loading if data exists
  python3 benchmark.py --reset             # Delete all data and reload from scratch
  python3 benchmark.py arcadedb            # Run only ArcadeDB (Docker)
  python3 benchmark.py kuzu duckpgq        # Run only specific systems
  python3 benchmark.py falkordb             # Run only FalkorDB
  python3 benchmark.py hugegraph            # Run only HugeGraph
  python3 benchmark.py surrealdb            # Run only SurrealDB
  python3 benchmark.py dgraph               # Run only Dgraph
  python3 benchmark.py --reset memgraph    # Reset and run only Memgraph
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import bench_common
from systems import AVAILABLE_SYSTEMS, GRAPHALYTICS_METRICS, DEFAULT_EXCLUDE

if __name__ == "__main__":
    bench_common.run_benchmarks(
        description="LDBC Graphalytics multi-vendor benchmark",
        available_systems=AVAILABLE_SYSTEMS,
        summary_title="datagen-7_5-fb (633K vertices, 34M edges)",
        metrics=GRAPHALYTICS_METRICS,
        default_exclude=DEFAULT_EXCLUDE,
    )
