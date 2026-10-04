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

Every vendor runs in its own process group with hard limits: --vendor-timeout
(total, default 3600 s), --idle-timeout (no output, default 600 s). A vendor
that exceeds a limit gets SIGTERM, then SIGKILL after --kill-grace seconds, its
containers are removed, and the suite continues with the next vendor. Loaded
data is kept between runs (see shared/bench_state.py); --reset wipes it.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import bench_common
from systems import AVAILABLE_SYSTEMS, GRAPHALYTICS_METRICS, DEFAULT_EXCLUDE
from systems._common import VERTEX_FILE, EDGE_FILE

if __name__ == "__main__":
    bench_common.run_benchmarks(
        description="LDBC Graphalytics multi-vendor benchmark",
        available_systems=AVAILABLE_SYSTEMS,
        summary_title=lambda: f"{bench_common.GRAPHALYTICS_DATASET}",
        metrics=GRAPHALYTICS_METRICS,
        default_exclude=DEFAULT_EXCLUDE,
        suite=bench_common.graphalytics_suite(),
        dataset_paths=lambda: [VERTEX_FILE, EDGE_FILE],
        hugegraph_datasets=os.path.abspath(bench_common.GRAPHS_DIR),
    )
