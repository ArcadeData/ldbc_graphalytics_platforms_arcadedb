"""Shared constants and imports for all LDBC Graphalytics system benchmarks."""

import time
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

import bench_common
from bench_common import fmt, GRAPHS_DIR

# Dataset under datasets/<name>/<name>.{v,e}; every system loads these files. Override with
# GRAPHALYTICS_DATASET (for example graph500-22-w, see README); the default is datagen-7_5-fb.
DATASET = bench_common.GRAPHALYTICS_DATASET
VERTEX_FILE = os.path.join(GRAPHS_DIR, DATASET, f"{DATASET}.v")
EDGE_FILE = os.path.join(GRAPHS_DIR, DATASET, f"{DATASET}.e")
SOURCE_VERTEX = 6
PR_DAMPING = 0.85
PR_ITERATIONS = 10
EXPECTED_VERTICES = 633432
EXPECTED_EDGES = 34185747
