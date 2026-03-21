"""Shared constants and imports for all LDBC Graphalytics system benchmarks."""

import time
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

import bench_common
from bench_common import fmt, GRAPHS_DIR

VERTEX_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb", "datagen-7_5-fb.v")
EDGE_FILE = os.path.join(GRAPHS_DIR, "datagen-7_5-fb", "datagen-7_5-fb.e")
SOURCE_VERTEX = 6
PR_DAMPING = 0.85
PR_ITERATIONS = 10
EXPECTED_VERTICES = 633432
EXPECTED_EDGES = 34185747
