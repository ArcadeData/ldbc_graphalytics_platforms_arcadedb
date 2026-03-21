"""LDBC Graphalytics benchmark system modules.

Each sub-module exposes a run_benchmark() function for its respective DBMS.
"""

from .kuzu import run_benchmark as _kuzu
from .duckpgq import run_benchmark as _duckpgq
from .memgraph import run_benchmark as _memgraph
from .neo4j import run_benchmark as _neo4j
from .arangodb import run_benchmark as _arangodb
from .falkordb import run_benchmark as _falkordb
from .hugegraph import run_benchmark as _hugegraph
from .arcadedb import run_benchmark as _arcadedb
from .surrealdb import run_benchmark as _surrealdb
from .dgraph import run_benchmark as _dgraph

AVAILABLE_SYSTEMS = {
    "arcadedb": ("ArcadeDB-Docker", _arcadedb),
    "kuzu": ("Kuzu", _kuzu),
    "duckpgq": ("DuckPGQ", _duckpgq),
    "memgraph": ("Memgraph", _memgraph),
    "neo4j": ("Neo4j", _neo4j),
    "arangodb": ("ArangoDB", _arangodb),
    "falkordb": ("FalkorDB", _falkordb),
    "hugegraph": ("HugeGraph", _hugegraph),
    "surrealdb": ("SurrealDB", _surrealdb),
    "dgraph": ("Dgraph", _dgraph),
}

GRAPHALYTICS_METRICS = ["load", "pagerank", "wcc", "lcc", "bfs", "sssp", "cdlp"]

# Systems excluded from default runs (must be explicitly named to run).
# SurrealDB and Dgraph lack built-in graph algorithms — most metrics
# return N/A. Still available via: python3 benchmark.py surrealdb dgraph
DEFAULT_EXCLUDE = {"surrealdb", "dgraph"}
