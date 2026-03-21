"""
LSQB benchmark system modules.

Each module exports a run_benchmark() function that runs the LSQB benchmark
for that specific DBMS.
"""

from . import kuzu
from . import duckdb
from . import neo4j
from . import arcadedb
from . import memgraph
from . import postgresql
from . import surrealdb
from . import dgraph
from . import _common

AVAILABLE_SYSTEMS = {
    "kuzu": ("Kuzu", kuzu.run_benchmark),
    "duckdb": ("DuckDB", duckdb.run_benchmark),
    "neo4j": ("Neo4j", neo4j.run_benchmark),
    "memgraph": ("Memgraph", memgraph.run_benchmark),
    "postgresql": ("PostgreSQL", postgresql.run_benchmark),
    "arcadedb": ("ArcadeDB-Server", arcadedb.run_benchmark),
    "surrealdb": ("SurrealDB", surrealdb.run_benchmark),
    "dgraph": ("Dgraph", dgraph.run_benchmark),
}

# Systems excluded from default runs (must be explicitly named to run).
# SurrealDB and Dgraph lack pattern matching / join semantics — all queries
# return N/A. Still available via: python3 lsqb_benchmark.py surrealdb dgraph
DEFAULT_EXCLUDE = {"surrealdb", "dgraph"}
