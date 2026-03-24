# CLAUDE.md — Project Instructions

## Project Overview

LDBC Graphalytics benchmark platform for ArcadeDB, with multi-vendor comparison across graph databases.
Three benchmark modes exist, plus standalone embedded Java benchmarks.

## Build

```bash
mvn package -DskipTests
# or use init.sh to build + extract the Mode 1 distribution:
./init.sh ~/path/to/graphs
```

The fat JAR is at: `target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar`

## Datasets

```bash
python3 datasets.py                          # list downloaded datasets
python3 datasets.py available                # list all downloadable
python3 datasets.py download datagen-7_5-fb  # Graphalytics (633K V, 34M E)
python3 datasets.py download lsqb-sf1        # LSQB SF1 (3.9M V, 17.9M E)
```

Datasets go into `datasets/`.

## Benchmark Modes

### Mode 1 — Official LDBC Graphalytics Framework

Single-platform (ArcadeDB only). Reloads the graph per algorithm for isolated measurements.

```bash
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT
bash bin/sh/run-benchmark.sh
```

Config files are in `config/`:
- `benchmark.properties` — `graphs.root-directory`, memory settings
- `graphs.properties` — vertex/edge file paths (relative to `graphs.root-directory`)
- `benchmarks/custom.properties` — algorithms, dataset, timeout, repetitions
- `platform.properties` — `platform.olap = true` for GAV/CSR acceleration

**Important**: vertex/edge file paths in `graphs.properties` must include the subdirectory:
```
graph.datagen-7_5-fb.vertex-file = datagen-7_5-fb/datagen-7_5-fb.v
graph.datagen-7_5-fb.edge-file = datagen-7_5-fb/datagen-7_5-fb.e
```

Results go to `report/<timestamp>-ARCADEDB-report-CUSTOM/`.

### Mode 2 — Multi-Vendor Graphalytics Comparison (ldbc-native/)

Runs 6 LDBC Graphalytics algorithms (PageRank, WCC, BFS, LCC, SSSP, CDLP) across multiple vendors.

```bash
cd ldbc-native
python3 benchmark.py                    # all default vendors
python3 benchmark.py arcadedb kuzu      # specific vendors only
python3 benchmark.py --reset neo4j      # force reload + run
```

**Default vendors**: arcadedb, kuzu, duckpgq, memgraph, neo4j, arangodb, falkordb, hugegraph
**Excluded by default** (must name explicitly): surrealdb, dgraph

#### ArcadeDB Embedded (Mode 2)

Standalone Java benchmark — no Docker, no Python.

```bash
cd ldbc-native
LDBC_JAR=../target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
javac --add-modules jdk.incubator.vector -cp "$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java
java --add-modules jdk.incubator.vector -Xms12g -Xmx12g -cp ".:$LDBC_JAR" ArcadeDBEmbeddedBenchmark
```

### Mode 3 — LSQB Benchmark (lsqb/)

9 subgraph pattern matching queries (Q1-Q9) on LDBC SNB social network data.

```bash
cd lsqb
python3 lsqb_benchmark.py                    # all default vendors
python3 lsqb_benchmark.py kuzu duckdb        # specific vendors
python3 lsqb_benchmark.py --reset arcadedb   # force reload
```

**Default vendors**: kuzu, duckdb, neo4j, memgraph, postgresql, arcadedb
**Excluded by default**: surrealdb, dgraph

#### ArcadeDB Embedded (Mode 3)

```bash
cd lsqb
LDBC_JAR=../target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
javac -cp "$LDBC_JAR" ArcadeDBEmbeddedLSQB.java
java -Xms12g -Xmx12g --add-modules jdk.incubator.vector -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB
# Use --reset to force reload
```

## Running Benchmarks — Critical Rules

### JVM heap: 12GB for all Java-based systems

All JVM-based systems (ArcadeDB, Neo4j) MUST use `-Xms12g -Xmx12g` (or equivalent Docker env vars). This ensures fair comparison — same heap budget for all vendors.

- ArcadeDB Embedded: `java -Xms12g -Xmx12g ...`
- ArcadeDB Docker: `-e ARCADEDB_OPTS_MEMORY="-Xms12g -Xmx12g"`
- Neo4j Docker: `-e NEO4J_server_memory_heap_initial__size=12g -e NEO4J_server_memory_heap_max__size=12g`

### Timeout: 5 minutes max per operation

Every operation (load, algorithm, query) MUST timeout after 5 minutes (300s).
This is configured in `shared/bench_common.py` as `QUERY_TIMEOUT = 300`.
The `run_timed()` wrapper enforces this via SIGALRM.

**Loading phases must also use `run_timed()`** — wrap the entire load in a function
and call `bench_common.run_timed("load", _load_func)`. If load times out, skip
all algorithms and return immediately.

### One vendor at a time — no parallel containers

**NEVER start multiple Docker containers simultaneously.** Run vendors sequentially:

1. Start the vendor's container(s)
2. Wait for readiness
3. Run the benchmark
4. Clean up ALL containers and temp data
5. Only then proceed to the next vendor

This prevents resource contention and ensures fair measurements.

### Setup and teardown per vendor

Each vendor benchmark MUST follow this lifecycle:

```
1. SETUP    — Start Docker container(s) if needed, wait for readiness
2. RUN      — Execute the benchmark (load + algorithms, each with 5min timeout)
3. CLEANUP  — Remove ALL Docker containers, temp dirs, data volumes
```

**Cleanup checklist per vendor:**

| Vendor | Docker containers | Temp dirs |
|--------|-------------------|-----------|
| ArcadeDB (Docker) | `arcadedb` | `/tmp/arcadedb-docker-data`, `/tmp/arcadedb-docker-log` |
| ArcadeDB (Embedded) | none | `/tmp/arcadedb_benchmark` |
| Kuzu | none | `/tmp/kuzu_benchmark`, `/tmp/ldbc_vertices.csv`, `/tmp/ldbc_edges.csv` |
| DuckPGQ | none | `/tmp/duckpgq_benchmark.db` |
| Memgraph | `memgraph` | none |
| Neo4j | `neo4j-gds` | none |
| ArangoDB | `arangodb` | none |
| FalkorDB | `falkordb` | `/tmp/falkordb_benchmark` |
| HugeGraph | `vermeer-master`, `vermeer-worker` + network `hugegraph-net` | none |

### Docker setup commands per vendor

**Memgraph:**
```bash
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
```

**Neo4j** (self-starts in the benchmark script, but if manual):
```bash
docker run -d --name neo4j-gds -p 7688:7687 -p 7476:7474 \
  -e NEO4J_AUTH=neo4j/benchmark123 \
  -e 'NEO4J_PLUGINS=["graph-data-science"]' \
  -e NEO4J_server_memory_heap_initial__size=12g \
  -e NEO4J_server_memory_heap_max__size=12g \
  neo4j:2025-community
```

**ArangoDB:**
```bash
docker run -d --name arangodb -p 8529:8529 \
  -e ARANGO_ROOT_PASSWORD=benchmark arangodb/arangodb:3.11.12
```

**FalkorDB:**
```bash
docker run -d --name falkordb -p 6379:6379 \
  -v /tmp/falkordb_benchmark:/var/lib/falkordb/data falkordb/falkordb:latest
```

**HugeGraph (Vermeer):**
```bash
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net \
  -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net \
  -p 6788:6788 -p 6789:6789 \
  -v "$(cd datasets && pwd)":/data/graphs:ro \
  hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
# Assign worker to pool:
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/$WORKER"
```

### Vendor-specific notes

- **Memgraph**: Loading 34M edges via Cypher MATCH+CREATE is extremely slow. The load phase WILL timeout at 5 minutes. This is expected — record as "timeout" and move on.
- **Neo4j**: The benchmark script auto-starts its own Docker container if not running. Still clean up after.
- **ArcadeDB Docker**: Uses a two-phase approach — embedded Java loader for fast data loading, then Docker for algorithm execution via HTTP API. After loading, must wait for GAV (CSR) to build (~60-90s).
- **HugeGraph/Vermeer**: Requires a Docker network with master + worker containers. Worker must be assigned to the `$` pool before running.
- **Kuzu, DuckPGQ**: Embedded (no Docker). Clean up their temp database dirs after.

### Example: Running all vendors sequentially

```bash
cd ldbc-native

# 1. ArcadeDB (self-manages Docker)
python3 benchmark.py arcadedb
# Cleanup is automatic (script removes container)

# 2. Kuzu (embedded)
python3 benchmark.py kuzu

# 3. DuckPGQ (embedded)
python3 benchmark.py duckpgq

# 4. Memgraph
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
sleep 3
python3 benchmark.py memgraph
docker rm -f memgraph

# 5. Neo4j (auto-starts, auto-cleans)
python3 benchmark.py neo4j

# 6. ArangoDB
docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb/arangodb:3.11.12
sleep 5
python3 benchmark.py arangodb
docker rm -f arangodb

# 7. FalkorDB
docker run -d --name falkordb -p 6379:6379 falkordb/falkordb:latest
sleep 3
python3 benchmark.py falkordb
docker rm -f falkordb

# 8. HugeGraph
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net -p 6788:6788 -p 6789:6789 \
  -v "$(cd ../datasets && pwd)":/data/graphs:ro hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
sleep 5
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/$WORKER"
python3 benchmark.py hugegraph
docker rm -f vermeer-master vermeer-worker
docker network rm hugegraph-net
```

## Key Files

- `shared/bench_common.py` — Timeout (`QUERY_TIMEOUT=300`), `run_timed()`, `cleanup_docker()`, CLI parsing
- `ldbc-native/systems/__init__.py` — Available systems and metrics for Mode 2
- `ldbc-native/systems/_common.py` — Dataset paths, constants
- `lsqb/systems/__init__.py` — Available systems for Mode 3
- `ldbc-native/ArcadeDBEmbeddedBenchmark.java` — Standalone embedded Mode 2
- `lsqb/ArcadeDBEmbeddedLSQB.java` — Standalone embedded Mode 3
