# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with the Graph Analytical View (GAV) engine, which builds a CSR (Compressed Sparse Row) adjacency index for high-performance graph algorithm execution with zero GC pressure.

This repository contains two benchmark modes:

1. **Official LDBC Graphalytics** — standardized framework with per-algorithm isolation, validation, and reporting
2. **Native multi-vendor comparison** — load once, run all algorithms, compare ArcadeDB vs Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs FalkorDB vs HugeGraph

## Supported Algorithms

| Algorithm | Implementation | Complexity |
|-----------|---------------|------------|
| **BFS** (Breadth-First Search) | Parallel frontier expansion with bitmap visited set and push/pull direction optimization | O(V + E) |
| **PR** (PageRank) | Pull-based parallel iteration via backward CSR | O(iterations * E) |
| **WCC** (Weakly Connected Components) | Synchronous parallel min-label propagation | O(diameter * E) |
| **CDLP** (Community Detection Label Propagation) | Synchronous parallel label propagation with sort-based mode finding | O(iterations * E * log(d)) |
| **LCC** (Local Clustering Coefficient) | Parallel sorted-merge triangle counting | O(E * sqrt(E)) |
| **SSSP** (Single Source Shortest Paths) | Dijkstra with binary min-heap on CSR + columnar weights | O((V + E) * log(V)) |

## Prerequisites

- Java 21 or later (required for `jdk.incubator.vector` SIMD support)
- Maven 3.x
- ArcadeDB engine built locally

## Build

```bash
# 1. Build ArcadeDB engine
cd /path/to/arcadedb
mvn install -DskipTests -pl engine -am -q

# 2. Build the LDBC platform driver
cd /path/to/ldbc_graphalytics_platforms_arcadedb
mvn package -DskipTests
```

The build produces a self-contained distribution in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/`.

## Dataset

Download datasets from the [LDBC Graphalytics data repository](https://ldbcouncil.org/benchmarks/graphalytics/). For example, `datagen-7_5-fb` (633K vertices, 34M edges):

```
/path/to/graphs/
  datagen-7_5-fb.v              # vertex file (one ID per line)
  datagen-7_5-fb.e              # edge file (src dst weight, space-separated)
  datagen-7_5-fb.properties     # graph metadata
  datagen-7_5-fb-BFS/           # validation data per algorithm
  datagen-7_5-fb-WCC/
  datagen-7_5-fb-PR/
  datagen-7_5-fb-CDLP/
  datagen-7_5-fb-LCC/
  datagen-7_5-fb-SSSP/
```

---

## Mode 1: Official LDBC Graphalytics Benchmark

Uses the official [LDBC Graphalytics framework](https://github.com/ldbc/ldbc_graphalytics) with ArcadeDB's platform driver. Produces standardized results with separate `load_time`, `processing_time`, and `makespan` measurements. The framework reloads the graph for each algorithm to ensure isolated measurements.

### Configuration

Edit files in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/config/`:

**benchmark.properties:**
```properties
graphs.root-directory = /path/to/graphs
graphs.validation-directory = /path/to/graphs
benchmark.runner.max-memory = 16384
```

**benchmarks/custom.properties:**
```properties
benchmark.custom.graphs = datagen-7_5-fb
benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC, SSSP
benchmark.custom.timeout = 7200
benchmark.custom.output-required = true
benchmark.custom.validation-required = true
benchmark.custom.repetitions = 1
```

**platform.properties:**
```properties
platform.arcadedb.olap = true
```

### Run

```bash
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT
bash bin/sh/run-benchmark.sh
```

Results are written to `report/<timestamp>-ARCADEDB-report-CUSTOM/json/results.json`.

### Extract Results

```bash
LATEST=$(ls -td report/*ARCADEDB* | head -1)
python3 -c "
import json
with open('$LATEST/json/results.json') as f:
    data = json.load(f)
result = data.get('result', data.get('experiments', {}))
runs = result.get('runs', {})
jobs = result.get('jobs', {})
for rid, r in sorted(runs.items(), key=lambda x: x[1]['timestamp']):
    algo = next(j['algorithm'] for j in jobs.values() if rid in j['runs'])
    print(f\"{algo:6} proc={r['processing_time']:>8}s  load={r['load_time']:>8}s\")
"
```

---

## Mode 2: Native Multi-Vendor Comparison

Located in `native-benchmark/`. Loads the graph once and runs all algorithms sequentially on the same in-memory structure. This provides a fair apples-to-apples comparison since all systems use the same approach.

**Systems tested:** ArcadeDB, Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB, FalkorDB, HugeGraph

### ArcadeDB (Java)

```bash
# Compile (use the LDBC platform fat JAR for dependencies)
LDBC_JAR=graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/lib/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
cd native-benchmark
javac --add-modules jdk.incubator.vector -cp "../$LDBC_JAR" ArcadeDBBenchmark.java

# Run
java --add-modules jdk.incubator.vector -Xms8g -Xmx8g -cp ".:../$LDBC_JAR" ArcadeDBBenchmark
```

### Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB (Python)

```bash
# Create virtual environment and install dependencies
cd native-benchmark
python3 -m venv .venv
source .venv/bin/activate
pip install kuzu duckdb pymgclient neo4j python-arango

# Run all available benchmarks
python3 benchmark.py
```

For Memgraph, start Docker first:
```bash
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
```

For Neo4j, start Docker with GDS plugin:
```bash
docker run -d --name neo4j -p 7474:7474 -p 7688:7687 \
  -e NEO4J_AUTH=neo4j/benchmark123 \
  -e NEO4J_PLUGINS='["graph-data-science"]' \
  neo4j:2026-community
```

For ArangoDB, start Docker (use 3.11 — Pregel was removed in 3.12):
```bash
docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb:3.11
```

For HugeGraph (Vermeer OLAP engine):
```bash
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net \
  -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net \
  -p 6788:6788 -p 6789:6789 \
  -v /path/to/graphs:/data/graphs:ro \
  hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
# Assign worker to common pool:
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/${WORKER}"
```

### Benchmark Results

Dataset: **datagen-7_5-fb** (633,432 vertices, 34,185,747 edges, undirected, weighted)

*Benchmarks run on a MacBook Pro 16" (2019), Intel Core i9-9880H 8-core @ 2.3GHz, 32GB RAM, macOS.*

#### Official LDBC Graphalytics Results (ArcadeDB)

Using the LDBC Graphalytics framework (graph reloaded per algorithm):

| Algorithm | processing_time | load_time | makespan |
|-----------|----------------|-----------|----------|
| **PR** | 16.12s | 95.04s | 48.80s |
| **WCC** | 8.36s | 95.04s | 37.67s |
| **BFS** | 22.81s | 95.04s | 57.52s |
| **CDLP** | 30.38s | 95.04s | 56.81s |
| **LCC** | 43.75s | 95.04s | 73.76s |
| **SSSP** | 28.72s | 115.50s | 144.84s |

All 6 algorithms passed with validation.

#### Native Comparison (load once, run all algorithms)

| System | Version | Edition | License | Mode | Overhead |
|--------|---------|---------|---------|------|----------|
| **ArcadeDB** | 26.4.1 | Open Source | Apache 2.0 | Embedded (in-process, Java 21) | None |
| **Neo4j** | 2026 | Community | GPL 3.0 | Server (Docker, Bolt protocol) | Network + Docker |
| **Kuzu** | 0.11.3 | Open Source | MIT | Embedded (in-process, C++ via Python) | None |
| **DuckPGQ** | DuckDB 1.5.0 | Open Source | MIT | Embedded (in-process, C++ via Python) | None |
| **Memgraph** | 3.8.1 | Community | BSL 1.1 | Server (Docker, Bolt protocol) | Network + Docker |
| **ArangoDB** | 3.11.14 | Community | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker |
| **FalkorDB** | 4.16.6 | Open Source | Source Available | Server (Docker, Redis protocol) | Network + Docker |
| **HugeGraph** | Vermeer latest | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker |

ArcadeDB, Kuzu, and DuckPGQ all run embedded (in-process, no network overhead). Memgraph, Neo4j, ArangoDB, FalkorDB, and HugeGraph run as Docker containers, which adds network serialization overhead. This mainly affects data loading times, not algorithm execution (computation happens server-side).

| Algorithm | ArcadeDB | Neo4j 2026 | Kuzu | DuckPGQ | Memgraph | ArangoDB | FalkorDB | HugeGraph |
|-----------|----------|------------|------|---------|----------|----------|----------|-----------|
| **PageRank** | **0.48s** | 11.15s | 4.30s | 6.14s | 16.90s | 157.01s | 1.67s | 4.01s |
| **WCC** | **0.30s** | 0.75s | 0.43s | 13.93s | crash | 78.03s | 0.85s | 6.71s |
| **BFS** | **0.13s** | 1.91s | 0.86s | 2,754s | 11.72s | 511.55s | 0.20s | 0.54s |
| **LCC** | **27.41s** | 45.78s | N/A | 38.59s | N/A | N/A | N/A | 272.04s |
| **SSSP** | **3.53s** | N/A | N/A | N/A | N/A | 301.93s | N/A | N/A |
| **CDLP** | **3.67s** | 6.43s | N/A | N/A | N/A | 407.41s | 5.38s | 62.70s |

*Memgraph crashes with segfault (exit 139) during edge loading at ~18-20M of 34M edges.*

ArcadeDB is the fastest on every comparable algorithm and the only system that successfully runs all 6 LDBC Graphalytics algorithms.

- **vs Neo4j 2026 GDS**: PageRank 23x faster, WCC 2.5x faster, BFS 15x faster, LCC 1.7x faster, CDLP 1.8x faster
- **vs Kuzu**: PageRank 9x faster, WCC 1.4x faster, BFS 6.6x faster
- **vs DuckPGQ**: PageRank 13x faster, WCC 46x faster, BFS 21,185x faster, LCC 1.4x faster
- **vs Memgraph**: PageRank 35x faster, BFS 90x faster (WCC/LCC/SSSP/CDLP: crash or unavailable)
- **vs ArangoDB**: PageRank 327x faster, WCC 260x faster, BFS 3,935x faster, SSSP 86x faster, CDLP 111x faster
- **vs FalkorDB**: PageRank 3.5x faster, WCC 2.8x faster, BFS 1.5x faster, CDLP 1.5x faster (LCC/SSSP: not available)
- **vs HugeGraph**: PageRank 8.4x faster, WCC 22x faster, BFS 4.2x faster, LCC 9.9x faster, CDLP 17x faster (SSSP: not available)

Notes:
- Memgraph 3.8.1 crashes with segfault (exit 139) during edge loading at ~18-20M edges. WCC previously failed with OOM at 7.6GB.
- ArangoDB 3.11 uses Pregel for PageRank/WCC/SSSP/CDLP and AQL traversal for BFS. Pregel was removed in ArangoDB 3.12.
- Kuzu and DuckPGQ lack native implementations for most algorithms beyond PageRank, WCC, and BFS.
- FalkorDB (RedisGraph fork) has no built-in LCC or full SSSP algorithm. Its `algo.SSpaths` is pair-oriented, not a full single-source Dijkstra.
- HugeGraph/Vermeer's SSSP is unweighted (hop-count only), so weighted SSSP is not available. Uses the Vermeer Go-based OLAP engine.
- None of the competing systems have official LDBC Graphalytics platform drivers. Only ArcadeDB has an official LDBC Graphalytics platform implementation.

### File Structure

```
native-benchmark/
  ArcadeDBBenchmark.java    # ArcadeDB standalone benchmark (Java, embedded)
  benchmark.py              # Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB benchmarks (Python)
```

---

## Architecture

### Graph Analytical View (GAV)

The GAV engine builds a CSR adjacency index from ArcadeDB's OLTP storage:

1. **Pass 1**: Scans all vertices, assigns dense integer IDs, collects edge pairs
2. **Pass 2**: Computes prefix sums from degree arrays, fills CSR neighbor arrays
3. **Result**: Packed `int[]` arrays for forward/backward offsets and neighbors, plus columnar edge property storage

All graph algorithms operate directly on these packed arrays with zero object allocation in hot loops.

### Algorithm Execution Modes

- **CSR-accelerated** (default when OLAP enabled): Algorithms run on the GAV's CSR arrays via `GraphAlgorithms.*` methods
- **OLTP fallback**: If GAV is unavailable, algorithms fall back to ArcadeDB's built-in graph traversal procedures

### JVM Flags

The benchmark runner uses:
```
-Xms16g -Xmx16g --add-modules jdk.incubator.vector
```

The `jdk.incubator.vector` module enables SIMD-accelerated operations in the GAV engine.

## License

Apache License, Version 2.0
