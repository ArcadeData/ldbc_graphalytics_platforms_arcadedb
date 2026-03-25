# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with the Graph Analytical View (GAV) engine, which builds a CSR (Compressed Sparse Row) adjacency index for high-performance graph algorithm execution with zero GC pressure.

This repository contains three benchmark modes:

1. **Official LDBC Graphalytics** — standardized framework with per-algorithm isolation, validation, and reporting
2. **Native multi-vendor comparison** — load once, run all algorithms, compare ArcadeDB vs Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs FalkorDB vs HugeGraph
3. **LSQB (Labelled Subgraph Query Benchmark)** — 9 subgraph pattern matching queries on the LDBC SNB social network, comparing ArcadeDB (Cypher) vs DuckDB (SQL)

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

## Build

```bash
mvn package -DskipTests
```

The build produces a self-contained distribution in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/`.

## Dataset

Use the built-in dataset manager to browse and download datasets from the [LDBC data repository](https://ldbcouncil.org/benchmarks/graphalytics/):

```bash
# See all available datasets (40+ Graphalytics + 9 LSQB scale factors)
python3 datasets.py available

# Download the standard Graphalytics benchmark dataset (633K vertices, 34M edges, ~155 MB)
python3 datasets.py download datagen-7_5-fb

# Download the LSQB social network dataset (SF1, ~3.9M vertices, ~17.9M edges)
python3 datasets.py download lsqb-sf1

# Show downloaded datasets with size and vertex/edge counts
python3 datasets.py
```

Datasets are downloaded into the `datasets/` directory (git-ignored). After downloading `datagen-7_5-fb`:

```
datasets/
  datagen-7_5-fb/
    datagen-7_5-fb.v              # vertex file (one ID per line)
    datagen-7_5-fb.e              # edge file (src dst weight, space-separated)
    datagen-7_5-fb.properties     # graph metadata
    datagen-7_5-fb-BFS/           # validation data per algorithm
    datagen-7_5-fb-WCC/
    ...
```

---

## Mode 1: Official LDBC Graphalytics Benchmark

Uses the official [LDBC Graphalytics framework](https://github.com/ldbc/ldbc_graphalytics) with ArcadeDB's platform driver. Produces standardized results with separate `load_time`, `processing_time`, and `makespan` measurements. The framework reloads the graph for each algorithm to ensure isolated measurements.

### Configuration

The build produces a ready-to-run distribution with sensible defaults. You can optionally tune the configuration files in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/config/`:

**benchmark.properties** — dataset paths and memory:
```properties
graphs.root-directory = ../datasets          # default: empty (set to your datasets location)
graphs.validation-directory = ../datasets    # default: empty
benchmark.runner.max-memory = 16384          # default: empty (MB, recommended: 16384)
```

**benchmarks/custom.properties** — which graphs and algorithms to run:
```properties
benchmark.custom.graphs = datagen-7_5-fb                       # default: datagen-7_5-fb
benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC, SSSP   # default: all 6 algorithms
benchmark.custom.timeout = 7200                                 # default: 7200 (seconds)
benchmark.custom.output-required = true                         # default: true
benchmark.custom.validation-required = true                     # default: true
benchmark.custom.repetitions = 1                                # default: 1
```

**platform.properties** — ArcadeDB-specific settings:
```properties
platform.olap = true   # default: false (enable CSR-accelerated graph algorithms)
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

Located in `ldbc-native/`. Loads the graph once and runs all algorithms sequentially on the same in-memory structure. This provides a fair apples-to-apples comparison since all systems use the same approach.

**Systems tested:** ArcadeDB, Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB, FalkorDB, HugeGraph

### ArcadeDB (Java)

```bash
# Compile (use the LDBC platform fat JAR for dependencies)
LDBC_JAR=target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
cd ldbc-native
javac --add-modules jdk.incubator.vector -cp "../$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java

# Run
java --add-modules jdk.incubator.vector -Xms8g -Xmx8g -cp ".:../$LDBC_JAR" ArcadeDBEmbeddedBenchmark
```

### Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB (Python)

```bash
# Create virtual environment and install dependencies
cd ldbc-native
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
  -v "$(pwd)/datasets":/data/graphs:ro \
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
| **ArcadeDB** (embedded) | 26.4.1 | Open Source | Apache 2.0 | Embedded (in-process, Java 21) | None |
| **ArcadeDB** (Docker) | 26.4.1 | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker |
| **Neo4j** | 2026 | Community | GPL 3.0 | Server (Docker, Bolt protocol) | Network + Docker |
| **Kuzu** | 0.11.3 | Open Source | MIT | Embedded (in-process, C++ via Python) | None |
| **DuckPGQ** | DuckDB 1.5.0 | Open Source | MIT | Embedded (in-process, C++ via Python) | None |
| **Memgraph** | 3.8.1 | Community | BSL 1.1 | Server (Docker, Bolt protocol) | Network + Docker |
| **ArangoDB** | 3.11.14 | Community | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker |
| **FalkorDB** | 4.16.6 | Open Source | Source Available | Server (Docker, Redis protocol) | Network + Docker |
| **HugeGraph** | Vermeer latest | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker |

ArcadeDB is tested in two modes: **embedded** (in-process Java, zero overhead) and **Docker** (same HTTP/network overhead as the other Docker-based systems). Kuzu and DuckPGQ run embedded. Neo4j, Memgraph, ArangoDB, FalkorDB, and HugeGraph run as Docker containers.

#### All Systems Comparison

| Algorithm | ArcadeDB | ArcadeDB Docker | Neo4j 2026 | Kuzu | DuckPGQ | Memgraph | ArangoDB | FalkorDB | HugeGraph |
|-----------|----------|----------------|------------|------|---------|----------|----------|----------|-----------|
| **PageRank** | **0.48s** | 0.83s | 11.15s | 4.30s | 6.14s | 16.90s | 157.01s | 1.67s | 4.01s |
| **WCC** | 0.30s | **0.22s** | 0.75s | 0.43s | 13.93s | crash | 78.03s | 0.85s | 6.71s |
| **BFS** | 0.13s | **0.07s** | 1.91s | 0.86s | 2,754s | 11.72s | 511.55s | 0.20s | 0.54s |
| **LCC** | **27.41s** | 34.98s | 45.78s | N/A | 38.59s | N/A | N/A | N/A | 272.04s |
| **SSSP** | 3.53s | **0.97s** | N/A | N/A | N/A | N/A | 301.93s | N/A | N/A |
| **CDLP** | 3.67s | **3.35s** | 6.43s | N/A | N/A | N/A | 407.41s | 5.38s | 62.70s |

*Memgraph crashes with segfault (exit 139) during edge loading at ~18-20M of 34M edges.*

ArcadeDB is the fastest on every comparable algorithm and the only system that successfully runs all 6 LDBC Graphalytics algorithms. Even when running as a Docker container (same conditions as Neo4j, Memgraph, FalkorDB, and HugeGraph), ArcadeDB leads on every algorithm.

**ArcadeDB Embedded vs other systems:**
- **vs Neo4j 2026 GDS**: PageRank 23x faster, WCC 2.5x faster, BFS 15x faster, LCC 1.7x faster, CDLP 1.8x faster
- **vs Kuzu**: PageRank 9x faster, WCC 1.4x faster, BFS 6.6x faster
- **vs DuckPGQ**: PageRank 13x faster, WCC 46x faster, BFS 21,185x faster, LCC 1.4x faster
- **vs Memgraph**: PageRank 35x faster, BFS 90x faster (WCC/LCC/SSSP/CDLP: crash or unavailable)
- **vs ArangoDB**: PageRank 327x faster, WCC 260x faster, BFS 3,935x faster, SSSP 86x faster, CDLP 111x faster
- **vs FalkorDB**: PageRank 3.5x faster, WCC 2.8x faster, BFS 1.5x faster, CDLP 1.5x faster (LCC/SSSP: not available)
- **vs HugeGraph**: PageRank 8.4x faster, WCC 22x faster, BFS 4.2x faster, LCC 9.9x faster, CDLP 17x faster (SSSP: not available)

**ArcadeDB Docker vs other Docker systems (apples-to-apples):**
- **vs Neo4j 2026 GDS**: PageRank 13.4x faster, WCC 3.4x faster, BFS 27x faster, LCC 1.3x faster, CDLP 1.9x faster
- **vs FalkorDB**: PageRank 2x faster, WCC 3.9x faster, BFS 2.9x faster, CDLP 1.6x faster (LCC/SSSP: not available in FalkorDB)
- **vs HugeGraph**: PageRank 4.8x faster, WCC 30x faster, BFS 7.7x faster, LCC 7.8x faster, CDLP 18.7x faster

Notes:
- Memgraph 3.8.1 crashes with segfault (exit 139) during edge loading at ~18-20M edges. WCC previously failed with OOM at 7.6GB.
- ArangoDB 3.11 uses Pregel for PageRank/WCC/SSSP/CDLP and AQL traversal for BFS. Pregel was removed in ArangoDB 3.12.
- Kuzu and DuckPGQ lack native implementations for most algorithms beyond PageRank, WCC, and BFS.
- FalkorDB (RedisGraph fork) has no built-in LCC or full SSSP algorithm. Its `algo.SSpaths` is pair-oriented, not a full single-source Dijkstra.
- HugeGraph/Vermeer's SSSP is unweighted (hop-count only), so weighted SSSP is not available. Uses the Vermeer Go-based OLAP engine.
- ArcadeDB Docker results measured warm (JIT-compiled) to match how production servers run. All Docker systems run on Docker Desktop for macOS with 16 CPUs and 24GB RAM.
- None of the competing systems have official LDBC Graphalytics platform drivers. Only ArcadeDB has an official LDBC Graphalytics platform implementation.

## Mode 3: LSQB (Labelled Subgraph Query Benchmark)

The [LSQB benchmark](https://github.com/ldbc/lsqb) is a lightweight microbenchmark from the LDBC council that focuses on **subgraph pattern matching** — counting how many times a given labelled graph pattern appears in the dataset. It tests the query optimizer's ability to handle multi-way joins, anti-patterns (NOT EXISTS), and type hierarchy (Message supertype with Post/Comment subtypes).

The benchmark uses the LDBC SNB social network dataset (SF1: ~3.9M vertices, ~17.9M edges) and runs 9 Cypher queries (Q1–Q9) covering patterns from simple 2-hop paths to complex 8-hop chains and triangle patterns.

### Dataset

LSQB datasets come in two formats (both contain the same data):

| Format | Entity CSVs | Relationships | Best for |
|--------|-------------|---------------|----------|
| **merged-fk** | ID + FK columns (e.g. `City.csv` has `ispartof_country`) | FKs in entity rows + separate CSVs for M:N | SQL databases (DuckDB, PostgreSQL), ArcadeDB, Neo4j |
| **projected-fk** | ID only | Every relationship in a separate edge CSV (e.g. `City_isPartOf_Country.csv`) | Graph DB bulk loaders (Kuzu) |

```bash
# Download LSQB SF1 (both merged-fk and projected-fk formats)
python3 datasets.py download lsqb-sf1

# Or download only the format you need
python3 datasets.py download lsqb-sf1 --format merged-fk    # for ArcadeDB, DuckDB, PostgreSQL, Neo4j
python3 datasets.py download lsqb-sf1 --format projected-fk # for Kuzu
```

### Run ArcadeDB (Java, embedded)

```bash
cd lsqb
LDBC_JAR=../target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar

# Compile
javac -cp "$LDBC_JAR" ArcadeDBEmbeddedLSQB.java

# Run (first run loads data, subsequent runs reuse the database)
java -Xms4g -Xmx4g --add-modules jdk.incubator.vector -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB

# Force reload from scratch
java -Xms4g -Xmx4g --add-modules jdk.incubator.vector -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB --reset
```

### Run DuckDB (Python)

```bash
cd lsqb
pip install duckdb
python3 lsqb_benchmark.py duckdb
```

### Run All Systems (Kuzu, DuckDB, Neo4j)

```bash
cd lsqb
python3 lsqb_benchmark.py              # Run all systems
python3 lsqb_benchmark.py --reset      # Delete all data and reload
python3 lsqb_benchmark.py kuzu duckdb  # Run specific systems only
```

### LSQB Queries

| Query | Pattern | Description |
|-------|---------|-------------|
| **Q1** | 8-hop chain | Country←City←Person←Forum→Post←Comment→Tag→TagClass |
| **Q2** | Diamond | Person-KNOWS-Person with Comment→Post creator path |
| **Q3** | Triangle | 3 Persons in same Country, all connected by KNOWS |
| **Q4** | Star | Message with Tag, Creator, Likes, and Replies (inner join) |
| **Q5** | Fork | Message←Reply with different Tags |
| **Q6** | 2-hop + interest | Person-KNOWS-Person-KNOWS-Person→Tag |
| **Q7** | Star (optional) | Same as Q4 but with OPTIONAL MATCH for Likes and Replies |
| **Q8** | Anti-pattern | Like Q5 but Comment must NOT have the parent's Tag |
| **Q9** | Anti-pattern | Like Q6 but Person1 must NOT know Person3 |

### LSQB Results

Dataset: **LDBC SNB SF1** (3,947,829 vertices, 17,882,623 edges)

*Benchmarks run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS.*

| System | Version | Mode | Language |
|--------|---------|------|----------|
| **ArcadeDB Embedded** | 26.4.1 | Embedded (Java 21) | Cypher |
| **ArcadeDB Docker** | 26.4.1 | Docker (HTTP API) | Cypher |
| **DuckDB** | 1.4.4 | Embedded (C++ via Python) | SQL |
| **Kuzu** | 0.11.3 | Embedded (C++ via Python) | Cypher |
| **Neo4j** | 2025 Community | Docker | Cypher |
| **PostgreSQL** | 17 | Docker | SQL |
| **Memgraph** | latest | Docker | Cypher |
| **Dgraph** | v25.3.0 | Docker (HTTP API) | DQL |
| **SurrealDB** | v2.6.4 | Docker (HTTP API) | SurrealQL |

| Query | Expected Count | ArcadeDB Embedded | ArcadeDB Docker | DuckDB | Kuzu | Neo4j | PostgreSQL | Memgraph | Dgraph | SurrealDB | Winner |
|-------|---------------|----------|-----------------|--------|------|-------|------------|----------|--------|-----------|--------|
| **Load** | — | 119.24s | 197.96s | — | — | — | — | — | — | — | — |
| **Q1** | 221,636,419 | 0.23s | 0.25s | **0.15s** | 5.83s | 8.25s | 6.56s | 60.45s | 2.52s | timeout | DuckDB |
| **Q2** | 1,085,627 | 0.18s | 0.19s | **0.02s** | 0.14s | 2.06s | 0.34s | timeout | N/A | timeout | DuckDB |
| **Q3** | 753,570 | 0.10s | 0.13s | **0.05s** | 2.44s | 14.31s | 2.12s | timeout | N/A | N/A | DuckDB |
| **Q4** | 14,836,038 | 0.03s | **0.03s** | 0.08s | N/A | 7.82s | 6.86s | 4.50s | 8.13s | timeout | ArcadeDB |
| **Q5** | 13,824,510 | 0.29s | 0.23s | **0.04s** | N/A | 6.72s | 0.69s | 3.86s | N/A | timeout | DuckDB |
| **Q6** | 1,668,134,320 | **0.11s** | 0.11s | 2.18s | 1.41s | 52.06s | 17.72s | 148.14s | N/A | N/A | ArcadeDB |
| **Q7** | 26,190,133 | 0.09s | **0.02s** | 0.08s | N/A | 10.45s | 11.22s | 5.59s | 5.97s | timeout | ArcadeDB |
| **Q8** | 6,907,213 | 0.19s | 0.19s | **0.07s** | N/A | 12.91s | 1.31s | 3.37s | N/A | N/A | DuckDB |
| **Q9** | 1,596,153,418 | 1.18s | **1.06s** | 7.77s | 6.15s | 59.09s | 22.25s | timeout | N/A | N/A | ArcadeDB |

All 9 queries produce correct results matching the [official LSQB expected output](https://github.com/ldbc/lsqb/blob/main/expected-output/expected-output.csv). Kuzu skips Q4/Q5/Q7/Q8 (no `:Message` supertype support). Memgraph times out on Q2/Q3/Q9 (600s limit). Dgraph answers 3 of 9 queries using DQL value-variable propagation and `math()` (see [Dgraph section](#dgraph) below). SurrealDB has queries written for Q1/Q2/Q4/Q5/Q7 but all timeout at 120s due to O(n*m) nested subquery execution without index acceleration (see [SurrealDB section](#surrealdb)). ArcadeDB Docker runs under the same conditions as Neo4j, PostgreSQL, Memgraph, Dgraph, and SurrealDB (Docker Desktop for macOS).

**Analysis:**

- **ArcadeDB is the fastest on 4 out of 9 queries** (Q4, Q6, Q7, Q9), DuckDB on the other 5.
- **Q4 and Q7** — star-shaped joins centered on Message (Tag, Creator, Likes, Replies). With the GAV's CSR acceleration, ArcadeDB completes these in 10–30ms, **3–8x faster than DuckDB**, and **261–1045x faster than Neo4j**. The benchmark uses `GraphTraversalProviderRegistry.awaitAll()` to ensure the GAV is fully registered with the query optimizer before timing queries.
- **Q6 and Q9** — multi-hop path traversals (Person-KNOWS-Person-KNOWS-Person) where graph adjacency lists outperform relational self-joins. These are the two heaviest queries with billion-scale result counts. ArcadeDB is **7–20x faster than DuckDB**, **55–473x faster than Neo4j**, and **21–161x faster than PostgreSQL**. Q6 in particular showcases the edge-scan algebraic optimization: ArcadeDB computes the 1.67-billion-row count in just 110ms — **20x faster than DuckDB**.
- **DuckDB wins on remaining queries** — Q1 (long chain), Q2 (diamond), Q3 (triangle), Q5 (fork), Q8 (anti-pattern) are join-intensive patterns where DuckDB's columnar vectorized execution excels. However, the gap has narrowed significantly: Q8 is now only 2.7x slower than DuckDB (down from 7.7x), thanks to the edge-scan anti-join optimization.
- **ArcadeDB Docker vs other Docker systems** — even with HTTP + network + Docker VM overhead, ArcadeDB Docker is **10–1045x faster than Neo4j**, **2–24x faster than PostgreSQL**, and **5–559x faster than Memgraph** on the queries Memgraph completes.
- **Neo4j and Memgraph** are significantly slower across the board. Memgraph times out on 3 of 9 queries. Neo4j completes all queries but is 9–1045x slower than ArcadeDB on every query.
- **PostgreSQL** is a solid middle ground for a traditional RDBMS — faster than Neo4j/Memgraph but significantly slower than both ArcadeDB and DuckDB.

---

## SurrealDB

SurrealDB is implemented in both benchmark modes but **excluded from default runs** because it scores N/A on every metric — all 6 Graphalytics algorithms and all 9 LSQB queries.

### Why it's excluded

Despite marketing itself as a multi-model database with "graph capabilities," SurrealDB lacks the fundamentals needed for graph benchmarking:

- **No graph algorithms** — zero support for PageRank, WCC, BFS, CDLP, LCC, or SSSP. Every other database in the benchmark ships with at least some of these.
- **Broken recursive traversal** — the `->edge.{1..N}->node` syntax doesn't actually recurse beyond 1 hop. On the real graph, "BFS" found only 34 nodes (direct neighbors) instead of the expected 633K.
- **No pattern matching** — no Cypher MATCH, no SQL JOINs, no table aliases. This makes self-joins and multi-table queries impossible. LSQB queries Q3, Q6, Q8, Q9 cannot be expressed at all. Queries Q1, Q2, Q4, Q5, Q7 are implemented using nested subqueries with `$parent` dereferencing and `array::len()` for cross-product counting, but all timeout at 120s — the O(n*m) nested loop execution without index acceleration is too slow for 3.9M vertices / 17.9M edges.
- **Extremely slow loading** — 34M edges took ~30 minutes via the HTTP API (1MB payload limit forces 3,400 round-trips), compared to seconds for embedded systems.
- **Stability issues** — OOM crashes (exit 137) during cleanup, connection resets during schema operations, and `{..+collect}` hangs the server indefinitely.

For the full analysis, see [SURREALDB.md](SURREALDB.md).

### How to enable SurrealDB

```bash
# Start SurrealDB (Docker)
docker run -d --name surrealdb -p 8000:8000 \
  -e SURREAL_LOG=warn \
  -v /tmp/surrealdb_data:/data \
  surrealdb/surrealdb:v2 start \
  --user root --pass benchmark \
  rocksdb:///data/bench.db

# Run Graphalytics benchmark (warning: loading takes ~30 minutes)
cd ldbc-native
python3 benchmark.py surrealdb

# Run LSQB benchmark (warning: loading takes ~9 minutes, Q1/Q2/Q4/Q5/Q7 timeout, rest N/A)
cd lsqb
python3 lsqb_benchmark.py surrealdb
```

*Tested with SurrealDB v2.6.4 on March 2026.*

---

## Dgraph

Dgraph v25.3.0 is implemented in both benchmark modes but **excluded from default runs**. It scores N/A on all 6 Graphalytics algorithms and answers only 3 of 9 LSQB queries.

### Why it's excluded

Dgraph is a distributed graph database with the DQL query language (formerly GraphQL+-). Unlike Cypher or SQL engines, DQL is a hierarchical traversal language that returns nested JSON — it has no `MATCH` clause, no `JOIN`, no table aliases, and no `NOT EXISTS`. This creates fundamental limitations:

- **No graph algorithms** — Dgraph has no built-in PageRank, WCC, BFS (single-source-all-destinations), LCC, SSSP, or CDLP. The only built-in algorithm is `shortest()`, which is point-to-point (requires both source and target UIDs), not single-source-all-destinations as LDBC Graphalytics requires.
- **No pattern matching** — DQL traverses the graph from root nodes outward and cannot express arbitrary join conditions between different parts of a pattern. This makes 6 of 9 LSQB queries impossible.
- **Loading via HTTP mutations** — 34M Graphalytics edges take ~204s via batched RDF N-Quad mutations. LSQB (3.9M vertices, 17.9M edges) takes ~214s.

### What Dgraph CAN do (LSQB Q1, Q4, Q7)

Despite lacking pattern matching, three LSQB queries can be expressed in DQL using creative techniques:

**Q1 (chain traversal)** — DQL value variable propagation. The 8-hop chain Country←City←Person←Forum→Post←Comment→Tag→TagClass is expressed as nested reverse-edge traversals (`~is_part_of`, `~is_located_in`, etc.). At the leaf level, `count(has_type)` counts TagClasses per Tag, then `sum(val())` at each parent level propagates the path count upward — giving the exact Cartesian product count (221,636,419). This works because each level's sum is equivalent to multiplying child path counts, which matches `count(*)` semantics for chain patterns.

**Q4 (star pattern)** — DQL `math()` function. For each Message with tags, likes, and replies, the tuple count equals `tags × likes × replies`. Two `var` blocks compute `math(t * l * r)` separately for Posts (replies via `~reply_of_post`) and Comments (replies via `~reply_of_comment`), then `sum()` aggregates both into the correct total (14,836,038).

**Q7 (optional star)** — Like Q4 but with `OPTIONAL MATCH` semantics. Messages without likes or replies still contribute one row each. Expressed as `math(tags × max(likes, 1) × max(replies, 1))` — the `max(count, 1)` emulates the NULL-becomes-one-row behavior of `OPTIONAL MATCH` (26,190,133).

### Why 6 queries are impossible in DQL

| Query | Limitation |
|-------|-----------|
| **Q2** (diamond) | Requires per-row correlation: "Comment created by Person1 replies to Post created by Person2, AND Person1 KNOWS Person2." DQL `var` blocks produce global UID sets, not per-row bindings. |
| **Q3** (triangle) | Requires self-join on Person (3 different Persons in same Country, all connected by KNOWS). DQL has no self-join. |
| **Q5** (fork) | Requires cross-reference inequality: `tag1 <> tag2` where tag1 is from the message and tag2 is from the reply. DQL cannot compare values across different nesting levels. |
| **Q6** (2-hop KNOWS) | Requires per-row inequality: `person1 <> person3`. DQL has no way to exclude specific nodes per-traversal. |
| **Q8** (anti-pattern) | Requires `NOT EXISTS`: "Comment must NOT have the parent's Tag." DQL has no anti-join operator. |
| **Q9** (anti-pattern) | Requires both `NOT EXISTS` and per-row inequality — combines Q6 and Q8 limitations. |

### Performance comparison (LSQB)

On the 3 queries Dgraph can answer:
- **Q1**: Dgraph 2.52s — faster than Kuzu (5.83s), Neo4j (8.25s), PostgreSQL (6.56s), and Memgraph (60.45s), but 11x slower than ArcadeDB (0.23s) and 17x slower than DuckDB (0.15s).
- **Q4**: Dgraph 8.13s — comparable to Neo4j (7.82s) and PostgreSQL (6.86s), but 400x slower than ArcadeDB (0.02s) and 100x slower than DuckDB (0.08s).
- **Q7**: Dgraph 5.97s — faster than Neo4j (10.45s) and PostgreSQL (11.22s), but 300x slower than ArcadeDB (0.02s) and 75x slower than DuckDB (0.08s).

### How to enable Dgraph

```bash
# Start Dgraph (Docker — requires two containers: Zero + Alpha)
docker network create dgraph-net
docker run -d --name dgraph-zero --network dgraph-net \
  -p 5080:5080 -p 6080:6080 \
  dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080
docker run -d --name dgraph-alpha --network dgraph-net \
  -p 8080:8080 -p 9080:9080 \
  -v /tmp/dgraph_data:/dgraph \
  dgraph/dgraph:latest dgraph alpha \
    --my=dgraph-alpha:7080 \
    --zero=dgraph-zero:5080 \
    --cache size-mb=8192 \
    --badger "compression=none; numgoroutines=8" \
    --security whitelist=0.0.0.0/0 \
    --limit "mutations-nquad=5000000; query-edge=10000000"

# Run Graphalytics benchmark (loading ~204s, all algorithms N/A)
cd ldbc-native
python3 benchmark.py dgraph

# Run LSQB benchmark (loading ~214s, Q1/Q4/Q7 answered, rest N/A)
cd lsqb
python3 lsqb_benchmark.py dgraph
```

*Tested with Dgraph v25.3.0 on March 2026.*

---

## File Structure

```
shared/
  bench_common.py                  # Shared benchmark infrastructure

ldbc-native/
  ArcadeDBEmbeddedBenchmark.java   # ArcadeDB Graphalytics benchmark (Java, embedded)
  ArcadeDBEmbeddedLoader.java      # ArcadeDB graph loader (Java, embedded)
  benchmark.py                     # Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB Graphalytics benchmarks (Python)

lsqb/
  ArcadeDBEmbeddedLSQB.java        # ArcadeDB LSQB benchmark (Java, embedded, Cypher)
  lsqb_benchmark.py                # Kuzu, DuckDB, Neo4j LSQB benchmarks (Python)
  tools/                           # Debug/profiling helpers
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
