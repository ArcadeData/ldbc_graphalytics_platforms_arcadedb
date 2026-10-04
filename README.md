# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with the Graph Analytical View (GAV) engine, which builds a CSR (Compressed Sparse Row) adjacency index for high-performance graph algorithm execution with zero GC pressure.

This repository contains three benchmark modes:

1. **Official LDBC Graphalytics** — standardized framework with per-algorithm isolation, validation, and reporting
2. **Native multi-vendor comparison** — load once, run all algorithms, compare ArcadeDB vs Kuzu vs DuckPGQ vs Memgraph vs Neo4j vs FalkorDB vs HugeGraph
3. **LSQB (Labelled Subgraph Query Benchmark)** — 9 subgraph pattern matching queries on the LDBC SNB social network, comparing ArcadeDB (Cypher) vs DuckDB (SQL) vs FalkorDB (Cypher) and others

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

- Java 21 or later (required for `jdk.incubator.vector` SIMD support). The published benchmark numbers use **Eclipse Temurin 25 with `-XX:+UseCompactObjectHeaders`** (GraalVM is not used)
- Maven 3.x
- Python 3.10+ (for Mode 2 and Mode 3 multi-vendor comparisons; see `pyproject.toml` for per-vendor install extras)

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

## Weekly unattended run

```bash
python3 weekend.py --jvm-flags "-XX:+UseCompactObjectHeaders"
```

Runs every suite and vendor in sequence (ArcadeDB embedded Java benchmarks, the Graphalytics and LSQB
multi-vendor suites, optionally Mode 1 with `--mode1-dist`) and writes a Markdown/JSON report to
`weekly-results/<timestamp>/`. Each vendor runs in its own process group with a total-time and a no-output
limit; a hung vendor gets SIGTERM, then SIGKILL, and the run continues. Loaded databases are kept under
`~/.cache/ldbc-graph-bench` and reused, so only the first run (or `--reset`, a new image, a changed dataset)
pays for the loads. See `CLAUDE.md` for the details and `python3 -m unittest discover -s tests` for the tests.

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

**Systems tested:** ArcadeDB, Kuzu, LadybugDB, DuckPGQ, Memgraph, Neo4j, ArangoDB, FalkorDB, HugeGraph

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
# Create virtual environment and install dependencies (from repo root)
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[neo4j,kuzu,duckdb,memgraph,arangodb,falkordb]"
# or: pip install -e ".[all]" for every vendor, including postgresql

# Run all available benchmarks
cd ldbc-native
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

*All benchmarks in this section were run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS (ArcadeDB on Eclipse Temurin 25 with `-XX:+UseCompactObjectHeaders`, 12 GB heap for every JVM system, Docker Desktop with 32 GB). Measured 2026-10-03.*

#### Official LDBC Graphalytics Results (ArcadeDB)

The official-framework (Mode 1) numbers, per ArcadeDB release, are in the next section.

#### ArcadeDB release-over-release (26.8.1 → 26.10.1-SNAPSHOT)

Same machine for every column (MacBook M5, `-Xms12g -Xmx12g`, OpenJDK 21), same datasets, one JVM at a time, measured 2026-10-02 (later runs use Temurin 25 only). This tracks ArcadeDB itself across versions; the multi-vendor tables below were re-measured on 2026-10-03 on the same machine with ArcadeDB `26.10.1-SNAPSHOT` and the newest release of every other system (raw detail, Java 21 vs 25 and decision log: [`results-m5-multivendor-2026-10-03.md`](results-m5-multivendor-2026-10-03.md)). The 26.10.1 column is the `26.10.1-SNAPSHOT` built from ArcadeDB `main` @ `02ac27327d` (not in a release yet). Raw numbers: [`results-26.10.1-SNAPSHOT-vs-26.8.1.md`](results-26.10.1-SNAPSHOT-vs-26.8.1.md).

**Mode 1 (official framework, datagen-7_5-fb), processing_time in seconds, all runs validated:**

| Algorithm | OLAP (GAV) 26.8.1 | OLAP 26.10.1 | OLTP 26.8.1 | OLTP 26.10.1 |
|-----------|------|------|------|------|
| **PR** | 3.04 | 2.61 | 43.6 | 45.8 |
| **BFS** | 7.14 | 8.25 | 98.8 | 91.1 |
| **WCC** | 3.14 | 3.92 | 94.3 | 75.8 |
| **CDLP** | 13.5 | 14.7 | 58.3 | 69.8 |
| **LCC** | 5.80 | 6.24 | 169 | 177 |
| **SSSP** | 6.45 | 7.00 | 54.6 | 54.9 |

OLTP numbers vary about 2x between runs on this machine (e.g. the same OLTP build gave CDLP 62.5s and 69.8s, LCC 168s and 177s in two runs); do not read small OLTP differences as real.

**Mode 2 (embedded, GAV, datagen-7_5-fb), seconds (26.8.1 → 26.10.1):** load 60.2 → 57.6, GAV build 19.5 → ~17, PR 0.14 → 0.21, WCC 0.075 → 0.076, BFS 0.058 → 0.075, LCC 2.19 → 2.35, SSSP 0.86 → 1.00, CDLP 1.06 → 1.13. These sub-second algorithm times are dominated by noise at this scale.

**Mode 3 (LSQB SF1, embedded Cypher), seconds; all 9 counts identical in every column:**

| Query | OLAP 26.8.1 | OLAP 26.10.1 | OLTP 26.8.1 | OLTP 26.10.1 |
|-------|------|------|------|------|
| **Load** | 145.0 | 126.5 | — | — |
| **Q1** | 0.32 | 0.42 | 3.21 | 3.73 |
| **Q2** | 0.15 | 0.24 | 7.85 | 5.75 |
| **Q3** | 0.09 | 0.12 | 8.02 | 3.78 |
| **Q4** | 0.01 | 0.12 | 4.10 | 1.11 |
| **Q5** | 0.21 | 0.28 | 14.14 | 13.37 |
| **Q6** | 0.09 | 0.16 | 26.99 | 14.22 |
| **Q7** | 0.01 | 0.11 | 3.89 | 1.16 |
| **Q8** | 0.11 | 0.20 | 10.87 | 9.46 |
| **Q9** | 0.97 | 1.66 | 1.09 | 1.39 |

*Keep this section updated: after every benchmark run on a new ArcadeDB version, add the new column here and in the results file.*

#### Systems and versions (both suites)

| System | Version | Edition | License | Mode | Overhead | Used in |
|--------|---------|---------|---------|------|----------|---------|
| **ArcadeDB** (embedded) | 26.10.1-SNAPSHOT | Open Source | Apache 2.0 | Embedded (in-process, Temurin 25) | None | Graphalytics, LSQB |
| **ArcadeDB** (Docker) | 26.10.1-SNAPSHOT | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics, LSQB |
| **Neo4j** | 2026.09.0 | Community | GPL 3.0 | Server (Docker, Bolt protocol, GDS) | Network + Docker | Graphalytics, LSQB |
| **Kuzu** | 0.11.3 (archived project) | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics, LSQB |
| **LadybugDB** (Kuzu fork) | 0.21.2 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics, LSQB |
| **DuckPGQ** | DuckDB 1.5.0 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics |
| **DuckDB** | 1.5.6 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | LSQB |
| **PostgreSQL** | 18 | Open Source | PostgreSQL | Server (Docker) | Network + Docker | LSQB |
| **Memgraph** | 3.13.1 (MAGE) | Community | BSL 1.1 | Server (Docker, Bolt protocol) | Network + Docker | Graphalytics, LSQB |
| **ArangoDB** | 3.11.14 \* | Community | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics |
| **FalkorDB** | 6.0.1 (Redis 8.10.2) | Open Source | Source Available | Server (Docker, Redis protocol) | Network + Docker | Graphalytics, LSQB |
| **HugeGraph** | Vermeer latest | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics |

All systems are the newest available release as of 2026-10-03. Exceptions: DuckPGQ is pinned to DuckDB 1.5.0 because the `duckpgq` extension is not published for newer DuckDB releases; ArangoDB is pinned to 3.11.14 (see the \* note below); Kuzu 0.11.3 is the last release of an archived project and is kept for reference next to its active fork LadybugDB. ArcadeDB is tested **embedded** (in-process Java, zero overhead) and in **Docker** (same HTTP/network overhead as the other Docker systems).

#### All Systems Comparison

Seconds, `datagen-7_5-fb`, one process at a time, 5-minute limit per operation. ArcadeDB embedded (Temurin 25, compact object headers) is the median of 3 runs.

| Algorithm | ArcadeDB | ArcadeDB Docker | Neo4j | Kuzu | LadybugDB | DuckPGQ | Memgraph | ArangoDB \* | FalkorDB | HugeGraph |
|-----------|----------|----------------|-------|------|-----------|---------|----------|------------------|----------|-----------|
| **Load** | 50.4 | **35.5** | 538 | 3.35 | 5.94 | 0.45 | 170.5 | 356.8 | timeout | 14.2 |
| **PageRank** | **0.17** | 0.44 | 3.44 | 0.97 | N/A | 2.44 | 3.09 | 51.01 | – | 1.21 |
| **WCC** | **0.07** | 0.20 | 0.18 | 0.10 | N/A | 6.63 | 34.15 | 25.80 | – | 1.60 |
| **BFS** | **0.09** | 0.18 | 0.55 | 0.24 | 3.28 | timeout | 1.95 | timeout | – | 0.15 |
| **LCC** | **2.07** | 2.42 | 15.14 | N/A | N/A | 17.92 | timeout | N/A | – | 109.27 |
| **SSSP** | 0.80 | **0.34** | N/A | N/A | N/A | N/A | N/A | 36.92 | – | N/A |
| **CDLP** | **0.99** | 1.16 | N/A | N/A | N/A | N/A | N/A | 128.43 | – | 22.26 |

Notes:
- **FalkorDB** did not finish loading the 34M edges within 15 minutes on two attempts (the 5-minute limit applies), so no algorithm results. Its LSQB load (17.9M edges) took 692 s.
- **Memgraph** now loads the graph (earlier versions segfaulted or timed out), but its LCC query hung for over 45 minutes and was killed (recorded as timeout; the client's `SIGALRM` timeout cannot interrupt a blocking C call).
- \* **ArangoDB** is run on 3.11.14, not the latest release, because the driver runs PageRank, WCC, SSSP and CDLP through Pregel, which ArangoDB 3.12 and later no longer provide (only BFS works there). On 3.11.14 BFS hit the 60 s client read timeout and LCC is rejected by AQL.
- **LadybugDB**: only BFS runs. `INSTALL algo` succeeds but the downloaded `algo` extension (0.21.0) for macOS arm64 fails to load (`Library not loaded: @rpath/libnetworkit.dylib`), so PageRank, WCC and LCC are unavailable. Not worked around; to be retried when a fixed extension ships.
- **Kuzu/DuckPGQ** lack native implementations of most algorithms beyond PageRank, WCC and BFS. DuckPGQ BFS was interrupted at the time limit. HugeGraph/Vermeer SSSP is unweighted only and not run. Neo4j has no weighted SSSP or CDLP in this setup.
- Neo4j and ArcadeDB use a 12 GB heap; Docker Desktop has 32 GB. ArcadeDB Docker loads through the embedded loader first, then serves queries over HTTP (GAV built before the timed algorithms).
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
pip install -e ".[duckdb]"
cd lsqb
python3 lsqb_benchmark.py duckdb
```

### Run All Systems (Kuzu, DuckDB, Neo4j, FalkorDB, ...)

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

*Benchmarks run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS. Measured 2026-10-03, one system at a time, 12 GB heap for JVM systems, Docker Desktop with 32 GB.*

Systems and versions: see the "Systems and versions" table in the Graphalytics results above (the only place versions are listed).

Seconds. ArcadeDB Embedded (Temurin 25, compact object headers) is shown with the Graph Analytical View (OLAP) and without it (OLTP); the embedded numbers are the mean of two runs on the same loaded database. The embedded load times are from the original JDK 21 load, since the Temurin runs reused that database. The two JVM engines, ArcadeDB Docker and Neo4j, are measured warm: one untimed run, then the median of 3 (a query whose first run exceeds 30 s is reported from that single run). The other systems run each query once.

| Query | Expected Count | ArcadeDB Embedded OLAP | ArcadeDB Embedded OLTP | ArcadeDB Docker | DuckDB | Kuzu | LadybugDB | Neo4j | PostgreSQL | Memgraph | FalkorDB | Winner |
|-------|---------------|----------|----------|-----------------|--------|------|-----------|-------|------------|----------|----------|--------|
| **Load** | — | 132.1 | 127.4 | 107.8 | **0.47** | 2.44 | 2.55 | 252.0 | 8.78 | 215.5 | 692.1 | DuckDB |
| **Q1** | 221,636,419 | 0.32 | 4.21 | 0.19 | **0.12** | 4.41 | 0.31 | 8.35 | 6.05 | 60.84 | 42.45 | DuckDB |
| **Q2** | 1,085,627 | 0.20 | 5.19 | 0.19 | **0.01** | 0.12 | 0.20 | 1.69 | 0.32 | timeout | 69.18 | DuckDB |
| **Q3** | 753,570 | 0.12 | 3.32 | 0.09 | **0.03** | 2.35 | 12.34 | 15.98 | 2.01 | timeout | 4.36 | DuckDB |
| **Q4** | 14,836,038 | 0.10 | 1.13 | **0.06** | 0.07 | N/A | N/A | 8.20 | 6.03 | 4.24 | 4.13 | ArcadeDB |
| **Q5** | 13,824,510 | 0.24 | 11.86 | 0.26 | **0.04** | N/A | N/A | 6.73 | 0.64 | 3.36 | 4.38 | DuckDB |
| **Q6** | 1,668,134,320 | **0.13** | 11.51 | 0.14 | 1.74 | 1.38 | 0.69 | 41.93 | 15.09 | 127.05 | 44.57 | ArcadeDB |
| **Q7** | 26,190,133 | **0.05** | 1.06 | 0.05 | 0.09 | N/A | N/A | 12.39 | 9.96 | 4.84 | 51.05 | ArcadeDB |
| **Q8** | 6,907,213 | 0.13 | 7.57 | 0.13 | **0.07** | N/A | N/A | 14.37 | 1.64 | 3.21 | 5.83 | DuckDB |
| **Q9** | 1,596,153,418 | 1.73 | 1.02 | 2.47 | 5.68 | 6.47 | **0.07** | 240.76 | 20.09 | timeout | 190.24 | LadybugDB |

All counts reported by every system match the [official LSQB expected output](https://github.com/ldbc/lsqb/blob/main/expected-output/expected-output.csv). Kuzu and LadybugDB skip Q4/Q5/Q7/Q8 (the driver has no `:Message` supertype support yet). Memgraph times out (5 min) on Q2, Q3 and Q9. Dgraph and SurrealDB are excluded by default and were not re-measured in this run; see their sections below for the older numbers.

**Analysis:**

- **DuckDB is the fastest on 5 of 9 queries** (Q1, Q2, Q3, Q5, Q8) with the fastest load by far (0.47 s). ArcadeDB is fastest on **Q6** (0.13 s embedded vs 0.69 s for LadybugDB and 1.74 s for DuckDB), **Q7** (0.05 s vs 0.09 s for DuckDB) and, narrowly, **Q4** (0.06 s Docker, warm, vs 0.07 s for DuckDB; embedded is 0.10 s).
- **Q9** is the surprise: LadybugDB answers in 0.07 s, far ahead of ArcadeDB (1.73 s with the GAV, 1.02 s without it) and DuckDB (5.68 s). Q9 is the anti-pattern variant of the two-hop Person-KNOWS query, so LadybugDB's plan for it is much better than Kuzu's (6.47 s) despite sharing the codebase.
- **ArcadeDB OLAP vs OLTP:** the GAV makes the star and chain queries 11-58x faster (Q4 1.13 -> 0.10 s, Q5 11.86 -> 0.24 s, Q7 1.06 -> 0.05 s, Q8 7.57 -> 0.13 s), but Q9 is faster without it (1.02 s vs 1.73 s).
- **ArcadeDB Docker** (warm) is on par with embedded (within 1.5x on every query, even slightly faster on Q1, Q3 and Q4; Q9 is 2.47 s vs 1.73 s) and 9-300x faster than Neo4j on every query. A single cold run of the same server is 5-14x slower on every query, so JIT warm-up matters for server numbers.
- **Neo4j** completes all queries but is 70-3400x slower than the fastest system on every query, with Q9 at 241 s, close to the 5-minute limit.
- **PostgreSQL** is a solid middle ground: faster than Neo4j/Memgraph/FalkorDB on most queries, and the best non-ArcadeDB Docker system on Q2, Q3 and Q5.
- **FalkorDB** now returns correct counts on all 9 queries (an older release returned wrong counts on four), but it is 69-6900x slower than the fastest system and has the slowest load (692 s).
- **Memgraph** completes 6 of 9 queries; Q2, Q3 and Q9 time out.
- **LadybugDB vs Kuzu:** on the five queries both run, the fork is faster on Q1 (0.31 vs 4.41 s), Q6 (0.69 vs 1.38 s) and Q9 (0.07 vs 6.47 s), slower on Q3 (12.34 vs 2.35 s) and Q2 (0.20 vs 0.12 s).

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

## FalkorDB

FalkorDB v4.16.8 is a Redis-based graph database that supports a subset of Cypher. It is included in default LSQB runs but produces **correct results on only 4 of 9 queries**.

### Issues found

- **Wrong counts on long pattern chains** — Q1 (8-hop chain) returns 5,375 instead of the expected 221,636,419. FalkorDB's query optimizer appears to silently truncate or miscalculate intermediate results on patterns with more than ~5 hops in a single `MATCH` clause. Splitting the pattern with `WITH` partially fixes the count (133M) but still does not match the expected result. Q5, Q6, and Q9 also return incorrect counts.
- **Timeouts on complex patterns** — Q2 (diamond pattern with multi-MATCH correlation) does not complete within the 5-minute timeout. Q9 (anti-pattern with `NOT KNOWS` and inequality) also times out.
- **Very slow loading** — Loading the LSQB dataset (3.9M vertices, 17.9M edges) via Cypher `UNWIND`/`CREATE` batches takes ~655s (over 10 minutes), compared to 119s for ArcadeDB Embedded and seconds for DuckDB/Kuzu.
- **On the 4 correct queries** (Q3, Q4, Q7, Q8), FalkorDB is 89x–1475x slower than the fastest system:
  - Q3: 147.49s (vs DuckDB 0.05s — **2950x slower**)
  - Q4: 7.19s (vs ArcadeDB 0.03s — **240x slower**)
  - Q7: 10.67s (vs ArcadeDB 0.02s — **534x slower**)
  - Q8: 6.22s (vs DuckDB 0.07s — **89x slower**)

### How to run FalkorDB (LSQB)

```bash
# Start FalkorDB (Docker)
docker run -d --name falkordb-lsqb -p 6379:6379 \
  -v /tmp/falkordb_lsqb:/var/lib/falkordb/data falkordb/falkordb:latest

# Run LSQB benchmark
cd lsqb
python3 lsqb_benchmark.py falkordb
```

*Tested with FalkorDB v4.16.8 on April 2026.*

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
  lsqb_benchmark.py                # Kuzu, DuckDB, Neo4j, FalkorDB LSQB benchmarks (Python)
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
