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

#### ArcadeDB across releases

The official-framework (Mode 1) numbers per ArcadeDB release, Mode 2 and LSQB (26.8.1 against 26.10.1), and the effect of later fixes such as the WCC union-find and the restored-view wait, are in [ArcadeDB-release-progress.md](ArcadeDB-release-progress.md).

#### Systems and versions (both suites)

| System | Version | Edition | License | Mode | Overhead | Used in |
|--------|---------|---------|---------|------|----------|---------|
| **ArcadeDB** (embedded) | 26.10.1 | Open Source | Apache 2.0 | Embedded (in-process, Temurin 25) | None | Graphalytics, LSQB |
| **ArcadeDB** (Docker) | 26.10.1 | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics, LSQB |
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

#### How we measure

Every number below is a **warm** number, the way a production server runs. Each algorithm (or query) is executed once as a warm-up call that is
never reported (it pays JIT compilation, page-cache and first-touch costs), and the reported value is the **median of 3 timed runs** that follow it
(5 for the embedded ArcadeDB Graphalytics benchmark, which is itself run in 3 separate JVM launches; the median of those is shown). An operation
whose warm-up call takes longer than 60 s (30 s for LSQB) gets a single timed run instead, so it is still warm, just not repeated. Loads are one-off and not
warmed. One system at a time, 5-minute limit per operation, AC power only, 12 GB heap for every JVM system, every output validated.

**Memory** is sampled once per second while the timed operations run: the working set of the vendor's Docker containers (`docker stats`) or the
resident memory (RSS) of the vendor's process for embedded engines. Read it with care: JVM systems (ArcadeDB, Neo4j) run with a fixed 12 GB heap, so their
process or container size mostly shows that heap (embedded ArcadeDB also prints its live heap after a full GC, 0.75 GiB for Graphalytics); Kuzu, LadybugDB and DuckDB
size their buffer pools from the machine's RAM. The only exception to the warm protocol is Mode 1 (the official LDBC framework), which runs each algorithm once after its own load.

#### All Systems Comparison

Seconds (warm medians), `datagen-7_5-fb`, last row peak memory in GiB. ArcadeDB embedded and ArcadeDB Docker run on Temurin 25 with compact object headers.

**Every result is checked against the official LDBC reference outputs** (`scripts/validate_outputs.py`; BFS and CDLP exact, WCC same partition, PageRank/LCC/SSSP within 1e-4). A value marked **✗** failed that check: the system computed something other than the Graphalytics algorithm, so its time is shown for completeness but is **not comparable** and is not ranked. Bold marks the fastest *valid* result per row.

| Algorithm | ArcadeDB | ArcadeDB Docker | Neo4j | Kuzu | LadybugDB | DuckPGQ | Memgraph | ArangoDB \* | FalkorDB | HugeGraph |
|-----------|----------|----------------|-------|------|-----------|---------|----------|------------------|----------|-----------|
| **Load** | 71.9 | 43.9 | 657 | 28.8 | 5.16 | 0.85 | 437 | 726 | 116 | 34.7 |
| **PageRank** | **0.086** | 0.156 | 6.98 § | 1.16 | N/A | 1.51 ✗ | 5.49 | 93.0 | 2.67 ✗ | 2.41 |
| **WCC** | **0.004** | 0.022 | 0.111 | 0.434 | N/A | 2.00 | 189 | 40.9 | 2.50 | 0.293 |
| **BFS** | **0.022** | 0.032 | 0.480 ‡ | 0.328 | 7.89 | timeout ¶ | 3.85 | 38.4 | 0.057 | 0.195 |
| **LCC** | **2.19** | 2.65 | 15.4 | N/A | N/A | 49.3 | N/A | N/A | N/A | 110 |
| **SSSP** | **0.84** | 1.56 | N/A | N/A | N/A | N/A | 75.9 | 173 | N/A | N/A |
| **CDLP** | 1.09 ✗ | 1.08 ✗ | N/A | N/A | N/A | N/A | timeout | 254 ✗ | 10.6 ✗ | 22.3 ✗ |
| **Peak memory (GiB)** | 6.4 / 0.75 live heap | 12.3 | 13.3 | 0.87 | 1.0 | 8.3 | 25.1 | 23.2 | 8.4 | 3.2 |

- **ArcadeDB** (embedded and Docker) is valid for PageRank, WCC, BFS, LCC and SSSP. Its CDLP fails validation: the engine's `algo.labelPropagation` breaks ties and seeds labels with dense node ids instead of vertex ids, so the labels differ from the reference (the official Mode 1 framework has its own vertex-id based CDLP and passes).
- **Load** times are not like for like: ArcadeDB loads with its embedded Java loader (and, for Docker, serves over HTTP afterwards), the other server systems load through Python batches over the network. Systems whose algorithms follow the stored edge direction (Memgraph, FalkorDB, ArangoDB; HugeGraph for PageRank/BFS) load every edge in both directions, and that cost is part of their load time. FalkorDB loads with its bulk loader (`falkordb-bulk-insert`, 116 s for the 68.4M edge records; the per-query path took 53 minutes). The ArcadeDB Docker load is the time of its original load; later runs reuse the data.
- ‡ **Neo4j BFS** returns only the reached vertices (no distances), so it is checked as a reachable set, which matches the reference.
- § **Neo4j PageRank** is exact but needs two GDS runs: GDS starts every vertex at 1-d, does not normalise and counts the initialisation as the first iteration, so its scores differ from the Graphalytics reference by 0.85·A¹⁰·1. The update is linear, so the reference follows from the scores of two runs (10 and 11 iterations): rank = (20·S11 − 17·S10) / 3 / N (verified against a simulation to 3e-14, and the output validates at 100%). The timed value is those two compute-only GDS runs; the validated export streams both score sets and applies the formula.
- ¶ **DuckPGQ BFS** (undirected shortest paths from vertex 6) does not finish within the 5-minute limit: DuckDB does not honour the in-process interrupt while its shortest-path operator runs, so the run only ends after about 25 minutes.
- ✗ reasons: **PageRank** of DuckPGQ (`pagerank()` takes no iteration or damping parameter; its ranks sum to 0.90) and FalkorDB (`algo.pageRank` has no parameters; 14.9% of the vertices within 1e-4) cannot be made to run the 10-iteration Graphalytics PageRank. **CDLP**: the ArcadeDB engine breaks ties and seeds labels with dense ids (see above); ArangoDB's label propagation returns dense ids and does not match; FalkorDB and HugeGraph find the same communities as the reference but with other label values, which the exact-match rule rejects. Memgraph's `community_detection` exceeds the 5-minute limit.
- Direction handling (all drivers compute on the undirected graph, as the reference does): Kuzu and LadybugDB add a reversed edge table, Memgraph, FalkorDB and ArangoDB store both directions, HugeGraph runs PageRank and BFS on a second graph loaded from a both-direction copy of the edge file, DuckPGQ uses a symmetric edge table, Neo4j projects an undirected GDS graph. Every timed call is the exact call that is exported and validated (full per-vertex output, no `LIMIT`), except Neo4j PageRank (see §). PageRank settings that matter: Kuzu `maxIterations 11` and ArangoDB Pregel `maxGSS 11` (the initialisation counts as the first iteration), Memgraph 10 iterations, Vermeer `compute.max_step 10` with no convergence threshold.
- N/A means the engine has no implementation: LCC in Kuzu, LadybugDB, Memgraph (only a NetworkX procedure that is not installed in the MAGE image), FalkorDB and ArangoDB (the AQL query is rejected); SSSP and CDLP in most systems; HugeGraph/Vermeer SSSP is unweighted only (ArangoDB's weighted SSSP is an AQL weighted traversal, since Pregel's is unweighted). **LadybugDB**: only BFS runs, because the downloaded `algo` extension (0.21.0) for macOS arm64 fails to load (`Library not loaded: @rpath/libnetworkit.dylib`; upstream packaging bug).

Notes:
- **Docker memory:** Docker Desktop had 36 GB for every run on 2026-10-05 except HugeGraph (32 GB). Memgraph (25 GiB) and ArangoDB (23 GiB, SSSP and BFS) use the most memory. ArangoDB keeps the in-memory graph copy of every finished Pregel job until its time-to-live expires, so the driver deletes each job after it finishes; without that, repeated warm runs were killed by the out-of-memory killer at 34 GiB.
- **Memgraph** needs `vm.max_map_count` of at least 524288 in the Docker VM (Docker Desktop's default is 262144, which makes its jemalloc fail and the connection drop mid-query); the harness raises it before each start. WCC and SSSP exceed 60 s, so they are a single timed run after the warm-up call.
- **FalkorDB**: Redis's background snapshots (default save points) fork the 20+ GB process during a load, the fork fails and Redis then refuses every write; the driver turns them off and takes one synchronous `SAVE` after the load. The default `RESULTSET_SIZE` of 10000 rows silently truncates full per-vertex outputs and is lifted.
- \* **ArangoDB** is run on 3.11.14, not the latest release, because the driver runs PageRank, WCC, SSSP and CDLP through Pregel, which ArangoDB 3.12 and later no longer provide (only BFS works there). PageRank, SSSP and CDLP are a single timed run after the warm-up call (each warm-up exceeded 60 s); the run was done alone on a quiet machine on 2026-10-05.
- Neo4j and ArcadeDB use a 12 GB heap; Docker Desktop has 32 GB. ArcadeDB Docker loads through the embedded loader first, then serves queries over HTTP (see † for the first call after a restart).
- None of the competing systems have official LDBC Graphalytics platform drivers. Only ArcadeDB has an official LDBC Graphalytics platform implementation.
- Systems and versions are listed in the table above; ArcadeDB is the official 26.10.1 release (measured on the identical pre-release snapshot built on 2026-10-04). Raw logs, the harness fixes and the remaining history are in `results-multivendor-validated-2026-10-05.md` and `ArcadeDB-release-progress.md`.

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

*Benchmarks run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS, AC power. Measured 2026-10-05, one system at a time, 12 GB heap for JVM systems, Docker Desktop with 36 GB. Warm numbers: each query runs once as an untimed warm-up and the reported value is the median of 3 timed runs (a single timed run when the warm-up call took longer than 30 s); the embedded ArcadeDB numbers are the median of 3 JVM launches.*

Systems and versions: see the "Systems and versions" table in the Graphalytics results above (the only place versions are listed).

Seconds. ArcadeDB Embedded (Temurin 25, compact object headers) is shown with the Graph Analytical View (OLAP) and without it (OLTP). Load times are one-off and remembered from the original load of each database; peak memory is in GiB (process RSS for embedded engines, container working set for Docker systems; the JVM rows are dominated by the fixed 12 GB heap).

| Query | Expected Count | ArcadeDB Embedded OLAP | ArcadeDB Embedded OLTP | ArcadeDB Docker | DuckDB | Kuzu | LadybugDB | Neo4j | PostgreSQL | Memgraph | FalkorDB | Winner |
|-------|---------------|----------|----------|-----------------|--------|------|-----------|-------|------------|----------|----------|--------|
| **Load** | — | 119.5 | 159.2 | 101.7 | **0.46** | 2.44 | 2.95 | 252.0 | 15.1 | 222.2 | 659.3 | DuckDB |
| **Q1** | 221,636,419 | **0.09** | 2.83 | 0.14 | 0.11 | 4.61 | 0.12 | 4.99 | 9.17 | 58.06 | 36.43 | ArcadeDB |
| **Q2** | 1,085,627 | 0.15 | 4.54 | 0.19 | **0.01** | 0.15 | 0.10 | 1.63 | 0.65 | timeout | 53.78 | DuckDB |
| **Q3** | 753,570 | 0.07 | 3.44 | 0.08 | **0.04** | 2.30 | 10.44 | 10.76 | 1.60 | timeout | 4.59 | DuckDB |
| **Q4** | 14,836,038 | **0.04** | 1.24 | 0.05 | 0.06 | N/A | 0.17 | 6.28 | 6.35 | 4.30 | 3.53 | ArcadeDB |
| **Q5** | 13,824,510 | 0.19 | 13.59 | 0.27 | **0.04** | N/A | 0.18 | 5.66 | 2.17 | 3.54 | 3.93 | DuckDB |
| **Q6** | 1,668,134,320 | **0.13** | 12.77 | 0.18 | 1.84 | 1.38 | 0.66 | 28.49 | 15.50 | 121.15 | 40.93 | ArcadeDB |
| **Q7** | 26,190,133 | **0.05** | 1.13 | 0.06 | 0.07 | N/A | 0.41 | 8.04 | 11.16 | 4.92 | 47.96 | ArcadeDB |
| **Q8** | 6,907,213 | 0.12 | 8.60 | 0.13 | **0.07** | N/A | 0.32 | 12.39 | 3.39 | 3.02 | 4.90 | DuckDB |
| **Q9** | 1,596,153,418 | 1.78 | 0.90 | 2.32 | 6.03 | 6.39 | **0.07** | 254.29 | 50.62 | timeout | 225.22 | LadybugDB |
| **Peak memory (GiB)** | — | 6.7 | 10.1 | 12.4 | 0.9 | 5.1 | 8.8 | 13.3 | 0.5 | 3.1 | 2.4 | — |

All counts reported by every system match the [official LSQB expected output](https://github.com/ldbc/lsqb/blob/main/expected-output/expected-output.csv). Kuzu skips Q4/Q5/Q7/Q8 (its driver has no `:Message` supertype support yet; LadybugDB's driver runs each of them as a Post part plus a Comment part and adds the counts, so all nine queries are covered). Memgraph times out (5 min) on Q2, Q3 and Q9. Dgraph and SurrealDB are excluded by default (see below). The embedded ArcadeDB live heap after a full GC is 0.67 GiB (OLAP) and 1.4 GiB (OLTP).

**Analysis:**

- **DuckDB is the fastest on 4 of 9 queries** (Q2, Q3, Q5, Q8) with the fastest load by far (0.46 s). **ArcadeDB (OLAP) is fastest on 4 of 9** (Q1 0.09 s vs 0.11 s for DuckDB, Q4, Q6 0.13 s vs 0.66 s for LadybugDB and 1.84 s for DuckDB, and Q7), and **LadybugDB on Q9**.
- **Q9** is the one where LadybugDB is far ahead: 0.07 s against 1.78 s for ArcadeDB with the GAV (0.90 s without it) and 6.03 s for DuckDB. Q9 is the anti-pattern variant of the two-hop Person-KNOWS query, so LadybugDB's plan for it is much better than Kuzu's (6.39 s) despite sharing the codebase.
- **ArcadeDB OLAP vs OLTP:** the GAV makes Q1 to Q8 23-98x faster (Q5 13.59 -> 0.19 s, Q6 12.77 -> 0.13 s, Q8 8.60 -> 0.12 s), but Q9 is faster without it (0.90 s vs 1.78 s).
- **ArcadeDB Docker** is on par with embedded OLAP (within 1.5x on every query) and 8-160x faster than Neo4j on every query.
- **Neo4j** completes all queries but is 55-3900x slower than the fastest system, with Q9 at 254 s, close to the 5-minute limit.
- **PostgreSQL** is a solid middle ground: faster than Neo4j on 6 of 9 queries (Q2, Q3, Q5, Q6, Q8, Q9) and faster than Memgraph and FalkorDB on most.
- **FalkorDB** returns correct counts on all 9 queries but is 40-6900x slower than the fastest system and has the slowest load (659 s).
- **Memgraph** completes 6 of 9 queries; Q2, Q3 and Q9 time out.
- **LadybugDB vs Kuzu:** on the five queries both run, the fork is faster on Q1 (0.12 vs 4.61 s), Q2 (0.10 vs 0.15 s), Q6 (0.66 vs 1.38 s) and Q9 (0.07 vs 6.39 s), slower on Q3 (10.44 vs 2.30 s).
- **Memory:** DuckDB (0.9 GiB) and PostgreSQL (0.5 GiB) need the least; the JVM systems and Neo4j sit at the size of their fixed 12 GB heap (ArcadeDB's live heap is under 1.5 GiB).

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

FalkorDB 6.0.1 (Redis 8.10.2) is a Redis-based graph database that supports a subset of Cypher. It is included in the default Graphalytics and LSQB runs. With this release it returns **correct counts on all 9 LSQB queries** (v4.16.8, tested in April 2026, returned wrong counts on four of them), but it is slow on the multi-hop patterns (3.5-225 s per query, see the LSQB table) and has the slowest LSQB load (659 s through Cypher `UNWIND`/`CREATE` batches).

### Things the harness has to handle

- **Bulk loading** — the Graphalytics graph (68.4M edge records, both directions) loads in 116 s with `falkordb-bulk-insert` (`pip install falkordb-bulk-loader`); creating the same edges with one Cypher write query per 5000 edges runs at about 25k edges/s and took 53 minutes. The LSQB driver still uses Cypher batches.
- **Snapshots** — with the default Redis save points a background save forks the 20+ GB process during the load, the fork fails and Redis then answers every write with `MISCONF`. The drivers set `save ""` and `stop-writes-on-bgsave-error no` and take one synchronous `SAVE` after the load, so a stopped container comes back with the full graph.
- **Result sets** — `RESULTSET_SIZE` defaults to 10000 rows and silently truncates full per-vertex outputs (the validation then fails); the Graphalytics driver sets it to unlimited.
- **Readiness** — the Redis port opens while the server is still loading its snapshot (-LOADING), so the harness probes with `PING` before the driver connects.
- **Graphalytics** — `algo.pageRank` has no parameters (invalid against the 10-iteration reference), WCC and BFS validate, CDLP finds the reference communities with other label values; no LCC or SSSP.

### How to run FalkorDB (LSQB)

```bash
# Start FalkorDB (Docker)
docker run -d --name falkordb-lsqb -p 6379:6379 \
  -v /tmp/falkordb_lsqb:/var/lib/falkordb/data falkordb/falkordb:latest

# Run LSQB benchmark
cd lsqb
python3 lsqb_benchmark.py falkordb
```

*Tested with FalkorDB 6.0.1 on October 2026.*

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
