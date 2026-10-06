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

## Benchmarks and results

Every number is a **warm** number (first call discarded, median of 3 timed runs), measured one system at a time on AC power and **validated against the official LDBC reference outputs** (or the official expected counts for LSQB). A value marked **✗** failed that check and is not ranked; **N/A** = no implementation; **timeout** = the 5-minute limit per operation. Footnote symbols (§ ‡ ¶ \*) are explained on the detailed pages. Machine: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB RAM; ArcadeDB on Eclipse Temurin 25 with `-XX:+UseCompactObjectHeaders`, 12 GB heap for every JVM system, Docker Desktop with 32 GB.

| Benchmark | What it measures | Detailed page |
|---|---|---|
| Graphalytics, official framework (Mode 1) | ArcadeDB only, official LDBC harness, one graph load per algorithm | [docs/benchmark-graphalytics-official.md](docs/benchmark-graphalytics-official.md) |
| Graphalytics, multi-vendor (Mode 2), `datagen-7_5-fb` | 6 algorithms, 10 systems, 633K vertices / 34M edges | [docs/benchmark-graphalytics-multivendor.md](docs/benchmark-graphalytics-multivendor.md) |
| Graphalytics, multi-vendor, `graph500-22` | 5 algorithms, 10 systems, 2.4M vertices / 64M edges | [docs/benchmark-graphalytics-graph500-22.md](docs/benchmark-graphalytics-graph500-22.md) |
| LSQB SF1 (Mode 3) | 9 subgraph pattern matching queries, 7+ systems | [docs/benchmark-lsqb.md](docs/benchmark-lsqb.md) |

How ArcadeDB itself changes from release to release (official framework, Mode 2 and LSQB): [ArcadeDB-release-progress.md](ArcadeDB-release-progress.md).

### Graphalytics, `datagen-7_5-fb` (633,432 vertices, 34,185,747 edges)

Seconds, last row peak memory in GiB. Bold marks the fastest *valid* result per row. Systems, versions, how we measure, per-system notes and how to run each vendor: [detailed page](docs/benchmark-graphalytics-multivendor.md).

| Algorithm | ArcadeDB | ArcadeDB Docker | Neo4j | Kuzu | LadybugDB | DuckPGQ | Memgraph | ArangoDB \* | FalkorDB | HugeGraph |
|-----------|----------|----------------|-------|------|-----------|---------|----------|------------------|----------|-----------|
| **Load** | 71.9 | 43.9 | 657 | 28.8 | 5.16 | 0.85 | 437 | 726 | 116 | 34.7 |
| **PageRank** | **0.085** | 0.16 | 6.98 § | 1.16 | N/A | 1.51 ✗ | 5.49 | 93.0 | 2.67 ✗ | 2.41 |
| **WCC** | **0.003** | 0.02 | 0.111 | 0.434 | N/A | 2.00 | 189 | 40.9 | 2.50 | 0.293 |
| **BFS** | **0.020** | 0.06 | 0.480 ‡ | 0.328 | 7.89 | timeout ¶ | 3.85 | 38.4 | 0.057 | 0.195 |
| **LCC** | **2.05** | 2.61 | 15.4 | N/A | N/A | 49.3 | N/A | N/A | N/A | 110 |
| **SSSP** | **0.75** | 1.39 | N/A | N/A | N/A | N/A | 75.9 | 173 | N/A | N/A |
| **CDLP** | **0.96** | 1.45 | N/A | N/A | N/A | N/A | timeout | 254 ✗ | 10.6 ✗ | 22.3 ✗ |
| **Peak memory (GiB)** | 5.6 | 12.3 | 13.3 | 0.87 | 1.0 | 8.3 | 25.1 | 23.2 | 8.4 | 3.2 |

### Graphalytics, `graph500-22` (2,396,657 vertices, 64,155,735 edges)

Seconds; no SSSP (not defined for this dataset). ArcadeDB is `26.11.1-SNAPSHOT`. Per-system notes, the harness problems found and how to reproduce: [detailed page](docs/benchmark-graphalytics-graph500-22.md).

| Vendor | Load | PageRank | WCC | LCC | BFS | CDLP | Peak memory (GiB) |
|---|---|---|---|---|---|---|---|
| ArcadeDB embedded (26.11.1-SNAPSHOT) | 114.5 | **0.268** | **0.013** | **46.9** | **0.076** | **1.78** | 1.6 live heap \*\* |
| ArcadeDB Docker (26.11.1-SNAPSHOT) | 101 † | 0.59 | 0.09 | 60.1 | 0.24 | 4.33 | 13.5 |
| Neo4j (GDS) | 2017 † | 12.5 | 0.20 | timeout | 1.05 | N/A | 13.3 |
| Kuzu | 53.8 † | 3.13 | 1.26 | N/A | 0.94 | N/A | 5.9 |
| LadybugDB | 10.1 † | N/A | N/A | N/A | 16.7 | N/A | 2.0 |
| DuckPGQ | 0.57 † | 9.27 ✗ | 4.94 | timeout | exceeds the limit ‡ | N/A | 15.7 |
| Memgraph | 932 § | 31.0 | out of memory | N/A | 12.5 | out of memory | 25.0 |
| ArangoDB 3.11.14 | 1700 † | 261.9 | 106.4 | N/A | out of memory ¶ | timeout | 30.6 |
| FalkorDB | 348.5 | 7.97 ✗ | 7.30 | N/A | 0.151 | 39.8 ✗ | 15.9 |
| HugeGraph (Vermeer) | 34.2 † | 6.03 | 0.67 | timeout | 0.42 | 44.7 ✗ | 8.8 |

† remembered original load, ‡ DuckPGQ BFS does not finish, § Memgraph load includes the reverse-edge step, ¶ ArangoDB container ran out of memory, \*\* live heap after GC (process peak not measured): see the detailed page.

### LSQB SF1 (3,947,829 vertices, 17,882,623 edges)

Seconds; ArcadeDB embedded is shown with the Graph Analytical View (OLAP) and without it (OLTP). All counts match the official expected output. Queries, run commands and analysis: [detailed page](docs/benchmark-lsqb.md).

| Query | Expected Count | ArcadeDB Embedded OLAP | ArcadeDB Embedded OLTP | ArcadeDB Docker | DuckDB | Kuzu | LadybugDB | Neo4j | PostgreSQL | Memgraph | FalkorDB | Winner |
|-------|---------------|----------|----------|-----------------|--------|------|-----------|-------|------------|----------|----------|--------|
| **Load** | — | 119.5 | 159.2 | 99.4 | **0.46** | 2.44 | 2.95 | 252.0 | 15.1 | 222.2 | 659.3 | DuckDB |
| **Q1** | 221,636,419 | **0.08** | 2.45 | 0.17 | 0.11 | 4.61 | 0.12 | 4.99 | 9.17 | 58.06 | 36.43 | ArcadeDB |
| **Q2** | 1,085,627 | 0.13 | 4.40 | 0.17 | **0.01** | 0.15 | 0.10 | 1.63 | 0.65 | timeout | 53.78 | DuckDB |
| **Q3** | 753,570 | 0.05 | 3.17 | 0.06 | **0.04** | 2.30 | 10.44 | 10.76 | 1.60 | timeout | 4.59 | DuckDB |
| **Q4** | 14,836,038 | **0.04** | 1.06 | 0.05 | 0.06 | N/A | 0.17 | 6.28 | 6.35 | 4.30 | 3.53 | ArcadeDB |
| **Q5** | 13,824,510 | 0.17 | 11.39 | 0.25 | **0.04** | N/A | 0.18 | 5.66 | 2.17 | 3.54 | 3.93 | DuckDB |
| **Q6** | 1,668,134,320 | **0.07** | 11.00 | 0.13 | 1.84 | 1.38 | 0.66 | 28.49 | 15.50 | 121.15 | 40.93 | ArcadeDB |
| **Q7** | 26,190,133 | **0.04** | 1.07 | 0.06 | 0.07 | N/A | 0.41 | 8.04 | 11.16 | 4.92 | 47.96 | ArcadeDB |
| **Q8** | 6,907,213 | 0.11 | 6.91 | 0.14 | **0.07** | N/A | 0.32 | 12.39 | 3.39 | 3.02 | 4.90 | DuckDB |
| **Q9** | 1,596,153,418 | 0.30 | 0.87 | 0.33 | 6.03 | 6.39 | **0.07** | 254.29 | 50.62 | timeout | 225.22 | LadybugDB |
| **Peak memory (GiB)** | — | 4.3 | 12.0 | 12.9 | 0.9 | 5.1 | 8.8 | 13.3 | 0.5 | 3.1 | 2.4 | — |

## More

- [docs/benchmark-graphalytics-official.md](docs/benchmark-graphalytics-official.md), [docs/benchmark-graphalytics-multivendor.md](docs/benchmark-graphalytics-multivendor.md), [docs/benchmark-graphalytics-graph500-22.md](docs/benchmark-graphalytics-graph500-22.md), [docs/benchmark-lsqb.md](docs/benchmark-lsqb.md): one page per benchmark (how to run, method, full tables, notes).
- [docs/vendor-notes.md](docs/vendor-notes.md): SurrealDB and Dgraph (excluded from default runs, see also [SURREALDB.md](SURREALDB.md)) and FalkorDB.
- [docs/architecture.md](docs/architecture.md): file structure, the Graph Analytical View (CSR) engine, execution modes.
- [ArcadeDB-release-progress.md](ArcadeDB-release-progress.md): ArcadeDB across releases. `results-*.md`: raw results and history of earlier runs. `CLAUDE.md`: benchmark rules for the harness.

## License

Apache License, Version 2.0
