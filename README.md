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

Seconds, last column peak memory in GiB. Bold marks the fastest *valid* result per column. Systems, versions, how we measure, per-system notes and how to run each vendor: [detailed page](docs/benchmark-graphalytics-multivendor.md).

| System | Load | PageRank | WCC | BFS | LCC | SSSP | CDLP | Peak memory (GiB) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ArcadeDB embedded | 71.9 | **0.085** | **0.003** | **0.020** | **2.05** | **0.75** | **0.96** | 5.6 |
| ArcadeDB Docker | 43.9 | 0.16 | 0.02 | 0.06 | 2.61 | 1.39 | 1.45 | 12.3 |
| Neo4j | 657 | 6.98§ | 0.111 | 0.480‡ | 15.4 | N/A | N/A | 13.3 |
| Kuzu | 28.8 | 1.16 | 0.434 | 0.328 | N/A | N/A | N/A | 0.87 |
| LadybugDB | 5.16 | N/A | N/A | 7.89 | N/A | N/A | N/A | 1.0 |
| DuckPGQ | 0.85 | 1.51✗ | 2.00 | timeout¶ | 49.3 | N/A | N/A | 8.3 |
| Memgraph | 437 | 5.49 | 189 | 3.85 | N/A | 75.9 | timeout | 25.1 |
| ArangoDB \* | 726 | 93.0 | 40.9 | 38.4 | N/A | 173 | 254✗ | 23.2 |
| FalkorDB | 116 | 2.67✗ | 2.50 | 0.057 | N/A | N/A | 10.6✗ | 8.4 |
| HugeGraph | 34.7 | 2.41 | 0.293 | 0.195 | 110 | N/A | 22.3✗ | 3.2 |

### Graphalytics, `graph500-22` (2,396,657 vertices, 64,155,735 edges)

Seconds; no SSSP (not defined for this dataset). ArcadeDB is `26.11.1-SNAPSHOT`; bold marks the fastest *valid* result per column. Per-system notes, the harness problems found and how to reproduce: [detailed page](docs/benchmark-graphalytics-graph500-22.md).

| System | Load | PageRank | WCC | BFS | LCC | CDLP | Peak memory (GiB) |
|---|---:|---:|---:|---:|---:|---:|---:|
| ArcadeDB embedded | 114.5 | **0.268** | **0.013** | **0.076** | **46.9** | **1.78** | 1.6\*\* |
| ArcadeDB Docker | 101† | 0.59 | 0.09 | 0.24 | 60.1 | 4.33 | 13.5 |
| Neo4j | 2017† | 12.5 | 0.20 | 1.05 | timeout | N/A | 13.3 |
| Kuzu | 53.8† | 3.13 | 1.26 | 0.94 | N/A | N/A | 5.9 |
| LadybugDB | 10.1† | N/A | N/A | 16.7 | N/A | N/A | 2.0 |
| DuckPGQ | 0.57† | 9.27✗ | 4.94 | timeout‡ | timeout | N/A | 15.7 |
| Memgraph | 932§ | 31.0 | OOM | 12.5 | N/A | OOM | 25.0 |
| ArangoDB 3.11.14 | 1700† | 261.9 | 106.4 | OOM¶ | N/A | timeout | 30.6 |
| FalkorDB | 348.5 | 7.97✗ | 7.30 | 0.151 | N/A | 39.8✗ | 15.9 |
| HugeGraph | 34.2† | 6.03 | 0.67 | 0.42 | timeout | 44.7✗ | 8.8 |

OOM = out of memory (Docker Desktop's 32 GB). † remembered original load, ‡ DuckPGQ BFS does not finish (ended after 20 min), § Memgraph load includes the reverse-edge step, ¶ ArangoDB container ran out of memory, \*\* live heap after GC (process peak not measured): see the detailed page.

### LSQB SF1 (3,947,829 vertices, 17,882,623 edges)

Seconds; ArcadeDB embedded is shown with the Graph Analytical View (OLAP) and without it (OLTP); bold marks the fastest result per query. All counts match the official expected output. Queries, run commands and analysis: [detailed page](docs/benchmark-lsqb.md).

| System | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 | Peak GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ArcadeDB OLAP | 119.5 | **0.08** | 0.13 | 0.05 | **0.04** | 0.17 | **0.07** | **0.04** | 0.11 | 0.30 | 4.3 |
| ArcadeDB OLTP | 159.2 | 2.45 | 4.40 | 3.17 | 1.06 | 11.39 | 11.00 | 1.07 | 6.91 | 0.87 | 12.0 |
| ArcadeDB Docker | 99.4 | 0.17 | 0.17 | 0.06 | 0.05 | 0.25 | 0.13 | 0.06 | 0.14 | 0.33 | 12.9 |
| DuckDB | **0.46** | 0.11 | **0.01** | **0.04** | 0.06 | **0.04** | 1.84 | 0.07 | **0.07** | 6.03 | 0.9 |
| Kuzu | 2.44 | 4.61 | 0.15 | 2.30 | N/A | N/A | 1.38 | N/A | N/A | 6.39 | 5.1 |
| LadybugDB | 2.95 | 0.12 | 0.10 | 10.44 | 0.17 | 0.18 | 0.66 | 0.41 | 0.32 | **0.07** | 8.8 |
| Neo4j | 252.0 | 4.99 | 1.63 | 10.76 | 6.28 | 5.66 | 28.49 | 8.04 | 12.39 | 254.29 | 13.3 |
| PostgreSQL | 15.1 | 9.17 | 0.65 | 1.60 | 6.35 | 2.17 | 15.50 | 11.16 | 3.39 | 50.62 | 0.5 |
| Memgraph | 222.2 | 58.06 | timeout | timeout | 4.30 | 3.54 | 121.15 | 4.92 | 3.02 | timeout | 3.1 |
| FalkorDB | 659.3 | 36.43 | 53.78 | 4.59 | 3.53 | 3.93 | 40.93 | 47.96 | 4.90 | 225.22 | 2.4 |

## More

- [docs/benchmark-graphalytics-official.md](docs/benchmark-graphalytics-official.md), [docs/benchmark-graphalytics-multivendor.md](docs/benchmark-graphalytics-multivendor.md), [docs/benchmark-graphalytics-graph500-22.md](docs/benchmark-graphalytics-graph500-22.md), [docs/benchmark-lsqb.md](docs/benchmark-lsqb.md): one page per benchmark (how to run, method, full tables, notes).
- [docs/vendor-notes.md](docs/vendor-notes.md): SurrealDB and Dgraph (excluded from default runs, see also [SURREALDB.md](SURREALDB.md)) and FalkorDB.
- [docs/architecture.md](docs/architecture.md): file structure, the Graph Analytical View (CSR) engine, execution modes.
- [ArcadeDB-release-progress.md](ArcadeDB-release-progress.md): ArcadeDB across releases. `results-*.md`: raw results and history of earlier runs. `CLAUDE.md`: benchmark rules for the harness.

## License

Apache License, Version 2.0
