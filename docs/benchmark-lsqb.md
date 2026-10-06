# Mode 3: LSQB (Labelled Subgraph Query Benchmark)

The [LSQB benchmark](https://github.com/ldbc/lsqb) is a lightweight microbenchmark from the LDBC council that focuses on **subgraph pattern matching** — counting how many times a given labelled graph pattern appears in the dataset. It tests the query optimizer's ability to handle multi-way joins, anti-patterns (NOT EXISTS), and type hierarchy (Message supertype with Post/Comment subtypes).

The benchmark uses the LDBC SNB social network dataset (SF1: ~3.9M vertices, ~17.9M edges) and runs 9 Cypher queries (Q1–Q9) covering patterns from simple 2-hop paths to complex 8-hop chains and triangle patterns.

## Dataset

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

## Run ArcadeDB (Java, embedded)

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

## Run DuckDB (Python)

```bash
pip install -e ".[duckdb]"
cd lsqb
python3 lsqb_benchmark.py duckdb
```

## Run All Systems (Kuzu, DuckDB, Neo4j, FalkorDB, ...)

```bash
cd lsqb
python3 lsqb_benchmark.py              # Run all systems
python3 lsqb_benchmark.py --reset      # Delete all data and reload
python3 lsqb_benchmark.py kuzu duckdb  # Run specific systems only
```

## LSQB Queries

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

## LSQB Results

Dataset: **LDBC SNB SF1** (3,947,829 vertices, 17,882,623 edges)

*Benchmarks run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS, AC power. Measured 2026-10-05, one system at a time, 12 GB heap for JVM systems, Docker Desktop with 36 GB. Warm numbers: each query runs once as an untimed warm-up and the reported value is the median of 3 timed runs (a single timed run when the warm-up call took longer than 30 s); the embedded ArcadeDB numbers are the median of 3 JVM launches.*

Systems and versions: see the "Systems and versions" table in [benchmark-graphalytics-multivendor.md](benchmark-graphalytics-multivendor.md#systems-and-versions-both-suites) (the only place versions are listed).

Seconds. ArcadeDB Embedded (Temurin 25, compact object headers) is shown with the Graph Analytical View (OLAP) and without it (OLTP). Load times are one-off and remembered from the original load of each database; peak memory is in GiB (process RSS for embedded engines, container working set for Docker systems; the JVM rows are dominated by the fixed 12 GB heap).

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

All counts reported by every system match the [official LSQB expected output](https://github.com/ldbc/lsqb/blob/main/expected-output/expected-output.csv). Kuzu skips Q4/Q5/Q7/Q8 (its driver has no `:Message` supertype support yet; LadybugDB's driver runs each of them as a Post part plus a Comment part and adds the counts, so all nine queries are covered). Memgraph times out (5 min) on Q2, Q3 and Q9. Dgraph and SurrealDB are excluded by default (see below). The embedded ArcadeDB live heap after a full GC is 0.67 GiB (OLAP) and 1.4 GiB (OLTP).

**Analysis:**

- **DuckDB is the fastest on 4 of 9 queries** (Q2, Q3, Q5, Q8) with the fastest load by far (0.46 s). **ArcadeDB (OLAP) is fastest on 4 of 9** (Q1 0.08 s vs 0.11 s for DuckDB, Q4, Q6 0.07 s vs 0.66 s for LadybugDB and 1.84 s for DuckDB, and Q7), and **LadybugDB on Q9**.
- **Q9** is the one where LadybugDB is far ahead: 0.07 s against 0.30 s for ArcadeDB with the GAV (0.87 s without it; 1.78 s on 26.10.1 before [#9282](https://github.com/ArcadeData/arcadedb/issues/9282)) and 6.03 s for DuckDB. Q9 is the anti-pattern variant of the two-hop Person-KNOWS query, so LadybugDB's plan for it is much better than Kuzu's (6.39 s) despite sharing the codebase.
- **ArcadeDB OLAP vs OLTP:** the GAV makes Q1 to Q8 26-157x faster (Q5 11.39 -> 0.17 s, Q6 11.00 -> 0.07 s, Q8 6.91 -> 0.11 s) and, since [#9282](https://github.com/ArcadeData/arcadedb/issues/9282), Q9 too (0.87 -> 0.30 s; on 26.10.1 it was faster without the GAV, 0.90 s vs 1.78 s).
- **ArcadeDB Docker** is on par with embedded OLAP (within 2.2x on every query) and 9-770x faster than Neo4j on every query.
- **Neo4j** completes all queries but is 55-3900x slower than the fastest system, with Q9 at 254 s, close to the 5-minute limit.
- **PostgreSQL** is a solid middle ground: faster than Neo4j on 6 of 9 queries (Q2, Q3, Q5, Q6, Q8, Q9) and faster than Memgraph and FalkorDB on most.
- **FalkorDB** returns correct counts on all 9 queries but is 40-6900x slower than the fastest system and has the slowest load (659 s).
- **Memgraph** completes 6 of 9 queries; Q2, Q3 and Q9 time out.
- **LadybugDB vs Kuzu:** on the five queries both run, the fork is faster on Q1 (0.12 vs 4.61 s), Q2 (0.10 vs 0.15 s), Q6 (0.66 vs 1.38 s) and Q9 (0.07 vs 6.39 s), slower on Q3 (10.44 vs 2.30 s).
- **Memory:** DuckDB (0.9 GiB) and PostgreSQL (0.5 GiB) need the least; the JVM systems and Neo4j sit at the size of their fixed 12 GB heap (ArcadeDB's live heap is under 1.5 GiB).
