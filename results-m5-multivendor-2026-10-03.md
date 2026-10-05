# Multi-vendor re-run and Java 21 vs Java 25 (MacBook M5 Pro, 2026-10-03)

Replaces the Intel i9 (2019) / ArcadeDB 26.4.1 multi-vendor measurements. Same machine for everything: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB RAM; ArcadeDB `26.10.1-SNAPSHOT` (main @ `02ac27327d`); 12 GB heap for every JVM system; Docker Desktop with 32 GB; one process at a time; 5-minute limit per operation. The headline tables are in `README.md`; this file holds the raw detail and the decisions taken during the unattended run.

## Java 21 vs Java 25

Only one JDK 25 was available: **Oracle GraalVM 25.0.3**, whose default JIT is Graal (not HotSpot C2). These first Java 25 numbers are GraalVM results only; the Temurin 25 vs GraalVM 25 comparison further down covers the stock HotSpot JDK. `-XX:+UseCompactObjectHeaders` is **off by default** in Java 25 (verified with `-XX:+PrintFlagsFinal`), so it was passed explicitly. ArcadeDB's `server.sh` enables it automatically only when the JVM supports it; the embedded benchmarks and the Mode 1 runner do not go through `server.sh`.

### Mode 2 embedded (datagen-7_5-fb), seconds, median of 3 runs (range for noisy columns)

| Variant | runs | Load | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|---|
| OpenJDK 21 (HotSpot C2) | 3 | 49.30 (49.19-49.87) | 0.17 | 0.06 | 0.07 | 2.16 (2.08-3.54) | 0.88 | 1.00 |
| GraalVM 25 (Graal JIT) | 3 | 70.10 (67.49-97.23) | 0.99 | 0.47 | 0.54 | 11.01 (3.80-11.14) | 3.03 | 7.33 |
| GraalVM 25, C2 (-XX:-UseJVMCICompiler) | 3 | 52.79 (48.30-138.08) | 0.19 | 0.06 | 0.08 | 5.13 (5.00-5.30) | 1.59 | 1.37 |
| GraalVM 25 Graal JIT + CompactObjectHeaders | 3 | 110.67 (47.27-127.23) | 0.20 | 0.09 | 0.18 | 4.51 (4.29-4.78) | 1.70 | 1.67 |
| GraalVM 25 C2 + CompactObjectHeaders | 3 | 112.51 (55.17-113.51) | 0.20 | 0.07 | 0.08 | 5.32 (2.03-10.61) | 1.82 | 1.35 |

Findings: OpenJDK 21 is the fastest and by far the most stable. The Graal JIT was 3-5x slower on the algorithms and erratic. Forcing C2 on GraalVM 25 restores PR/WCC/BFS but LCC stays about 2x slower than on Java 21 (5.1 s vs 2.2 s). Compact object headers showed no consistent gain. Load time swings between about 48 s and 138 s for the *same* configuration, so load differences between variants are noise.

### LSQB embedded (ArcadeDB), seconds, two runs each

OLAP (GAV) results were identical within noise on OpenJDK 21 and GraalVM 25 with C2, with and without compact headers. The Graal JIT was slower on Q6 (0.75-0.83 s vs 0.15-0.18 s) and Q9 (3.5-3.7 s vs 1.9-2.4 s). OLTP (no GAV) was flat across all four variants (Q1 3.1-4.8 s, Q5 12.3-13.7 s, Q6 12.6-14.4 s). Raw output is not kept in the repository.

### Mode 1 (official framework), processing_time seconds, one run per cell, all validated

| Algorithm | OLAP JDK21 | OLAP GraalVM25 | OLAP GraalVM25 compact | OLTP JDK21 | OLTP GraalVM25 | OLTP GraalVM25 compact |
|---|---|---|---|---|---|---|
| PR | 2.99 | 2.87 | 7.36 | 41.3 | 139.9 | 39.3 |
| BFS | 8.99 | 9.27 | 12.6 | 141.0 | 85.2 | 80.0 |
| WCC | 3.55 | 15.5 | 8.51 | 118.9 | 224.2 | 68.1 |
| CDLP | 12.8 | 80.8 | 25.0 | 57.2 | 66.0 | 51.5 |
| LCC | 8.71 | 21.5 | 11.8 | 310.4 | 200.1 | 166.2 |
| SSSP | 6.42 | 24.3 | 9.74 | 67.8 | 55.8 | 45.5 |

These single runs are too noisy to rank the JVMs (the same OLTP JDK 21 build gave LCC 177 s earlier the same day and 310 s here; the OLAP WCC/BFS spikes match the known bulk-UPDATE free-space-scan behaviour, ArcadeDB #8660). Mode 1 was not repeated; medians of several runs would be needed before drawing any conclusion. Runner heap was 12 GB (executor 4 GB, runner defaults to 3x the executor).

## Temurin 25 vs GraalVM 25 (ArcadeDB only, both with `-XX:+UseCompactObjectHeaders`)

Temurin 25.0.4.1+1 (HotSpot, C2) against Oracle GraalVM 25.0.3 (Graal JIT, its default), same machine, same JAR, 12 GB heap, runs interleaved. Mode 2: 3 runs each (median); LSQB: 2 runs each on the loaded databases; Mode 1: 2 passes with the JVM order swapped on the second pass (mean, min-max in brackets).

**Mode 2 embedded, seconds (median of 3):**

| JVM | Load | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|
| Temurin 25 | 50.4 | 0.17 | 0.07 | 0.09 | 2.07 | 0.80 | 0.99 |
| GraalVM 25 | 45.3 | 0.24 | 0.17 | 0.17 | 2.18 | 0.94 | 1.20 |

**LSQB embedded, seconds (mean of 2):**

| | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|
| OLAP Temurin | 0.32 | 0.20 | 0.12 | 0.10 | 0.24 | 0.13 | 0.05 | 0.13 | 1.73 |
| OLAP GraalVM | 0.36 | 0.21 | 0.18 | 0.07 | 0.21 | 0.57 | 0.06 | 0.13 | 2.35 |
| OLTP Temurin | 4.21 | 5.19 | 3.32 | 1.13 | 11.86 | 11.51 | 1.06 | 7.57 | 1.02 |
| OLTP GraalVM | 4.37 | 4.72 | 3.60 | 1.05 | 11.57 | 11.36 | 0.95 | 8.00 | 1.21 |

**Mode 1, processing time in seconds (mean of 2 passes):**

| Algorithm | OLAP Temurin | OLAP GraalVM | OLTP Temurin | OLTP GraalVM |
|---|---|---|---|---|
| PR | 2.9 (2.8-2.9) | 4.5 (4.2-4.9) | 39.7 (35.2-44.2) | 41.0 (36.8-45.1) |
| BFS | 8.3 (8.1-8.5) | 8.1 (7.9-8.4) | 85.6 (79.8-91.4) | 87.7 (82.2-93.3) |
| WCC | 3.4 (3.3-3.5) | 4.0 (3.7-4.2) | 74.9 (69.4-80.3) | 78.9 (72.2-85.5) |
| CDLP | 11.6 (11.1-12.0) | 14.2 (13.2-15.1) | 48.0 (42.3-53.6) | 49.5 (42.4-56.6) |
| LCC | 5.0 (4.9-5.1) | 5.8 (4.7-6.9) | 168.1 (159.6-176.5) | 168.7 (159.4-178.0) |
| SSSP | 6.3 (6.2-6.5) | 6.7 (6.2-7.3) | 41.2 (37.7-44.8) | 45.5 (42.4-48.6) |

Findings: Temurin is equal or faster almost everywhere, and the gap is clearest in the OLAP paths, where the vectorised (Vector API) algorithms run: Mode 2 WCC and BFS about 2x faster (0.07 vs 0.17 s, 0.09 vs 0.17 s) and PR 1.4x; Mode 1 OLAP PR 1.5x, CDLP 1.2x; LSQB OLAP Q6 4.4x (0.13 vs 0.57 s) and Q9 1.4x. OLTP results (disk and object-graph bound) are within noise on both JVMs, in LSQB and in Mode 1 (the ranges overlap in every cell). GraalVM loads about 10% faster in Mode 2 but the load figure varies by that much between runs anyway. The runs were stable this time (the erratic multi-second swings seen in the earlier GraalVM runs did not recur), so the difference looks like the JIT, not noise. Compared with OpenJDK 21 from earlier the same day, Temurin 25 with compact headers is equal within noise (Mode 2 median PR 0.17, WCC 0.06, BFS 0.07, LCC 2.16 on JDK 21), i.e. no measurable gain from Java 25 or from compact headers on these workloads.

**Policy from here on: ArcadeDB benchmarks run on Eclipse Temurin 25 with `-XX:+UseCompactObjectHeaders` only; GraalVM is never used again.** The multi-vendor tables in `README.md` use the Temurin 25 values below (Mode 2 and LSQB embedded). The GraalVM numbers in this file are kept as a historical comparison only.

## Summary: JDK 21 vs GraalVM 25 vs Temurin 25 (ArcadeDB, same machine, 12 GB heap)

JDK 21 = Homebrew OpenJDK 21 (HotSpot C2, no compact headers: not available there). GraalVM 25 and Temurin 25 both run with `-XX:+UseCompactObjectHeaders`; GraalVM keeps its default Graal JIT. Mode 2: median of 3 runs; LSQB: mean of 2 runs on the loaded databases; Mode 1: mean of 2 passes for the Java 25 columns, but **one run** for JDK 21.

**Mode 2 embedded (Graphalytics), seconds**

| | Load | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|
| JDK 21 | 49.3 | 0.17 | 0.06 | 0.07 | 2.16 | 0.88 | 1.00 |
| GraalVM 25 | 45.3 | 0.24 | 0.17 | 0.17 | 2.18 | 0.94 | 1.20 |
| Temurin 25 | 50.4 | 0.17 | 0.07 | 0.09 | 2.07 | 0.80 | 0.99 |

**LSQB embedded OLAP (GAV), seconds**

| | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|
| JDK 21 | 0.36 | 0.22 | 0.14 | 0.10 | 0.33 | 0.17 | 0.10 | 0.19 | 2.29 |
| GraalVM 25 | 0.35 | 0.21 | 0.18 | 0.07 | 0.21 | 0.57 | 0.06 | 0.12 | 2.35 |
| Temurin 25 | 0.32 | 0.20 | 0.11 | 0.10 | 0.23 | 0.13 | 0.05 | 0.12 | 1.73 |

**LSQB embedded OLTP (no GAV), seconds**

| | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|
| JDK 21 | 3.50 | 5.79 | 4.52 | 1.40 | 13.25 | 13.89 | 1.38 | 10.00 | 1.22 |
| GraalVM 25 | 4.37 | 4.72 | 3.59 | 1.04 | 11.56 | 11.36 | 0.95 | 8.00 | 1.21 |
| Temurin 25 | 4.21 | 5.19 | 3.32 | 1.12 | 11.86 | 11.51 | 1.06 | 7.57 | 1.02 |

**Mode 1 (official framework), processing time in seconds**

| Algorithm | OLAP JDK 21 | OLAP GraalVM 25 | OLAP Temurin 25 | OLTP JDK 21 | OLTP GraalVM 25 | OLTP Temurin 25 |
|---|---|---|---|---|---|---|
| PR | 3.0 | 4.5 | 2.9 | 41.3 | 41.0 | 39.7 |
| BFS | 9.0 | 8.1 | 8.3 | 141.0 | 87.7 | 85.6 |
| WCC | 3.6 | 4.0 | 3.4 | 118.9 | 78.9 | 74.9 |
| CDLP | 12.8 | 14.2 | 11.6 | 57.2 | 49.5 | 48.0 |
| LCC | 8.7 | 5.8 | 5.0 | 310.4 | 168.7 | 168.1 |
| SSSP | 6.4 | 6.7 | 6.3 | 67.8 | 45.5 | 41.2 |

Caveat for the Mode 1 JDK 21 column: it is a single run, and its OLTP cells (BFS 141 s, WCC 119 s, LCC 310 s, SSSP 68 s) are far above what the same JDK 21 build measured earlier the same day (OLTP BFS 91 s, WCC 76 s, CDLP 70 s, LCC 177 s, SSSP 55 s; OLAP PR 2.6, BFS 8.3, WCC 3.9, CDLP 14.7, LCC 6.2, SSSP 7.0), so those OLTP cells reflect machine noise, not the JVM. Against the earlier JDK 21 run, both Java 25 columns are equal within noise.

## Check of the dataset label on arcadedb.com/benchmarks.html

The page's Graphalytics chart is labelled `graph500-22` (2,396,657 vertices, 64,155,735 edges, unweighted), but its numbers come from `datagen-7_5-fb` (633,432 vertices, 34,185,747 edges, weighted):

- The official `graph500-22` supports only BFS, CDLP, LCC, PR and WCC (`graph500-22.properties`: `algorithms = bfs, cdlp, lcc, pr, wcc`, no edge weights, no SSSP reference output), yet the chart shows SSSP for ArcadeDB (0.92 s embedded, 0.41 s Docker) and ArangoDB (33.36 s).
- Measured here on the real `graph500-22` (ArcadeDB embedded, Temurin 25 + compact headers, 12 GB heap, GAV): load 144.8 s, PR 0.31 s, WCC 0.54 s, BFS 0.17 s, **LCC 44.3 s**, CDLP 4.46 s (SSSP not defined). The page shows LCC 2.35 s and CDLP 1.11 s, which matches `datagen-7_5-fb` (our 2026-10-02 run: LCC 2.345 s, CDLP 1.13 s), not `graph500-22`.
- Competitor values on the page also match `datagen-7_5-fb` runs: Kuzu PR 0.97 s / WCC 0.10 s equal our measurements on that graph; HugeGraph 1.23 / 2.00 / 0.17 / 122 / 23.9 s is close to ours (1.21 / 1.60 / 0.15 / 109 / 22.3 s).

Conclusion: the chart label should read `datagen-7_5-fb`, or the numbers should be re-measured on `graph500-22` (the harness would then need an unweighted load path; the Java loader assumes a weight column). The page's own "Reproduce" snippet also uses `./run-benchmark.sh graph500-22`, which does not exist in this repository.

## ArcadeDB Docker and Neo4j on LSQB, warm re-measure (2026-10-03)

The first Docker run used a single cold query per query (new JVM, no warm-up) and was 5-14x slower than the warm figures on the website. Re-measured with 1 untimed run followed by the median of 3 (queries whose first run exceeds 30 s: single run), counts identical to the official expected output:

| | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|---|
| ArcadeDB Docker, warm (26.10.1, snapshot build at the time) | 107.8 | 0.19 | 0.19 | 0.09 | 0.06 | 0.26 | 0.14 | 0.05 | 0.13 | 2.47 |
| ArcadeDB Docker, cold single run | 113.6 | 2.01 | 1.45 | 1.26 | 0.53 | 1.65 | 1.61 | 0.41 | 0.82 | 12.38 |
| arcadedb.com page (26.4.1, warm) | - | 0.25 | 0.19 | 0.13 | 0.03 | 0.23 | 0.11 | 0.02 | 0.19 | 1.06 |
| Neo4j 2026.09.0, warm | 252.0 | 8.35 | 1.69 | 15.98 | 8.20 | 6.73 | 41.93 | 12.39 | 14.37 | 240.76 |

Against the page's 26.4.1 values, warm 26.10.1 is equal or better on Q1, Q2, Q3, Q5, Q6 and Q8, and slower on Q4 (0.06 vs 0.03 s), Q7 (0.05 vs 0.02 s) and Q9 (2.47 vs 1.06 s, 2.3x): real changes between 26.4.1 and 26.10.1, not measurement artifacts. The harness now supports this protocol (`LSQB_WARMUP`, `LSQB_REPS`; the weekly driver sets 1 and 3), applied to the two JVM engines only.

## LDBC Graphalytics on graph500-22 (2026-10-03)

Dataset `graph500-22`: 2,396,657 vertices, 64,155,735 edges, undirected, unweighted, official algorithms BFS, CDLP, LCC, PR, WCC (no SSSP). The harness loaders expect a weight column, so the data was loaded from a derived copy `graph500-22-w` with a constant weight of 1.0 and with vertex ids 6 and 248533 swapped (an isomorphic graph in which the official BFS source 248533 is vertex 6, the id the drivers use). SSSP is therefore not reported. Seconds, one run per vendor, 5-minute limit per algorithm, 12 GB heap, same machine as the other results; ArcadeDB runs the Graph Analytical View (OLAP).

| | Load | PageRank | WCC | LCC | BFS | CDLP |
|---|---|---|---|---|---|---|
| ArcadeDB (Docker) | 101 | **1.51** | 0.81 | **50.0** | 0.36 | **4.95** |
| Neo4j | 1228 | 6.64 | **0.40** | 224 | 1.21 | N/A |
| Kuzu | 10.2 | 3.48 | 0.53 | N/A | invalid | N/A |
| LadybugDB | 10.1 | N/A | N/A | N/A | invalid | N/A |
| DuckPGQ | 0.57 | 5.91 | 6.38 | 128 | invalid | N/A |
| Memgraph | 414 | 8.79 | 184 | N/A | invalid | 170 |
| ArangoDB (3.11.14) | 659 | 241 | 84.4 | N/A | N/A | timeout |
| HugeGraph | 34.2 | 3.25 | 4.64 | timeout | invalid | 21.7 |
| FalkorDB | timeout (>25 min) | - | - | - | - | - |

Notes:
- **BFS validity:** both datasets store every undirected edge once. The official graph500-22 source has only incoming edges, so engines that traverse in the stored direction (Kuzu, LadybugDB, DuckPGQ, Memgraph) reach 0 nodes; their BFS cells are invalid, not fast. ArcadeDB reaches 2,395,189 nodes and Neo4j 2,395,190 (counts the source), so those two agree. HugeGraph's 0.20 s is invalid too: re-running its Vermeer `sssp` task from the same source and reading the output file shows a distance for exactly 1 of 2,396,657 vertices (the source itself, distance 0), because Vermeer follows the stored edge direction. The only valid BFS results on this graph are therefore ArcadeDB (0.36 s) and Neo4j (1.21 s).
- Direction handling differs per driver (Neo4j: undirected GDS projection; ArangoDB: `ANY`; Kuzu, LadybugDB, DuckPGQ, Memgraph, FalkorDB: stored direction), and the vendor outputs are not validated against the LDBC reference outputs, so only timings of the same semantics are comparable.
- ArcadeDB loads with its embedded Java loader and then serves over HTTP; the other server vendors load through Python batches over the network, so load times are not like for like.
- WCC will be re-measured once ArcadeData/arcadedb#9133 (union-find) lands.

Scaling from `datagen-7_5-fb` (ArcadeDB Docker): PR 0.44 -> 1.51 s, WCC 0.20 -> 0.81, BFS 0.18 -> 0.36, LCC 2.42 -> 50.0 (21x), CDLP 1.16 -> 4.95. Neo4j: PR 3.44 -> 6.64, WCC 0.18 -> 0.40, LCC 15.1 -> 224, BFS 0.55 -> 1.21; loads: Neo4j 538 -> 1228 s, Memgraph 170 -> 414, ArangoDB 357 -> 659.

## WCC re-test after ArcadeData/arcadedb#9133 (parallel union-find, PR #9135 merged 2026-10-04)

Same machine, Temurin 25 with compact headers, 12 GB heap, ArcadeDB `26.10.1` (pre-release snapshot build) rebuilt after the merge (engine JAR 00:37, Docker image 00:39; the benchmark JAR was rebuilt with `-Darcadedb.version=26.10.1-SNAPSHOT`) against the previous snapshot (`02ac27327d`). Both engines were timed with the identical harness on the already loaded ArcadeDB databases (fresh JVM per measurement, 5 repetitions, medians; "cold" = the first WCC call in a JVM, which is what the multi-vendor benchmark times; "warm" = best of the following calls). Neo4j: GDS projection built first (not timed), same protocol, 7 runs. Seconds.

| WCC | datagen-7_5-fb cold | datagen warm | graph500-22 cold | graph500-22 warm |
|---|---|---|---|---|
| ArcadeDB embedded, previous engine | 0.148 | 0.036 | 0.570 | 0.428 |
| **ArcadeDB embedded, with #9133** | 0.171 | **0.003** | 0.233 | **0.012** |
| ArcadeDB Docker (HTTP, `count(*)` over all rows), with #9133 | 0.338 | 0.022 | 0.61 | 0.077 |
| Neo4j, benchmark query (stream, group, top 10) | 0.205 | 0.059 | 0.502 | 0.215 |
| Neo4j, `gds.wcc.stats` (algorithm only) | 0.083 | 0.027 | 0.132 | 0.061 |

- Correctness: the new WCC output is valid against the official reference on both graphs (100% match; 1 component on datagen-7_5-fb, 734 on graph500-22, identical to the previous engine).
- The kernel is 12x (datagen) and 36x (graph500-22) faster once warm. The first call in a fresh JVM improves much less (and not at all on the small graph): about 0.15-0.25 s of it is JIT compilation and first-touch cost, not graph work.
- Warm, ArcadeDB beats Neo4j on both graphs (embedded 0.003 s vs 0.027 s algorithm-only and 0.059 s with the benchmark query on datagen-7_5-fb; 0.012 s vs 0.061 s and 0.215 s on graph500-22; the Docker/HTTP path 2.7-2.8x faster than Neo4j's benchmark query).
- Cold, a single call as the multi-vendor benchmark measures it: embedded ArcadeDB wins on graph500-22 (0.233 vs 0.502 s) and narrowly on datagen-7_5-fb (0.171 vs 0.205 s), but through Docker Neo4j is still slightly faster on the first call (datagen 0.338 vs 0.205 s, graph500-22 0.61 vs 0.502 s).
- Observation: the first WCC call through the server right after a restart took 173 s on the datagen database when the persisted analytical view was only restored from disk (old-engine files); after an explicit `REBUILD GRAPH ANALYTICAL VIEW` it was normal. Calls issued before the restored view is usable seem to run on the record-by-record path; worth a look.

## First algorithm call after reopening a database with a persisted Graph Analytical View (local, uncommitted fix, 2026-10-04)

Problem: after a restart, the first `algo.*` call took the record-by-record path (173 s for `algo.wcc` through Docker on `datagen-7_5-fb`), because `GraphAnalyticalView.isReady()` starts the deferred restore and returns false for that call, and `AbstractAlgoProcedure.findProvider` treated false as "no view". The ArcadeDB agent's local change makes `findProvider` wait for a covering view that is still `BUILDING` (capped at 10 minutes) and look again. It is not committed; reviewed separately.

Measured here, embedded, `datagen-7_5-fb` database persisted with its view, `CALL algo.wcc() YIELD componentId RETURN count(*)` as the very first call after `open()` (no `awaitReady`), Temurin 25, 12 GB heap. Baseline = merged main (including #9133); with fix = the working-tree engine jar built with `mvn package -pl engine` (not installed, so `~/.m2` was untouched) placed first on the classpath. Seconds:

| Persisted view | merged main, first call | with fix, first call | later calls (both) |
|---|---|---|---|
| valid (restored from disk) | 87.8 | **0.65** | 0.05-0.16 |
| unusable (file truncated to 4 KB, rebuild fallback) | 110.8 | **28.0** | 0.05-0.16 |

**Measured on battery power** (the machine was unplugged from 2026-10-04 09:31, per the power log), so the absolute values are pessimistic because macOS throttles the CPU; the before/after ratio (about 135x and 4x) does not depend on it. Repeated on AC power on 2026-10-04 after PR #9221 was merged (engine rebuilt from main; same database, 633,432 vertices and 34,185,747 edges, persisted view; Temurin 25, 12 GB heap, compact headers; first `algo.wcc()` after `open()`, 2 runs each, alternating):

| engine | first call after reopen (run 1 / run 2) | later calls |
|---|---|---|
| before #9221 (has the WCC union-find fix) | 69.4 s / 60.2 s | 0.07-0.15 s |
| after #9221 (merged main) | 1.02 s / 0.67 s | 0.02-0.06 s |

That is about 60-90x faster, and the AC numbers replace the battery ones above (the "before" is lower on AC: 60-69 s instead of 87.8 s).

In the valid case the call now waits for the sub-second restore and then uses the view; in the unusable case it waits for the rebuild (about 27 s) instead of scanning the records. Review points that remain open before merging: the wait applies to any `BUILDING` view (including the rebuilds that `ASYNCHRONOUS` views start after commits), it ignores the existing `GAV_RESTORE_AWAIT_TIMEOUT` setting and cannot be aborted by a query timeout, and the test covers only WCC on a four-node graph.

## Multi-vendor Mode 2 and LSQB

See `README.md` ("All Systems Comparison" and "LSQB Results"). All LSQB counts from every system match the official expected output. Kuzu/LadybugDB skip Q4/Q5/Q7/Q8 (driver limitation).

## Decisions and incidents during the unattended run

- Java 25 = Oracle GraalVM 25.0.3 (only JDK 25 installed). Default `java` on PATH is Homebrew OpenJDK 21 (used for baselines).
- Mode 1 run from a scratch copy of the extracted dist (not the repo), heap forced to 12g (dist script said 16g; CLAUDE.md rule is 12g). Runner JVM gets the same flags via JDK_JAVA_OPTIONS.
- Java 25 variants: plain, and -XX:+UseCompactObjectHeaders (default is OFF in 25; server.sh probe enables it only when used).
- Fixed along the way: greadlink missing -> used `config` symlink; validation dir needed datasets/datagen-7_5-fb.
- Runner JVM heap: framework default is 3x executor (=36g). Set benchmark.runner.max-memory=12288 (12g rule). Today's earlier Mode 1 numbers may have used another value, so a Java 21 run was added in this same harness for a strict like-for-like comparison (jdk21 / jdk25 / jdk25+compact).
- benchmark.runner.max-memory rejects "12288" ("Failed to parse") so it was left blank; instead executor heap = 4g so the runner (default 3x executor) gets 12g. The runner does the actual algorithm work. Verified via ps.
- Java 25 here is Oracle GraalVM (Graal JIT by default). It was much slower/erratic than OpenJDK 21 on Mode 2 (LCC ~11s vs ~2s). Added C2 variants (-XX:-UseJVMCICompiler) to separate JDK 25 from the Graal JIT.
- LSQB embedded run WITHOUT --reset (the auto-mode classifier denied the unattended --reset deletion of /tmp/arcadedb_lsqb): DB loaded once by the first run (jdk21) and reused by later variants; only that run has a valid load time.
- DuckPGQ: duckdb 1.5.6 had no duckpgq extension; pinned duckdb==1.5.0 in .venv (matches README).
- Latest versions (checked 2026-10-03, Docker Hub / PyPI): Neo4j 2026.09.0-community (pin updated), PostgreSQL 18.6 (pin postgres:18), Memgraph MAGE 3.13.1 (latest), FalkorDB 6.0.1/Redis 8.10.2 (latest), HugeGraph vermeer latest, Kuzu 0.11.3 (newest on PyPI), DuckDB 1.5.0 for DuckPGQ (extension unavailable for newer), ArangoDB: trying latest 3.12.4-3 first; if Pregel unavailable fall back to 3.11.14 and document.
- LadybugDB added (user-confirmed PyPI package ladybug==0.21.2): ldbc-native/systems/ladybug.py, lsqb/systems/ladybug.py (adapted from kuzu drivers). Kuzu kept as archived-project reference.
- A Temurin 25 JDK (25.0.4.1+1, checksum verified) is installed at `~/Library/Java/JavaVirtualMachines/jdk-25.0.4.1+1`; it was used for the Temurin vs GraalVM comparison below.
- Memgraph 3.13.1 Mode 2: LCC hung >45min (SIGALRM cannot interrupt the blocking mgclient call); killed manually, LCC recorded as timeout (>300s). Memgraph load succeeded this time (older versions segfaulted/timed out).
- ArangoDB latest 3.12.4-3: no Pregel (/_api/control_pregel 404) -> only BFS works; also run 3.11.14 (last with Pregel) after phase 2; report both.
- (watchdog v1 mis-fired at 12:09:51 and killed an idle helper zsh shell, pid 6938; no benchmark affected. Rewritten.)
- WATCHDOG 12:10:17: killed '../.venv/bin/python -u benchmark.py falkordb' (pid 45695): no output for 1201s in m2-falkordb.txt; remaining ops recorded as timeout.
- FalkorDB 6.0.1 Mode 2 first attempt: stuck in 'Loading edges' >20min, killed (by first watchdog); rerun queued after ArangoDB 3.11.14 (m2-falkordb-rerun.txt). If it hangs again, FalkorDB Mode 2 = load timeout.
- LadybugDB 0.21.2 Mode 2: only BFS (3.28s, load 5.94s). INSTALL algo succeeds but the downloaded macOS-arm64 extension (0.21.0) fails to dlopen: Library not loaded @rpath/libnetworkit.dylib (upstream packaging bug). Not worked around; PR/WCC/LCC/SSSP/CDLP = N/A. Retry when a fixed extension ships.
- LSQB done so far (counts verified identical to ArcadeDB): kuzu 0.11.3, ladybug 0.21.2, duckdb 1.5.6. Kuzu/Ladybug drivers skip Q4/Q5/Q7/Q8 (Message union) - existing driver limitation, could be extended later. LadybugDB LSQB: Q1 0.31 Q2 0.20 Q3 12.34 Q6 0.69 Q9 0.07; Kuzu: Q1 4.41 Q2 0.12 Q3 2.35 Q6 1.38 Q9 6.47.
- phase2 script used wrong ports for LSQB postgres (5433) and neo4j (container neo4j-lsqb, port 7688, +12g heap) -> failed instantly; rerun in phase2b.
- 13:11 phase2b (LSQB postgres/neo4j) and arango311 accidentally started concurrently; both killed and restarted serially (phase2b -> arango311 -> falkor rerun). Their 13:11 partial outputs discarded.
- LSQB ArcadeDB Docker first attempt failed: 'Request body too large (arcadedb.server.httpBodyContentMaxSize)'; rerun queued last with -Darcadedb.server.httpBodyContentMaxSize=4294967296.
- WATCHDOG 13:52:09: killed '../.venv/bin/python -u benchmark.py falkordb' (pid 77064): no output for 970s in m2-falkordb-rerun.txt; remaining ops recorded as timeout.
- FalkorDB 6.0.1 Mode 2 (34M edges): load did not finish in >15 min on 2 attempts (watchdog-killed; rule is 5 min) -> recorded as LOAD TIMEOUT, no algorithm results. LSQB load took 692s.
