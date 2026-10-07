# ArcadeDB progress across releases

How ArcadeDB itself changes from release to release on this benchmark: same machine for every column,
same datasets, one JVM at a time. This is the history; the current multi-vendor comparison is in the
[README](README.md#all-systems-comparison), and the raw data of the latest run is in
[results-multivendor-validated-2026-10-05.md](results-multivendor-validated-2026-10-05.md) (earlier raw data: [results-m5-multivendor-2026-10-03.md](results-m5-multivendor-2026-10-03.md)). The regressions found between
26.8.1 and 26.10.1 and how they were fixed are in
[fix-plan-26.10.1-regressions.md](fix-plan-26.10.1-regressions.md).

*Keep this file updated: after every benchmark run on a new ArcadeDB version (or a fix branch), add the new
column here and in the results file, and say which engine build or PR each column refers to.*

## 26.8.1 to 26.10.1


Same machine for every column (MacBook M5, `-Xms12g -Xmx12g`, OpenJDK 21), same datasets, one JVM at a time, measured 2026-10-02 (later runs use Temurin 25 only). This tracks ArcadeDB itself across versions; the multi-vendor tables below were re-measured on 2026-10-03 on the same machine with ArcadeDB `26.10.1-SNAPSHOT` and the newest release of every other system (raw detail, Java 21 vs 25 and decision log: [`results-m5-multivendor-2026-10-03.md`](results-m5-multivendor-2026-10-03.md)). The 26.10.1 column is the `26.10.1-SNAPSHOT` built from ArcadeDB `main` @ `02ac27327d` (not in a release yet).

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

## Changes merged after the columns above (2026-10-04, AC power, Temurin 25, compact object headers)

| Change | Effect on this benchmark |
|---|---|
| WCC union-find ([#9133](https://github.com/ArcadeData/arcadedb/issues/9133)) | Embedded WCC on the warm view: 0.003 s on `datagen-7_5-fb` and 0.012 s on `graph500-22` (details in the results file). |
| `algo.*` waits for the restored view ([#9220](https://github.com/ArcadeData/arcadedb/issues/9220), PR [#9221](https://github.com/ArcadeData/arcadedb/pull/9221)) | First `algo.wcc()` after reopening a database with a persisted view (633K vertices, 34M edges): 69.4 s / 60.2 s before, 1.02 s / 0.67 s after (2 runs each, alternating). Later calls are unchanged (0.02-0.15 s). |

Current Mode 2 and LSQB numbers of the release (warm, validated) are in the last section.

## 26.10.1, warm runs of 2026-10-05

Official release 26.10.1 (measured on the identical pre-release snapshot JAR built 2026-10-04 14:11; the smoke run on the rebuilt release JAR gives the same numbers).
Temurin 25.0.4.1 with `-XX:+UseCompactObjectHeaders`, AC power, MacBook M5 Pro. Warm: the first call of each algorithm or query is discarded and the value is the median of
5 (Graphalytics, per JVM launch) or 3 (LSQB) timed runs, then the median of 3 JVM launches; all outputs validated except CDLP (engine tie-break, see README).
The earlier cold single-run numbers of 2026-10-02 to 2026-10-04 are superseded.

| Variant | Load | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|
| Embedded Graphalytics (`datagen-7_5-fb`) | 71.9 | 0.086 | 0.004 | 0.022 | 2.19 | 0.84 | 1.09 (invalid) |
| Docker Graphalytics | 43.9 | 0.156 | 0.022 | 0.032 | 2.65 | 1.56 | 1.08 (invalid) |

| LSQB SF1 | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|---|
| Embedded OLAP (GAV) | 119.5 | 0.09 | 0.15 | 0.07 | 0.04 | 0.19 | 0.13 | 0.05 | 0.12 | 1.78 |
| Embedded OLTP | 159.2 | 2.83 | 4.54 | 3.44 | 1.24 | 13.59 | 12.77 | 1.13 | 8.60 | 0.90 |
| Server (Docker) | 101.7 | 0.14 | 0.19 | 0.08 | 0.05 | 0.27 | 0.18 | 0.06 | 0.13 | 2.32 |

Memory: embedded Graphalytics process peak 6.4 GiB with a 0.75 GiB live heap after GC (fixed 12 GB heap); Docker container 12.3 GiB.

Full tables and the other vendors: [results-multivendor-validated-2026-10-05.md](results-multivendor-validated-2026-10-05.md).

## 26.11.1-SNAPSHOT, warm runs of 2026-10-06

ArcadeDB `main` @ `cbf701d66e` (`26.11.1-SNAPSHOT`, engine installed locally and Docker image `arcadedata/arcadedb:26.11.1-SNAPSHOT`, image id `b216d25c81e1`), which adds
[#9282](https://github.com/ArcadeData/arcadedb/issues/9282) (LSQB Q9 with the anti-pattern) and
[#9285](https://github.com/ArcadeData/arcadedb/issues/9285) (CDLP tie-break by vertex id). Same machine and method as the section above (Temurin 25.0.4.1, compact object
headers, AC power, swap 2 GB, warm medians; raw logs in `weekly-results/20261006-26.11.1/`). Every Graphalytics output validates at 100% against the reference (all six
algorithms, embedded and Docker) and all nine LSQB counts equal the official expected counts (embedded OLAP, OLTP and Docker).

| Variant | Load | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|
| Embedded Graphalytics (`datagen-7_5-fb`) | 71.9 | 0.085 | 0.003 | 0.020 | 2.05 | 0.75 | 0.96 (was 1.09, invalid) |
| Docker Graphalytics | 43.9 | 0.16 | 0.02 | 0.06 | 2.61 | 1.39 | 1.45 (was 1.08, invalid) |

| LSQB SF1 | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|---|
| Embedded OLAP (GAV) | 119.5 | 0.08 | 0.13 | 0.05 | 0.04 | 0.17 | 0.07 | 0.04 | 0.11 | 0.30 (was 1.78) |
| Embedded OLTP | 159.2 | 2.45 | 4.40 | 3.17 | 1.06 | 11.39 | 11.00 | 1.07 | 6.91 | 0.87 |
| Server (Docker) | 99.4 | 0.17 | 0.17 | 0.06 | 0.05 | 0.25 | 0.13 | 0.06 | 0.14 | 0.33 (was 2.32) |

Load times: the embedded Graphalytics, Docker Graphalytics and embedded LSQB databases were reused, so their load times are the remembered original loads (made with 26.10.1); only the Docker LSQB database was loaded fresh on the new image (99.4 s).
Memory: embedded Graphalytics process peak 5.6 GiB (live heap 0.77 GiB after GC), LSQB OLAP 4.3 GiB (0.67), OLTP 12.0 GiB (1.4); Docker 12.3 GiB (Graphalytics), 12.9 GiB (LSQB).
Q9 is now 5.9x faster embedded and 7x faster in Docker, and the GAV is faster than the OLTP path on all nine queries. The other LSQB queries and algorithms are unchanged within run-to-run noise.
The Docker CDLP time includes computing the vertex-id tie-break rank inside the procedure; the embedded kernel gets a precomputed rank.

### Mode 1 (official framework) on 26.11.1-SNAPSHOT, 2026-10-06

Same engine build (`cbf701d66e`), machine and rules as above (Temurin 25, compact object headers, AC power, 12 GB runner heap; raw logs in `weekly-results/20261006-2611-final-part2/`).
`datagen-7_5-fb`, `processing_time` in seconds, **one run per cell** (the framework runs each algorithm once after its own load and cannot be warmed), every run validated by the framework. The framework ran the algorithms in its own order
(CDLP, PR, LCC, WCC, BFS, SSSP for OLAP; WCC, SSSP, BFS, PR, CDLP, LCC for OLTP).

| Algorithm | OLAP (GAV) 26.8.1 | OLAP 26.10.1 | OLAP 26.11.1 | OLTP 26.8.1 | OLTP 26.10.1 | OLTP 26.11.1 |
|-----------|------|------|------|------|------|------|
| **PR** | 3.04 | 2.61 | 2.99 | 43.6 | 45.8 | 90.1 |
| **BFS** | 7.14 | 8.25 | 9.13 | 98.8 | 91.1 | 79.9 |
| **WCC** | 3.14 | 3.92 | 3.60 | 94.3 | 75.8 | 113.9 |
| **CDLP** | 13.5 | 14.7 | 10.97 | 58.3 | 69.8 | 40.2 |
| **LCC** | 5.80 | 6.24 | 5.17 | 169 | 177 | 320 |
| **SSSP** | 6.45 | 7.00 | 6.71 | 54.6 | 54.9 | 41.4 |

OLAP BFS runs after four other algorithms and still takes 9.1 s, so the bulk-UPDATE regression (#8660) is gone.

**OLTP PR, WCC and LCC: noise, not a regression.** The first run was slower than the 26.10.1 column for these three (PR 90.1, WCC 113.9, LCC 320). A repeat of exactly these three on the same build and machine (AC power, fresh load, validated, 2026-10-06 12:52) gave PR 36.1, WCC 68.1, LCC 149.6, at or below the 26.10.1 column (45.8, 75.8, 177). The same build therefore varies 2.5x (PR), 1.7x (WCC) and 2.1x (LCC) between two runs, and between the 26.10.1 release and this build only 6 engine files changed (none on the no-view PR/WCC/LCC path). Read OLTP differences below about 2.5x as noise; the table above keeps the first run, the repeat is in the text.

## ArcadeDB Docker: Bolt against the HTTP API (image 26.11.1-SNAPSHOT `b216d25c81e1`, 2026-10-06, AC power)

The Docker drivers now send the timed calls over Bolt (the earlier tables were measured over HTTP). To see what the protocol changes, the same image and cached data were run twice per protocol in opposite orders (Bolt, HTTP, then HTTP, Bolt); every result valid (LSQB counts equal the official ones, Graphalytics outputs equal the reference). Seconds, mean of the two rounds; the last column is the largest difference between the two rounds of one protocol.

| | Bolt | HTTP | Bolt vs HTTP | spread within a protocol |
|---|---|---|---|---|
| LSQB Q1 | 0.139 | 0.139 | +0.1% | 9% |
| Q2 | 0.148 | 0.132 | +12% | 10% |
| Q3 | 0.058 | 0.064 | -10% | 15% |
| Q4 | 0.049 | 0.052 | -6% | 7% |
| Q5 | 0.206 | 0.228 | -10% | 10% |
| Q6 | 0.084 | 0.100 | -16% | 20% |
| Q7 | 0.051 | 0.053 | -4% | 13% |
| Q8 | 0.124 | 0.125 | -1% | 14% |
| Q9 | 0.303 | 0.299 | +1% | 1% |
| Graphalytics PageRank | 0.135 | 0.134 | +1% | 16% |
| WCC | 0.019 | 0.022 | -12% | 13% |
| BFS | 0.036 | 0.042 | -14% | 19% |
| LCC | 2.414 | 2.374 | +2% | 3% |
| SSSP | 1.372 | 1.368 | +0% | 3% |
| CDLP | 1.388 | 1.400 | -1% | 6% |

Every difference is within the spread between two runs of the same protocol, so Bolt and HTTP are indistinguishable here. That is expected: each timed call returns one row (`count(*)`), so the protocol adds only per-request overhead. The protocol would matter for calls that return many rows (the per-vertex exports are not timed). Container memory peaks are the same (12.5-12.8 GiB, the fixed heap).

### Bolt against HTTP when a call returns many rows (same image and day, AC power, `scripts/bolt_transfer_bench.py`)

The calls above return one row. Calls that return many rows show the real protocol cost (warm median of 3, rows consumed on the client, row counts equal over both protocols):

| Call | Bolt | HTTP | Bolt / HTTP | Bolt rows/s | HTTP rows/s |
|---|---|---|---|---|---|
| vertex ids, 633,432 rows x 1 | 14.12 s | 0.37 s | 38x | 44,900 | 1,694,000 |
| PageRank scores, 633,432 rows x 2 | 7.57 s | 1.16 s | 6.5x | 83,600 | 545,000 |
| edge sample, 2,000,000 rows x 2 | 44.77 s | 1.92 s | 23x | 44,700 | 1,043,000 |

Over Bolt the server returns about 45,000 rows per second for these shapes (the PageRank call, whose rows come from the analytical view, about 84,000), against 0.5-1.7 million over HTTP. Moving a full per-vertex result over Bolt is therefore the one place where the protocol matters; this is the baseline to compare an improved Bolt image with.

### Why Bolt looked slower than HTTP for many rows (new image `661e5110188f`, 2026-10-06, on battery, so relative numbers)

After the Bolt buffering fix (#9165) the Python client still needed 4-12 s for what HTTP does in 0.4-2.5 s. Experiments, all on the same container:

| Experiment | Result |
|---|---|
| Same scan, 1 row on the wire (engine only) | HTTP 0.9 s, Bolt 0.8 s: the engine is not slower under Bolt |
| Python driver with the Rust codec (`neo4j-rust-ext`) against pure Python | 7-12% faster: value decoding is a small part |
| Python driver, rows per PULL 1,000 / 10,000 / 100,000 / all (edge sample, 2M rows) | 15.0 / 11.1 / 10.8 / 10.8 s: each PULL is a round trip, worth about 25% |
| Server CPU profile (JFR) while streaming the edge sample | the Bolt encoder (`sendRecord`, `PackStreamWriter`) is about 9% of the samples; the server thread is idle about 60% of the time, waiting for the client's next PULL |
| **Official Java driver, same server, same calls** (`scripts/JavaBoltBench.java`) | vertex ids **0.25 s**, edge sample **1.48 s** with all rows in one PULL (HTTP: 0.40 s and 2.5 s); with the default 1,000 rows per PULL 1.40 s and 4.19 s |

So the server streams about 1.35 million rows/s over Bolt, faster than HTTP, and the slowness seen in the benchmarks is the **Python driver** (about 5 us of Python per record in its message loop and result handling, which the Rust codec does not cover), plus the round trip of every PULL (about 1.3 ms each through Docker Desktop on macOS, so 2,000 of them cost about 2.7 s). The published Graphalytics and LSQB numbers are unaffected, since each timed call returns one row. For many-row results use a large fetch size (`ARCADEDB_BENCH_BOLT_FETCH_SIZE=-1` for the Python helper) or a fast client.

### gRPC against HTTP and Bolt for many rows (new image `661e5110188f`, Python client, on battery so relative, `scripts/grpc_transfer_bench.py`)

The gRPC plugin (`arcadedb-grpcw`) ships in the image and starts with `-Darcadedb.server.plugins=...,GRPC:com.arcadedb.server.grpc.GrpcServerPlugin`; the streaming call `StreamQuery` returns batches of typed records. Seconds, warm median of 3, rows consumed on the client (every column read except in the "count only" line), row counts equal everywhere:

| | vertex ids (633K x 1) | PageRank scores (633K x 2) | edge sample (2M x 2) |
|---|---|---|---|
| HTTP | 0.54 | 0.98 | 1.61 |
| Bolt (Python driver, all rows in one PULL) | 2.59 | 2.81 | 8.55 |
| gRPC, 1,000 rows per batch | 0.44 | 1.80 | 2.95 |
| gRPC, 10,000 rows per batch | 0.35 | 0.85 | 1.50 |
| gRPC, 100,000 rows per batch | 0.30 | 0.79 | 1.34 |
| gRPC, 10,000 per batch, rows only counted | 0.33 | 0.82 | 1.44 |

With a batch of 10,000 rows or more gRPC matches or beats HTTP (up to 1.2x on the edge sample, 1.8x on the vertex ids) and is 3x to 8x faster than the Python Bolt client; with 1,000 rows per batch it is no better than HTTP on the wider rows. Reading the values adds about 5% over only counting the records, so the Python protobuf decoding is not the limit. Authentication goes in the call metadata (`x-arcade-user`, `x-arcade-password`). For a Python user with large results, gRPC streaming with a large batch size is the fastest of the three on this machine; for the Java driver Bolt was the fastest (1.48 s on the edge sample).

## The three 26.8.1 to 26.10.1 regressions: status on 26.11.1-SNAPSHOT

Details and root causes: [fix-plan-26.10.1-regressions.md](fix-plan-26.10.1-regressions.md). Verdicts from the warm runs above plus the bulk UPDATE reproducer (`scripts/bulk_update_repro.py`, AC power, Temurin 25).

| # | Regression (bad build) | Engine fix | 26.11.1-SNAPSHOT |
|---|---|---|---|
| 1 | Bulk UPDATE quadratic in `LocalBucket.findAvailableSpace` (Mode 1 BFS 7 s to 80-260 s) | PR #8955 (#8660), merged 2026-10-02 | every bulk UPDATE 2.0-3.8 s in both property orders; Mode 1 OLAP BFS 8.25 s on 26.10.1 |
| 2 | Star-join push-down declined for labelled arms (LSQB Q4/Q7 OLAP 0.01 s to 5-12 s) | `6e54555ea0` (#6337) | OLAP Q4 0.04 s, Q7 0.04 s; OLTP 1.06 s, 1.07 s |
| 3 | Q5 push-down declined (OLAP 0.2 s to ~3.5 s) | not identified | OLAP Q5 0.17 s |

**LSQB Q8 after #9290.** The 0.11 s OLAP time of Q8 in the 26.11.1-SNAPSHOT column came from an operator fast path that ignored the labels of `t1` and `t2` and was wrong with parallel edges. [#9290](https://github.com/ArcadeData/arcadedb/issues/9290) (merged after that build) made the push-down decline the shape, so Q8 fell back to the row pipeline: 3.9 s with the analytical view and 9.2 s without (battery, relative measurement, same count 6,907,213). [#9354](https://github.com/ArcadeData/arcadedb/pull/9354) (merged 2026-10-06) counts the shape exactly on both paths and brings it back to 0.09 s and 1.2 s. Measured on AC power (embedded LSQB SF1, median of 7 warm runs, two interleaved rounds, same count 6,907,213 in all eight runs): OLAP 3.82 s and 3.84 s before, 0.084 s and 0.090 s after (about 44x); OLTP 9.31 s and 9.31 s before, 1.36 s and 1.28 s after (about 7x). Builds: `9c7da2c230` (before) and `246821a605` (the #9354 merge); raw logs in `weekly-results/20261006-ab-bolt-q8/`.

Still to measure on the final build: Mode 1 in the default algorithm order (BFS after other algorithms had written results), 3 repetitions.
The guard against a repeat is the `bulk-update` step and the previous-run comparison in `weekend.py` (`scripts/check_regressions.py`).

## graph500-22-w (2.4M vertices, 64M edges), 26.11.1-SNAPSHOT, 2026-10-06

Warm, validated against the official `graph500-22` reference (ids 6 and 248533 swapped back), same machine and JVM rules as above; no SSSP (not defined for this dataset).
ArcadeDB embedded: load 114.5 s, PageRank 0.268, WCC 0.013, BFS 0.076, LCC 46.9, CDLP 1.78 (all valid; the first cold run of 2026-10-03 had PR 0.31, WCC 0.54, BFS 0.17, LCC 44.3, CDLP 4.46).
ArcadeDB Docker: PageRank 0.59, WCC 0.09, BFS 0.24, LCC 60.1, CDLP 4.33 (all valid, 13.5 GiB container). All other systems: [docs/benchmark-graphalytics-graph500-22.md](docs/benchmark-graphalytics-graph500-22.md).

## Decision 2026-10-07: ArcadeDB Docker is benchmarked over HTTP only, with full-output calls

- **Protocol.** The Bolt sections above are history: Bolt is not usable for the benchmarks and the Docker drivers no longer have a Bolt (or gRPC) path. gRPC through the official Java client (`RemoteGrpcDatabase.queryStream`) was measured against HTTP on one image, order gRPC, HTTP, HTTP, gRPC, AC power, all outputs valid: no consistent difference, for one-row calls (PageRank 0.14-0.15 s on both, LCC 2.50-2.52 gRPC against 2.55-2.64 HTTP) and for full 633K-row outputs (PageRank 1.50 and 1.12 gRPC against 1.11 and 1.11 HTTP, LCC 3.12 against 3.25-3.26).
- **Fairness correction.** The ArcadeDB Docker Graphalytics calls returned `count(*)`, the other server systems returned the full per-vertex output, so the published ArcadeDB Docker row left out the result transfer. The calls now return the exact output that is exported and validated. `datagen-7_5-fb`, mean of two HTTP runs: PageRank 1.11, WCC 0.83, BFS 0.88, LCC 3.26, SSSP 2.09, CDLP 2.17 s (compute only before: 0.16, 0.02, 0.06, 2.61, 1.39, 1.45), 12.7 GiB, all six valid. `graph500-22-w`: PageRank 4.28, WCC 3.69, BFS 3.59, LCC 51.7, CDLP 7.59 s (before: 0.59, 0.09, 0.24, 60.1, 4.33), 13.1 GiB, all five valid. Embedded ArcadeDB and Mode 1 run in process and are unchanged. LSQB returns a count for every vendor and is unchanged. Raw logs: `weekly-results/20261006-full-output/`, `weekly-results/20261007-graph500-22-w-arcadedb-full/`.
- **Mode 1 on 26.11.1-SNAPSHOT, default order, one repetition, AC power, validated** (raw: `weekly-results/20261006-grpc/mode1.log`): OLAP BFS 9.09, CDLP 12.9, LCC 5.87, PR 3.68, SSSP 6.73, WCC 3.84 s; OLTP BFS 93.0, CDLP 52.8, LCC 194, PR 47.0, SSSP 50.6, WCC 96.2 s (within the 2.5x OLTP noise band). OLAP BFS after four other algorithms is 9.1 s, so the bulk UPDATE regression (#8660) stays fixed.
