# ArcadeDB progress across releases

How ArcadeDB itself changes from release to release on this benchmark: same machine for every column,
same datasets, one JVM at a time. This is the history; the current multi-vendor comparison is in the
[README](README.md#all-systems-comparison), and the raw data of the latest run is in
[results-multivendor-validated-2026-10-05.md](results-multivendor-validated-2026-10-05.md) (earlier raw data: [results-m5-multivendor-2026-10-03.md](results-m5-multivendor-2026-10-03.md)). The regressions found between
26.8.1 and 26.10.1 and how they were fixed are in
[results-26.10.1-vs-26.8.1.md](results-26.10.1-vs-26.8.1.md) and
[fix-plan-26.10.1-regressions.md](fix-plan-26.10.1-regressions.md).

*Keep this file updated: after every benchmark run on a new ArcadeDB version (or a fix branch), add the new
column here and in the results file, and say which engine build or PR each column refers to.*

## 26.8.1 to 26.10.1


Same machine for every column (MacBook M5, `-Xms12g -Xmx12g`, OpenJDK 21), same datasets, one JVM at a time, measured 2026-10-02 (later runs use Temurin 25 only). This tracks ArcadeDB itself across versions; the multi-vendor tables below were re-measured on 2026-10-03 on the same machine with ArcadeDB `26.10.1-SNAPSHOT` and the newest release of every other system (raw detail, Java 21 vs 25 and decision log: [`results-m5-multivendor-2026-10-03.md`](results-m5-multivendor-2026-10-03.md)). The 26.10.1 column is the `26.10.1-SNAPSHOT` built from ArcadeDB `main` @ `02ac27327d` (not in a release yet). Raw numbers: [`results-26.10.1-vs-26.8.1.md`](results-26.10.1-vs-26.8.1.md).

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
