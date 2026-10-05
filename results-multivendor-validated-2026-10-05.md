# Validated multi-vendor Graphalytics and LSQB results, warm (MacBook M5 Pro, 2026-10-04 / 2026-10-05)

Same machine as `results-m5-multivendor-2026-10-03.md` (MacBook Pro 16" 2026, Apple M5 Pro, 48 GB RAM), always on AC power, one process / one container
at a time, 5-minute limit per operation, JVM systems with `-Xms12g -Xmx12g`. Docker Desktop had 36 GB (the 2026-10-05 runs; HugeGraph was measured with 32 GB).
ArcadeDB: `26.10.1` (measured on the identical pre-release snapshot JAR built 2026-10-04 14:11; the release was published 2026-10-05), Temurin 25.0.4.1 with `-XX:+UseCompactObjectHeaders`.

## Method

- **Warm only.** The first call of every algorithm or query is an untimed warm-up (JIT, page cache, first touch); the reported value is the median of 3 timed runs
  (5 in the embedded ArcadeDB Graphalytics JVM, which is launched 3 times: median of those). When the warm-up call takes longer than 60 s (LSQB 30 s) there is a
  single timed run. Loads are one-off and not warmed. Mode 1 (the official LDBC framework) is the exception: each algorithm runs once after its own load.
- **Validated.** Every Graphalytics output is exported in full (no `LIMIT`) from the exact call that is timed and checked with `scripts/validate_outputs.py`
  against the official reference outputs; every LSQB count is checked against the official expected counts. Cells that fail are marked invalid and not ranked.
- **Memory.** One sample per second while the timed operations run: the working set of the vendor's containers (`docker stats`) or the RSS of its process.
  JVM systems run with a fixed 12 GB heap, so their process or container size mostly shows that heap; embedded ArcadeDB also reports its live heap after a full GC (Graphalytics 0.75 GiB, LSQB OLAP 0.67, OLTP 1.4); it was not measured for the other systems.
  Kuzu, DuckDB and LadybugDB size their buffer pools from the machine RAM.

## Graphalytics, `datagen-7_5-fb` (seconds, warm medians)

| Vendor | Load | PageRank | WCC | BFS | LCC | SSSP | CDLP | Peak memory (GiB) |
|---|---|---|---|---|---|---|---|---|
| ArcadeDB embedded | 71.9 | 0.086 | 0.004 | 0.022 | 2.19 | 0.84 | 1.09 invalid | 6.4 process |
| ArcadeDB Docker | 43.9 | 0.156 | 0.022 | 0.032 | 2.65 | 1.56 | 1.08 invalid | 12.3 container |
| Neo4j | 657 | 6.98 (two GDS runs, valid) | 0.111 | 0.480 | 15.4 | N/A | N/A | 13.3 container |
| Kuzu | 28.8 | 1.16 | 0.434 | 0.328 | N/A | N/A | N/A | 0.87 process |
| LadybugDB | 5.16 | N/A | N/A | 7.89 | N/A | N/A | N/A | 1.0 process |
| DuckPGQ | 0.85 | 1.51 invalid | 2.00 | exceeds the limit | 49.3 | N/A | N/A | 8.3 process |
| Memgraph | 437 | 5.49 | 189 | 3.85 | N/A | 75.9 | timeout | 25.1 container |
| ArangoDB 3.11.14 | 726 | 93.0 | 40.9 | 38.4 | N/A | 173 | 254 invalid | 23.2 container |
| FalkorDB (bulk load) | 116 | 2.67 invalid | 2.50 | 0.057 | N/A | N/A | 10.6 (same communities, other labels) | 8.4 container |
| HugeGraph (Vermeer) | 15.2 + 22.4 | 2.41 | 0.293 | 0.195 | 110 | N/A | 22.3 (same communities, other labels) | 3.2 containers |

Why the invalid and N/A cells cannot be fixed from the driver:
- Neo4j PageRank is exact but needs two GDS runs. GDS starts every vertex at 1-d, does not normalise and counts the initialisation as the first iteration (`maxIterations 10` is 9 updates, verified to 1e-14 against a simulation). The update is linear, so the reference follows from the scores S10 and S11 of two runs: rank = (20·S11 − 17·S10) / 3 / N (3e-14 against the reference, 100% validation). The timed value is the two compute-only GDS runs (`gds.pageRank.stats`); the validated export streams both score sets and applies the formula.
- DuckPGQ: `pagerank()` has no iteration or damping parameter (ranks sum to 0.90). BFS exceeds the 5-minute limit because DuckDB does not honour the in-process interrupt while its shortest-path operator runs (the run ends after about 25 minutes).
- FalkorDB PageRank: `algo.pageRank` has no parameters (14.9% of vertices within 1e-4). CDLP finds the reference communities with other label values.
- ArcadeDB CDLP: the engine breaks ties and seeds labels with dense ids instead of vertex ids (Mode 1 passes).
- ArangoDB CDLP: Pregel label propagation returns dense ids, 0.0% match (254 s). LCC: the AQL query is rejected.
- LadybugDB: the `algo` extension for macOS arm64 does not load (`libnetworkit.dylib`), so only BFS runs.
- LCC: no native implementation in Kuzu, LadybugDB, Memgraph (the MAGE image lacks NetworkX) and FalkorDB.
- ArangoDB: quiet-machine rerun on 2026-10-05 (19:00-19:30; PageRank, SSSP and CDLP are one timed run each); PageRank, WCC, BFS and SSSP validate. An earlier run under about 5 GB of swap was slower (SSSP 253 s) and its CDLP hit the time limit.

## LSQB, SF1 (seconds, warm medians, all counts match the official expected counts)

| Vendor | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 | Peak memory (GiB) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ArcadeDB embedded OLAP (median of 3 JVM launches) | 119.5 | 0.09 | 0.15 | 0.07 | 0.04 | 0.19 | 0.13 | 0.05 | 0.12 | 1.78 | 6.7 process |
| ArcadeDB embedded OLTP | 159.2 | 2.83 | 4.54 | 3.44 | 1.24 | 13.59 | 12.77 | 1.13 | 8.60 | 0.90 | 10.1 process |
| ArcadeDB Server (Docker) | 101.7 | 0.14 | 0.19 | 0.08 | 0.05 | 0.27 | 0.18 | 0.06 | 0.13 | 2.32 | 12.4 container |
| DuckDB | 0.46 | 0.11 | 0.01 | 0.04 | 0.06 | 0.04 | 1.84 | 0.07 | 0.07 | 6.03 | 0.9 process |
| Kuzu | 2.44 | 4.61 | 0.15 | 2.30 | N/A | N/A | 1.38 | N/A | N/A | 6.39 | 5.1 process |
| LadybugDB | 2.95 | 0.12 | 0.10 | 10.44 | 0.17 | 0.18 | 0.66 | 0.41 | 0.32 | 0.07 | 8.8 process |
| Neo4j | 252 | 4.99 | 1.63 | 10.76 | 6.28 | 5.66 | 28.49 | 8.04 | 12.39 | 254.29 | 13.3 container |
| PostgreSQL 18 | 15.1 | 9.17 | 0.65 | 1.60 | 6.35 | 2.17 | 15.50 | 11.16 | 3.39 | 50.62 | 0.5 container |
| Memgraph | 222 | 58.06 | timeout | timeout | 4.30 | 3.54 | 121.15 | 4.92 | 3.02 | timeout | 3.1 container |
| FalkorDB | 659 | 36.43 | 53.78 | 4.59 | 3.53 | 3.93 | 40.93 | 47.96 | 4.90 | 225.22 | 2.4 container |

Kuzu's driver does not cover Q4, Q5, Q7 and Q8 (`:Message` supertype); LadybugDB's driver runs each of them as a Post part plus a Comment part and adds the counts.
ArcadeDB's GAV makes Q1 to Q8 23-98x faster than OLTP, but Q9 is faster without it.

## Harness and driver fixes made for these runs

Harness (`shared/`):
- Warm protocol (`run_timed_warm`, `measure_repeated`), per-second memory sampling (`bench_memory.py`), container log capture before removal.
- `run_timed`: when the `SIGALRM` fired inside a C client library (mgclient) the alarm fired a second time during clean-up and crashed the whole vendor run (no outputs, nothing validated). The alarm is now disarmed safely and such cases are recorded as `timeout`.
- Readiness probes (Bolt for Memgraph, SSLRequest for PostgreSQL, `PING` for FalkorDB): Docker opens a port before the engine behind it is ready.
- Memgraph's container needs `vm.max_map_count >= 524288`; Docker Desktop's VM default of 262144 made jemalloc fail (`Error in munmap(): Cannot allocate memory`) and dropped the connection in the middle of the algorithm sequence. It is raised before each Memgraph start.
- FalkorDB: a 1800 s graceful stop timeout, and background snapshots turned off (see below).
- A vendor waits (up to 30 minutes) for AC power and (up to 10 minutes) while free memory is below 25% instead of measuring on battery or under swap.

Drivers (`ldbc-native/systems/`, `lsqb/systems/`):
- Memgraph: reverse edges, full-output queries, LCC skipped (a failed procedure call also kills the Bolt session).
- FalkorDB: edges in both directions, bulk loader (116 s instead of 53 minutes), `RESULTSET_SIZE` unlimited (the default 10000 rows silently truncated every output), `save ""` + `stop-writes-on-bgsave-error no` + one synchronous `SAVE` (a failed background-save fork made Redis answer every write with `MISCONF` and aborted the load at 35M edges).
- ArangoDB: both directions, Pregel PageRank with `maxGSS 11` (10 and 12 do not match), weighted SSSP through an AQL weighted traversal (`LAST(p.weights)`), BFS `OUTBOUND` with a long client timeout, and **finished Pregel jobs are deleted**: ArangoDB keeps the in-memory graph copy of a finished job until its time-to-live expires, so the repeated warm runs grew from 5 to 34 GiB in 7 minutes and the container was killed by the out-of-memory killer; with the cleanup the peak is 23-25 GiB.
- HugeGraph: PageRank and BFS on a second graph loaded from a both-direction copy of the edge file, PageRank with `compute.max_step 10` and no convergence threshold.
- Neo4j: exact PageRank through the two-run correction described above.
- LadybugDB LSQB: Q4, Q5, Q7 and Q8 as Post + Comment parts (`REPLY_OF_C` table); single-file database handling for Kuzu and LadybugDB.
- Kuzu, LadybugDB, DuckPGQ: reversed edge tables / symmetric edge table, full outputs, the exact timed query is exported.
