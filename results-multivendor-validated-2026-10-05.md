# Validated multi-vendor Graphalytics and LSQB re-run (MacBook M5 Pro, 2026-10-04 / 2026-10-05)

Same machine as `results-m5-multivendor-2026-10-03.md` (MacBook Pro 16" 2026, Apple M5 Pro, 48 GB RAM, Docker Desktop 32 GB), always on AC power,
one process / one container at a time, 5-minute limit per operation, JVM systems with `-Xms12g -Xmx12g`.
ArcadeDB: `26.10.1` (measured on the identical pre-release snapshot JAR built 2026-10-04 14:11; the release was published 2026-10-05), Temurin 25.0.4.1 with `-XX:+UseCompactObjectHeaders`.

Every Graphalytics output was exported in full (no `LIMIT`, no top-10 aggregation) from the exact call that was timed and checked with
`scripts/validate_outputs.py` against the official reference outputs. A cell is only comparable when it validated.

## Graphalytics, `datagen-7_5-fb` (seconds)

Sources: `weekly-results/20261004-175349/weekly.md` (ArcadeDB, Neo4j, Kuzu, LadybugDB, DuckPGQ) and the re-runs of 2026-10-05 (Memgraph, ArangoDB, FalkorDB, HugeGraph).

| Vendor | Load | PageRank | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|---|
| ArcadeDB embedded (median of 3) | 71.9 | 0.22 | 0.08 | 0.08 | 2.12 | 0.85 | 1.01 invalid |
| ArcadeDB Docker | 43.9 | 1.56 | 0.24 | 0.20 | 2.77 | 1.62 | 1.30 invalid |
| Neo4j | 657 | 3.52 invalid | 0.18 | 0.65 | 15.09 | N/A | N/A |
| Kuzu | 28.8 | 4.11 | 0.69 | 0.39 | N/A | N/A | N/A |
| LadybugDB | 5.16 | N/A | N/A | 37.2 | N/A | N/A | N/A |
| DuckPGQ | 0.85 | 9.95 invalid | 12.85 | timeout | 220 | N/A | N/A |
| Memgraph | 437 | 5.72 | 119 | 3.57 | N/A | 67.5 | timeout |
| ArangoDB 3.11.14 | 726 | 89.2 | 36.7 | 27.9 | N/A | 185 | 213 invalid |
| FalkorDB | 3199 | 2.64 invalid | 2.42 | 0.16 | N/A | N/A | 8.01 invalid (same communities, other labels) |
| HugeGraph (Vermeer) | 13.0 + 21.7 | 2.19 | 1.56 | 0.28 | 101.8 | N/A | 20.7 invalid (same communities, other labels) |

Load of Memgraph, ArangoDB and FalkorDB includes writing every edge in both directions (68.4M edge records); HugeGraph loads a second graph
(`bench_u`, 21.7 s) for PageRank and BFS.

Why the invalid and N/A cells cannot be fixed from the driver:
- Neo4j PageRank: GDS starts from 1-d instead of 1/N and does not normalise; after 10 iterations the values are about 5e5 times the reference and do not follow a constant factor.
- DuckPGQ PageRank: `pagerank()` has no iteration or damping parameter (ranks sum to 0.90). DuckPGQ BFS exceeds the 5-minute limit.
- FalkorDB PageRank: `algo.pageRank` has no parameters (14.9% of vertices within 1e-4). CDLP finds the reference communities with other label values.
- ArcadeDB CDLP: the engine's `algo.labelPropagation` breaks ties and seeds labels with dense ids instead of vertex ids (Mode 1 passes).
- ArangoDB CDLP: Pregel label propagation returns dense ids; 0% match.
- LadybugDB: the `algo` extension for macOS arm64 does not load (`libnetworkit.dylib`), so only BFS runs.
- LCC: no native implementation in Kuzu, LadybugDB, Memgraph (the MAGE image lacks NetworkX), FalkorDB; the ArangoDB AQL query is rejected.

## LSQB, SF1 (seconds), counts verified against the official expected counts

| Vendor | Load | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 |
|---|---|---|---|---|---|---|---|---|---|---|
| ArcadeDB embedded OLAP | 119.5 | 0.28 | 0.18 | 0.11 | 0.06 | 0.20 | 0.13 | 0.05 | 0.12 | 1.68 |
| ArcadeDB embedded OLTP | 159.2 | 3.19 | 5.04 | 3.71 | 1.18 | 11.52 | 42.52 | 5.51 | 40.57 | 0.98 |
| ArcadeDB Server (Docker) | 101.7 | 0.15 | 0.17 | 0.08 | 0.05 | 0.20 | 0.16 | 0.05 | 0.11 | 1.89 |
| DuckDB | 0.46 | 0.12 | 0.01 | 0.03 | 0.07 | 0.04 | 1.70 | 0.08 | 0.06 | 5.70 |
| PostgreSQL 18 (re-run 2026-10-05) | 15.1 | 9.86 | 0.69 | 1.66 | 9.05 | 1.73 | 21.37 | 12.56 | 3.53 | 82.67 |
| Neo4j | 252 | 7.74 | 1.99 | 17.09 | 7.92 | 6.57 | 47.67 | 12.71 | 17.65 | 407.89 |
| Kuzu | 2.44 | 4.12 | 0.11 | 2.26 | N/A | N/A | 1.33 | N/A | N/A | 5.96 |
| LadybugDB | 2.87 | 0.28 | 0.18 | 14.66 | N/A | N/A | 0.64 | N/A | N/A | 0.06 |
| Memgraph | 222 | 57.23 | timeout | timeout | 3.79 | 3.16 | 157.11 | 4.72 | 2.99 | timeout |
| FalkorDB | 659 | 40.63 | 64.40 | 4.24 | 3.96 | 4.17 | 41.18 | 48.16 | 5.45 | 180.21 |

PostgreSQL failed in the 2026-10-04 weekly run (connection refused: the container's port opened before Postgres accepted connections) and was re-run
after the readiness probe was added; all 9 counts match.

## Harness and driver fixes made for this run

Harness (`shared/`):
- `run_timed`: when the `SIGALRM` fired inside a C client library (mgclient) the alarm fired a second time during clean-up and crashed the whole vendor run (no outputs, nothing validated). The alarm is now disarmed safely and such cases are recorded as `timeout`.
- Readiness probes (`bench_containers.py`): Docker opens the port before the engine behind it is ready. Memgraph (Bolt handshake, up to 30 min for recovering the 21 GB snapshot) and PostgreSQL (SSLRequest) are probed at protocol level.
- Memgraph's container needs `vm.max_map_count >= 524288`; Docker Desktop's VM default of 262144 made jemalloc fail (`Error in munmap(): Cannot allocate memory`) and dropped the connection in the middle of the algorithm sequence. It is raised before each Memgraph start.
- FalkorDB's graceful stop timeout is 1800 s: with 180 s the shutdown snapshot of the 68M-edge graph was killed and the next start loaded an older, partial snapshot (62.8M edges).

Drivers (`ldbc-native/systems/`):
- Memgraph: reverse edges, full-output WCC/CDLP queries, LCC skipped (a failed procedure call also kills the Bolt session), CDLP timeout handled with a new session.
- FalkorDB: edges loaded in both directions in one pass (reused only when the edge count is exactly twice the file's), `RESULTSET_SIZE` set to unlimited (the default 10000 silently truncated every output), full outputs and exports (BFS distances from a depth sweep of `algo.BFS`), progress output during the load so the idle watchdog stays quiet.
- ArangoDB: both directions loaded (marker document `meta/both_directions`), Pregel PageRank with `maxGSS 11` (10 and 12 do not match), weighted SSSP through an AQL weighted traversal (`LAST(p.weights)`), BFS `OUTBOUND` with a long client timeout (the 60 s default made it fail).
- HugeGraph: PageRank and BFS on a second graph loaded from a both-direction copy of the edge file, PageRank with `compute.max_step 10` and no convergence threshold.
- Kuzu, LadybugDB, DuckPGQ: see the commit history; reversed edge tables / symmetric edge table, full outputs, exact timed query exported.

## Cold first call vs warm median (added 2026-10-05)

The tables above are one cold call per algorithm. For the JVM engines the cold call includes JIT and first-touch costs, so the warm median is
also measured (new: `bench_common.run_timed_warm`, `GRAPHALYTICS_WARMUP` / `GRAPHALYTICS_REPS`, `-Dwarm.reps` in `ArcadeDBEmbeddedBenchmark`).

| System | Algorithm | Cold first call | Warm median |
|---|---|---|---|
| ArcadeDB Docker | PageRank | 2.200 | 0.133 |
| ArcadeDB Docker | WCC | 0.230 | 0.025 |
| ArcadeDB Docker | BFS | 0.191 | 0.033 |
| ArcadeDB Docker | LCC | 2.679 | 2.599 |
| ArcadeDB Docker | SSSP | 1.430 | 1.304 |
| ArcadeDB Docker | CDLP (invalid) | 1.217 | 1.077 |
| ArcadeDB embedded (2 JVMs, warm = median of 5) | PageRank | 0.228 / 0.201 | 0.092 / 0.085 |
| ArcadeDB embedded | WCC | 0.087 / 0.086 | 0.004 / 0.003 |
| ArcadeDB embedded | BFS | 0.065 / 0.069 | 0.028 / 0.029 |
| ArcadeDB embedded | LCC | 2.156 / 2.100 | 2.101 / 2.132 |
| ArcadeDB embedded | SSSP | 0.852 / 0.840 | 0.753 / 0.776 |
| ArcadeDB embedded | CDLP (invalid) | 1.004 / 1.006 | 0.982 / 0.963 |
| Neo4j | PageRank (invalid) | 3.585 | 3.293 |
| Neo4j | WCC | 0.208 | 0.059 |
| Neo4j | BFS | 0.608 | 0.442 |
| Neo4j | LCC | 14.741 | 14.655 |

Consequences:
- The embedded PageRank of 0.22 s (compared with 0.10 s in the April blog post, ArcadeDB 26.3.2, same machine) is the cold first call; warm it is 0.085-0.092 s, so there is no PageRank regression in the compute itself.
- The ArcadeDB Docker PageRank of 1.56 s in the weekly run was not the slow path of ArcadeDB #9220 (about 71 s); it is the cold first call (2.2 s here, 0.133 s warm).
- Non-JVM engines (Kuzu, DuckPGQ, FalkorDB, Memgraph, ArangoDB, HugeGraph, LadybugDB) still have only the cold call; their warm numbers were not measured.
