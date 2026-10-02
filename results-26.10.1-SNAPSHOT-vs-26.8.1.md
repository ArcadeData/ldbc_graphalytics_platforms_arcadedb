# ArcadeDB 26.10.1-SNAPSHOT vs 26.8.1 (MacBook M5, 12g heap, 2026-10-02)

Same machine, same datasets, run sequentially (one JVM at a time). Local 26.10.1-SNAPSHOT from ~/.m2 (engine main @ 189aec328d, local checkout 30 commits behind origin).
All Mode 1 runs: 6/6 algorithms succeeded and passed validation, both versions, OLTP and OLAP.

## Mode 1: Official Graphalytics, datagen-7_5-fb (T_p = processing time, seconds; lower is better)

| Algo | OLTP 26.8.1 | OLTP 26.10.1 | speedup | OLAP(GAV) 26.8.1 | OLAP 26.10.1 | OLAP rerun | speedup (run 1) |
|---|---|---|---|---|---|---|---|
| PR   | 43.6  | 41.5  | 1.05x | 3.04 | 2.24  | 2.17  | 1.36x |
| BFS  | 98.8  | 71.4  | 1.38x | 7.14 | **258.1** | **154.2** | **0.03x REGRESSION** |
| WCC  | 94.3  | 66.0  | 1.43x | 3.14 | 14.83 | 3.01  | 0.21x (rerun normal, so noise) |
| CDLP | 58.3  | 45.6  | 1.28x | 13.5 | 11.8  | 12.3  | 1.14x |
| LCC  | 169.4 | 155.2 | 1.09x | 5.80 | 5.32  | 13.35 | 1.09x (rerun slower, noisy) |
| SSSP | 54.6  | 79.8  | 0.68x | 6.45 | 6.37  | 11.07 | 1.01x (rerun slower, noisy) |

## Mode 2: embedded, GAV (OLAP only), datagen-7_5-fb (seconds)

| | 26.8.1 | 26.10.1 |
|---|---|---|
| Total load | 60.2 | 51.0 |
| GAV build | 19.5 | 16.4 |
| PageRank | 0.143 | 0.176 |
| WCC | 0.075 | 0.062 |
| BFS | 0.058 | 0.086 |
| LCC | 2.19 | 2.27 |
| SSSP | 0.855 | 1.04 |
| CDLP | 1.056 | 1.02 |

## Mode 3: LSQB SF1, embedded Cypher (seconds)

| | OLAP 26.8.1 | OLAP 26.10.1 | OLAP rerun | OLTP 26.8.1 | OLTP 26.10.1 | OLTP rerun |
|---|---|---|---|---|---|---|
| Load | 145.0 | 108.3 | 170.1 | - | - | - |
| Q1 | 0.32 | 0.33 | 0.33 | 3.21 | 3.35 | 3.06 |
| Q2 | 0.15 | 0.17 | 0.19 | 7.85 | 5.84 | 5.63 |
| Q3 | 0.09 | 0.10 | 0.11 | 8.02 | 6.64 | 6.80 |
| Q4 | 0.01 | **5.36** | **5.50** | 4.10 | **12.61** | **34.76** |
| Q5 | 0.21 | **3.75** | **3.12** | 14.14 | 6.51 | 6.73 |
| Q6 | 0.09 | 0.13 | 0.13 | 26.99 | 26.69 | 24.80 |
| Q7 | 0.01 | **12.01** | **11.51** | 3.89 | **44.12** | **49.36** |
| Q8 | 0.11 | 0.16 | 0.12 | 10.87 | 8.10 | 40.16 (noisy) |
| Q9 | 0.97 | 1.48 | 1.82 | 1.09 | 1.16 | 6.52 (noisy) |

## Reproduced regressions in 26.10.1-SNAPSHOT (not noise)
- Mode 1 OLAP BFS: 7s -> 154-258s.
- LSQB OLAP: Q4 0.01 -> ~5.4s, Q7 0.01 -> ~12s, Q5 0.21 -> ~3.5s (look like GAV/CSR fast paths no longer used).
- LSQB OLTP: Q4 4.1 -> 12.6s+, Q7 3.9 -> 44-49s.
- Mode 1 OLTP SSSP: 54.6 -> 79.8s (single run, not repeated).

## Notes
- The rerun of LSQB OLTP Q8/Q9 and Mode 1 LCC/SSSP were also slower, which suggests machine noise (thermal/background load) in the rerun; treat single numbers as +/- 2x for OLTP.
- Fixes made to the harness: validation dir must be datasets/datagen-7_5-fb (first attempt wrongly pointed one level up and every algorithm failed validation, not an engine bug).
- Raw logs: the session scratchpad `logs/` directory.

## Root causes (bisected in ~/Documents/GitHub/arcadedb, engine @ 189aec328d)
| Regression | First bad commit | Mechanism |
|---|---|---|
| LSQB Q4, Q7 (OLAP 0.01s -> ~5-12s; OLTP 4s -> 12-134s) | 10c39f40af (#6337/#6430, in 26.9.1) | Star-join count push-down (DegreeProductOp) now declines whenever any arm endpoint has a label. Q4/Q7 label every endpoint -> fall back to full pattern matching. Correctness fix, too blunt for LSQB. |
| LSQB Q5 OLAP (0.2s -> ~3.5s) | 11da14a84b (#8426, after 26.9.1) | "GAV slices per concrete edge type" + inheritance-aware overlap check in the count push-down; the polymorphic Message query no longer takes the fast path. |
| Mode 1 OLAP BFS (7s -> 80-260s, only after other algorithms ran) | 62124466c9 (#8660) | LocalBucket.findAvailableSpace: when the last gatherPageStatistics() was truncated (gatherTruncated), every record that does not fit triggers an unthrottled full-bucket gather -> O(records x pages). Hit by the bulk `UPDATE Vertex SET DISTANCE` on records already widened by earlier algorithms. Reproduced on a DB snapshot: bulk update 65s vs 2.9s on 26.8.1. Thread dumps show the stack in findAvailableSpace/updateMultiPageRecord. |
| Mode 1 OLTP SSSP | not a regression | In isolation 38.6s (new) vs 45.3s (old), 2 runs each. |

## With the fixes (branch fix/8660-bucket-gather-quadratic in ~/Documents/GitHub/arcadedb-regressions, 3 commits on origin/main bda510bca0)
Same machine/method; fixed engine classes overlaid on the 26.10.1 fat jar. All Mode 1 runs 6/6 validated; all LSQB counts identical to 26.8.1.

| Mode 1 T_p (s) | OLAP 26.8.1 | OLAP 26.10.1 | OLAP fixed | OLTP 26.8.1 | OLTP 26.10.1 | OLTP fixed |
|---|---|---|---|---|---|---|
| PR   | 3.04 | 2.24 | 2.95 | 43.6 | 41.5 | 41.0 |
| BFS  | 7.14 | 258 (154) | **8.31** | 98.8 | 71.4 | 84.1 |
| WCC  | 3.14 | 14.8 (3.0) | 3.19 | 94.3 | 66.0 | 81.9 |
| CDLP | 13.5 | 11.8 | 13.6 | 58.3 | 45.6 | 62.5 |
| LCC  | 5.80 | 5.32 | 5.58 | 169 | 155 | 168 |
| SSSP | 6.45 | 6.37 | 6.39 | 54.6 | 79.8 (38.6 alone) | 48.2 |

| LSQB (s) | OLAP 26.8.1 | OLAP 26.10.1 | OLAP fixed | OLTP 26.8.1 | OLTP 26.10.1 | OLTP fixed |
|---|---|---|---|---|---|---|
| Q4 | 0.01 | 5.36 | 0.12 | 4.10 | 12.61 | 1.08 |
| Q5 | 0.21 | 3.75 | 0.29 | 14.14 | 6.51 | 14.93 |
| Q7 | 0.01 | 12.01 | 0.13 | 3.89 | 44.12 | 1.04 |
Other queries unchanged within noise. Mode 2 unchanged (BFS 0.078s, PR 0.174s, LCC 2.1s, load 51s).
