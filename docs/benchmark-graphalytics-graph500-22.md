# LDBC Graphalytics on graph500-22 (derived `graph500-22-w`), warm and validated, 2026-10-06

Dataset: `graph500-22` as `graph500-22-w` (2,396,657 vertices, 64,155,735 undirected edges, stored once; ids 6 and 248533 swapped, constant weight 1.0). Official algorithms BFS, CDLP, LCC, PR, WCC.
Machine: MacBook Pro M5 Pro, 48 GB, AC power, Docker Desktop 32 GB, 5-minute limit per operation, 12 GB heap for JVM systems, ArcadeDB `26.11.1-SNAPSHOT`
(ArcadeDB `main` @ `cbf701d66e`, with the Q9 fix #9282 and the CDLP tie-break #9285), Temurin 25 with `-XX:+UseCompactObjectHeaders`. Raw logs: `weekly-results/20261006-graph500-22-w/`.
This replaces the cold, unvalidated single-run table of 2026-10-03 in [`results-m5-multivendor-2026-10-03.md`](../results-m5-multivendor-2026-10-03.md).

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

- Seconds, warm medians (the first call of every algorithm is an untimed warm-up; median of 3 timed runs, 5 in the embedded ArcadeDB JVM which is launched 3 times and the median of those is shown; when the warm-up call takes longer than 60 s there is a single timed run). Bold = fastest valid result in the column.
- **✗** = the output was exported in full and **failed** the check against the official `graph500-22` reference outputs, so the time is shown for completeness and is not ranked. **N/A** = the system has no implementation (or its extension does not load). **timeout** = the 5-minute limit per operation. **out of memory** = the system ran out of memory inside Docker Desktop's 32 GB.
- Every other cell was validated at 100% against the official reference (BFS and CDLP exact, WCC same partition, PageRank and LCC within 1e-4), after swapping ids 6 and 248533 back (the derived `graph500-22-w` swaps them so that the official BFS source 248533 is the vertex 6 the drivers use, and it carries a constant edge weight of 1.0). SSSP is not part of `graph500-22` (no weights, no reference output) and was skipped for every system.
- \*\* The embedded benchmark is launched as a plain Java process without the memory sampler of the multi-vendor harness, so the process peak was not measured; the table shows its live heap after a full GC (1.6 GiB, 3.6 GiB in the first launch). It uses a fixed 12 GB heap.
- † The load time is the remembered original load of the same database (2026-10-03 / 2026-10-04); the data was reused in this run. The ArcadeDB Docker database was built on 2026-10-03 by the 26.10.1-era embedded loader; the embedded benchmark loaded its own database in this run. Load times are not like for like (ArcadeDB loads with its embedded Java loader, the other server systems through Python batches over the network, FalkorDB with its bulk loader).
- ‡ DuckPGQ's BFS ran for 20 minutes without finishing (DuckDB does not honour the in-process interrupt while its shortest-path operator runs); the run was ended by hand. The same happens on `datagen-7_5-fb`.
- § Memgraph stores every edge in both directions (128M edge records): the load includes the one-off reverse-edge step (553 s). WCC and CDLP exceed the 31-32 GiB the engine may use in Docker's 32 GB; they fail inside the engine.
- ¶ ArangoDB's container was killed by the out-of-memory limit (peak 30.6 GiB) during BFS, after PageRank (261.9 s, one timed run) and WCC (106.4 s, one timed run); CDLP hit the 5-minute limit and the AQL LCC query is rejected.

Reproduce: `cd ldbc-native && GRAPHALYTICS_DATASET=graph500-22-w GRAPHALYTICS_SKIP=sssp python3 benchmark.py <vendor>` (add `GRAPHALYTICS_DUMP_DIR=<dir>` to export and validate the outputs, and `--vendor-timeout 5400` for the slow loaders); embedded ArcadeDB: `java ... -Dgraph=graph500-22-w -Dskip.sssp=true -Ddb.path=<dir> ArcadeDBEmbeddedBenchmark`. Docker Desktop needs 32 GB for the in-memory systems.

## Per system

- **ArcadeDB embedded** (Graph Analytical View): all five algorithms valid. CDLP uses the tie-break by vertex id from [ArcadeData/arcadedb#9285](https://github.com/ArcadeData/arcadedb/issues/9285) (rank built once, outside the timed call). 22 vertices (the community of vertex 6) settle on label 17 instead of 6 because ids 6 and 248533 are swapped in the derived dataset, which changes the "smallest id" tie-break for that community; `validate_outputs.py` counts these as `swap_tie_break`, not as mismatches. LCC is the slow one (46.9 s), as in the earlier cold run (44.3 s).
- **ArcadeDB Docker**: all five valid (CDLP with the same 22 swap tie-break vertices). The server needs `-Darcadedb.server.httpQueryMaxResultRows=5000000` for the full 2.4M-row exports (the default cap is 1,000,000 rows); the timed calls return `count(*)` and are not affected.
- **Neo4j (GDS)**: PageRank, WCC and BFS valid; LCC exceeds 5 minutes (15 s on `datagen-7_5-fb`); no CDLP in the driver.
- **Kuzu**: PageRank, WCC and BFS valid; no LCC or CDLP. **LadybugDB**: only BFS runs (the `algo` extension for macOS arm64 fails to load), valid.
- **DuckPGQ**: WCC valid; PageRank fails validation (`pagerank()` has no iteration or damping parameter); LCC timeout; BFS does not finish.
- **Memgraph**: PageRank and BFS valid; WCC and CDLP run out of memory.
- **ArangoDB**: PageRank and WCC were timed (single runs); see the table for their validation state.
- **FalkorDB**: WCC and BFS valid; PageRank fails (no parameters); CDLP finds the same communities as the reference with other label values, which the exact-match rule rejects.
- **HugeGraph (Vermeer)**: PageRank, WCC and BFS valid; CDLP returns other label values; LCC timeout.

## Harness problems found and fixed during this run

- ArcadeDB Docker: the server capped HTTP result sets at 1,000,000 rows, so the validation exports failed; raised for the benchmark container.
- Memgraph: the one-off reverse-edge load step ran under the 5-minute per-operation limit, was cut off, and the algorithms then ran on a graph with only some reverse edges (PageRank 0.04% valid). The step now has a 40-minute bound and stops the vendor if it still fails; the database was reloaded (`--reset`).
- FalkorDB: the bulk loader's Redis client timed out sending the 128M edge records (`Timeout writing to socket`); the loader now gets `socket_timeout=3600`.
- Exports that hang (HugeGraph and Neo4j LCC re-run the slow algorithm for the export): every export is now bounded to 10 minutes (`DUMP_TIMEOUT`), and an algorithm listed in `GRAPHALYTICS_SKIP` is neither timed nor exported. Two vendor children (DuckPGQ BFS, HugeGraph LCC export) had to be ended by hand because their clients ignore the alarm.
- Validator: for derived datasets with swapped ids, CDLP labels equal to a swapped id can legitimately differ (see above); they are counted as `swap_tie_break`.
- Embedded Java benchmark: `-Dgraph=<dataset>` and `-Dskip.sssp=true`.
