# Decisions log, 2026-10-07 (autonomous session; review and steer)

Context: the ArcadeDB Docker Graphalytics row timed `count(*)` calls; I changed it to full output, then found (code audit) that the vendors are inconsistent.
Owner's decision: one rule for everybody, **compute only**: the algorithm runs completely server-side and the timed call returns a count/summary (no per-vertex transfer).
If a compute-only call turns out to be optimised away for some vendor, the fallback is: **all vendors full output**.

## Audit of what each driver returned in the timed call (before this session)
| Vendor | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|
| ArcadeDB Docker (before 10-07) | count | count | count | count | count | count |
| Neo4j | `stats` mode (compute only, two runs for the PR correction) | top-10 | one count | top-10 | n/a | n/a |
| Kuzu | full | full | full | n/a | n/a | n/a |
| LadybugDB | top-10 | top-10 | full | top-10 | n/a | n/a |
| DuckPGQ | full | full | full | full | n/a | n/a |
| Memgraph | full | full | full | n/a | full | full |
| FalkorDB | full | full | one row | n/a | n/a | full |
| ArangoDB | server-side Pregel | server-side Pregel | grouped by depth | top-10 | full | server-side |
| HugeGraph | server-side task | server-side | server-side | server-side | server-side | server-side |

## Decisions
1. 10-07 00:50 Fixed the false sentence in docs (the note claimed every other system returns full output) and pushed.
2. Correction to the audit above: Neo4j PageRank was already compute only (`gds.pageRank.stats`, no scores shipped; the validated export streams them).
3. Implementation of "compute only" (env `GRAPHALYTICS_OUTPUT=count`, the default; `=full` is the all-full-output fallback and is kept in every driver):
   every timed call runs the whole algorithm and returns a **summary row** `(n, agg)` where `agg` depends on every output value
   (PR/LCC: sum, WCC/CDLP: count distinct, BFS/SSSP: max distance). A value-dependent aggregate (not a bare `count(*)`) is used so an engine cannot skip
   computing values. `bench_common.check_summary()` compares each summary with the same summary computed from the official reference output
   (`datasets/<graph>/<graph>-<ALGO>`; tolerance 1e-4 relative) and prints `[summary] <vendor> <ALGO>: ok | MISMATCH`; results carry `_summary` and `_output`.
   A summary mismatch means the timed call did not compute the defined algorithm: that time must not be ranked. This is an extra correctness guard on the
   TIMED call itself; the untimed full-output export + `validate_outputs.py` remain the authoritative validation (not re-run in this session, the export
   code and the algorithm calls are unchanged since the 2026-10-05 validated runs).
4. Per vendor (what the timed call is now):
   - ArcadeDB Docker: `CALL ... YIELD node, x WITH node, x RETURN count(*) AS n, <agg> AS agg` over HTTP, one row back.
     **Engine bug found:** on 26.11.1-SNAPSHOT an aggregate directly after `CALL ... YIELD` returns 0/null for `sum`, `max` and `count(DISTINCT)` (only `count(*)` is right);
     a `WITH node, x` clause in between gives the right values (verified against the reference: PR sum 1.0000000000000224, WCC 1 component, LCC sum 55493.73, CDLP 218 labels,
     BFS/SSSP max 5 / 5.44446). Worth a bug report to ArcadeData/arcadedb (not filed by me).
   - Kuzu, LadybugDB: `CALL ... RETURN count(*), <agg>` (Ladybug PR/WCC/LCC still fail: the algo extension does not load on macOS arm64). Kuzu BFS `RETURN count(*), max(length(e))`.
   - DuckPGQ: `SELECT count(*), <agg> FROM <table function>`; BFS also summary (it still times out as before).
   - Memgraph, FalkorDB: same procedure call, `RETURN count(*), <agg>`; FalkorDB BFS was already a reached-count query.
   - Neo4j: WCC/LCC `stream` reduced on the server to `count(*), <agg>` (no `asNode` lookup, no top-10 sort); BFS reached-count (unchanged); PR `stats` (unchanged).
   - ArangoDB: Pregel PR/WCC/CDLP unchanged (`store=False`, nothing returned; no summary to check); LCC and SSSP AQL reduced with `COLLECT AGGREGATE`; BFS already grouped by depth.
   - HugeGraph (Vermeer): unchanged (compute task, output stays on the server; no summary to check).
   - Embedded ArcadeDB, Mode 1: unchanged (in process).
5. BFS/SSSP summaries add 1 to the row count where the engine does not return the source vertex itself (the export adds it with distance 0).

## Progress notes (datagen-7_5-fb compute-only run finished 02:35; graph500-22-w running)
- All summaries ok except: DuckPGQ PR (known wrong algorithm: `pagerank()` has no iteration/damping parameter, was already flagged), FalkorDB BFS (check bug, source row not counted: fixed in the driver after the run; time unaffected).
- ArangoDB LCC is N/A as before (the AQL query is rejected by this ArangoDB version: `uniqueVertices: 'global'` with ANY traversal).
- Two timings moved the "wrong" way and need a look once the queue is idle (an optimiser could be the cause): DuckPGQ LCC 49.3 s (full output) -> 13.2 s (count/sum), Neo4j LCC 15.4 s (top-10 query) -> 60.0 s (stream + sum). Plan: time the old and new query forms back to back on an idle machine and decide whether the compute-only form is representative.

## 05:35 Second iteration: cheap timed aggregate for WCC / CDLP
- First graph500-22-w compute-only pass (logs `weekly-results/20261007-compute-only/`) showed the `count(DISTINCT label)` aggregate is itself expensive in ArcadeDB's Cypher pipeline
  (CDLP 20.2 s with it, against 7.6 s with the full output and 4.3 s with a bare `count(*)`), i.e. the aggregate, not the algorithm, dominated. That would unfairly penalise ArcadeDB.
- Decision: the timed call now uses `count(*)` + a cheap `max(label)` for WCC/CDLP (sum for PR/LCC, max distance for BFS/SSSP), and the number of distinct labels is verified in ONE extra
  UNTIMED call after the timed runs (so the summary check against the reference stays). Applied to ArcadeDB, Kuzu, DuckPGQ, Memgraph, FalkorDB, Neo4j (WCC).
- Rerun scope (logs `weekly-results/20261007-compute-only-v2/`): ArcadeDB, Kuzu, FalkorDB fully; Neo4j fully on datagen, without LCC on graph500-22-w (it times out); DuckPGQ and Memgraph only WCC (+CDLP for Memgraph).
  The other cells (LadybugDB, ArangoDB, HugeGraph, DuckPGQ/Neo4j/Memgraph unaffected algorithms) are taken from the first pass: their timed call did not change.
- Known and accepted summary difference: on `graph500-22-w` CDLP has one label fewer than the reference (the 22 vertices of vertex 6's community settle on label 17 because ids 6 and 248533 are swapped); the check allows exactly that (-1).

## 07:45 Results, review points (please read)
- **Final numbers** are in README / docs (datagen: ArcadeDB Docker, Kuzu, FalkorDB from `weekly-results/20261007-compute-only-v3/`; Neo4j from `-v2/`; DuckPGQ PR, LCC, Memgraph, ArangoDB, HugeGraph, Ladybug from `20261007-compute-only/`;
  Memgraph and DuckPGQ WCC from `-v2/`. graph500-22-w: ArcadeDB Docker, Kuzu, FalkorDB, Neo4j (LCC from the first pass) from `-v2/`, the rest from `20261007-compute-only/`). Load times and the embedded/Mode 1/LSQB rows were not re-measured.
- **Optimisation check (the risk you named):** DuckPGQ LCC 47.7 s (returns 633K rows) vs 13.0 s (summary), same summary, so the difference is the row transfer, not skipped work. Neo4j LCC: old top-10 form 14.2 s, new summary form 14.4 s (`weekly-results/20261007-investigate/`): the 55-60 s seen in the first two passes was machine noise (swap was at 3 GB of 4 GB), so the numbers
  in the tables come from runs where the value agrees with this check. I did NOT see an optimised-away call: every summary matched the reference, except the known cases (DuckPGQ PR wrong algorithm, Memgraph graph500 CDLP 775 labels vs 1485 = wrong result, shown ✗).
- **Weak spot:** the PageRank summary (sum of scores) equals 1 for any normalised PageRank, so it cannot detect a wrong variant (FalkorDB PR passes it but was ✗ in the full validation of 2026-10-05); ✗ marks come from that full validation, which I did not repeat today. If you want, rerun with `GRAPHALYTICS_DUMP_DIR` + `scripts/validate_outputs.py`.
- **Noise:** the datagen runs of the second pass (05:35-05:47) were slower than the first pass for several vendors (swap pressure); I reran ArcadeDB, FalkorDB, Kuzu and DuckPGQ-WCC on an idle machine (`-v3`). DuckPGQ WCC stays very variable (1.8 / 3.3 / 8.0 s, median shown with a footnote). Memgraph/ArangoDB/HugeGraph/Ladybug were measured once.
- **ArcadeDB aggregate cost:** a value aggregate over the Cypher pipeline costs ArcadeDB more than a bare `count(*)` (PageRank 0.14 -> 0.30 s on datagen). I kept the value aggregate for all vendors so none can skip the work; a bare count would give ArcadeDB Docker lower numbers but would not prove the values were computed. Your call.
- **Engine bug to report:** aggregates (`sum`, `max`, `count(DISTINCT)`) directly after `CALL ... YIELD` return 0/null in ArcadeDB 26.11.1-SNAPSHOT Cypher; `WITH node, x` in between fixes it.
- **Not done:** HugeGraph/ArangoDB Pregel summaries (nothing returned); Mode 1 and embedded benchmarks unchanged; LSQB unchanged; the per-vertex validation re-run; the README/docs still carry the 2026-10-05 validation marks. Everything is committed and pushed; to revert to full-output timing set `GRAPHALYTICS_OUTPUT=full` and rerun.

## 10:30 Full-output re-validation done (item 1)
All nine vendors, both datasets, exported with `GRAPHALYTICS_DUMP_ONLY=1` and checked by `scripts/validate_outputs.py` (verdicts in `weekly-results/20261007-validate/validation.txt`, logs next to it).
The ✗ marks in the tables are confirmed, nothing changed: ArcadeDB Docker valid for all six (datagen) / five (graph500-22-w) algorithms; DuckPGQ PR invalid; FalkorDB PR and CDLP invalid; Memgraph CDLP invalid; ArangoDB and HugeGraph CDLP invalid.
Everything else exported is valid (Kuzu, LadybugDB BFS, Neo4j PR/WCC/LCC/BFS-reach on datagen, HugeGraph PR/WCC/BFS/LCC, ArangoDB PR/WCC/BFS/SSSP, Memgraph PR/WCC/BFS/SSSP).
Only new observation: Neo4j LCC on graph500-22-w exports with 23 mismatching vertices (all other vertices match); the timed LCC there times out anyway, so no table cell is affected.
