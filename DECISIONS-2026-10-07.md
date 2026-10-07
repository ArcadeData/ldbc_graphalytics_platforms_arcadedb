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
