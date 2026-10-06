# Fix plan: ArcadeDB 26.10.x performance regressions (found by the Graphalytics/LSQB suite)

Target repo: ~/Documents/GitHub/arcadedb (engine). Evidence: the 26.8.1 vs 26.10.1 numbers in `ArcadeDB-release-progress.md`.
Rule for all three: **keep the correctness the original commit bought, restore the fast path where it is provably sound.** Do not revert the commits.

Priority order (impact / risk): 1 > 2 > 3.

---

## 1. Bulk update quadratic in `LocalBucket.findAvailableSpace` (commit 62124466c9, #8660)

Impact: Mode 1 OLAP BFS 7s -> 80-260s; any bulk UPDATE that grows records on a bucket whose free-space map is "full but useless". This is an engine-wide write-path hazard, not just a benchmark issue, so fix first.

Mechanism (LocalBucket ~6294 and `gatherPageStatistics()` ~6426):
- `gatherTruncated == true` means the last gather stopped at `MAX_PAGES_GATHER_STATS`.
- In `findAvailableSpace`, if nothing in the map fits, `if (bestPageAnalysis == null && gatherTruncated) gatherPageStatistics()` runs.
- `gatherPageStatistics()` now treats `resuming` as "due whatever the clock and change counter say", then scans pages (up to the whole file, wrapping) calling `getOrderedRecordsInPage` on each. When the pages that fit are rare, a single call is O(pages), and it is repeated for **every record** that does not fit.

Fix (in order of preference, combine 1+2):
1. **Bound the work per call**: a page budget per gather (e.g. a few hundred pages or a time slice) that advances `gatherResumePage` and stays `gatherTruncated` if unfinished, instead of scanning to a full map or a full wrap.
2. **Negative cache for "nothing fits"**: remember the smallest `spaceNeeded` for which a full resume pass found nothing. Requests >= that size skip the resume gather and go straight to a new page. Invalidate on: a `freeSpaceInPages.put` from `updatePageStatistics` (space freed), a delete/shrink, or `MAX_TIMEOUT_GATHER_STATS` expiry.
3. Keep the #8660 goal (reuse freed pages) by letting a resume gather run at most once per timeout window when it found nothing last time.

Tests:
- Extend `Issue8660BucketSpaceReuseTest`: fill a bucket with full pages plus a few sparse pages beyond the cap, run a bulk update that grows every record, assert the number of `gatherPageStatistics` page scans is O(pages), not O(records x pages) (add a package-private scan counter), and that wall time is bounded.
- Keep the existing reuse assertions (freed pages still get reused).

Acceptance (reproducers exist, see "Reproducers"):
- BFS-after-other-algorithms bulk update: 65s -> <= ~3s (26.8.1 was 2.9s).
- Mode 1 OLAP BFS T_p back to ~7s regardless of algorithm order.

---

## 2. Star-join count push-down declined for any labelled arm endpoint (commit 10c39f40af, #6337)

Impact: LSQB Q4/Q7 OLAP 0.01s -> 5-12s, OLTP 4s -> 12-134s. Biggest SNB-style query regression.

Mechanism (`CypherExecutionPlan` ~5148): `DegreeProductOp.Arm` has no per-endpoint label, so the commit declines the whole push-down if any non-central node `hasLabels()`. Correct, but Q4/Q7 label every endpoint (`:Tag`, `:Person`, `:Person`, `:Comment`) and fall back to full pattern matching.

Fix: give `Arm` an optional endpoint label and enforce it; keep the decline only for what cannot be enforced.
1. `DegreeProductOp.Arm`: add `endpointLabel` (nullable) for single-hop arms. Resolve it at execute time to an `IntHashSet` of bucket ids (same helper `PairHashJoinOp` uses for `arm1Buckets`/`arm2Buckets`, includes sub-types).
2. Execution:
   - **Implied label** (common case, the LSQB one): if every neighbor bucket reachable through that edge type/direction is already in the label's bucket set, the filter is a no-op, so the existing `executeFastScan` array arithmetic stays untouched. Cheapest way to know: have `CSRBuilder` record, per edge-type slice and direction, the set of neighbor bucket ids (a small `IntHashSet`, filled during the build pass it already makes), exposed through `GraphTraversalProvider`/`NeighborView`.
   - **Real filter**: compute the arm's degree as "count of neighbors whose bucket is in the set" using a `bucketIds[]` table (built once per execution, as `PairHashJoinOp` does). O(E) for that arm, still far cheaper than pattern matching.
   - Multi-hop arms with interior labels: route through `CSRCountUtils.walkArm(provider, v, types, dirs, buckets)` (already takes per-hop buckets) in `executePerNode`.
3. `CypherExecutionPlan`: replace the blanket `return null` with building the arm label; still return `null` for a label shape the op cannot express (e.g. label on a node reached by `BOTH` with mixed types).
4. OLTP path (`executeOLTP`, no GAV): apply the same filter via vertex type check, otherwise the OLTP fallback loses the fix (the OLTP Q4/Q7 regression is the same commit).

Tests:
- Keep `CypherStarCountArmLabelIssue6337Test` (the over-count case `(:Author)` vs `()` must still return the correct number), and `CypherCountPushDownLabelIssue6322Test`.
- Add: labelled endpoints where the label is implied (assert result equals the unlabelled/enumerated count AND that `EXPLAIN`/profile shows `DegreeProductOp`), and where it is not implied (assert correct filtered count).
- `GAVEligibilityTest`: update expectations from "declines" to "pushes down with endpoint filter".

Acceptance: LSQB Q4/Q7 OLAP back to ~0.01-0.05s, OLTP <= 4-5s, with #6337 tests green.

---

## 3. Count push-down declines Q5 (commit 11da14a84b, #8426)

Impact: LSQB Q5 OLAP 0.2s -> ~3.5s.

Mechanism: `CypherExecutionPlan.chainHopsAreUnique` (new in that commit) only accepts an overlapping pair of hops if it is **adjacent** (`overlapSecondHop == overlapFirstHop + 1`) and the inequality is on nodes `i` and `i+2`. Q5 = `(t1)<-[:HAS_TAG]-(m)<-[:REPLY_OF]-(c)-[:HAS_TAG]->(t2) WHERE t1 <> t2`: hops 0 and 2 overlap (both `HAS_TAG`), non-adjacent, so it is declined. It is actually safe: the two hops bind the same edge only if `m = c` **and** `t1 = t2`, and `t1 <> t2` forbids that.

Fix: generalise the rule instead of special-casing adjacency.
- For each overlapping pair of hops (i, j), same-edge binding forces specific node coincidences: the near end of i = near end of j and the far end of i = far end of j (taking each hop's direction into account; for `BOTH` either orientation). An inequality on **one such forced pair** protects the pair.
- A chain is unique if every overlapping pair is protected. Keep the existing limit that the ops support a single inequality, so this accepts: one overlapping pair at any distance whose forced endpoints carry the inequality (adjacent case = today's behaviour; Q5 = non-adjacent case), and declines multiple pairs / unprotected pairs as today.
- Keep the counter-example from the commit message as a test (three-hop chain with `WHERE a <> d` that lets hops 1 and 3 share an edge must still decline).

Tests: add Q5-shaped chain (non-adjacent overlap, inequality on the forced endpoints) asserting push-down and an exact count versus the enumerated result, plus parallel-edge/self-loop variants (`occurrencesOf` from the same commit already covers the counting side).

Acceptance: LSQB Q5 OLAP back to ~0.2s.

---

## Order of work and validation

1. Fix 1 (isolated to `LocalBucket`, small, highest blast radius). Validate with the BFS reproducer, then Mode 1 full sequence.
2. Fix 3 (small planner change).
3. Fix 2 (larger: op + CSR metadata + OLTP path).
4. Re-run the full suite on this MacBook (Mode 1 OLTP+OLAP, Mode 2, Mode 3 OLTP+OLAP) against 26.8.1 and the fixed snapshot; update `results-*.md`.

## Reproducers (kept in this session's scratchpad, copy into the repo if wanted)

- `repro/BfsRepro.java` + a database snapshot taken at the start of BFS in the Mode 1 sequence (`snap.sh`): bulk update 65s (bad) vs 2.9s (good).
- LSQB: load once with 26.8.1, copy the DB, run `ArcadeDBEmbeddedLSQB` against each engine build with its `engine/target/classes` first on the classpath (`try.sh`, `bis3.sh`).
- `git bisect run` scripts: `bis.sh` (Q7), `bis2.sh` (BFS bulk update), `bis3.sh` (parameterised by query/threshold).

## Guarding against recurrence

- This repo's daily benchmark workflow should fail or alert when any ArcadeDB number regresses > 2x versus the previous run (LSQB Q1-Q9 OLAP/OLTP, Mode 2 algorithms, Mode 1 full sequence **in the default algorithm order**, since BFS only regressed after other algorithms had written results).
- Add a push-down eligibility test in the engine that fails when Q1-Q9 stop using their count push-down ops.
