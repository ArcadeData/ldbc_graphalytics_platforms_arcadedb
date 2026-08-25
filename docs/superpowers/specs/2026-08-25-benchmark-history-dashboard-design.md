# Benchmark History Dashboard — Design

## Context

`.github/workflows/arcadedb-benchmark.yml` (see
`2026-08-07-arcadedb-benchmark-workflow-design.md`) already runs three ArcadeDB-only
benchmark jobs weekly (`mode1-official`, `mode2-embedded`, `mode3-lsqb`), each writing
a markdown summary to `$GITHUB_STEP_SUMMARY` and uploading raw results as a 90-day
build artifact. That spec explicitly deferred "historical trend tracking /
dashboards across runs" as a follow-up. This is that follow-up.

Goal: publish a public GitHub Pages site showing how ArcadeDB's Graphalytics
performance evolves over time, using `benchmark-action/github-action-benchmark` to
accumulate history and render charts, so trends and regressions are visible to
anyone without digging through Actions run logs.

Explicitly out of scope: the multi-vendor Docker-based comparisons
(`ldbc-native/benchmark.py`, `lsqb/lsqb_benchmark.py`) — not run in CI at all today,
different concern.

## Architecture

Three run jobs stay exactly as they are (parallel, `contents: read`, no behavior
changes) and each gains one step that converts its own output into
`github-action-benchmark`'s generic JSON format and uploads it as a build artifact.
A new fourth job, `publish-benchmarks`, depends on all three (`needs: [...]`,
`if: always()`) and is the only job with `contents: write`. It downloads whatever
artifacts exist, calls `github-action-benchmark` once per mode it received data for,
and finally publishes a hand-authored landing page.

```
mode1-official ──┐
mode2-embedded ──┼─(bench-data.json artifacts, if: always())─▶ publish-benchmarks ──▶ gh-pages branch
mode3-lsqb ──────┘                                              (needs: all three, if: always())
```

Rationale for funneling through one job rather than each of the three pushing to
`gh-pages` directly: three parallel jobs pushing to the same branch race and fail
intermittently. A single sequential job makes every push deterministic and is the
only place that needs write access, keeping the existing jobs' permissions
untouched.

## Data conversion (per mode → `customSmallerIsBetter` JSON)

`github-action-benchmark`'s generic tool expects a JSON array of
`{"name": ..., "unit": ..., "value": <number>}`. All three modes report seconds, so
`unit` is always `"s"`. Each run job gets one new inline `python3 -c` step (same
style as its existing "Summarize results" step — no new script files) that writes
`bench-data.json`, followed by `actions/upload-artifact` with `if: always()`.

**Mode 1** (`mode1-official`) — parses the same `report/*/json/results.json` the
summary step already reads. Confirmed via `graphalytics-core`'s
`ResultData`/`BenchmarkMetric` sources: `load_time`/`processing_time` are plain
numeric strings already in seconds (e.g. `"12.345"`, produced by
`ArcadeDBCollector`'s `BigDecimal.divide(1000, ...)` — no unit suffix in the
string), or the literal string `"nan"` if that run failed. For each run, sorted by
timestamp, resolve its algorithm via `jobs` and emit two entries:

```
{"name": "<ALGO> load", "unit": "s", "value": float(load_time)}
{"name": "<ALGO> processing", "unit": "s", "value": float(processing_time)}
```

Skip (don't emit) any value equal to `"nan"` — log a one-line warning to stdout
instead of crashing the `float()` conversion. With the current algorithm list
(BFS, WCC, PR, CDLP, LCC, SSSP) this produces up to 12 entries.

**Mode 2 / Mode 3** — reuse the exact `grep -E "^(LOAD|PR|WCC|...)\s"` pattern
already used for their step summaries. Important: the Java source's print format is
`"%-10s %9.2fs%n"` (`ArcadeDBEmbeddedBenchmark.java:211`,
`ArcadeDBEmbeddedLSQB.java:226`) — the numeric field and the literal `s` suffix are
directly adjacent with **no space**, so a matched line's second whitespace-split
token is e.g. `"1.23s"`, not `"1.23"`. The conversion step must
`rstrip("s")` before `float()`. Per matched line: `{"name": fields[0], "unit": "s",
"value": float(fields[1].rstrip("s"))}`.

- Mode 2 names: `LOAD, PR, WCC, BFS, LCC, SSSP, CDLP` (7 entries).
- Mode 3 names: `LOAD, Q1, Q2, ..., Q9` (10 entries).

## `publish-benchmarks` job

Third-party actions here (`benchmark-action/github-action-benchmark`,
`peaceiris/actions-gh-pages`) must be pinned to a full commit SHA with a trailing
`# vX.Y.Z` comment, matching how every other action in this workflow is already
pinned (see `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`).

```yaml
publish-benchmarks:
  needs: [mode1-official, mode2-embedded, mode3-lsqb]
  if: always()
  runs-on: ubuntu-latest
  permissions:
    contents: write
  steps:
    - actions/checkout
    - actions/download-artifact (mode1-bench-data / mode2-bench-data / mode3-bench-data,
      each continue-on-error so a missing artifact — upstream job failed before
      producing one — doesn't fail the whole job)
    - one benchmark-action/github-action-benchmark@v1 step per mode, each gated by
      `if: hashFiles('mode{N}-bench-data/bench-data.json') != ''` so a missing mode
      is silently skipped, each with:
        name: "ArcadeDB Mode {N} — <label>"
        tool: 'customSmallerIsBetter'
        output-file-path: mode{N}-bench-data/bench-data.json
        gh-pages-branch: gh-pages
        benchmark-data-dir-path: dev/bench/mode{N}
        github-token: ${{ secrets.GITHUB_TOKEN }}
        auto-push: true
        alert-threshold: '150%'
        comment-on-alert: true
        fail-on-alert: true
      and `continue-on-error: true` with an `id` (`mode1_publish`, etc.) so a
      regression in one mode doesn't skip publishing the other two.
    - peaceiris/actions-gh-pages@v4 to publish the landing page:
        publish_dir: ./.github/pages
        destination_dir: .
        keep_files: true   # must not delete dev/bench/* written above
        github_token: ${{ secrets.GITHUB_TOKEN }}
      Runs last, after all three benchmark-action pushes, so it layers the landing
      page on top of their latest commits. `keep_files: true` is load-bearing —
      without it this step would wipe the three dev/bench/* directories on every run.
    - final bash step: inspect `steps.mode1_publish.outcome` /
      `mode2_publish` / `mode3_publish`; `exit 1` if any is `"failure"`. This is what
      makes the overall workflow run go red on a regression despite the
      `continue-on-error: true` above (steps with that flag don't fail their job by
      themselves).
```

## Landing page

Source lives at `.github/pages/index.html` on `main` — a real, reviewable,
hand-written static file (no Jekyll, no build step). Content: page title
("ArcadeDB Graphalytics Benchmarks"), one-paragraph description, and three
cards/links to `dev/bench/mode1/`, `dev/bench/mode2/`, `dev/bench/mode3/` with a
one-line description of what each mode measures (official LDBC framework /
embedded ArcadeDB-only run / LSQB subgraph queries). Every `publish-benchmarks` run
recopies this file to `gh-pages`, so edits to it on `main` show up on the next
scheduled run — no separate deploy step needed for landing-page-only changes.

## Error handling

- A run job fails entirely (e.g. Mode 1 OOMs) → no artifact → its
  `download-artifact` step reports nothing → its `github-action-benchmark` step is
  skipped via the `hashFiles(...) != ''` gate → that mode's chart simply doesn't get
  a new data point this run; existing history is untouched. The other two modes
  still publish normally.
- A mode regresses past the 150% threshold → its new (bad) data point is still
  committed (so the trend/regression is visible on the chart) and a PR-style commit
  comment is posted, but the final bash step fails the job, so the workflow run is
  clearly red in the Actions UI.

## One-time manual step (not automatable from here)

Repo Settings → Pages → Source = "Deploy from a branch" → branch `gh-pages`,
folder `/ (root)`. This can only be done by someone with repo admin access via the
GitHub UI (or a Pages-scoped API token this session doesn't have) — flagged here so
it isn't missed after the workflow change merges.

**Correction (discovered during Task 6's real end-to-end run, not assumed):** the
`gh-pages` branch is NOT auto-created by `benchmark-action/github-action-benchmark` —
its README explicitly documents that the branch must already exist before the first
run (`git checkout --orphan gh-pages && git push origin gh-pages:gh-pages`), and on a
repo with no prior `gh-pages` branch its `git fetch origin gh-pages:gh-pages` fails
outright (`fatal: couldn't find remote ref gh-pages`). `peaceiris/actions-gh-pages`
(the landing-page step), by contrast, does gracefully create a fresh orphan branch on
first use. In practice this means the very first run's three `github-action-benchmark`
calls fail while the landing-page step succeeds and creates `gh-pages` — after which
every subsequent run (including a re-triggered one) succeeds normally. If setting this
up on a fresh repo again, create the `gh-pages` branch before the first run to avoid
this one-time failure.

## Out of scope (explicitly deferred)

- Multi-vendor comparison charts (Neo4j, Kuzu, etc.) — those benchmarks aren't run
  in CI at all; adding them is a separate, larger effort (Docker orchestration in
  CI) independent of this dashboard.
- Custom chart styling/branding beyond `github-action-benchmark`'s default
  Chart.js-rendered page — the generated per-mode pages are used as-is.
- Alert-threshold tuning (150% is a starting default, not derived from historical
  variance data we don't have yet) — revisit once a few weeks of real history exist.

## Implementation plan outline

1. Add the `bench-data.json`-producing step + `upload-artifact` to each of the three
   existing jobs.
2. Author `.github/pages/index.html`.
3. Add the `publish-benchmarks` job (download artifacts, three gated
   `github-action-benchmark` calls, `peaceiris/actions-gh-pages` landing-page copy,
   final outcome-check step).
4. Validate via `workflow_dispatch` (mode: all) before relying on the weekly
   schedule — confirm `gh-pages` branch is created with the expected
   `dev/bench/mode{1,2,3}/` structure and the landing page at the root.
5. Manually enable GitHub Pages in repo settings (see above) and confirm the public
   URL serves the landing page and all three chart pages.
