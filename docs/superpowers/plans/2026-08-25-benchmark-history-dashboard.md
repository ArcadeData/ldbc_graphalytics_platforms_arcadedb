# Benchmark History Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish ArcadeDB's Graphalytics benchmark results (Modes 1, 2, 3) as a public,
history-tracking GitHub Pages dashboard, using `github-action-benchmark` to accumulate
data points and render charts, with regression alerting.

**Architecture:** Each of the three existing CI benchmark jobs
(`mode1-official`, `mode2-embedded`, `mode3-lsqb`) gains one step that converts its own
output into `github-action-benchmark`'s generic `customSmallerIsBetter` JSON format and
uploads it as a build artifact. A new `publish-benchmarks` job, depending on all three
(`if: always()`), downloads whatever artifacts exist, calls
`benchmark-action/github-action-benchmark` once per mode it received data for (each
pushing to a separate path on the `gh-pages` branch), then republishes a hand-authored
landing page on top.

**Tech Stack:** GitHub Actions YAML, Python 3 (stdlib only — no new pip dependencies),
`benchmark-action/github-action-benchmark`, `peaceiris/actions-gh-pages`, static HTML.

**Spec:** `docs/superpowers/specs/2026-08-25-benchmark-history-dashboard-design.md`

## Global Constraints

- All three modes report time in seconds; every JSON entry's `unit` is `"s"`.
- `alert-threshold: '150%'`, `comment-on-alert: true`, `fail-on-alert: true` on every
  `github-action-benchmark` call (from the spec's chosen alerting behavior).
- Every third-party action reference must be pinned to a full commit SHA with a
  trailing `# vX.Y.Z` comment, matching this workflow's existing convention (e.g.
  `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`). Verified SHAs
  for this plan (resolved via `gh api` against the GitHub API, not guessed):
  - `benchmark-action/github-action-benchmark@52576c92bccf6ac60c8223ec7eb2565637cae9ba # v1.22.1`
  - `peaceiris/actions-gh-pages@84c30a85c19949d7eee79c4ff27748b70285e453 # v4.1.0`
  - `actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1`
- `publish-benchmarks` is the only job with `contents: write`; the workflow-level
  default (`contents: read`) and the three existing jobs are untouched.
- Landing page source lives at `.github/pages/index.html` on `main` (real, reviewable
  file — no Jekyll, no build step) and is copied to the `gh-pages` root on every
  `publish-benchmarks` run with `keep_files: true`, so it never deletes the
  `dev/bench/mode{1,2,3}/` directories written by the benchmark-action calls.
- No new pip dependencies: the two conversion scripts and their tests use only the
  Python 3 standard library (`json`, `re`, `sys`, `unittest`) — this repo has no
  existing Python test suite or pytest dependency, so stdlib `unittest` is used
  instead of introducing one for two small scripts.

---

### Task 1: Mode 1 results.json → benchmark JSON converter

**Files:**
- Create: `.github/scripts/mode1_to_bench_json.py`
- Create: `.github/scripts/test_mode1_to_bench_json.py`

**Interfaces:**
- Produces: `convert(data: dict) -> list[dict]` in `mode1_to_bench_json.py`, importable
  by its test file (both live in the same directory, so a plain `from
  mode1_to_bench_json import convert` works when run from `.github/scripts/`). `main()`
  reads a file path from `sys.argv[1]`, loads it as JSON, and writes `convert(...)`'s
  result as JSON to stdout.
- Consumes: nothing from other tasks.

This mirrors the existing "Summarize results" step's exact data-access pattern
(`.github/workflows/arcadedb-benchmark.yml:86-96`) against a real Mode 1
`results.json`: `result.jobs[*].algorithm` / `result.jobs[*].runs` resolve each run's
algorithm name; `result.runs[*].load_time` / `processing_time` are plain numeric
strings already in seconds (confirmed against `graphalytics-core`'s `ResultData`/
`BenchmarkMetric` sources — `BenchmarkMetric.toString()` returns a bare
`BigDecimal.toString()`, no unit suffix), or the literal string `"nan"` if that run
failed.

- [ ] **Step 1: Write the failing test**

Create `.github/scripts/test_mode1_to_bench_json.py`:

```python
import unittest

from mode1_to_bench_json import convert


class ConvertTests(unittest.TestCase):
    def test_two_algorithms_sorted_by_timestamp(self):
        data = {
            "result": {
                "jobs": {
                    "job-pr": {"algorithm": "PR", "runs": ["run-pr"]},
                    "job-wcc": {"algorithm": "WCC", "runs": ["run-wcc"]},
                },
                "runs": {
                    "run-pr": {"timestamp": "2", "load_time": "1.500", "processing_time": "4.250"},
                    "run-wcc": {"timestamp": "1", "load_time": "0.900", "processing_time": "2.100"},
                },
            }
        }
        self.assertEqual(
            convert(data),
            [
                {"name": "WCC load", "unit": "s", "value": 0.9},
                {"name": "WCC processing", "unit": "s", "value": 2.1},
                {"name": "PR load", "unit": "s", "value": 1.5},
                {"name": "PR processing", "unit": "s", "value": 4.25},
            ],
        )

    def test_skips_nan_values(self):
        data = {
            "result": {
                "jobs": {"job-bfs": {"algorithm": "BFS", "runs": ["run-bfs"]}},
                "runs": {
                    "run-bfs": {"timestamp": "1", "load_time": "nan", "processing_time": "3.0"}
                },
            }
        }
        self.assertEqual(
            convert(data),
            [{"name": "BFS processing", "unit": "s", "value": 3.0}],
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd .github/scripts && python3 -m unittest test_mode1_to_bench_json -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'mode1_to_bench_json'`
(the implementation file doesn't exist yet).

- [ ] **Step 3: Write the implementation**

Create `.github/scripts/mode1_to_bench_json.py`:

```python
#!/usr/bin/env python3
"""Convert a Mode 1 (official LDBC Graphalytics) results.json into
github-action-benchmark's customSmallerIsBetter JSON format."""
import json
import sys


def convert(data):
    result = data.get("result", data.get("experiments", {}))
    runs = result.get("runs", {})
    jobs = result.get("jobs", {})

    entries = []
    for run_id, run in sorted(runs.items(), key=lambda item: item[1]["timestamp"]):
        algorithm = next(j["algorithm"] for j in jobs.values() if run_id in j["runs"])
        for metric, label in (("load_time", "load"), ("processing_time", "processing")):
            raw = run.get(metric)
            if raw is None or raw == "nan":
                print(f"skipping {algorithm} {label}: no value ({raw!r})", file=sys.stderr)
                continue
            entries.append({"name": f"{algorithm} {label}", "unit": "s", "value": float(raw)})
    return entries


def main():
    with open(sys.argv[1]) as f:
        data = json.load(f)
    json.dump(convert(data), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd .github/scripts && python3 -m unittest test_mode1_to_bench_json -v`
Expected: `OK` — both tests pass.

- [ ] **Step 5: Commit**

```bash
git add .github/scripts/mode1_to_bench_json.py .github/scripts/test_mode1_to_bench_json.py
git commit -m "Add Mode 1 results.json to benchmark-JSON converter"
```

---

### Task 2: Mode 2/3 log → benchmark JSON converter

**Files:**
- Create: `.github/scripts/log_to_bench_json.py`
- Create: `.github/scripts/test_log_to_bench_json.py`

**Interfaces:**
- Produces: `convert(log_text: str, names: list[str]) -> list[dict]` in
  `log_to_bench_json.py`. `main()` reads the log path from `sys.argv[1]` and the list
  of metric names to match from `sys.argv[2:]`, then writes `convert(...)`'s result as
  JSON to stdout. Shared by both Mode 2 and Mode 3 (same print format, different name
  lists), keeping the parsing logic DRY.
- Consumes: nothing from other tasks.

Both `ArcadeDBEmbeddedBenchmark.java` (`:211`) and `ArcadeDBEmbeddedLSQB.java` (`:226`)
print summary lines with `"%-10s %9.2fs%n"` — the numeric field and the literal `s`
suffix are directly adjacent with **no space** (e.g. `"PR             1.23s"`), so a
naive whitespace-split's second token is `"1.23s"`, not `"1.23"`. This must be
`rstrip("s")`'d before `float()` — verified against the actual Java source, not
assumed.

- [ ] **Step 1: Write the failing test**

Create `.github/scripts/test_log_to_bench_json.py`:

```python
import unittest

from log_to_bench_json import convert


class ConvertTests(unittest.TestCase):
    def test_mode2_style_log(self):
        log = (
            "[ArcadeDB] Running PageRank...\n"
            "Algorithm  ArcadeDB\n"
            "----------------------\n"
            "LOAD            1.23s\n"
            "PR               4.56s\n"
            "WCC               0.78s\n"
        )
        names = ["LOAD", "PR", "WCC", "BFS", "LCC", "SSSP", "CDLP"]
        self.assertEqual(
            convert(log, names),
            [
                {"name": "LOAD", "unit": "s", "value": 1.23},
                {"name": "PR", "unit": "s", "value": 4.56},
                {"name": "WCC", "unit": "s", "value": 0.78},
            ],
        )

    def test_ignores_non_matching_lines(self):
        log = "Vertices: 12345\nLOAD              2.00s\n"
        self.assertEqual(
            convert(log, ["LOAD"]),
            [{"name": "LOAD", "unit": "s", "value": 2.0}],
        )

    def test_mode3_style_query_names(self):
        log = "Q1                0.05s\nQ2                0.11s\n"
        self.assertEqual(
            convert(log, ["LOAD"] + [f"Q{i}" for i in range(1, 10)]),
            [
                {"name": "Q1", "unit": "s", "value": 0.05},
                {"name": "Q2", "unit": "s", "value": 0.11},
            ],
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd .github/scripts && python3 -m unittest test_log_to_bench_json -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'log_to_bench_json'`.

- [ ] **Step 3: Write the implementation**

Create `.github/scripts/log_to_bench_json.py`:

```python
#!/usr/bin/env python3
"""Convert a Mode 2/3 embedded-benchmark stdout log into
github-action-benchmark's customSmallerIsBetter JSON format.

Matches lines printed as "%-10s %9.2fs%n" (e.g. "PR             1.23s") for
each of the given metric names, in the order they appear in the log.
"""
import json
import re
import sys


def convert(log_text, names):
    pattern = re.compile(r"^(" + "|".join(re.escape(n) for n in names) + r")\s+(\S+)")
    entries = []
    for line in log_text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        name, raw_value = match.group(1), match.group(2)
        entries.append({"name": name, "unit": "s", "value": float(raw_value.rstrip("s"))})
    return entries


def main():
    log_path, names = sys.argv[1], sys.argv[2:]
    with open(log_path) as f:
        log_text = f.read()
    json.dump(convert(log_text, names), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd .github/scripts && python3 -m unittest test_log_to_bench_json -v`
Expected: `OK` — all three tests pass.

- [ ] **Step 5: Commit**

```bash
git add .github/scripts/log_to_bench_json.py .github/scripts/test_log_to_bench_json.py
git commit -m "Add Mode 2/3 log to benchmark-JSON converter"
```

---

### Task 3: Landing page

**Files:**
- Create: `.github/pages/index.html`

**Interfaces:**
- Produces: a static file that `publish-benchmarks` (Task 5) copies verbatim to the
  `gh-pages` branch root via `peaceiris/actions-gh-pages`.
- Consumes: nothing from other tasks — links to `dev/bench/mode1/`,
  `dev/bench/mode2/`, `dev/bench/mode3/` are relative paths that
  `benchmark-action/github-action-benchmark` creates (Task 5), not paths this task
  needs to create itself.

- [ ] **Step 1: Write the page**

Create `.github/pages/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ArcadeDB Graphalytics Benchmarks</title>
<style>
  :root { color-scheme: light dark; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    max-width: 780px;
    margin: 3rem auto;
    padding: 0 1.5rem;
    line-height: 1.5;
  }
  h1 { margin-bottom: 0.25rem; }
  p.lead { color: #666; margin-top: 0; }
  .cards { display: grid; gap: 1rem; margin-top: 2rem; }
  .card {
    display: block;
    border: 1px solid #ccc;
    border-radius: 8px;
    padding: 1.25rem 1.5rem;
    text-decoration: none;
    color: inherit;
  }
  .card:hover { border-color: #888; }
  .card h2 { margin: 0 0 0.35rem; font-size: 1.1rem; }
  .card p { margin: 0; color: #666; }
  footer { margin-top: 3rem; font-size: 0.85rem; color: #888; }
</style>
</head>
<body>
  <h1>ArcadeDB Graphalytics Benchmarks</h1>
  <p class="lead">
    Performance history for ArcadeDB on the LDBC Graphalytics and LSQB benchmark
    suites, updated weekly by
    <a href="https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb">CI</a>.
  </p>

  <div class="cards">
    <a class="card" href="dev/bench/mode1/">
      <h2>Mode 1 — Official LDBC Graphalytics</h2>
      <p>The official LDBC Graphalytics framework, reloading the graph per algorithm
      for isolated load/processing time measurements (datagen-7_5-fb).</p>
    </a>
    <a class="card" href="dev/bench/mode2/">
      <h2>Mode 2 — ArcadeDB Embedded</h2>
      <p>A standalone embedded-Java run: load once, build the Graph Analytical View
      once, run all six algorithms (datagen-7_5-fb).</p>
    </a>
    <a class="card" href="dev/bench/mode3/">
      <h2>Mode 3 — LSQB Embedded</h2>
      <p>Nine subgraph pattern-matching queries (Q1&ndash;Q9) against LDBC SNB social
      network data (SF0.1).</p>
    </a>
  </div>

  <footer>Generated by <code>.github/workflows/arcadedb-benchmark.yml</code>.</footer>
</body>
</html>
```

- [ ] **Step 2: Verify it**

Run: `python3 -c "import html.parser; p = html.parser.HTMLParser(); p.feed(open('.github/pages/index.html').read())"`
Expected: no output, exit code 0 (confirms the file parses as HTML without error).

Run: `grep -c 'href="dev/bench/mode' .github/pages/index.html`
Expected: `3` (one link per mode).

- [ ] **Step 3: Commit**

```bash
git add .github/pages/index.html
git commit -m "Add benchmark dashboard landing page"
```

---

### Task 4: Wire conversion + artifact upload into the three existing jobs

**Files:**
- Modify: `.github/workflows/arcadedb-benchmark.yml:76-105` (`mode1-official`, after
  the existing "Summarize results" step)
- Modify: `.github/workflows/arcadedb-benchmark.yml:143-160` (`mode2-embedded`, after
  the existing "Summarize results" step)
- Modify: `.github/workflows/arcadedb-benchmark.yml:198-215` (`mode3-lsqb`, after the
  existing "Summarize results" step)

**Interfaces:**
- Consumes: `.github/scripts/mode1_to_bench_json.py` and
  `.github/scripts/log_to_bench_json.py` (Tasks 1 and 2), invoked as CLI scripts.
- Produces: three build artifacts — `mode1-bench-data`, `mode2-bench-data`,
  `mode3-bench-data` — each containing exactly one file, `bench-data.json`, which Task
  5's `publish-benchmarks` job downloads.

All three new step-pairs use `if: always()`, matching the existing "Summarize
results" steps' own tolerance of a missing/partial report (same file, same
established pattern already shipped in this workflow) — if the benchmark run failed
before producing usable output, the conversion step itself fails (consistent with
`bash -e` already governing the "Summarize results" steps), which correctly leaves
the job status red; `actions/upload-artifact`'s default `if-no-files-found: warn`
means the upload step doesn't additionally hard-fail when there's nothing to upload.

- [ ] **Step 1: Add the two steps to `mode1-official`**

In `.github/workflows/arcadedb-benchmark.yml`, immediately after the existing
`- name: Summarize results` step block for `mode1-official` (ends at line 97) and
before `- name: Upload report` (line 99), insert:

```yaml
      - name: Convert results to benchmark JSON
        if: always()
        run: |
          cd "$DIST"
          LATEST=$(ls -td report/*ARCADEDB* | head -1)
          python3 "$GITHUB_WORKSPACE/.github/scripts/mode1_to_bench_json.py" "$LATEST/json/results.json" > "$GITHUB_WORKSPACE/bench-data.json"

      - name: Upload benchmark JSON
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: mode1-bench-data
          path: bench-data.json
          retention-days: 90
```

- [ ] **Step 2: Add the two steps to `mode2-embedded`**

Immediately after `mode2-embedded`'s `- name: Summarize results` step block (ends at
line 152) and before `- name: Upload log` (line 154), insert:

```yaml
      - name: Convert results to benchmark JSON
        if: always()
        run: |
          python3 .github/scripts/log_to_bench_json.py mode2-output.log LOAD PR WCC BFS LCC SSSP CDLP > bench-data.json

      - name: Upload benchmark JSON
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: mode2-bench-data
          path: bench-data.json
          retention-days: 90
```

- [ ] **Step 3: Add the two steps to `mode3-lsqb`**

Immediately after `mode3-lsqb`'s `- name: Summarize results` step block (ends at line
207) and before `- name: Upload log` (line 209), insert:

```yaml
      - name: Convert results to benchmark JSON
        if: always()
        run: |
          python3 .github/scripts/log_to_bench_json.py mode3-output.log LOAD Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 > bench-data.json

      - name: Upload benchmark JSON
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: mode3-bench-data
          path: bench-data.json
          retention-days: 90
```

- [ ] **Step 4: Validate the YAML**

Run: `actionlint .github/workflows/arcadedb-benchmark.yml`
Expected: no output (no errors). If actionlint flags anything, fix it before
continuing — do not proceed with a red actionlint run.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/arcadedb-benchmark.yml
git commit -m "Upload benchmark JSON artifacts from all three benchmark jobs"
```

---

### Task 5: Add the `publish-benchmarks` job

**Files:**
- Modify: `.github/workflows/arcadedb-benchmark.yml` (append a new top-level job
  after `mode3-lsqb`, i.e. after the final line of the file as it stands after Task 4)

**Interfaces:**
- Consumes: the three artifacts produced by Task 4 (`mode1-bench-data`,
  `mode2-bench-data`, `mode3-bench-data`, each containing `bench-data.json`) and
  `.github/pages/index.html` (Task 3).
- Produces: the `gh-pages` branch, with `dev/bench/mode{1,2,3}/` (charts, created by
  `github-action-benchmark`) and a root `index.html` (the landing page).

- [ ] **Step 1: Append the job**

At the end of `.github/workflows/arcadedb-benchmark.yml`, add:

```yaml

  publish-benchmarks:
    needs: [mode1-official, mode2-embedded, mode3-lsqb]
    if: always()
    runs-on: ubuntu-latest
    timeout-minutes: 15
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1

      - name: Download Mode 1 benchmark data
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        continue-on-error: true
        with:
          name: mode1-bench-data
          path: mode1-bench-data

      - name: Download Mode 2 benchmark data
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        continue-on-error: true
        with:
          name: mode2-bench-data
          path: mode2-bench-data

      - name: Download Mode 3 benchmark data
        uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        continue-on-error: true
        with:
          name: mode3-bench-data
          path: mode3-bench-data

      - name: Publish Mode 1 results
        id: mode1_publish
        if: hashFiles('mode1-bench-data/bench-data.json') != ''
        continue-on-error: true
        uses: benchmark-action/github-action-benchmark@52576c92bccf6ac60c8223ec7eb2565637cae9ba # v1.22.1
        with:
          name: "ArcadeDB Mode 1 — Official LDBC Graphalytics"
          tool: 'customSmallerIsBetter'
          output-file-path: mode1-bench-data/bench-data.json
          gh-pages-branch: gh-pages
          benchmark-data-dir-path: dev/bench/mode1
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
          alert-threshold: '150%'
          comment-on-alert: true
          fail-on-alert: true

      - name: Publish Mode 2 results
        id: mode2_publish
        if: hashFiles('mode2-bench-data/bench-data.json') != ''
        continue-on-error: true
        uses: benchmark-action/github-action-benchmark@52576c92bccf6ac60c8223ec7eb2565637cae9ba # v1.22.1
        with:
          name: "ArcadeDB Mode 2 — Embedded (datagen-7_5-fb)"
          tool: 'customSmallerIsBetter'
          output-file-path: mode2-bench-data/bench-data.json
          gh-pages-branch: gh-pages
          benchmark-data-dir-path: dev/bench/mode2
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
          alert-threshold: '150%'
          comment-on-alert: true
          fail-on-alert: true

      - name: Publish Mode 3 results
        id: mode3_publish
        if: hashFiles('mode3-bench-data/bench-data.json') != ''
        continue-on-error: true
        uses: benchmark-action/github-action-benchmark@52576c92bccf6ac60c8223ec7eb2565637cae9ba # v1.22.1
        with:
          name: "ArcadeDB Mode 3 — LSQB Embedded (SF0.1)"
          tool: 'customSmallerIsBetter'
          output-file-path: mode3-bench-data/bench-data.json
          gh-pages-branch: gh-pages
          benchmark-data-dir-path: dev/bench/mode3
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
          alert-threshold: '150%'
          comment-on-alert: true
          fail-on-alert: true

      - name: Publish landing page
        uses: peaceiris/actions-gh-pages@84c30a85c19949d7eee79c4ff27748b70285e453 # v4.1.0
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./.github/pages
          destination_dir: .
          keep_files: true
          exclude_assets: ''

      - name: Fail if any mode regressed
        run: |
          status=0
          for outcome in "${{ steps.mode1_publish.outcome }}" "${{ steps.mode2_publish.outcome }}" "${{ steps.mode3_publish.outcome }}"; do
            if [ "$outcome" = "failure" ]; then
              status=1
            fi
          done
          exit $status
```

Note on `exclude_assets: ''`: `peaceiris/actions-gh-pages` defaults this input to
`.github`, which excludes paths *within* `publish_dir` matching that pattern. Since
`publish_dir` here is itself `./.github/pages` (not a top-level `public/`-style dir),
the default wouldn't match anything inside it (`index.html` isn't nested under
another `.github`), but the input is set explicitly to `''` to remove any ambiguity
rather than relying on that reasoning holding at runtime.

- [ ] **Step 2: Validate the YAML**

Run: `actionlint .github/workflows/arcadedb-benchmark.yml`
Expected: no output. Fix any errors before continuing.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/arcadedb-benchmark.yml
git commit -m "Add publish-benchmarks job to publish results to GitHub Pages"
```

---

### Task 6: End-to-end validation and GitHub Pages enablement

**Files:** none (verification-only task, plus the one-time manual repo setting)

**Interfaces:**
- Consumes: everything from Tasks 1-5, run for real in GitHub Actions.
- Produces: a live `gh-pages` branch and a working public dashboard URL.

- [ ] **Step 1: Push and trigger a full run**

Push the branch containing all five prior commits (or merge to `main` per the
existing workflow's trigger — `schedule` + `workflow_dispatch` only, no `push`/`pull_request`
trigger, so a manual dispatch is required either way). Trigger via:

```bash
gh workflow run arcadedb-benchmark.yml -f mode=all
```

- [ ] **Step 2: Watch the run and inspect each job**

```bash
gh run watch $(gh run list --workflow=arcadedb-benchmark.yml --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expected: `mode1-official`, `mode2-embedded`, `mode3-lsqb` each produce a
`bench-data.json` artifact (check the run's Artifacts list); `publish-benchmarks`
runs after all three, downloads the artifacts, and completes.

- [ ] **Step 3: Verify the `gh-pages` branch content**

```bash
git fetch origin gh-pages
git ls-tree -r --name-only origin/gh-pages | grep -E '^(index.html|dev/bench/mode[123]/index.html)$'
```

Expected: four matching lines — the root `index.html` and one `index.html` per mode
under `dev/bench/mode{1,2,3}/`.

- [ ] **Step 4: Enable GitHub Pages (one-time, manual — cannot be done from this
  session without a Pages-scoped token)**

In the repo's GitHub Settings → Pages: set Source = "Deploy from a branch", Branch =
`gh-pages`, Folder = `/ (root)`. Save. GitHub will report the public URL (typically
`https://arcadedata.github.io/ldbc_graphalytics_platforms_arcadedb/`).

- [ ] **Step 5: Confirm the public site**

Once Pages finishes its own deploy (usually under a minute), fetch the landing page
and confirm it links to all three chart pages:

```bash
curl -s https://arcadedata.github.io/ldbc_graphalytics_platforms_arcadedb/ | grep -c 'href="dev/bench/mode'
```

Expected: `3`.
