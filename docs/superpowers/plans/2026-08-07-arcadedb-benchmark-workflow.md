# ArcadeDB Benchmark Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a GitHub Actions workflow (`.github/workflows/arcadedb-benchmark.yml`) that runs all three of this repo's ArcadeDB-only benchmark modes (official LDBC Graphalytics, embedded native, LSQB embedded) on a weekly schedule and on manual dispatch, publishing results as job summaries and artifacts.

**Architecture:** Three independent jobs in one workflow file, each self-contained (checkout → build → download dataset → run → summarize → upload). Two prerequisite code fixes unblock jobs 1 and 3: a broken Maven assembly include pattern (Mode 1 can't produce a runnable distribution without it) and a hardcoded dataset path in the LSQB embedded benchmark (needs to be swappable so CI can point it at a smaller scale factor).

**Tech Stack:** GitHub Actions (`ubuntu-latest`), Maven, Java 21 (Temurin), Python 3.11 (`datasets.py`), `zstd`.

## Global Constraints

- Workflow file: `.github/workflows/arcadedb-benchmark.yml`.
- Triggers: `workflow_dispatch` (with a `mode` choice input: `all`/`mode1`/`mode2`/`mode3`) and a weekly `schedule` cron. No `push`/`pull_request` trigger — these are performance measurements, not correctness gates.
- `permissions: contents: read` at the workflow level (matches `java-ci.yml`'s convention).
- One `concurrency` group for the whole workflow, `cancel-in-progress: false` (queue rather than kill an in-progress benchmark run).
- Each job: `runs-on: ubuntu-latest`, `timeout-minutes: 30`.
- JVM heap for all benchmark runs: `-Xms8g -Xmx8g` — sized for GitHub-hosted runners, intentionally different from `CLAUDE.md`'s 12GB local multi-vendor-fairness rule (not applicable to a single-vendor CI job), and matching the heap size `README.md` already documents for the Mode 2 embedded example.
- Datasets, pinned per job and not to be changed without re-verifying format compatibility (see spec `docs/superpowers/specs/2026-08-07-arcadedb-benchmark-workflow-design.md`):
  - Mode 1: `wiki-Talk`
  - Mode 2: `datagen-7_5-fb` (its existing hardcoded default — do not touch `ldbc-native/ArcadeDBEmbeddedBenchmark.java`'s parsing logic)
  - Mode 3: `lsqb-sf0.1`, `merged-fk` format only
- No changes to `ldbc-native/ArcadeDBEmbeddedBenchmark.java`.

---

### Task 1: Fix the Mode 1 assembly bug

**Files:**
- Modify: `src/main/assembly/bin.xml:14`

**Interfaces:**
- Produces: a `mvn package -DskipTests` build whose `graphalytics-*-arcadedb-*-bin.tar.gz` contains `bin/sh/run-benchmark.sh` and `config-template/graphs/wiki-Talk.properties`. Task 3 depends on both existing.

- [ ] **Step 1: Fix the dependencySet include pattern**

In `src/main/assembly/bin.xml`, change:

```xml
			<includes>
				<include>*:resources</include>
			</includes>
```

to:

```xml
			<includes>
				<include>science.atlarge.graphalytics:graphalytics-core:tar.gz:resources</include>
			</includes>
```

- [ ] **Step 2: Rebuild and verify**

Run:

```bash
mvn --batch-mode --no-transfer-progress package -DskipTests
TARBALL=$(ls graphalytics-*-arcadedb-*-bin.tar.gz)
tar tzf "$TARBALL" | grep -E "bin/sh/run-benchmark\.sh|config-template/graphs/wiki-Talk\.properties"
```

Expected output (both lines present, directory prefix will match the current `graphalytics.version` from `pom.xml`):

```
graphalytics-1.10.0-arcadedb-0.1-SNAPSHOT/bin/sh/run-benchmark.sh
graphalytics-1.10.0-arcadedb-0.1-SNAPSHOT/config-template/graphs/wiki-Talk.properties
```

If either line is missing, the include pattern in Step 1 doesn't match — re-check the `groupId:artifactId:type:classifier` against `pom.xml`'s `graphalytics-core` dependency with `<classifier>resources</classifier>`.

- [ ] **Step 3: Commit**

```bash
git add src/main/assembly/bin.xml
git commit -m "$(cat <<'EOF'
Fix assembly include pattern so the distribution bundles run-benchmark.sh

The dependencySet include *:resources is groupId:artifactId syntax and
never matched graphalytics-core's resources classifier, so a fresh
mvn package silently produced a distribution missing
bin/sh/run-benchmark.sh and config-template/graphs.properties.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Parameterize the LSQB embedded benchmark's dataset directory

**Files:**
- Modify: `lsqb/ArcadeDBEmbeddedLSQB.java:27`

**Interfaces:**
- Produces: a `-Ddataset.dir=<path>` system property, defaulting to the existing `../datasets/social-network-sf1-merged-fk` when unset. Task 5's `mode3-lsqb` job relies on this exact property name.

- [ ] **Step 1: Make `DATA_DIR` read from a system property**

In `lsqb/ArcadeDBEmbeddedLSQB.java`, change:

```java
  static final String DATA_DIR = "../datasets/social-network-sf1-merged-fk";
```

to:

```java
  static final String DATA_DIR = System.getProperty("dataset.dir", "../datasets/social-network-sf1-merged-fk");
```

- [ ] **Step 2: Compile and verify the property is honored**

Run (from repo root, after Task 1's build has produced the fat jar — if `target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar` doesn't exist yet, run `mvn --batch-mode --no-transfer-progress package -DskipTests` first):

```bash
LDBC_JAR="$(pwd)/target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar"
cd lsqb
javac -cp "$LDBC_JAR" ArcadeDBEmbeddedLSQB.java
java -Ddataset.dir=/tmp/lsqb-property-check -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB --reset
```

Expected: the process fails with a `java.io.FileNotFoundException` whose message includes the path `/tmp/lsqb-property-check/Country.csv`. That confirms the overridden property — not the hardcoded SF1 default — was used as the data directory. (A non-zero exit code here is expected and correct; there's no real data at that path.)

- [ ] **Step 3: Clean up compiled artifacts**

```bash
rm -f lsqb/*.class
```

(`*.class` is already gitignored — this is just local workspace cleanup, not a git operation.)

- [ ] **Step 4: Commit**

```bash
git add lsqb/ArcadeDBEmbeddedLSQB.java
git commit -m "$(cat <<'EOF'
Make LSQB embedded benchmark's dataset directory configurable

Reads DATA_DIR from a -Ddataset.dir system property, defaulting to the
existing hardcoded SF1 path when unset, so CI can point it at a smaller
scale factor without touching parsing logic.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Create the workflow with the Mode 1 job

**Files:**
- Create: `.github/workflows/arcadedb-benchmark.yml`

**Interfaces:**
- Consumes: Task 1's fixed `bin.xml` (the job runs `mvn package` itself, so it picks up the committed fix).
- Produces: the workflow's trigger block (`workflow_dispatch` with `mode` input, `schedule`, `permissions`, `concurrency`) and job-gating `if:` pattern that Tasks 4 and 5 append jobs into. Job name `mode1-official`.

- [ ] **Step 1: Write the workflow file with triggers and the `mode1-official` job**

Create `.github/workflows/arcadedb-benchmark.yml`:

```yaml
name: ArcadeDB Benchmark

on:
  workflow_dispatch:
    inputs:
      mode:
        description: "Which benchmark mode to run"
        required: false
        default: all
        type: choice
        options:
          - all
          - mode1
          - mode2
          - mode3
  schedule:
    - cron: "0 3 * * 1"

permissions:
  contents: read

concurrency:
  group: arcadedb-benchmark-${{ github.ref }}
  cancel-in-progress: false

jobs:
  mode1-official:
    if: github.event_name != 'workflow_dispatch' || github.event.inputs.mode == 'all' || github.event.inputs.mode == 'mode1'
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
          cache: maven

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install zstd
        run: sudo apt-get update -y && sudo apt-get install -y zstd

      - name: Install Python dependencies
        run: pip install requests

      - name: Build
        run: mvn --batch-mode --no-transfer-progress package -DskipTests

      - name: Download dataset
        run: python3 datasets.py download wiki-Talk

      - name: Extract and configure distribution
        run: |
          TARBALL=$(ls graphalytics-*-arcadedb-*-bin.tar.gz)
          tar xf "$TARBALL"
          DIST=$(tar tzf "$TARBALL" | head -1 | cut -d/ -f1)
          cd "$DIST"
          cp -r config-template config
          sed -i "s|^graphs.root-directory =\$|graphs.root-directory = $GITHUB_WORKSPACE/datasets|" config/benchmark.properties
          sed -i "s|^graphs.validation-directory =\$|graphs.validation-directory = $GITHUB_WORKSPACE/datasets|" config/benchmark.properties
          sed -i "s|^benchmark.custom.graphs = .*|benchmark.custom.graphs = wiki-Talk|" config/benchmarks/custom.properties
          sed -i "s|^benchmark.custom.algorithms = .*|benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC|" config/benchmarks/custom.properties
          sed -i "s|^platform.olap = false|platform.olap = true|" config/platform.properties
          echo "DIST=$DIST" >> "$GITHUB_ENV"

      - name: Run Mode 1 benchmark
        run: |
          cd "$DIST"
          bash bin/sh/run-benchmark.sh

      - name: Summarize results
        if: always()
        run: |
          cd "$DIST"
          LATEST=$(ls -td report/*ARCADEDB* | head -1)
          {
            echo "### Mode 1 — Official LDBC Graphalytics (wiki-Talk)"
            echo
            echo "| Algorithm | Load (s) | Processing (s) |"
            echo "|---|---|---|"
            python3 -c "
import json
with open('$LATEST/json/results.json') as f:
    data = json.load(f)
result = data.get('result', data.get('experiments', {}))
runs = result.get('runs', {})
jobs = result.get('jobs', {})
for rid, r in sorted(runs.items(), key=lambda x: x[1]['timestamp']):
    algo = next(j['algorithm'] for j in jobs.values() if rid in j['runs'])
    print(f\"| {algo} | {r['load_time']} | {r['processing_time']} |\")
"
          } >> "$GITHUB_STEP_SUMMARY"

      - name: Upload report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: mode1-official-report
          path: ${{ env.DIST }}/report/
          retention-days: 90
```

- [ ] **Step 2: Validate YAML syntax**

Run:

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/arcadedb-benchmark.yml'))" && echo "YAML OK"
```

Expected: `YAML OK` with no traceback. (This only checks the file parses as YAML — it does not validate GitHub Actions semantics. End-to-end validation happens in Task 6.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/arcadedb-benchmark.yml
git commit -m "$(cat <<'EOF'
Add ArcadeDB benchmark workflow with the Mode 1 official job

Runs the LDBC Graphalytics official framework against wiki-Talk on a
weekly schedule or manual dispatch, publishing results to the job
summary and as an artifact.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add the Mode 2 job

**Files:**
- Modify: `.github/workflows/arcadedb-benchmark.yml`

**Interfaces:**
- Consumes: the trigger/permissions/concurrency block and `if:` gating pattern from Task 3.
- Produces: job name `mode2-embedded`.

- [ ] **Step 1: Append the `mode2-embedded` job**

Add this job to `.github/workflows/arcadedb-benchmark.yml`, after `mode1-official` (same indentation level, i.e. directly under `jobs:`):

```yaml
  mode2-embedded:
    if: github.event_name != 'workflow_dispatch' || github.event.inputs.mode == 'all' || github.event.inputs.mode == 'mode2'
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
          cache: maven

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install zstd
        run: sudo apt-get update -y && sudo apt-get install -y zstd

      - name: Install Python dependencies
        run: pip install requests

      - name: Build
        run: mvn --batch-mode --no-transfer-progress package -DskipTests

      - name: Download dataset
        run: python3 datasets.py download datagen-7_5-fb

      - name: Compile and run embedded benchmark
        run: |
          LDBC_JAR="$GITHUB_WORKSPACE/target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar"
          cd ldbc-native
          javac --add-modules jdk.incubator.vector -cp "$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java
          java --add-modules jdk.incubator.vector -Xms8g -Xmx8g -cp ".:$LDBC_JAR" ArcadeDBEmbeddedBenchmark | tee "$GITHUB_WORKSPACE/mode2-output.log"

      - name: Summarize results
        if: always()
        run: |
          {
            echo "### Mode 2 — ArcadeDB Embedded (datagen-7_5-fb)"
            echo
            echo "| Metric | Time |"
            echo "|---|---|"
            grep -E "^(LOAD|PR|WCC|BFS|LCC|SSSP|CDLP)\s" mode2-output.log | awk '{printf "| %s | %s |\n", $1, $2}'
          } >> "$GITHUB_STEP_SUMMARY"

      - name: Upload log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: mode2-embedded-log
          path: mode2-output.log
          retention-days: 90
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/arcadedb-benchmark.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/arcadedb-benchmark.yml
git commit -m "$(cat <<'EOF'
Add Mode 2 embedded benchmark job to the ArcadeDB benchmark workflow

Runs the standalone embedded Java benchmark against its native
datagen-7_5-fb dataset, unmodified, publishing timings to the job
summary and the raw log as an artifact.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add the Mode 3 job

**Files:**
- Modify: `.github/workflows/arcadedb-benchmark.yml`

**Interfaces:**
- Consumes: the trigger/permissions/concurrency block and `if:` gating pattern from Task 3; Task 2's `dataset.dir` system property.
- Produces: job name `mode3-lsqb`.

- [ ] **Step 1: Append the `mode3-lsqb` job**

Add this job to `.github/workflows/arcadedb-benchmark.yml`, after `mode2-embedded` (same indentation level, directly under `jobs:`):

```yaml
  mode3-lsqb:
    if: github.event_name != 'workflow_dispatch' || github.event.inputs.mode == 'all' || github.event.inputs.mode == 'mode3'
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
          cache: maven

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install zstd
        run: sudo apt-get update -y && sudo apt-get install -y zstd

      - name: Install Python dependencies
        run: pip install requests

      - name: Build
        run: mvn --batch-mode --no-transfer-progress package -DskipTests

      - name: Download dataset
        run: python3 datasets.py download lsqb-sf0.1 --format merged-fk

      - name: Compile and run LSQB embedded benchmark
        run: |
          LDBC_JAR="$GITHUB_WORKSPACE/target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar"
          cd lsqb
          javac -cp "$LDBC_JAR" ArcadeDBEmbeddedLSQB.java
          java -Xms8g -Xmx8g -Ddataset.dir="$GITHUB_WORKSPACE/datasets/social-network-sf0.1-merged-fk" -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB --reset | tee "$GITHUB_WORKSPACE/mode3-output.log"

      - name: Summarize results
        if: always()
        run: |
          {
            echo "### Mode 3 — LSQB Embedded (SF0.1)"
            echo
            echo "| Query | Time |"
            echo "|---|---|"
            grep -E "^(LOAD|Q[1-9])\s" mode3-output.log | awk '{printf "| %s | %s |\n", $1, $2}'
          } >> "$GITHUB_STEP_SUMMARY"

      - name: Upload log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: mode3-lsqb-log
          path: mode3-output.log
          retention-days: 90
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/arcadedb-benchmark.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/arcadedb-benchmark.yml
git commit -m "$(cat <<'EOF'
Add Mode 3 LSQB embedded job to the ArcadeDB benchmark workflow

Runs the standalone LSQB embedded Java benchmark against the smallest
scale factor (SF0.1) via the new dataset.dir system property,
publishing per-query timings to the job summary and the raw log as an
artifact.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Validate end-to-end on GitHub Actions

**Files:** none (verification only).

**Interfaces:**
- Consumes: the complete workflow from Tasks 3–5.

This task pushes a branch and triggers real GitHub Actions runs, which spend the repo's Actions minutes and creates visible remote state. **Stop and get explicit confirmation from the user before Step 1.** Do not push or dispatch anything without that confirmation.

- [ ] **Step 1: Confirm with the user, then push a branch**

After the user confirms, push the current branch (with Tasks 1–5's commits) to `origin` so the workflow file exists on a ref GitHub can dispatch against:

```bash
git push -u origin HEAD
```

- [ ] **Step 2: Dispatch a single-mode run first**

```bash
gh workflow run arcadedb-benchmark.yml --ref <branch-name> -f mode=mode1
```

Wait for it to appear, then watch it:

```bash
gh run list --workflow=arcadedb-benchmark.yml --branch <branch-name> --limit 1
gh run watch $(gh run list --workflow=arcadedb-benchmark.yml --branch <branch-name> --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expected: the `mode1-official` job completes successfully (green check), and `mode2-embedded`/`mode3-lsqb` are skipped (grey, due to the `if:` condition). Open the run in the Actions UI and confirm the job summary shows a populated results table and the `mode1-official-report` artifact is present.

If it fails, read the failing step's log — common first-run issues are a `sed` pattern not matching (re-check the exact text in `config-template/benchmark.properties`/`custom.properties`/`platform.properties` against what Task 3 assumed) or the dataset download step timing out (network flakiness — re-run).

- [ ] **Step 3: Dispatch `mode2` and `mode3` individually**

Repeat Step 2's dispatch/watch cycle with `-f mode=mode2`, then `-f mode=mode3`. Confirm each produces a populated job summary table and its log artifact.

- [ ] **Step 4: Dispatch `all` to confirm the three jobs run together without interfering**

```bash
gh workflow run arcadedb-benchmark.yml --ref <branch-name> -f mode=all
```

Watch as in Step 2. Expected: all three jobs run (in parallel), all succeed, all three summaries appear in the same run's page, three artifacts are uploaded.

- [ ] **Step 5: Report results to the user**

Summarize which runs passed, link the Actions run(s), and ask whether to open a PR (or merge directly, per the user's normal workflow for this repo) now that the workflow is verified working.

---

## Self-Review Notes

- **Spec coverage:** bin.xml fix (Task 1), wiki-Talk pre-configured dataset for Mode 1 (Task 3), datagen-7_5-fb unchanged for Mode 2 (Task 4), lsqb-sf0.1 + `dataset.dir` parameterization for Mode 3 (Tasks 2 & 5), `workflow_dispatch` + weekly `schedule` triggers (Task 3), 8GB heap (Tasks 3–5), per-job timeout (Tasks 3–5), job summaries + artifact uploads (Tasks 3–5), no cross-job aggregation (each job's summary step is independent), end-to-end validation before considering this done (Task 6). All covered.
- **Placeholder scan:** no TBD/TODO; every step has literal file content or an exact command with expected output.
- **Type/name consistency:** `dataset.dir` system property name matches between Task 2 (Java default fallback) and Task 5 (`-Ddataset.dir=...` invocation). `DIST` env var is set in Task 3's "Extract and configure distribution" step and consumed by the same job's later steps (`Run Mode 1 benchmark`, `Summarize results`, `Upload report` via `${{ env.DIST }}`) — no cross-job reuse, which is correct since env vars don't cross jobs. Job names (`mode1-official`, `mode2-embedded`, `mode3-lsqb`) match between the `if:` input choices (`mode1`/`mode2`/`mode3`) and Task 6's per-mode dispatch calls.
