# ArcadeDB Benchmark Workflow — Design

## Context

The repo has three benchmark modes (see `CLAUDE.md`) but no automated way to run any
of them — they're all manual, local-only workflows today. We want a GitHub Actions
workflow that runs ArcadeDB's own performance benchmarks (not the multi-vendor
comparisons) on a schedule, so performance regressions and trends are visible without
someone remembering to run things by hand.

Scope is deliberately ArcadeDB-only: the `ldbc-native`/`lsqb` Python scripts that
compare ArcadeDB against Kuzu, Neo4j, etc. all depend on Docker and are out of scope
here — this workflow only exercises the pure-embedded-Java paths, which need no
Docker at all.

## Blocking finding from feasibility research

**Mode 1's distribution build is broken on a fresh checkout.**
`src/main/assembly/bin.xml` has a `<dependencySet>` with:

```xml
<includes>
  <include>*:resources</include>
</includes>
```

This pattern is `groupId:artifactId`, not `groupId:artifactId:type:classifier` — it
never matches the actual dependency, `graphalytics-core:1.10.0:tar.gz:resources`
(declared in `pom.xml` with `<classifier>resources</classifier>`). The result: a
clean `mvn package` silently produces a distribution tar.gz **missing**
`bin/sh/run-benchmark.sh` and `config-template/graphs.properties` entirely — Mode 1
cannot run at all from a fresh build. This was confirmed by building from a clean
`~/.m2` state and inspecting the resulting tar.gz contents.

The two `.tar.gz` files currently committed at the repo root are stale artifacts from
some earlier working state (predating this break, or built with a different local
`~/.m2` layout) and are not affected by this bug — they're just not reproducible by
CI or a fresh clone.

**Fix**, confirmed to work by rebuilding and inspecting the output:

```xml
<include>science.atlarge.graphalytics:graphalytics-core:tar.gz:resources</include>
```

With this fix, the distribution correctly includes `bin/sh/run-benchmark.sh` and a
full `config-template/graphs.properties` + `config-template/graphs/*.properties` set
— including a **pre-configured `wiki-Talk` entry** (vertex/edge file paths, directed
flag, supported algorithms `bfs, cdlp, lcc, pr, wcc`, BFS source vertex `2`). No
manual per-graph config authoring is needed for Mode 1's dataset.

## Dataset choices

| Job | Dataset | Why |
|-----|---------|-----|
| Mode 1 (official) | `wiki-Talk` (2.4M V, 5M E, ~33MB) | Config-driven — the platform driver's loader handles any dataset shape declared in `graphs.properties`, and wiki-Talk is small enough to load and benchmark quickly in CI. |
| Mode 2 (embedded) | `datagen-7_5-fb` (633K V, 34M E, ~155MB) — its existing default, unchanged | `ArcadeDBEmbeddedBenchmark.java` hardcodes 3-column weighted-edge parsing (`src dst weight`) and unconditionally runs SSSP. Verified by downloading `wiki-Talk` directly: its edge file is 2-column and unweighted (`0 1`, no weight field), matching its properties file excluding SSSP from supported algorithms. Feeding it to Mode 2 as-is throws `ArrayIndexOutOfBoundsException`. Rather than teach the harness to branch on edge format, Mode 2 keeps its native dataset — the CI job just downloads it into the relative path (`../datasets/datagen-7_5-fb`) the code already expects. Zero changes to parsing/algorithm logic. |
| Mode 3 (LSQB embedded) | `lsqb-sf0.1` (~400K V, ~1.8M E) | Smallest available LSQB scale factor. Unlike Mode 2, this is a safe swap: the CSV schema is scale-invariant, so pointing `ArcadeDBEmbeddedLSQB.java` at a different scale factor's directory is a pure path change, no parsing changes. |

## Design

### `.github/workflows/arcadedb-benchmark.yml`

**Triggers**: `workflow_dispatch` (on-demand) and a weekly `schedule` cron. Not on
push/PR — these are performance measurements, not correctness gates, and full runs
(build + download + load + all algorithms) are too slow to put in the normal PR
feedback loop.

**Concurrency**: one `concurrency` group for the whole workflow, so a scheduled run
and a manual dispatch can't clobber each other.

**Jobs**: three independent jobs, each `runs-on: ubuntu-latest`, `timeout-minutes: 30`,
running in parallel (separate GitHub-hosted runners, so no resource contention like
the local "one vendor at a time" Docker rule — that rule is about not overloading a
single local machine, which doesn't apply here).

Common per-job steps:
1. `actions/checkout`
2. `actions/setup-java@v4` — Temurin 21, `cache: maven`
3. `apt-get install -y zstd` — `datasets.py` shells out to `zstd`/`unzstd` to extract
   downloaded archives
4. `pip install requests` (or `pip install .`) — `datasets.py`'s only runtime
   dependency

**Heap sizing**: `-Xms8g -Xmx8g` for all three, sized for GitHub-hosted runners
(currently 4 vCPU / 16GB RAM). This intentionally deviates from `CLAUDE.md`'s 12GB
rule — that rule exists to keep multi-vendor *local* comparisons fair across systems
sharing one machine; it doesn't apply to a single-vendor CI smoke/perf job.

#### Job `mode1-official`

1. Apply the `bin.xml` fix (committed to the repo, see above — not a workflow-only
   patch).
2. `mvn package -DskipTests`.
3. `python3 datasets.py download wiki-Talk`.
4. Extract the distribution tar.gz; copy `config-template/` → `config/` (same as
   `init.sh` does) and point `graphs.root-directory` / `graphs.validation-directory`
   at the downloaded dataset directory.
5. Edit `config/benchmarks/custom.properties`:
   - `benchmark.custom.graphs = wiki-Talk`
   - `benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC` (no SSSP — unsupported
     on this unweighted graph)
6. `bash bin/sh/run-benchmark.sh`.
7. Parse `report/*/json/results.json` (same extraction approach as documented in
   `README.md`) into a markdown table appended to `$GITHUB_STEP_SUMMARY`.
8. `actions/upload-artifact@v4` — upload the `report/` directory.

#### Job `mode2-embedded`

1. `mvn package -DskipTests`.
2. `python3 datasets.py download datagen-7_5-fb`.
3. From `ldbc-native/`: `javac --add-modules jdk.incubator.vector -cp "$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java`,
   then run it with `-Xms8g -Xmx8g` (no source changes to the file).
4. Capture stdout; parse the per-algorithm timing table it already prints into
   `$GITHUB_STEP_SUMMARY`.
5. Upload the raw stdout log as an artifact.

#### Job `mode3-lsqb`

1. `mvn package -DskipTests`.
2. `python3 datasets.py download lsqb-sf0.1`.
3. Small parameterization of `lsqb/ArcadeDBEmbeddedLSQB.java`: read the dataset
   directory from a system property (`-Ddataset.dir=...`), defaulting to the current
   hardcoded `../datasets/social-network-sf1-merged-fk` when unset — existing local
   usage (`java ... ArcadeDBEmbeddedLSQB`) is unaffected.
4. Compile and run against `social-network-sf0.1-merged-fk` with
   `-Ddataset.dir=../datasets/social-network-sf0.1-merged-fk`.
5. Capture stdout; parse per-query timings into `$GITHUB_STEP_SUMMARY`.
6. Upload the raw stdout log as an artifact.

### Reporting

Each job writes its own markdown table to `$GITHUB_STEP_SUMMARY` — visible directly
in the Actions run UI. No cross-job aggregation: the three modes measure different,
non-comparable things (official isolated per-algorithm reloads vs. load-once-run-all
vs. subgraph query patterns), so combining them into one table would misrepresent
what's being compared. `actions/upload-artifact@v4` keeps the raw report/log per job
for later trend analysis, default 90-day retention.

### Out of scope (explicitly deferred)

- The multi-vendor Docker-based comparison scripts (`ldbc-native/benchmark.py`,
  `lsqb/lsqb_benchmark.py`) — different concern, different infra needs.
- Historical trend tracking / dashboards across runs — this workflow only produces
  per-run summaries and artifacts; wiring those into a persisted trend view is a
  separate follow-up if wanted.
- Failing the workflow on a performance regression — these are measurement jobs, not
  gates. A job only fails if the benchmark itself errors out.
- `example-directed`/`example-undirected` as a Mode 1 dataset — LDBC's own tiny test
  graphs are referenced in `config-template/graphs.properties` but their actual
  vertex/edge data files aren't bundled anywhere reachable from this repo's tooling,
  so they're not usable without hand-authoring fixture data (not attempted here since
  `wiki-Talk` already covers the "small real dataset" need).

## Implementation plan outline

1. Fix `src/main/assembly/bin.xml`'s dependencySet include pattern.
2. Verify locally: `mvn package -DskipTests`, extract the tar.gz, confirm
   `bin/sh/run-benchmark.sh` and `config-template/graphs/wiki-Talk.properties` exist.
3. Parameterize `lsqb/ArcadeDBEmbeddedLSQB.java`'s `DATA_DIR` via a system property
   with the current value as default.
4. Write `.github/workflows/arcadedb-benchmark.yml` with the three jobs described
   above.
5. Validate via `workflow_dispatch` before relying on the weekly schedule.
