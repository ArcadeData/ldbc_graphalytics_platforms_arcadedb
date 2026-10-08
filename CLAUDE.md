# CLAUDE.md — Project Instructions

## Project Overview

LDBC Graphalytics benchmark platform for ArcadeDB, with multi-vendor comparison across graph databases.
Three benchmark modes exist, plus standalone embedded Java benchmarks.

## Build

```bash
mvn package -DskipTests
# or use init.sh to build + extract the Mode 1 distribution:
./init.sh ~/path/to/graphs
```

The fat JAR is at: `target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar`

## Datasets

```bash
python3 datasets.py                          # list downloaded datasets
python3 datasets.py available                # list all downloadable
python3 datasets.py download datagen-7_5-fb  # Graphalytics (633K V, 34M E)
python3 datasets.py download lsqb-sf1        # LSQB SF1 (3.9M V, 17.9M E)
```

Datasets go into `datasets/`.

## Benchmark Modes

### Mode 1 — Official LDBC Graphalytics Framework

Single-platform (ArcadeDB only). Reloads the graph per algorithm for isolated measurements.

```bash
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT
bash bin/sh/run-benchmark.sh
```

Config files are in `config/`:
- `benchmark.properties` — `graphs.root-directory`, memory settings
- `graphs.properties` — vertex/edge file paths (relative to `graphs.root-directory`)
- `benchmarks/custom.properties` — algorithms, dataset, timeout, repetitions
- `platform.properties` — `platform.olap = true` for GAV/CSR acceleration

**Important**: vertex/edge file paths in `graphs.properties` must include the subdirectory:
```
graph.datagen-7_5-fb.vertex-file = datagen-7_5-fb/datagen-7_5-fb.v
graph.datagen-7_5-fb.edge-file = datagen-7_5-fb/datagen-7_5-fb.e
```

Results go to `report/<timestamp>-ARCADEDB-report-CUSTOM/`.

### Mode 2 — Multi-Vendor Graphalytics Comparison (ldbc-native/)

Runs 6 LDBC Graphalytics algorithms (PageRank, WCC, BFS, LCC, SSSP, CDLP) across multiple vendors.

```bash
cd ldbc-native
python3 benchmark.py                    # all default vendors
python3 benchmark.py arcadedb kuzu      # specific vendors only
python3 benchmark.py --reset neo4j      # force reload + run
```

**Default vendors**: arcadedb, kuzu, ladybug, duckpgq, memgraph, neo4j, arangodb, falkordb, hugegraph
**Excluded by default** (must name explicitly): surrealdb, dgraph

#### ArcadeDB Embedded (Mode 2)

Standalone Java benchmark — no Docker, no Python.

```bash
cd ldbc-native
LDBC_JAR=../target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
javac --add-modules jdk.incubator.vector -cp "$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java
java --add-modules jdk.incubator.vector -Xms12g -Xmx12g -cp ".:$LDBC_JAR" ArcadeDBEmbeddedBenchmark
```

### Mode 3 — LSQB Benchmark (lsqb/)

9 subgraph pattern matching queries (Q1-Q9) on LDBC SNB social network data.

```bash
cd lsqb
python3 lsqb_benchmark.py                    # all default vendors
python3 lsqb_benchmark.py kuzu duckdb        # specific vendors
python3 lsqb_benchmark.py --reset arcadedb   # force reload
```

**Default vendors**: kuzu, ladybug, duckdb, neo4j, memgraph, postgresql, arcadedb
**Excluded by default**: surrealdb, dgraph

#### ArcadeDB Embedded (Mode 3)

```bash
cd lsqb
LDBC_JAR=../target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
javac -cp "$LDBC_JAR" ArcadeDBEmbeddedLSQB.java
java -Xms12g -Xmx12g --add-modules jdk.incubator.vector -cp ".:$LDBC_JAR" ArcadeDBEmbeddedLSQB
# Use --reset to force reload
```

## Running Benchmarks — Critical Rules

### JVM: Eclipse Temurin 25 only, with compact object headers

All ArcadeDB benchmark runs (embedded Java, Mode 1 runner, the Java loader used by the Docker benchmark)
MUST use **Eclipse Temurin 25** with `-XX:+UseCompactObjectHeaders`. **Never use GraalVM** (its default
Graal JIT was 2-4x slower and erratic on the vectorised OLAP paths) and no other JDK for published numbers.
`shared/bench_java.py` finds Temurin 25 (`LDBC_JAVA_HOME`, `~/Library/Java/JavaVirtualMachines`, ...) and
refuses anything else; `weekend.py` and `scripts/run_mode1.py` use it automatically, so no flags are needed.
When running by hand: `$TEMURIN_HOME/bin/java --add-modules jdk.incubator.vector -Xms12g -Xmx12g -XX:+UseCompactObjectHeaders ...`.
Compact headers are OFF by default in Java 25 (`server.sh` only enables them when the JVM supports them), so the flag
must be passed explicitly. Reference numbers for JDK 21 vs GraalVM 25 vs Temurin 25 are in
`results-m5-multivendor-2026-10-03.md`; the multi-vendor tables in `README.md` use the Temurin 25 values.

### Power: measure on AC power only

macOS throttles the CPU on battery, so timings taken unplugged are not comparable with the rest. `weekend.py` refuses
to run on battery (`--allow-battery` overrides it, and the numbers are then unreliable); the Python suites print a
warning and flag every vendor measured on battery in the summary and the JSON report. Check with `pmset -g batt`.

### Correctness: always validate results, never report timing alone

Every benchmark result must be checked for correctness, not only timed.
- **Graphalytics (Mode 1):** the official framework validates every output (`benchmark.custom.validation-required = true`); a run that fails validation is not a result.
- **Graphalytics (Mode 2, all vendors):** the timed compute-only call returns a summary that `check_summary` compares with the reference summary (a sanity check, weak for PageRank because any normalised PageRank sums to 1); drivers can export full per-vertex outputs (`GRAPHALYTICS_DUMP_DIR=<dir>`, plus `GRAPHALYTICS_DUMP_ONLY=1` to export right after the load and skip the timed runs). `scripts/validate_outputs.py` compares them with the official reference outputs `datasets/<graph>/<graph>-<ALGO>`: BFS and CDLP exact, WCC same partition, PageRank/LCC/SSSP within 1e-4 relative error. For the derived `graph500-22-w` use `--swap 6:248533`. Engines that return only the visited set (Neo4j BFS) are checked with `BFSREACH`.
- **LSQB:** every query count must equal the official expected count.
- A timing whose output is invalid is reported as **invalid**, not ranked. Known driver issues found by validation (BFS capped at `LIMIT 50000`, BFS/PageRank following stored edge direction on graphs that store each undirected edge once, top-10 aggregations instead of full outputs) must be fixed or flagged before comparing vendors.

### JVM heap: 12GB for all Java-based systems

All JVM-based systems (ArcadeDB, Neo4j) MUST use `-Xms12g -Xmx12g` (or equivalent Docker env vars). This ensures fair comparison — same heap budget for all vendors.

- ArcadeDB Embedded: `java -Xms12g -Xmx12g ...`
- ArcadeDB Docker: `-e ARCADEDB_OPTS_MEMORY="-Xms12g -Xmx12g"`
- Neo4j Docker: `-e NEO4J_server_memory_heap_initial__size=12g -e NEO4J_server_memory_heap_max__size=12g`

### Timeouts: 5 minutes per operation, hard limits per vendor

Three layers, because a client blocked in a C extension ignores Python signals:

1. **Per operation (in process):** every load/algorithm/query times out after 5 minutes
   (`QUERY_TIMEOUT = 300` in `shared/bench_common.py`, `run_timed()` via SIGALRM). This only
   works if Python regains control, so it is the *first* defence, not the guarantee.
2. **Per vendor (parent process, the guarantee):** `benchmark.py` / `lsqb_benchmark.py` run every
   vendor in its own child process group. The parent kills the whole group when the vendor
   exceeds `--vendor-timeout` (default 3600 s total) or prints nothing for `--idle-timeout`
   (default 600 s): SIGTERM, then SIGKILL after `--kill-grace` (default 10 s), then the vendor's
   containers are stopped. Operations that finished before the kill are kept in the results,
   the operation in flight is reported as `timeout`, never-started ones as `N/A`.
   If the vendor was killed before its first load ever completed, its persisted data is wiped.
   `--no-isolate` runs in-process for debugging (no hard limits).
3. **Mode 1 (official framework):** `benchmark.custom.timeout` in `config-template/benchmarks/custom.properties`
   (1800 s per run); the framework kills the runner with `kill -9`.

Orchestrator code: `shared/bench_isolation.py` (kill escalation, watchdogs), `shared/bench_containers.py`
(Docker lifecycle), `shared/bench_state.py` (persistent data). Tests: `python3 -m unittest discover -s tests`
(fake vendors that hang, ignore SIGTERM, block SIGALRM, leave children behind; no real database needed).

### Load once, reuse every run

Loaded data survives between runs: Docker vendors mount their data directory from
`~/.cache/ldbc-graph-bench/data/<suite>-<vendor>-<image id>` (override with `LDBC_BENCH_STATE`) and are
stopped gracefully, not wiped; embedded vendors keep their database under the same root. Each vendor module
already skips the import when the data is there. The original load time is remembered in a marker and
shown in the summary with a note. Data is reloaded only on `--reset`, when the image changes (new image id
= new directory) or when the dataset files change. ArcadeDB's Java benchmarks reuse their database through
a sibling `<db>.loaded` marker (`--reset` forces a reload, `--discard` deletes it after the run, `-Ddb.path=`
relocates it).

### Weekly unattended run

```bash
python3 weekend.py --jvm-flags "-XX:+UseCompactObjectHeaders"   # everything, 3 reps for ArcadeDB Java
python3 weekend.py --only py-m2,py-lsqb                          # only the multi-vendor suites
python3 weekend.py --mode1-dist graphalytics-1.10.0-arcadedb-0.1-SNAPSHOT --java-home /path/to/jdk
```
`python3 weekend.py --dry-run` runs preflight and the Java compile, then prints every command, working directory and limit of the plan without starting a benchmark.
It runs preflight (Docker >= 24 GB, disk, datasets, JAR, stray containers), the ArcadeDB Java benchmarks,
both multi-vendor suites and optionally Mode 1, under the limits above, and writes `weekly-results/<timestamp>/weekly.md`
and `weekly.json`. A failed step never stops the next one. The first run after a new image or dataset
reloads; later runs reuse the data.

### Regression guards

`python3 scripts/bulk_update_repro.py` (also the `bulk-update` step of `weekend.py`) is the #8660 reproducer: one SQL bulk UPDATE per algorithm
property on a clone of the loaded embedded database, two property orders, fails when one UPDATE takes more than `--limit` (15 s; good engine 2-4 s,
bad engine 65-260 s). At the end of every `weekend.py` run `scripts/check_regressions.py` compares the ArcadeDB numbers with the previous
weekly run and flags anything more than 2x and 0.25 s slower (or newly invalid); OLTP numbers are noisy, read their flags as "look at it".

### Harness gotchas (learned the hard way)

- A container's port opens before the engine is ready: `shared/bench_containers.py` probes Memgraph (Bolt) and PostgreSQL at protocol level; add a `ready_probe` for any new slow-starting vendor.
- Memgraph needs `vm.max_map_count >= 524288` in the Docker VM (applied automatically before each start); otherwise it drops the connection mid-run.
- The datasets store each undirected edge once: drivers must compute on the undirected graph (reverse edge copies / symmetric tables) and time the same procedure call that is exported and validated, reduced to a summary row on the server (compute only, see `GRAPHALYTICS_OUTPUT` in `shared/bench_common.py`), and report the engine's own time of the algorithm (`bench_common.report_server_time` / `ServerTimer` / `measure_server_time`; the published tables show it, the wall-clock summary call is the client view). A new driver that returns a top-10, a bare count, or the full output in the timed call is a bug. FalkorDB needs `RESULTSET_SIZE -1` (default 10000 truncates outputs) and a long `stop_timeout` (the shutdown snapshot is large).
- Never run another vendor's container or an unrelated docker-compose stack during measurements (CPU/RAM contention and port clashes: 5433, 7687, 6379, 2480).

### Reporting a running queue: always use the queue picture

Whenever benchmarks run (a single vendor, a chain of vendors, `weekend.py`), report progress with the ASCII queue picture, in ONE
fenced code block: `python3 scripts/queue_status.py` (finished steps with their time, the running step with a bar and its last log
line, waiting steps, estimated finish, power / free memory / swap / containers). Write the plan first: a JSON file at
`~/.cache/ldbc-graph-bench/queue-plan.json` (format in the script's docstring) listing the steps, their logs and expected minutes, and
append `<step> done HH:MM` to the plan's progress file after each step. For a long queue, refresh the picture every 5 minutes with a
recurring cron job; add one line under the picture when a step finishes (with its validation verdict) and one line when the Mac is on
battery or swap is above 6 GB. Never start, stop or change a benchmark from the picture job.

### Warm measurements only — never publish or rank a cold number

Production servers run warm, so every published number is a warm number. The first call of every algorithm/query is the
warm-up: it is executed but never reported (it pays JIT, page-cache and first-touch costs). The reported value is the median
of 3 timed runs that follow (5 in the embedded Graphalytics JVM, `-Dwarm.reps`); when the warm-up call takes longer than
60 s (30 s for LSQB) there is a single timed run instead. Implemented in `shared/bench_common.py` (`run_timed_warm` for
Graphalytics, `measure_repeated` for LSQB), `ArcadeDBEmbeddedBenchmark.java` and `ArcadeDBEmbeddedLSQB.java`; every driver
must go through one of them (a new driver that times a single call is a bug). Extra untimed runs: `GRAPHALYTICS_WARMUP` /
`LSQB_WARMUP`; repetitions: `GRAPHALYTICS_REPS` / `LSQB_REPS` (defaults 0 and 3). Load times are one-off and not warmed.
The only exception is Mode 1 (the official LDBC framework): it runs each algorithm once after its own load and cannot be warmed.

### Memory is reported next to every result

The orchestrator samples memory once per second while a vendor runs (`shared/bench_memory.py`): the working set of the vendor's Docker
containers (`docker stats`) or the RSS of its process; the harness stores per-operation peaks in `result["_memory"]` and prints a
`Memory <vendor>: ...` line. README tables show the peak in GiB with a basis note: JVM systems (ArcadeDB, Neo4j) run with a fixed
12 GB heap, so their process/container size is mostly that heap (the embedded ArcadeDB benchmark also prints the live heap after a full
GC); Kuzu, DuckDB and LadybugDB size their buffer pools from the machine RAM. A vendor that starts its own container (the ArcadeDB
Graphalytics driver) must be listed in `own_containers` in `run_vendor_isolated`. Check `sysctl vm.swapusage` and
`memory_pressure` before a long run: a Docker VM that still holds its memory plus an in-process engine can push the Mac into swap and
distort timings (the harness waits while free memory is below 25%).

### One vendor at a time — no parallel containers

**NEVER start multiple Docker containers simultaneously.** The orchestrator runs vendors sequentially and does this for you; the manual procedure is:

1. Start the vendor's container(s)
2. Wait for readiness
3. Run the benchmark
4. Clean up ALL containers and temp data
5. Only then proceed to the next vendor

This prevents resource contention and ensures fair measurements.

### Setup and teardown per vendor

Each vendor benchmark MUST follow this lifecycle:

```
1. SETUP    — Start Docker container(s) if needed, wait for readiness
2. RUN      — Execute the benchmark (load + algorithms, each with 5min timeout)
3. CLEANUP  — Remove ALL Docker containers, temp dirs, data volumes
```

**Cleanup checklist per vendor:**

| Vendor | Docker containers | Temp dirs |
|--------|-------------------|-----------|
| ArcadeDB (Docker) | `arcadedb` | `/tmp/arcadedb-docker-data`, `/tmp/arcadedb-docker-log` |
| ArcadeDB (Embedded) | none | `/tmp/arcadedb_benchmark` |
| Kuzu | none | `/tmp/kuzu_benchmark`, `/tmp/ldbc_vertices.csv`, `/tmp/ldbc_edges.csv` |
| LadybugDB | none | `/tmp/ladybug_benchmark`, `/tmp/ldbc_vertices.csv`, `/tmp/ldbc_edges.csv` |
| DuckPGQ | none | `/tmp/duckpgq_benchmark.db` |
| Memgraph | `memgraph` | none |
| Neo4j | `neo4j-gds` | none |
| ArangoDB | `arangodb` | none |
| FalkorDB | `falkordb` | `/tmp/falkordb_benchmark` |
| HugeGraph | `vermeer-master`, `vermeer-worker` + network `hugegraph-net` | none |

### Docker setup commands per vendor

**Memgraph:**
```bash
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
```

**Neo4j** (self-starts in the benchmark script, but if manual):
```bash
docker run -d --name neo4j-gds -p 7688:7687 -p 7476:7474 \
  -e NEO4J_AUTH=neo4j/benchmark123 \
  -e 'NEO4J_PLUGINS=["graph-data-science"]' \
  -e NEO4J_server_memory_heap_initial__size=12g \
  -e NEO4J_server_memory_heap_max__size=12g \
  neo4j:2026-community
```

**ArangoDB:**
```bash
docker run -d --name arangodb -p 8529:8529 \
  -e ARANGO_ROOT_PASSWORD=benchmark arangodb/arangodb:3.11.12
```

**FalkorDB:**
```bash
docker run -d --name falkordb -p 6379:6379 \
  -v /tmp/falkordb_benchmark:/var/lib/falkordb/data falkordb/falkordb:latest
```

**HugeGraph (Vermeer):**
```bash
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net \
  -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net \
  -p 6788:6788 -p 6789:6789 \
  -v "$(cd datasets && pwd)":/data/graphs:ro \
  hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
# Assign worker to pool:
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/$WORKER"
```

### Vendor-specific notes

- **Memgraph**: Loading 34M edges via Cypher MATCH+CREATE is extremely slow. The load phase WILL timeout at 5 minutes. This is expected — record as "timeout" and move on.
- **Neo4j**: The benchmark script auto-starts its own Docker container if not running. Still clean up after.
- **ArcadeDB Docker**: Uses a two-phase approach — embedded Java loader for fast data loading, then Docker for algorithm execution over the **HTTP API only**. **Never use Bolt or gRPC for ArcadeDB benchmarks** (decision 2026-10-07: Bolt is not usable; gRPC measured no faster than HTTP, see `ArcadeDB-release-progress.md`; the Bolt helper and the transfer-study scripts were removed). The timed Graphalytics calls of **every vendor** are **compute only** (`GRAPHALYTICS_OUTPUT=count`, default): the whole algorithm runs and one summary row `(n, aggregate)` comes back, checked against the reference by `bench_common.check_summary`; `GRAPHALYTICS_OUTPUT=full` returns the full output instead (fallback). Numbers published up to 2026-10-06 used mixed methods and are not comparable. After loading, must wait for GAV (CSR) to build (~60-90s).
- **HugeGraph/Vermeer**: Requires a Docker network with master + worker containers. Worker must be assigned to the `$` pool before running.
- **Kuzu, DuckPGQ**: Embedded (no Docker). Clean up their temp database dirs after.

### Example: Running all vendors sequentially

```bash
cd ldbc-native

# 1. ArcadeDB (self-manages Docker)
python3 benchmark.py arcadedb
# Cleanup is automatic (script removes container)

# 2. Kuzu (embedded)
python3 benchmark.py kuzu

# 3. DuckPGQ (embedded)
python3 benchmark.py duckpgq

# 4. Memgraph
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
sleep 3
python3 benchmark.py memgraph
docker rm -f memgraph

# 5. Neo4j (auto-starts, auto-cleans)
python3 benchmark.py neo4j

# 6. ArangoDB
docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb/arangodb:3.11.12
sleep 5
python3 benchmark.py arangodb
docker rm -f arangodb

# 7. FalkorDB
docker run -d --name falkordb -p 6379:6379 falkordb/falkordb:latest
sleep 3
python3 benchmark.py falkordb
docker rm -f falkordb

# 8. HugeGraph
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net -p 6788:6788 -p 6789:6789 \
  -v "$(cd ../datasets && pwd)":/data/graphs:ro hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
sleep 5
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/$WORKER"
python3 benchmark.py hugegraph
docker rm -f vermeer-master vermeer-worker
docker network rm hugegraph-net
```

## Keeping results current

Documentation layout: `README.md` holds only the three results tables (Graphalytics `datagen-7_5-fb`, Graphalytics `graph500-22`, LSQB) with a link to one detailed page per benchmark in `docs/` (`benchmark-graphalytics-official.md`, `benchmark-graphalytics-multivendor.md`, `benchmark-graphalytics-graph500-22.md`, `benchmark-lsqb.md`; also `vendor-notes.md`, `architecture.md`). Change a number in the README table and on its detailed page together, and keep long notes, footnotes and how-to-run text on the detailed page, not in the README.

After any benchmark run on a new ArcadeDB version (or a fix branch), update `ArcadeDB-release-progress.md`, the README table with its detailed page, and `results-*.md` with the new numbers (same machine, same method, one JVM at a time), and note which engine build or PR each column refers to. Do not overwrite the multi-vendor tables unless those vendors were re-run too.

## Key Files

- `shared/bench_common.py` — Timeout (`QUERY_TIMEOUT=300`), `run_timed()`, `cleanup_docker()`, CLI parsing
- `ldbc-native/systems/__init__.py` — Available systems and metrics for Mode 2
- `ldbc-native/systems/_common.py` — Dataset paths, constants
- `lsqb/systems/__init__.py` — Available systems for Mode 3
- `ldbc-native/ArcadeDBEmbeddedBenchmark.java` — Standalone embedded Mode 2
- `lsqb/ArcadeDBEmbeddedLSQB.java` — Standalone embedded Mode 3
