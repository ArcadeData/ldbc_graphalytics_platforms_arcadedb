# ArcadeDB native image against the JVM Docker image, warm and validated (measured 2026-10-09 and 2026-10-10)

`arcadedata/arcadedb:latest-native` is the ArcadeDB server compiled with GraalVM Native Image (Substrate VM, distroless, no JVM inside). This page compares it with the
normal JVM Docker image on the Graphalytics workload: same engine commit, same HTTP + OpenCypher driver, same compute-only timed calls, same validation against the
official reference outputs. LSQB was not run for the native image.

**Result in one paragraph.** The native image is **never faster on compute**: equal on the short algorithms (PageRank, WCC, BFS on `graph500-22-w`), 1.1x slower on
CDLP and 1.5-1.6x slower on LCC. The wall-clock of a call through HTTP is slower still (1.0-2.4x). In return it **starts 4.5-5x faster** (0.46 s against 2.0-2.3 s
from `docker run` to `/ready`) and its container uses **20-40% less peak memory** (7.9 against 12.7 GiB on `datagen-7_5-fb`, 10.4 against 13.1 GiB on `graph500-22-w`).
Every output of both variants is valid. Use it where start-up time and footprint matter; keep the JVM image for heavy analytical work.

Machine and rules: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB, AC power, Docker Desktop 32 GB, 12 GB heap for both variants (`-Xms12g -Xmx12g`), 5-minute limit per operation.
Raw per-run values, discarded attempts and image ids: [results-native-vs-jvm-2026-10-10.md](../results-native-vs-jvm-2026-10-10.md).

## What was compared

| variant | image | id | size | engine commit | runtime |
|---|---|---|---|---|---|
| native | `arcadedata/arcadedb:latest-native` (built 2026-10-08) | `1ffc43621a8d` | 1345 MB | 26.11.1-SNAPSHOT `af4ce03048` | Substrate VM 25.0.2 (GraalVM CE 25.0.2+10.1), Linux 7.0.14 |
| jvm | `arcadedb-bench:af4ce03048-jvm` (built 2026-10-09 from the same commit) | `f5944466c4c7` | 988 MB | 26.11.1-SNAPSHOT `af4ce03048` | OpenJDK 64-Bit Server VM 21.0.12.1 (Temurin), Linux 7.0.14 |
| jvm-current-tag (datagen only, context) | `arcadedata/arcadedb:26.11.1-SNAPSHOT` | `886731372b6a` | 986 MB | 26.11.1-SNAPSHOT `b54ebfd391` | same Temurin 21 |

- **Same commit on purpose.** The native image is published on its own schedule, so the `26.11.1-SNAPSHOT` JVM tag is usually another commit. The JVM image was built from the
  commit the native image prints at start-up (`af4ce03048`) with the Dockerfile of the published image. The banner of that JVM image shows `00f15b5fb7` as build because of
  Maven's build number; the source is `af4ce03048`. A comparison against the moving tag would mix engine changes with the runtime.
- **The JVM image has the runtime of the published image**: Temurin 21 inside the container (the existing "ArcadeDB Docker" rows use it too), not the Temurin 25 with compact object headers
  that the embedded benchmark and the Java loader use. The loader that creates the databases is the usual Temurin 25 one for both variants.
- The native container receives the heap size and the server settings as command-line arguments (`-Xms12g -Xmx12g ...`, there is no JVM and no `JAVA_OPTS`), the JVM container through
  `JAVA_OPTS` and `ARCADEDB_OPTS_MEMORY`.

## How it was measured

- HTTP only, OpenCypher, compute-only timed calls (a summary row, `check_summary` against the reference), full per-vertex outputs exported and validated with
  `scripts/validate_outputs.py`. The table headline is the **engine-reported compute time** (`PROFILE` of the `CALL algo.…` step, a separate profiled call), the second table the
  **wall-clock** of the timed call (HTTP round trip and summary aggregate included).
- Warm: the first call of each algorithm is the warm-up; the value of one run is the median of 3 timed calls (a single call when the warm-up takes longer than 60 s, as for
  LCC on `graph500-22-w`). Each variant is run **3 times, each in its own container start**, interleaved (native 1, jvm 1, native 2, jvm 2, ...); the tables show the median of the 3 runs.
- A restored Graph Analytical View rebuilds asynchronously after a start (BUILDING, then READY); the driver waits for READY before the first call, so the warm-up never runs
  against a view that is still being built.
- **Quiet machine.** The runner waits until no other build or test process is running (Maven surefire, byte-buddy, plexus-classworlds, Gradle) and the CPU is at least 90% idle,
  watches during the run, and **discards and repeats** an attempt during which one of them appeared. On `graph500-22-w` the series waited 46 minutes at the gate, and
  `native-1` needed three attempts (two discarded as contended, one of them also with an LCC timeout; 8 more gate checks waited between them). The discarded attempts are kept in the results file.
- Container memory is the working set from `docker stats`, sampled once per second.

## `datagen-7_5-fb` (633,432 vertices, 34,185,747 edges), median of 3 runs

Engine-reported compute time, seconds:

| algorithm | native | jvm | jvm-current-tag | native / jvm |
|---|---:|---:|---:|---:|
| PageRank | 0.129 | 0.114 | 0.111 | 1.14x |
| WCC | 0.006 | 0.005 | 0.004 | 1.28x |
| BFS | 0.009 | 0.009 | 0.009 | 1.06x |
| LCC | 3.748 | 2.509 | 2.390 | 1.49x |
| SSSP | 1.294 | 1.146 | 1.120 | 1.13x |
| CDLP | 1.726 | 1.559 | 1.402 | 1.11x |

Wall-clock of the call (client view), seconds:

| algorithm | native | jvm | jvm-current-tag | native / jvm |
|---|---:|---:|---:|---:|
| PageRank | 0.540 | 0.382 | 0.399 | 1.41x |
| WCC | 0.376 | 0.175 | 0.184 | 2.15x |
| BFS | 0.343 | 0.213 | 0.206 | 1.61x |
| LCC | 3.678 | 2.691 | 2.611 | 1.37x |
| SSSP | 1.662 | 1.603 | 1.431 | 1.04x |
| CDLP | 2.146 | 1.627 | 1.589 | 1.32x |

| metric | native | jvm | jvm-current-tag | native / jvm |
|---|---:|---:|---:|---:|
| `docker run` to `/ready` (s) | 0.46 | 2.05 | 2.25 | 0.22x |
| GAV (CSR) build after the load (s), first runs | 33.6 | 25.7 | - | 1.31x |
| container memory before the first call (GiB) | 5.8 | 12.3 | 12.3 | 0.47x |
| container memory peak (GiB) | 7.9 | 12.7 | 12.8 | 0.62x |

Correctness: 3 runs per variant, every summary check ok, all 18 exported outputs valid for all three variants.

## `graph500-22-w` (2,396,657 vertices, 64,155,735 edges), median of 3 runs

Derived dataset described on the [graph500-22 page](benchmark-graphalytics-graph500-22.md). No SSSP (the dataset has no reference output).

Engine-reported compute time, seconds:

| algorithm | native | jvm | native / jvm |
|---|---:|---:|---:|
| PageRank | 0.504 | 0.511 | 0.99x |
| WCC | 0.020 | 0.020 | 0.99x |
| BFS | 0.058 | 0.055 | 1.05x |
| LCC | 149.3 | 91.4 | 1.63x |
| CDLP | 6.49 | 5.80 | 1.12x |

Wall-clock of the call (client view), seconds:

| algorithm | native | jvm | native / jvm |
|---|---:|---:|---:|
| PageRank | 2.649 | 1.525 | 1.74x |
| WCC | 2.001 | 0.846 | 2.37x |
| BFS | 2.135 | 1.107 | 1.93x |
| LCC | 156.9 | 91.5 | 1.72x |
| CDLP | 9.526 | 7.365 | 1.29x |

| metric | native | jvm | native / jvm |
|---|---:|---:|---:|
| `docker run` to `/ready` (s) | 0.46 | 2.33 | 0.20x |
| GAV (CSR) build after the load (s) | - (not in a kept run) | 110.3 | - |
| GAV restore to READY after a start (s) | 90.6-98.7 | 72.4-151.3 | - |
| container memory before the first call (GiB) | 10.1 | 12.6 | 0.80x |
| container memory peak (GiB) | 10.4 | 13.1 | 0.80x |

Correctness: 3 runs per variant, every summary check ok, all 15 exported outputs valid for both variants (BFS and CDLP exact, WCC same partition, PageRank and LCC within 1e-4).

## Reading the numbers

- **Compute.** Both graphs agree: the heavy algorithms are slower on the native image (LCC 1.5-1.6x, CDLP 1.1x), the sub-second ones are equal within noise. In every one of the three
  paired `graph500-22-w` runs the native LCC wall-clock is above the JVM one (149-158 s against 73-92 s).
- **Wall-clock through HTTP.** The gap between the wall-clock and the engine time of a call is larger on the native image and grows with the graph: about 0.1-0.2 s more per call on
  `datagen-7_5-fb`, about 1.1 s more on `graph500-22-w` (WCC: 0.02 s engine time on both, 2.0 s against 0.85 s wall-clock). The engine time covers the `CALL` step only, so the extra
  time is spent around it (the summary aggregate over all vertices, serialising the reply, the HTTP stack). **The cause was not investigated.**
- **Start-up and memory.** The native image answers `/ready` in 0.46 s, with 5.8 GiB used before the first call on `datagen-7_5-fb` (JVM 12.3 GiB: the fixed heap). On
  `graph500-22-w` the difference is smaller (10.4 against 13.1 GiB peak) because the graph itself takes most of the memory.
- **A faster start is not a faster restart of a big graph.** After a start both variants need 1.2-2.5 minutes until the restored view is READY on `graph500-22-w` (native 91-99 s,
  JVM 72-151 s); the 4.5-5x faster start only concerns the server becoming reachable.

## Caveats

- **One machine, three runs per variant, two datasets**; the spread between runs is in the results file. Container memory of the native image varies a lot between runs on
  `datagen-7_5-fb` (before the first call 3.5, 5.8 and 10.4 GiB; peak 6.6, 7.9 and 8.8 GiB), so read the peak (JVM: 12.6-12.8 GiB in every run), not the "before" row. The engine time comes from a separate profiled call and can differ from the
  timed calls: LCC on `graph500-22-w` shows 140 s engine time against 72.6 s wall-clock in `jvm-2` (the other two JVM runs: 91 s on both) and 115 s against 149 s in `native-2`. The
  medians are what this page reports; the wall-clock LCC medians (156.9 against 91.5 s) say the same as the engine ones.
- **The `graph500-22-w` JVM numbers are not comparable with the published "ArcadeDB Docker" row** of the [graph500-22 page](benchmark-graphalytics-graph500-22.md) (image `753d7332`:
  PageRank 0.362, LCC 48.6, CDLP 3.58 s; here the JVM image gives 0.511, 91.4 and 5.80 s). On `datagen-7_5-fb` the same JVM image agrees with the published row (LCC 2.51 against
  2.41 s). The difference on the large graph is **not explained** (another engine build, the load on the machine and the restored view are candidates). Only the ratios of this series, which
  was measured in one sitting, are meaningful; the README tables were not changed.
- The JVM image runs Temurin 21 with the settings of the published image; a JVM image on Temurin 25 with compact object headers was not built.
- The native image tag moves with every publication; the image id is part of the result.

## Reproduce

```bash
# The native image next to the JVM image (excluded from the default vendors, name it explicitly):
cd ldbc-native
ARCADEDB_IMAGE=arcadedb-bench:af4ce03048-jvm python3 benchmark.py arcadedb arcadedb-native
ARCADEDB_NATIVE_IMAGE=<image id or name@sha256:digest> python3 benchmark.py arcadedb-native   # pin the native image

# The paired runner used for this page (3 interleaved runs per variant, quiet-machine gate, discards contended attempts, validates every output):
uv run --no-project --python 3.12 --with requests python -u scripts/native_vs_jvm.py \
    --jvm-image arcadedb-bench:af4ce03048-jvm --dataset graph500-22-w --runs 3 --reset --step-minutes 25,18 \
    --out-dir weekly-results/<date>-native-vs-jvm-graph500-22-w
```

The runner writes `native-vs-jvm.md` and `.json`, `discarded.json`, per-run logs and a queue plan for `scripts/queue_status.py`. Build the JVM image from the commit the native
image prints at start-up: clone the engine, `git checkout <sha>`, `mvn package -DskipTests -pl package -am`, then
`docker build -f package/src/main/docker/Dockerfile package/target/arcadedb-<version>.dir`.

## Harness changes made for this comparison

- `arcadedb-native` is a registered variant of the ArcadeDB Docker driver (`ldbc-native/systems/arcadedb.py`, one driver for both builds), with the build string, start-up time and engine
  of the server recorded in every result, an authenticated login probe instead of a blind wait, and the READY wait for a restored view.
- `reset_vendor_data` (`shared/bench_state.py`) matched directories by prefix, so `--reset arcadedb` also deleted `arcadedb-native-*` and the embedded Java benchmark's
  `graphalytics-arcadedb-java`. It now matches the exact directory names (`tests/test_state.py`).
