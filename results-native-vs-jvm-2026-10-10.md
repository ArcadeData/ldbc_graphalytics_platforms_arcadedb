# ArcadeDB native image against the JVM image: raw results, 2026-10-09 / 2026-10-10

Per-run values behind [docs/benchmark-arcadedb-native-image.md](docs/benchmark-arcadedb-native-image.md) (method, medians, reading and caveats are there). Every value is one run: a separate container start, warm-up call, then the median of 3 timed calls (a single call for LCC on `graph500-22-w`). All outputs of all runs validated against the official reference. Generated from `weekly-results/20261009-native-vs-jvm-datagen/native-vs-jvm.json` and `weekly-results/20261010-native-vs-jvm-graph500-22-w/native-vs-jvm.json` (the folder is git-ignored; the logs of every run are there).

Memory columns are the `docker stats` working set of the container in MiB (the page converts to GiB). On `datagen-7_5-fb` the native container shows a wide spread between runs (before the first call 3571 to 10660 MiB, peak 6795 to 9000 MiB); the JVM containers stay at 12.5-13.1 GiB.

Machine: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB, AC power, Docker Desktop 32 GB, 12 GB heap for both variants, HTTP + OpenCypher, 5-minute limit per operation.

## graph500-22-w

| variant | image | id | size MB | created | engine commit | runtime |
|---|---|---|---|---|---|---|
| native | `arcadedata/arcadedb:latest-native` | 1ffc43621a8d | 1345 | 2026-10-08T08:25:10 | `af4ce03048` | Linux 7.0.14-linuxkit - Substrate VM 25.0.2 (GraalVM CE 25.0.2+10.1) |
| jvm | `arcadedb-bench:af4ce03048-jvm` | f5944466c4c7 | 988 | 2026-10-09T16:11:06 | `00f15b5fb7` | Linux 7.0.14-linuxkit - OpenJDK 64-Bit Server VM 21.0.12.1 (Temurin-21.0.12.1+1) |

**Engine-reported compute time per run, seconds**

| run | PR | WCC | BFS | LCC | CDLP |
|---|---:|---:|---:|---:|---:|
| native-1 | 0.506 | 0.020 | 0.058 | 152.6 | 6.576 |
| native-2 | 0.470 | 0.020 | 0.055 | 115.5 | 5.983 |
| native-3 | 0.504 | 0.021 | 0.058 | 149.3 | 6.488 |
| jvm-1 | 0.933 | 0.023 | 0.064 | 91.447 | 5.803 |
| jvm-2 | 0.374 | 0.020 | 0.048 | 140.4 | 5.927 |
| jvm-3 | 0.511 | 0.015 | 0.055 | 91.077 | 5.742 |

**Wall-clock of the timed call per run (HTTP round trip and summary aggregate included), seconds**

| run | PR | WCC | BFS | LCC | CDLP |
|---|---:|---:|---:|---:|---:|
| native-1 | 2.649 | 2.001 | 2.193 | 157.6 | 9.605 |
| native-2 | 2.561 | 1.766 | 1.914 | 149.0 | 5.863 |
| native-3 | 2.883 | 2.029 | 2.135 | 156.9 | 9.526 |
| jvm-1 | 1.526 | 0.846 | 1.107 | 91.582 | 7.536 |
| jvm-2 | 1.124 | 0.787 | 0.911 | 72.559 | 4.320 |
| jvm-3 | 1.525 | 1.203 | 1.417 | 91.464 | 7.365 |

**Start-up, view and memory per run**

| run | `docker run` to `/ready` (s) | GAV build after the load (s) | GAV restore to READY (s) | memory before the first call (MiB) | memory peak (MiB) |
|---|---:|---:|---:|---:|---:|
| native-1 | 0.47 | - | 92.5 | 9824 | 10588 |
| native-2 | 0.46 | - | 90.6 | 10332 | 10732 |
| native-3 | 0.46 | - | 98.7 | 10353 | 10660 |
| jvm-1 | 2.46 | 110.3 | - | 13076 | 13466 |
| jvm-2 | 2.11 | - | 151.3 | 12923 | 13384 |
| jvm-3 | 2.33 | - | 72.4 | 12902 | 13312 |

Validation: 30 exported outputs checked in 6 runs, not valid: none; summary checks not ok: none.

## datagen-7_5-fb

| variant | image | id | size MB | created | engine commit | runtime |
|---|---|---|---|---|---|---|
| native | `arcadedata/arcadedb:latest-native` | 1ffc43621a8d | 1345 | 2026-10-08T08:25:10 | `af4ce03048` | Linux 7.0.14-linuxkit - Substrate VM 25.0.2 (GraalVM CE 25.0.2+10.1) |
| jvm | `arcadedb-bench:af4ce03048-jvm` | f5944466c4c7 | 988 | 2026-10-09T16:11:06 | `00f15b5fb7` | Linux 7.0.14-linuxkit - OpenJDK 64-Bit Server VM 21.0.12.1 (Temurin-21.0.12.1+1) |
| jvm-current-tag | `arcadedata/arcadedb:26.11.1-SNAPSHOT` | 886731372b6a | 986 | 2026-10-09T01:48:21 | `b54ebfd391` | Linux 7.0.14-linuxkit - OpenJDK 64-Bit Server VM 21.0.12.1 (Temurin-21.0.12.1+1) |

**Engine-reported compute time per run, seconds**

| run | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---:|---:|---:|---:|---:|---:|
| native-1 | 0.129 | 0.006 | 0.009 | 3.765 | 1.294 | 1.726 |
| native-2 | 0.129 | 0.006 | 0.009 | 3.379 | 1.197 | 1.535 |
| native-3 | 0.133 | 0.006 | 0.010 | 3.748 | 1.323 | 1.741 |
| jvm-1 | 0.106 | 0.004 | 0.009 | 2.456 | 1.137 | 1.360 |
| jvm-2 | 0.118 | 0.005 | 0.008 | 2.509 | 1.239 | 1.849 |
| jvm-3 | 0.114 | 0.005 | 0.009 | 2.579 | 1.146 | 1.559 |
| jvm-current-tag-1 | 0.111 | 0.004 | 0.009 | 2.380 | 1.120 | 1.402 |
| jvm-current-tag-2 | 0.107 | 0.004 | 0.009 | 2.463 | 1.116 | 1.401 |
| jvm-current-tag-3 | 0.118 | 0.005 | 0.009 | 2.390 | 1.196 | 1.431 |

**Wall-clock of the timed call per run (HTTP round trip and summary aggregate included), seconds**

| run | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---:|---:|---:|---:|---:|---:|
| native-1 | 0.540 | 0.379 | 0.334 | 3.792 | 1.662 | 2.092 |
| native-2 | 0.592 | 0.376 | 0.351 | 3.642 | 1.574 | 2.251 |
| native-3 | 0.469 | 0.322 | 0.343 | 3.678 | 2.183 | 2.146 |
| jvm-1 | 0.309 | 0.172 | 0.216 | 2.618 | 1.603 | 1.533 |
| jvm-2 | 0.382 | 0.200 | 0.213 | 2.691 | 1.534 | 1.676 |
| jvm-3 | 0.387 | 0.175 | 0.213 | 2.999 | 1.698 | 1.627 |
| jvm-current-tag-1 | 0.375 | 0.169 | 0.227 | 2.508 | 1.432 | 1.589 |
| jvm-current-tag-2 | 0.399 | 0.201 | 0.198 | 2.611 | 1.344 | 1.562 |
| jvm-current-tag-3 | 0.468 | 0.184 | 0.206 | 2.683 | 1.431 | 1.643 |

**Start-up, view and memory per run**

| run | `docker run` to `/ready` (s) | GAV build after the load (s) | GAV restore to READY (s) | memory before the first call (MiB) | memory peak (MiB) |
|---|---:|---:|---:|---:|---:|
| native-1 | 0.46 | 33.5 | - | 10660 | 9000 |
| native-2 | 0.47 | 33.7 | - | 5960 | 6795 |
| native-3 | 0.46 | - | - | 3571 | 8074 |
| jvm-1 | 1.98 | 25.7 | - | 12820 | 12943 |
| jvm-2 | 2.05 | - | - | 12585 | 13015 |
| jvm-3 | 2.16 | - | - | 12575 | 13056 |
| jvm-current-tag-1 | 2.26 | - | - | 12564 | 13056 |
| jvm-current-tag-2 | 2.03 | - | - | 12575 | 13066 |
| jvm-current-tag-3 | 2.25 | - | - | 12575 | 13087 |

Validation: 54 exported outputs checked in 9 runs, not valid: none; summary checks not ok: none.

## graph500-22-w: timeline, discarded attempts and the quiet-machine gate

The runner only starts an attempt when no build or test process of another session is running and the CPU is at least 90% idle, and it discards an attempt during which such a process appeared (or that is incomplete).

| step | start | end | note |
|---|---|---|---|
| gate | 00:22 | 01:08 | 74 checks over about 46 min saw competing Maven test processes of another session; the series started in the first gap |
| native-1 attempt 1 | 01:08 | ~01:35 | **discarded**: 39 competing pids appeared during the run, and the LCC call hit the 5-minute limit (its own validation line read valid) |
| gate | 01:35 | 01:40 | 8 checks, competing processes again |
| native-1 attempt 2 | 01:40 | ~02:05 | **discarded**: complete and valid, but 38 short-lived competing pids were seen during the run |
| native-1 attempt 3 | 02:05 | 02:24 | kept |
| jvm-1 | 02:24 | 02:39 | kept (the only kept run that built the view after the load: 110.3 s) |
| native-2 | 02:39 | 03:00 | kept |
| jvm-2 | 03:00 | 03:19 | kept |
| native-3 | 03:19 | 03:38 | kept |
| jvm-3 | 03:38 | 03:50 | kept |

The other five runs passed on their first attempt. (82 checks in total waited for a quiet machine: 74 before attempt 1 and 8 before attempt 2; none later.) The gate counts every Maven launcher, including idle ones, so it is strict; the contended attempt 1 showed it matters (engine PageRank 2.16 s against 0.50 s clean, CDLP 28.4 s against 6.0 s, LCC timeout). Mac on AC power and swap at 2.9 GB for the whole series.
