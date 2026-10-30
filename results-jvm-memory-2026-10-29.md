# JVM memory: what ArcadeDB and Neo4j actually use under the fixed 12 GB heap (2026-10-29)

The multi-vendor tables report the container working set of ArcadeDB Docker and Neo4j as 12.3-13.5 GiB on every dataset. That is the
fixed `-Xms12g -Xmx12g` heap the JVM commits at start, not what the engines need. This run looks inside both JVMs with the new probe
(`BENCH_JVM_PROBE=1`, `shared/bench_memory.py`), keeping the 12 GB heap rule and the normal warm protocol.

Machine: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB, AC power, Docker Desktop 32 GB, one container at a time. Dataset
`datagen-7_5-fb` (633,432 vertices, 34,185,747 edges). Images: `arcadedata/arcadedb:26.10.2` (`7a498ebbbb9a`, ZGC),
`neo4j:2026.09.0-community` (`0dfcadbd51e1`, G1, GDS plugin, image-default page cache 512 MiB). Raw data:
`weekly-results/20261029-jvm-memory/`. Every output validated against the LDBC reference outputs (ArcadeDB 6/6, Neo4j 4/4); compute
times match the published table (ArcadeDB PR 0.108 s, LCC 2.31 s; Neo4j PR 6.71 s, LCC 14.8 s), so the probe did not disturb the timings.

## Method

Once before the first timed operation and once after the timed runs of every operation (outside every timed window), via `docker exec`:

- **live heap**: `jcmd <pid> GC.class_histogram` forces a full GC on any collector and totals the live objects. `jcmd GC.run` is not
  usable: Neo4j starts with `-XX:+DisableExplicitGC`, which makes it a no-op.
- **JVM native memory**: `jcmd <pid> VM.native_memory summary` (`-XX:NativeMemoryTracking=summary`), total committed minus the heap.
- **container footprint** = container working set (`docker stats`) - (committed heap - live heap): the measured container with only the
  unused part of the fixed heap removed. It also counts native memory the JVM does not track (Neo4j's page cache, native libraries).

These are the floor of what each engine holds between operations. They leave out the transient memory an algorithm allocates while it
runs and the headroom a collector wants above the live set, so they are not a recommended heap size.

## Results (MiB; steady across all operations, range over the 6 or 4 probes after the first)

| System | Live heap | JVM native | Container footprint | Container working set (published basis) |
|---|---:|---:|---:|---:|
| ArcadeDB Docker, default page cache (a quarter of the heap, 3 GB) | 2,170-2,173 | 211-262 | **2,457-2,521** | 12,575-12,636 |
| ArcadeDB Docker, `-Darcadedb.maxPageRAM=512` (same page cache as Neo4j) | 1,019-1,048 | 141-180 | **1,234-1,278** | n/a (same heap) |
| Neo4j 2026.09 + GDS, projection `bench` loaded, page cache 512 MiB | 775-776 | 575-607 | **1,973-2,036** | 13,486-13,548 |

For reference, Memgraph (in-memory, C++, so the container working set is its real use) holds 8.1 GiB on this graph before the first
algorithm and peaks at 24.4-25.6 GiB during WCC (runs of 2026-10-05 and 2026-10-07).

## Reading

- **Algorithms leave nothing behind.** In both engines the live heap is flat across all operations: the resident set is the loaded graph,
  ArcadeDB's Graph Analytical View or Neo4j's GDS projection, and the caches.
- **ArcadeDB's default page cache dominates its footprint.** It lives on the heap and is sized to a quarter of `-Xmx`, so with a 12 GB heap
  it keeps about 1.15 GiB of pages that this workload does not need: capping it at 512 MiB halves the footprint (2.5 to 1.25 GiB) with no
  change in compute time (PR 0.110 s, LCC 2.35 s, CDLP 1.35 s) and all outputs still valid.
- **At the same page cache budget ArcadeDB needs about 0.62x of Neo4j** (1.25 against 2.0 GiB). With defaults it needs 1.25x of Neo4j.
- **Against Memgraph's 8.1 GiB resting size** ArcadeDB (capped page cache) is about 6.5x smaller and Neo4j about 4x. Memgraph's peak also
  includes algorithm memory, which the JVM probe does not see, so only the resting numbers compare.
- The embedded ArcadeDB benchmark reports 0.77 GiB live heap on the same graph, below the server's 1.0 GiB with a 512 MiB page cache.
  Why is not measured yet (how full the page cache is in each, server-side buffers); the class histogram of both would tell.

## Not measured yet

- Peak memory during an algorithm. The defensible number for "how much RAM does it need" is the smallest heap at which every algorithm
  still finishes and validates (and how much slower it gets there), with the container capped at heap plus overhead. Planned for
  ArcadeDB Docker and Neo4j on `datagen-7_5-fb` and `graph500-22-w`.
- `graph500-22-w` and LSQB with the probe.

## Reproduce

```bash
cd ldbc-native
BENCH_JVM_PROBE=1 ../.venv/bin/python benchmark.py arcadedb neo4j --out <dir>/results.json
BENCH_JVM_PROBE=1 ARCADEDB_SERVER_FLAGS="-Darcadedb.maxPageRAM=512" ../.venv/bin/python benchmark.py arcadedb --out <dir>/pc512.json
```
