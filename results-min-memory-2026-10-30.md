# Minimum memory: ArcadeDB Docker, Neo4j and Memgraph on the Graphalytics graphs (2026-10-29/30)

How much RAM each system needs to run the whole Graphalytics suite, and how fast it runs there. The JVM heap (ArcadeDB, Neo4j) or
Memgraph's `--memory-limit` is lowered step by step until a run fails; a setting passes when every operation that is valid at the
baseline finishes within the 5-minute limit, validates against the LDBC reference outputs, and the container is not OOM-killed.
The container is **not** capped, so its peak working set (`docker stats`, once per second) is the RAM the system really used.
This is an exception to the 12 GB heap rule for this experiment only; every published timing stays at 12 GB.

Machine: MacBook Pro 16" (2026), Apple M5 Pro, 48 GB, AC power, Docker Desktop 32 GB, one container at a time. Images:
`arcadedata/arcadedb:26.10.2` (`7a498ebbbb9a`), `neo4j:2026.09.0-community` (`0dfcadbd51e1`, GDS, page cache 512 MiB),
`memgraph/memgraph-mage:latest` (`97383094cd26`). Each engine keeps its default page cache (ArcadeDB: a quarter of the heap, on the
heap). Driver: `scripts/min_heap.py`; raw data in `weekly-results/20261029-min-heap/` (`<dataset>/min-heap.json`, one log per run).
Times are engine-reported compute seconds (wall-clock where the engine reports none, marked ʷ), one warm run or the median of three.

## Result

| | datagen-7_5-fb (633K vertices, 34M edges) | graph500-22 (2.4M vertices, 64M edges) |
|---|---|---|
| **ArcadeDB, with the Graph Analytical View** | 3 GB heap, **3.30 GiB** peak | 4 GB heap, **4.38 GiB** peak |
| **ArcadeDB, without the view** (OLTP storage) | 2 GB heap, **2.44 GiB** peak (needs `algoMaxWorkingMemory=-1`, see below) | not measured |
| **Neo4j + GDS** | 1.5 GB heap, **2.59 GiB** peak | 4 GB heap, **5.21 GiB** peak |
| **Memgraph** | more than 24 GB: **25.63 GiB** peak unlimited, WCC fails at 24 GB | fails at Docker's 32 GB (earlier runs) |

At the smallest setting that passes, the speed:

| datagen-7_5-fb | PageRank | WCC | BFS | LCC | SSSP | CDLP |
|---|---:|---:|---:|---:|---:|---:|
| ArcadeDB with view, 3 GB | 0.116 | 0.005 | 0.007 | 2.41 | 1.15 | 1.42 |
| ArcadeDB without view, 2 GB | 103 | 201ʷ | 216 | 295 | 166 | 214 |
| Neo4j, 1.5 GB | 7.26 | 0.022 | 0.5ʷ | 14.9 | N/A | N/A |
| Memgraph, unlimited | 6.02 | 308 | 3.55 | N/A | 68.4 | timeout |

| graph500-22 | PageRank | WCC | BFS | LCC | CDLP |
|---|---:|---:|---:|---:|---:|
| ArcadeDB with view, 4 GB | 0.357 | 0.015 | 0.035 | 52.1 | 4.85 |
| Neo4j, 4 GB | 13.4 | 0.057 | 1.4ʷ | 255 ✗ | N/A |

✗ Neo4j's LCC on graph500-22 differs from the reference on 23 of 2.4M vertices at every heap (12 GB too), so it is not a memory effect;
the README table shows that value without a ✗ and needs checking.

## Every run

| Dataset | System | Setting | Result | Container peak (GiB) | Live heap (GiB) |
|---|---|---|---|---:|---:|
| datagen | ArcadeDB, view | 4 GB | pass | 4.33 | 1.52 |
| datagen | ArcadeDB, view | 3 GB | pass | 3.30 | 1.27 |
| datagen | ArcadeDB, view | 2.5 GB | OOM restoring the view | n/a | n/a |
| datagen | ArcadeDB, view | 2 GB | OOM restoring the view | n/a | n/a |
| datagen | ArcadeDB, no view | 4 GB | pass (PR 78.5, WCC 230, BFS 160, LCC 251, SSSP 67.7, CDLP 138) | 4.39 | 0.98 |
| datagen | ArcadeDB, no view | 2 GB | WCC, BFS, LCC refused by the algorithm working-memory budget | 2.34 | 0.78 |
| datagen | ArcadeDB, no view, budget off | 2 GB | pass | 2.44 | 0.78 |
| datagen | ArcadeDB, no view, budget off | 1.5 GB | WCC timeout, then OOM | 1.87 | 0.63 |
| datagen | Neo4j | 4 GB | pass | 5.15 | 0.74 |
| datagen | Neo4j | 2 GB | pass | 3.08 | 0.75 |
| datagen | Neo4j | 1.5 GB | pass | 2.59 | 0.75 |
| datagen | Neo4j | 1 GB | OOM in `gds.graph.project` | 1.93 | n/a |
| datagen | Memgraph | unlimited | pass | 25.63 | n/a |
| datagen | Memgraph | 24 GB | WCC: `Memory limit exceeded` | 23.37 | n/a |
| graph500 | ArcadeDB, view | 12 GB | pass | 12.75 | 4.07 |
| graph500 | ArcadeDB, view | 4 GB | pass | 4.38 | 2.28 |
| graph500 | ArcadeDB, view | 3 GB | OOM restoring the view | n/a | n/a |
| graph500 | Neo4j | 12 GB | pass | 13.35 | 1.40 |
| graph500 | Neo4j | 4 GB | pass | 5.21 | 1.39 |
| graph500 | Neo4j | 3 GB | computes, but GDS refuses to stream the full outputs (`AvailableMemoryReservationExceededException`) | 4.18 | 1.39 |

## Reading

- **Speed does not degrade until it fails**, with one exception. At the smallest heap that passes, ArcadeDB with the view and Neo4j run
  as fast as at 12 GB; one step lower they fail outright. ArcadeDB without the view is the exception: PageRank takes 89 s at 4 GB,
  115 s at 2 GB and 182 s at 1.5 GB, where the collector no longer keeps up.
- **ArcadeDB's floor with the view is the restore, not the view.** Once built, the view and the data take 1.3-2.3 GiB of live heap,
  but restoring it on open needs more: the restore thread (`gav-worker-0`) throws `OutOfMemoryError` at 2.5 GB (datagen) and 3 GB
  (graph500). The view then stays `BUILDING` forever instead of failing, and the client waits until its own timeout.
- **Without the view ArcadeDB needs the least memory on datagen** (2.44 against Neo4j's 2.59 GiB, and it also runs SSSP and CDLP),
  at 100-300 s per algorithm. Its default algorithm working-memory budget (`arcadedb.cypher.algoMaxWorkingMemory`, an eighth of the
  heap: 256 MB at 2 GB) refuses the 280 MB adjacency list of WCC/BFS/LCC although it fits in the heap.
- **On graph500-22 both JVM systems need a 4 GB heap**; ArcadeDB's container peaks at 4.38 GiB, Neo4j's at 5.21 GiB.
- **Memgraph holds the whole graph and the algorithm state in RAM**: 25.6 GiB on datagen, and WCC fails at a 24 GB limit. Its WCC (308 s)
  and SSSP (68-97 s) are slower than the published run (128 s, 58 s): the Mac swapped about 2 GB while Memgraph held 25 GiB, so its
  times from this run are not compared.

## For ArcadeDB

1. A restore that fails should end the view in an error state instead of `BUILDING`, so clients stop waiting.
2. The restore needs more memory than the view it builds; lowering that peak would make the view the cheapest configuration in RAM too.
3. The algorithm working-memory budget refuses work that fits in the heap; an eighth of the heap is conservative for the adjacency list.
4. The server is stopped with `docker rm -f` by the harness, so every start recovers and rebuilds the view (`gavPersistCsr` reuses the
   persisted CSR only after a clean close); a clean stop would likely avoid the restore peak entirely. Not measured yet.

## Reproduce

```bash
python3 scripts/min_heap.py --out <dir>/datagen-7_5-fb --heaps 4g 3g 2560m 2g                        # ArcadeDB + Neo4j
python3 scripts/min_heap.py --out <dir>/graph500-22-w --dataset graph500-22-w --heaps 12g 4g 3g 2g
python3 scripts/min_heap.py --out <dir>/datagen-7_5-fb --vendors memgraph --heaps none 24g 16g 12g
ARCADEDB_NO_GAV=1 ARCADEDB_SERVER_FLAGS="-Darcadedb.cypher.algoMaxWorkingMemory=-1" MIN_HEAP_VENDOR_TIMEOUT=7200 \
  python3 scripts/min_heap.py --out <dir>/datagen-7_5-fb-nogav-nobudget --vendors arcadedb --heaps 2g 1536m \
  --expect pagerank,wcc,bfs,lcc,sssp,cdlp
```
