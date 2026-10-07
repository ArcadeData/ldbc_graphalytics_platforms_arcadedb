# Mode 2: Native Multi-Vendor Comparison

Located in `ldbc-native/`. Loads the graph once and runs all algorithms sequentially on the same in-memory structure. This provides a fair apples-to-apples comparison since all systems use the same approach.

**Systems tested:** ArcadeDB, Kuzu, LadybugDB, DuckPGQ, Memgraph, Neo4j, ArangoDB, FalkorDB, HugeGraph

## ArcadeDB (Java)

```bash
# Compile (use the LDBC platform fat JAR for dependencies)
LDBC_JAR=target/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
cd ldbc-native
javac --add-modules jdk.incubator.vector -cp "../$LDBC_JAR" ArcadeDBEmbeddedBenchmark.java

# Run
java --add-modules jdk.incubator.vector -Xms8g -Xmx8g -cp ".:../$LDBC_JAR" ArcadeDBEmbeddedBenchmark
```

## Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB (Python)

```bash
# Create virtual environment and install dependencies (from repo root)
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[neo4j,kuzu,duckdb,memgraph,arangodb,falkordb]"
# or: pip install -e ".[all]" for every vendor, including postgresql

# Run all available benchmarks
cd ldbc-native
python3 benchmark.py
```

For Memgraph, start Docker first:
```bash
docker run -d --name memgraph -p 7687:7687 memgraph/memgraph-mage
```

For Neo4j, start Docker with GDS plugin:
```bash
docker run -d --name neo4j -p 7474:7474 -p 7688:7687 \
  -e NEO4J_AUTH=neo4j/benchmark123 \
  -e NEO4J_PLUGINS='["graph-data-science"]' \
  neo4j:2026-community
```

For ArangoDB, start Docker (use 3.11 — Pregel was removed in 3.12):
```bash
docker run -d --name arangodb -p 8529:8529 -e ARANGO_ROOT_PASSWORD=benchmark arangodb:3.11
```

For HugeGraph (Vermeer OLAP engine):
```bash
docker network create hugegraph-net
docker run -d --name vermeer-master --network hugegraph-net \
  -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
docker run -d --name vermeer-worker --network hugegraph-net \
  -p 6788:6788 -p 6789:6789 \
  -v "$(pwd)/datasets":/data/graphs:ro \
  hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
# Assign worker to common pool:
WORKER=$(curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
curl -X POST "http://localhost:6688/api/v1/admin/workers/group/\$/${WORKER}"
```

## Benchmark Results

Dataset: **datagen-7_5-fb** (633,432 vertices, 34,185,747 edges, undirected, weighted)

*All benchmarks in this section were run on a MacBook Pro 16" (2026), Apple M5 Pro, 48GB RAM, 1TB SSD, macOS (ArcadeDB on Eclipse Temurin 25 with `-XX:+UseCompactObjectHeaders`, 12 GB heap for every JVM system, Docker Desktop with 32 GB). Measured 2026-10-03.*

### ArcadeDB across releases

The official-framework (Mode 1) numbers per ArcadeDB release, Mode 2 and LSQB (26.8.1 against 26.10.1), and the effect of later fixes such as the WCC union-find and the restored-view wait, are in [ArcadeDB-release-progress.md](../ArcadeDB-release-progress.md).

### Systems and versions (both suites)

| System | Version | Edition | License | Mode | Overhead | Used in |
|--------|---------|---------|---------|------|----------|---------|
| **ArcadeDB** (embedded) | 26.11.1-SNAPSHOT | Open Source | Apache 2.0 | Embedded (in-process, Temurin 25) | None | Graphalytics, LSQB |
| **ArcadeDB** (Docker) | 26.11.1-SNAPSHOT | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics, LSQB |
| **Neo4j** | 2026.09.0 | Community | GPL 3.0 | Server (Docker, Bolt protocol, GDS) | Network + Docker | Graphalytics, LSQB |
| **Kuzu** | 0.11.3 (archived project) | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics, LSQB |
| **LadybugDB** (Kuzu fork) | 0.21.2 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics, LSQB |
| **DuckPGQ** | DuckDB 1.5.0 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | Graphalytics |
| **DuckDB** | 1.5.6 | Open Source | MIT | Embedded (in-process, C++ via Python) | None | LSQB |
| **PostgreSQL** | 18 | Open Source | PostgreSQL | Server (Docker) | Network + Docker | LSQB |
| **Memgraph** | 3.13.1 (MAGE) | Community | BSL 1.1 | Server (Docker, Bolt protocol) | Network + Docker | Graphalytics, LSQB |
| **ArangoDB** | 3.11.14 \* | Community | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics |
| **FalkorDB** | 6.0.1 (Redis 8.10.2) | Open Source | Source Available | Server (Docker, Redis protocol) | Network + Docker | Graphalytics, LSQB |
| **HugeGraph** | Vermeer latest | Open Source | Apache 2.0 | Server (Docker, HTTP API) | Network + Docker | Graphalytics |

All systems are the newest available release as of 2026-10-03. Exceptions: DuckPGQ is pinned to DuckDB 1.5.0 because the `duckpgq` extension is not published for newer DuckDB releases; ArangoDB is pinned to 3.11.14 (see the \* note below); Kuzu 0.11.3 is the last release of an archived project and is kept for reference next to its active fork LadybugDB. ArcadeDB is tested **embedded** (in-process Java, zero overhead) and in **Docker** (queries over Bolt like Neo4j and Memgraph, the same network overhead as the other Docker systems; the numbers in the tables below were measured over the HTTP API, before the switch to Bolt, and are replaced when the benchmarks are re-run).

### How we measure

Every number below is a **warm** number, the way a production server runs. Each algorithm (or query) is executed once as a warm-up call that is
never reported (it pays JIT compilation, page-cache and first-touch costs), and the reported value is the **median of 3 timed runs** that follow it
(5 for the embedded ArcadeDB Graphalytics benchmark, which is itself run in 3 separate JVM launches; the median of those is shown). An operation
whose warm-up call takes longer than 60 s (30 s for LSQB) gets a single timed run instead, so it is still warm, just not repeated. Loads are one-off and not
warmed. One system at a time, 5-minute limit per operation, AC power only, 12 GB heap for every JVM system, every output validated.

**Memory** is sampled once per second while the timed operations run: the working set of the vendor's Docker containers (`docker stats`) or the
resident memory (RSS) of the vendor's process for embedded engines. Read it with care: JVM systems (ArcadeDB, Neo4j) run with a fixed 12 GB heap, so their
process or container size mostly shows that heap (the embedded ArcadeDB benchmark also prints its live heap after a full GC, 0.77 GiB for Graphalytics, 0.67 GiB for LSQB OLAP and 1.4 GiB for OLTP; this was not measured for Neo4j or the other systems, so it is not in the tables); Kuzu, LadybugDB and DuckDB
size their buffer pools from the machine's RAM. The only exception to the warm protocol is Mode 1 (the official LDBC framework), which runs each algorithm once after its own load.

### All Systems Comparison

Seconds (warm medians), `datagen-7_5-fb`, last column peak memory in GiB. ArcadeDB embedded and ArcadeDB Docker run on Temurin 25 with compact object headers.

**Every result is checked against the official LDBC reference outputs** (`scripts/validate_outputs.py`; BFS and CDLP exact, WCC same partition, PageRank/LCC/SSSP within 1e-4). A value marked **✗** failed that check: the system computed something other than the Graphalytics algorithm, so its time is shown for completeness but is **not comparable** and is not ranked. Bold marks the fastest *valid* result per column.

| System | Load | PageRank | WCC | BFS | LCC | SSSP | CDLP | Peak memory (GiB) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ArcadeDB embedded | 71.9 | **0.085** | **0.003** | **0.020** | **2.05** | **0.75** | **0.96** | 5.6 |
| ArcadeDB Docker | 43.9 | 1.11 | 0.83 | 0.88 | 3.26 | 2.09 | 2.17 | 12.7 |
| Neo4j | 657 | 6.98§ | 0.111 | 0.480‡ | 15.4 | N/A | N/A | 13.3 |
| Kuzu | 28.8 | 1.16 | 0.434 | 0.328 | N/A | N/A | N/A | 0.87 |
| LadybugDB | 5.16 | N/A | N/A | 7.89 | N/A | N/A | N/A | 1.0 |
| DuckPGQ | 0.85 | 1.51✗ | 2.00 | timeout¶ | 49.3 | N/A | N/A | 8.3 |
| Memgraph | 437 | 5.49 | 189 | 3.85 | N/A | 75.9 | timeout | 25.1 |
| ArangoDB \* | 726 | 93.0 | 40.9 | 38.4 | N/A | 173 | 254✗ | 23.2 |
| FalkorDB | 116 | 2.67✗ | 2.50 | 0.057 | N/A | N/A | 10.6✗ | 8.4 |
| HugeGraph | 34.7 | 2.41 | 0.293 | 0.195 | 110 | N/A | 22.3✗ | 3.2 |

- **ArcadeDB** (embedded and Docker) is valid for all six algorithms. CDLP used to fail validation because the engine broke ties by dense node index instead of vertex id ([ArcadeData/arcadedb#9285](https://github.com/ArcadeData/arcadedb/issues/9285)); from 26.11.1-SNAPSHOT `algo.labelPropagation` takes a `tieBreakProperty` (the Docker driver passes `VID`) and the embedded kernel takes a tie-break rank, and the output matches the reference exactly. The embedded benchmark builds the vertex-id rank once, outside the timed call (like the node mapping); the Docker call computes it inside the timed procedure, and returns all rows to the client, which is part of why it is slower (2.17 s against 0.96 s).
- **ArcadeDB Docker, method change (2026-10-07), under review.** Until 2026-10-06 the ArcadeDB Docker row timed calls that returned only `count(*)`. It now times the full per-vertex output over HTTP (mean of two runs, all six outputs valid). **The systems in this table do not all handle the output the same way**: Kuzu, DuckPGQ and Memgraph return the full output in the timed call; FalkorDB does for PageRank, WCC and CDLP; Neo4j returns the full PageRank but only a top-10 or a count for WCC, BFS and LCC; LadybugDB returns a top-10 for PageRank, WCC and LCC; ArangoDB (Pregel) and HugeGraph (Vermeer) leave the results on the server and ArangoDB's LCC and BFS return a top-10 or a grouped count. An earlier version of this note wrongly said that every other system returns the full output. The benchmark is being moved to one rule for all systems (compute only: the algorithm runs completely and the call returns a count), see [DECISIONS-2026-10-07.md](../DECISIONS-2026-10-07.md); until then compare ArcadeDB Docker with the systems that also return the full output (Kuzu, DuckPGQ, Memgraph, FalkorDB). Embedded ArcadeDB runs in process, has no transfer, and is unchanged. The earlier ArcadeDB Docker row was PageRank 0.16, WCC 0.02, BFS 0.06, LCC 2.61, SSSP 1.39, CDLP 1.45 (compute only).
- **Load** times are not like for like: ArcadeDB loads with its embedded Java loader (and, for Docker, serves over HTTP afterwards), the other server systems load through Python batches over the network. Systems whose algorithms follow the stored edge direction (Memgraph, FalkorDB, ArangoDB; HugeGraph for PageRank/BFS) load every edge in both directions, and that cost is part of their load time. FalkorDB loads with its bulk loader (`falkordb-bulk-insert`, 116 s for the 68.4M edge records; the per-query path took 53 minutes). The ArcadeDB Docker load is the time of its original load; later runs reuse the data.
- ‡ **Neo4j BFS** returns only the reached vertices (no distances), so it is checked as a reachable set, which matches the reference.
- § **Neo4j PageRank** is exact but needs two GDS runs: GDS starts every vertex at 1-d, does not normalise and counts the initialisation as the first iteration, so its scores differ from the Graphalytics reference by 0.85·A¹⁰·1. The update is linear, so the reference follows from the scores of two runs (10 and 11 iterations): rank = (20·S11 − 17·S10) / 3 / N (verified against a simulation to 3e-14, and the output validates at 100%). The timed value is those two compute-only GDS runs; the validated export streams both score sets and applies the formula.
- ¶ **DuckPGQ BFS** (undirected shortest paths from vertex 6) does not finish within the 5-minute limit: DuckDB does not honour the in-process interrupt while its shortest-path operator runs, so the run only ends after about 25 minutes.
- ✗ reasons: **PageRank** of DuckPGQ (`pagerank()` takes no iteration or damping parameter; its ranks sum to 0.90) and FalkorDB (`algo.pageRank` has no parameters; 14.9% of the vertices within 1e-4) cannot be made to run the 10-iteration Graphalytics PageRank. **CDLP**: ArangoDB's label propagation returns dense ids and does not match; FalkorDB and HugeGraph find the same communities as the reference but with other label values, which the exact-match rule rejects. Memgraph's `community_detection` exceeds the 5-minute limit.
- Direction handling (all drivers compute on the undirected graph, as the reference does): Kuzu and LadybugDB add a reversed edge table, Memgraph, FalkorDB and ArangoDB store both directions, HugeGraph runs PageRank and BFS on a second graph loaded from a both-direction copy of the edge file, DuckPGQ uses a symmetric edge table, Neo4j projects an undirected GDS graph. Every timed call is the exact call that is exported and validated (full per-vertex output, no `LIMIT`), except Neo4j PageRank (see §). PageRank settings that matter: Kuzu `maxIterations 11` and ArangoDB Pregel `maxGSS 11` (the initialisation counts as the first iteration), Memgraph 10 iterations, Vermeer `compute.max_step 10` with no convergence threshold.
- N/A means the engine has no implementation: LCC in Kuzu, LadybugDB, Memgraph (only a NetworkX procedure that is not installed in the MAGE image), FalkorDB and ArangoDB (the AQL query is rejected); SSSP and CDLP in most systems; HugeGraph/Vermeer SSSP is unweighted only (ArangoDB's weighted SSSP is an AQL weighted traversal, since Pregel's is unweighted). **LadybugDB**: only BFS runs, because the downloaded `algo` extension (0.21.0) for macOS arm64 fails to load (`Library not loaded: @rpath/libnetworkit.dylib`; upstream packaging bug).

Notes:
- **Docker memory:** Docker Desktop had 36 GB for every run on 2026-10-05 except HugeGraph (32 GB). Memgraph (25 GiB) and ArangoDB (23 GiB, SSSP and BFS) use the most memory. ArangoDB keeps the in-memory graph copy of every finished Pregel job until its time-to-live expires, so the driver deletes each job after it finishes; without that, repeated warm runs were killed by the out-of-memory killer at 34 GiB.
- **Memgraph** needs `vm.max_map_count` of at least 524288 in the Docker VM (Docker Desktop's default is 262144, which makes its jemalloc fail and the connection drop mid-query); the harness raises it before each start. WCC and SSSP exceed 60 s, so they are a single timed run after the warm-up call.
- **FalkorDB**: Redis's background snapshots (default save points) fork the 20+ GB process during a load, the fork fails and Redis then refuses every write; the driver turns them off and takes one synchronous `SAVE` after the load. The default `RESULTSET_SIZE` of 10000 rows silently truncates full per-vertex outputs and is lifted.
- \* **ArangoDB** is run on 3.11.14, not the latest release, because the driver runs PageRank, WCC, SSSP and CDLP through Pregel, which ArangoDB 3.12 and later no longer provide (only BFS works there). PageRank, SSSP and CDLP are a single timed run after the warm-up call (each warm-up exceeded 60 s); the run was done alone on a quiet machine on 2026-10-05.
- Neo4j and ArcadeDB use a 12 GB heap; Docker Desktop has 32 GB. ArcadeDB Docker loads through the embedded loader first, then serves queries over HTTP (see † for the first call after a restart).
- None of the competing systems have official LDBC Graphalytics platform drivers. Only ArcadeDB has an official LDBC Graphalytics platform implementation.
- Systems and versions are listed in the table above; the ArcadeDB rows were re-measured on 2026-10-06 on `26.11.1-SNAPSHOT` (ArcadeDB `main` @ `cbf701d66e`, which contains the Q9 fix [#9282](https://github.com/ArcadeData/arcadedb/issues/9282) and the CDLP tie-break [#9285](https://github.com/ArcadeData/arcadedb/issues/9285)); the other systems were measured on 2026-10-03 to 2026-10-05, and ArcadeDB 26.10.1 numbers are in `ArcadeDB-release-progress.md`. Raw logs, the harness fixes and the remaining history are in `results-multivendor-validated-2026-10-05.md` and `ArcadeDB-release-progress.md`.

## Larger dataset

The same suite on `graph500-22` (2.4M vertices, 64M edges, no SSSP): [benchmark-graphalytics-graph500-22.md](benchmark-graphalytics-graph500-22.md).
