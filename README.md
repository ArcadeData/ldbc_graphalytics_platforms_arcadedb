# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with the Graph Analytical View (GAV) engine, which builds a CSR (Compressed Sparse Row) adjacency index for high-performance graph algorithm execution with zero GC pressure.

## Supported Algorithms

| Algorithm | Implementation | Complexity |
|-----------|---------------|------------|
| **BFS** (Breadth-First Search) | Parallel frontier expansion with AtomicIntegerArray CAS | O(V + E) |
| **PR** (PageRank) | Pull-based parallel iteration via backward CSR | O(iterations * E) |
| **WCC** (Weakly Connected Components) | Synchronous parallel min-label propagation | O(diameter * E) |
| **CDLP** (Community Detection Label Propagation) | Synchronous parallel label propagation with sort-based mode finding | O(iterations * E * log(d)) |
| **LCC** (Local Clustering Coefficient) | Parallel sorted-merge triangle counting | O(E * sqrt(E)) |
| **SSSP** (Single Source Shortest Paths) | Dijkstra with binary min-heap on CSR + columnar weights | O((V + E) * log(V)) |

## Prerequisites

- Java 21 or later (required for `jdk.incubator.vector` SIMD support)
- Maven 3.x
- ArcadeDB engine built locally (see below)

## Build

```bash
# 1. Build ArcadeDB engine first
cd /path/to/arcadedb
mvn install -DskipTests -pl engine -am -q

# 2. Build this platform driver
cd /path/to/ldbc_graphalytics_platforms_arcadedb
mvn package -DskipTests
```

The build produces a self-contained distribution in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/`.

## Dataset Setup

Download datasets from the [LDBC Graphalytics data repository](https://ldbcouncil.org/benchmarks/graphalytics/). For example, `datagen-7_5-fb` (633K vertices, 34M edges):

```
/path/to/graphs/
  datagen-7_5-fb.v              # vertex file (one ID per line)
  datagen-7_5-fb.e              # edge file (src dst weight, space-separated)
  datagen-7_5-fb.properties     # graph metadata
  datagen-7_5-fb-BFS/           # validation data per algorithm
  datagen-7_5-fb-WCC/
  datagen-7_5-fb-PR/
  datagen-7_5-fb-CDLP/
  datagen-7_5-fb-LCC/
  datagen-7_5-fb-SSSP/
```

## Configuration

Edit files in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/config/`:

### benchmark.properties
```properties
# Point to your dataset directory
graphs.root-directory = /path/to/graphs
graphs.validation-directory = /path/to/graphs

# JVM memory for the benchmark runner
benchmark.runner.max-memory = 16384
```

### benchmarks/custom.properties
```properties
# Select dataset and algorithms
benchmark.custom.graphs = datagen-7_5-fb
benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC, SSSP
benchmark.custom.timeout = 7200
benchmark.custom.output-required = true
benchmark.custom.validation-required = true
benchmark.custom.repetitions = 1
```

### platform.properties
```properties
# Enable Graph Analytical View (CSR-accelerated algorithms)
platform.arcadedb.olap = true
```

## Running the Benchmark

```bash
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT
bash bin/sh/run-benchmark.sh
```

The framework runs each algorithm as a separate job:
1. Load graph from CSV into embedded ArcadeDB
2. Build Graph Analytical View (CSR index)
3. Execute algorithm on CSR arrays
4. Serialize results to output files
5. Validate against reference output
6. Delete graph and repeat for next algorithm

Results are written to `report/<timestamp>-ARCADEDB-report-CUSTOM/`.

## Reading Results

```bash
LATEST=$(ls -td report/*ARCADEDB* | head -1)
python3 -c "
import json
with open('$LATEST/json/results.json') as f:
    data = json.load(f)
runs = data['experiments']['runs']
jobs = data['experiments']['jobs']
print(f\"{'Algo':6} | {'proc_time':>10} | {'load_time':>10} | {'makespan':>10}\")
print('-' * 50)
for rid, r in sorted(runs.items(), key=lambda x: x[1]['timestamp']):
    algo = next(j['algorithm'] for j in jobs.values() if rid in j['runs'])
    print(f\"{algo:6} | {r['processing_time']:>9}s | {r['load_time']:>9}s | {r['makespan']:>9}s\")
"
```

## Architecture

### Graph Analytical View (GAV)

The GAV engine builds a CSR adjacency index from ArcadeDB's OLTP storage:

1. **Pass 1**: Scans all vertices, assigns dense integer IDs, collects edge pairs
2. **Pass 2**: Computes prefix sums from degree arrays, fills CSR neighbor arrays
3. **Result**: Packed `int[]` arrays for forward/backward offsets and neighbors, plus columnar edge property storage

All graph algorithms operate directly on these packed arrays with zero object allocation in hot loops.

### Algorithm Execution Modes

- **CSR-accelerated** (default when OLAP enabled): Algorithms run on the GAV's CSR arrays via `GraphAlgorithms.*` methods
- **OLTP fallback**: If GAV is unavailable, algorithms fall back to ArcadeDB's built-in graph traversal procedures

### JVM Flags

The benchmark runner uses:
```
-Xms16g -Xmx16g --add-modules jdk.incubator.vector
```

The `jdk.incubator.vector` module enables SIMD-accelerated operations in the GAV engine.

## License

Apache License, Version 2.0
