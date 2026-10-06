# Architecture and file structure

## File structure

```
shared/
  bench_common.py                  # Shared benchmark infrastructure

ldbc-native/
  ArcadeDBEmbeddedBenchmark.java   # ArcadeDB Graphalytics benchmark (Java, embedded)
  ArcadeDBEmbeddedLoader.java      # ArcadeDB graph loader (Java, embedded)
  benchmark.py                     # Kuzu, DuckPGQ, Memgraph, Neo4j, ArangoDB Graphalytics benchmarks (Python)

lsqb/
  ArcadeDBEmbeddedLSQB.java        # ArcadeDB LSQB benchmark (Java, embedded, Cypher)
  lsqb_benchmark.py                # Kuzu, DuckDB, Neo4j, FalkorDB LSQB benchmarks (Python)
  tools/                           # Debug/profiling helpers
```



## Graph Analytical View (GAV)

The GAV engine builds a CSR adjacency index from ArcadeDB's OLTP storage:

1. **Pass 1**: Scans all vertices, assigns dense integer IDs, collects edge pairs
2. **Pass 2**: Computes prefix sums from degree arrays, fills CSR neighbor arrays
3. **Result**: Packed `int[]` arrays for forward/backward offsets and neighbors, plus columnar edge property storage

All graph algorithms operate directly on these packed arrays with zero object allocation in hot loops.

## Algorithm Execution Modes

- **CSR-accelerated** (default when OLAP enabled): Algorithms run on the GAV's CSR arrays via `GraphAlgorithms.*` methods
- **OLTP fallback**: If GAV is unavailable, algorithms fall back to ArcadeDB's built-in graph traversal procedures

## JVM Flags

The benchmark runner uses:
```
-Xms16g -Xmx16g --add-modules jdk.incubator.vector
```

The `jdk.incubator.vector` module enables SIMD-accelerated operations in the GAV engine.
