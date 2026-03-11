# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://graphalytics.org) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with native graph algorithms invoked via Cypher.

## Supported Algorithms

| Algorithm | ArcadeDB Procedure |
|-----------|-------------------|
| BFS (Breadth-First Search) | `algo.bfs` |
| PR (PageRank) | `algo.pagerank` |
| WCC (Weakly Connected Components) | `algo.wcc` |
| CDLP (Community Detection Label Propagation) | `algo.labelpropagation` |
| LCC (Local Clustering Coefficient) | `algo.localClusteringCoefficient` |
| SSSP (Single Source Shortest Paths) | `algo.dijkstra.singleSource` |

## Prerequisites

- Java 11 or later
- Maven 3.x

## Quick Start

```bash
# 1. Build the project
./init.sh ~/graphs

# 2. Run the benchmark
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/
bin/sh/run-benchmark.sh
```

## Architecture

The driver uses an embedded architecture (no server needed):

1. **Graph Loading**: `ArcadeDBLoader` creates an embedded database and imports vertices/edges directly using the ArcadeDB Java API with batched transactions
2. **Algorithm Execution**: Opens the pre-loaded database in embedded mode, invokes `algo.*` Cypher procedures via `database.command("cypher", ...)`, stores results as vertex properties
3. **Result Serialization**: `OutputSerializer` reads result properties from vertices and writes them to the Graphalytics output format

This mirrors the Neo4j driver's embedded approach for maximum performance.

## License

Apache License, Version 2.0
