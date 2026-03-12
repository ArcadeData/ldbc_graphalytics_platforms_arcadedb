# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://graphalytics.org) benchmark using [ArcadeDB](https://arcadedb.com).

Uses ArcadeDB in **embedded mode** with native graph algorithms implemented directly on the ArcadeDB Java API.

## Supported Algorithms

| Algorithm | Description |
|-----------|-------------|
| BFS (Breadth-First Search) | Queue-based BFS traversal |
| PR (PageRank) | Iterative PageRank computation |
| WCC (Weakly Connected Components) | BFS-based component detection |
| CDLP (Community Detection Label Propagation) | Label propagation with majority voting |
| LCC (Local Clustering Coefficient) | Triangle counting per vertex |
| SSSP (Single Source Shortest Paths) | Dijkstra's algorithm |

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
2. **Algorithm Execution**: Opens the pre-loaded database in embedded mode, runs graph algorithms using direct graph traversal via the ArcadeDB Java API
3. **Result Serialization**: `OutputSerializer` reads result properties from vertices and writes them to the Graphalytics output format

## License

Apache License, Version 2.0
