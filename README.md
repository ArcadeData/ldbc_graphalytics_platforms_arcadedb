# LDBC Graphalytics ArcadeDB Platform Driver

Platform driver implementation for the [LDBC Graphalytics](https://graphalytics.org) benchmark using [ArcadeDB](https://arcadedb.com).

This driver connects to ArcadeDB via the **Neo4j-compatible Bolt protocol** and uses ArcadeDB's **native graph algorithms** for all six benchmark algorithms.

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

- ArcadeDB server running with Bolt protocol enabled (default port 7687)
- Java 8 or later
- Maven 3.x

## Quick Start

```bash
# 1. Build the project
./init.sh ~/graphs ~/arcadedb

# 2. Run the benchmark
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/
bin/sh/run-benchmark.sh
```

## Configuration

Edit `config/platform.properties`:

```properties
platform.arcadedb.home = /path/to/arcadedb
platform.arcadedb.bolt-uri = bolt://localhost:7687
platform.arcadedb.http-uri = http://localhost:2480
platform.arcadedb.username = root
platform.arcadedb.password = playwithdata
```

## Architecture

The driver uses a client-server architecture:
- **Graph Loading**: Creates a database and imports vertices/edges via the ArcadeDB HTTP API
- **Algorithm Execution**: Connects via Bolt, invokes ArcadeDB's native `algo.*` Cypher procedures
- **Result Collection**: Streams algorithm results directly from procedure YIELD output

This is in contrast to the Neo4j driver which uses embedded mode. The client-server approach is more representative of real-world deployments.

## License

Apache License, Version 2.0
