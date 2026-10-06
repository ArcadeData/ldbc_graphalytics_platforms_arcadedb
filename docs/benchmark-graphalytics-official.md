# Mode 1: Official LDBC Graphalytics Benchmark

Uses the official [LDBC Graphalytics framework](https://github.com/ldbc/ldbc_graphalytics) with ArcadeDB's platform driver. Produces standardized results with separate `load_time`, `processing_time`, and `makespan` measurements. The framework reloads the graph for each algorithm to ensure isolated measurements.

## Configuration

The build produces a ready-to-run distribution with sensible defaults. You can optionally tune the configuration files in `graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/config/`:

**benchmark.properties** — dataset paths and memory:
```properties
graphs.root-directory = ../datasets          # default: empty (set to your datasets location)
graphs.validation-directory = ../datasets    # default: empty
benchmark.runner.max-memory = 16384          # default: empty (MB, recommended: 16384)
```

**benchmarks/custom.properties** — which graphs and algorithms to run:
```properties
benchmark.custom.graphs = datagen-7_5-fb                       # default: datagen-7_5-fb
benchmark.custom.algorithms = BFS, WCC, PR, CDLP, LCC, SSSP   # default: all 6 algorithms
benchmark.custom.timeout = 7200                                 # default: 7200 (seconds)
benchmark.custom.output-required = true                         # default: true
benchmark.custom.validation-required = true                     # default: true
benchmark.custom.repetitions = 1                                # default: 1
```

**platform.properties** — ArcadeDB-specific settings:
```properties
platform.olap = true   # default: false (enable CSR-accelerated graph algorithms)
```

## Run

```bash
cd graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT
bash bin/sh/run-benchmark.sh
```

Results are written to `report/<timestamp>-ARCADEDB-report-CUSTOM/json/results.json`.

## Extract Results

```bash
LATEST=$(ls -td report/*ARCADEDB* | head -1)
python3 -c "
import json
with open('$LATEST/json/results.json') as f:
    data = json.load(f)
result = data.get('result', data.get('experiments', {}))
runs = result.get('runs', {})
jobs = result.get('jobs', {})
for rid, r in sorted(runs.items(), key=lambda x: x[1]['timestamp']):
    algo = next(j['algorithm'] for j in jobs.values() if rid in j['runs'])
    print(f\"{algo:6} proc={r['processing_time']:>8}s  load={r['load_time']:>8}s\")
"
```
