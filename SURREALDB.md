# Why SurrealDB Can't Compete in Graph Database Benchmarks

We maintain the [LDBC Graphalytics benchmark suite](https://ldbcouncil.org/benchmarks/graphalytics/) for multiple databases (ArcadeDB, Neo4j, Kuzu, DuckPGQ, Memgraph, ArangoDB, FalkorDB, HugeGraph). Someone suggested adding SurrealDB. We spent a full day implementing and debugging it against both Graphalytics (graph algorithms on 633K vertices / 34M edges) and LSQB (subgraph pattern matching on LDBC SNB SF1). Here's what we found.

**TL;DR: SurrealDB scored N/A on every single benchmark metric — all 6 Graphalytics algorithms and all 9 LSQB queries.**

SurrealDB is included in this repository but excluded from default benchmark runs. You can still run it explicitly with `python3 benchmark.py surrealdb` or `python3 lsqb_benchmark.py surrealdb`.

---

## Graphalytics (Graph Algorithms)

**No built-in graph algorithms whatsoever.** SurrealDB has zero support for PageRank, Weakly Connected Components, Community Detection (Label Propagation), Local Clustering Coefficient, or Single-Source Shortest Path (weighted Dijkstra). Every other database in the benchmark (even FalkorDB and ArangoDB) ships with at least some of these. SurrealDB has none.

**BFS doesn't actually work.** SurrealDB advertises recursive graph traversal with syntax like `->edge.{1..30}->node`. In practice, **`{1..30}` returns the exact same result as `{1..1}`** — it does NOT compose multi-hop paths. We verified this on a simple 3-node chain: `n:1->e->n:2->e->n:3`. Querying `n:1->e.{1..3}->n` returns only `[n:2]`, never reaching `n:3`. On the real graph, "BFS" reported 34 nodes reached (just the direct neighbors of the source vertex) instead of the expected 633K.

The unlimited recursive syntax `{..+collect}` simply **hangs indefinitely** — even on a tiny 3-node test graph.

**Loading 34M edges took 30 minutes** via the HTTP API, compared to seconds for ArcadeDB embedded and under a minute for most Docker-based competitors. The **1MB HTTP payload limit** forces tiny batch sizes (10K edges per request = 3,400 HTTP round-trips for 34M edges).

---

## LSQB (Pattern Matching Queries)

LSQB tests 9 Cypher-style pattern matching queries on a social network. SurrealDB failed every single one:

**Q1, Q2, Q4, Q5, Q7 — Timed out (>120s each).** SurrealDB has no SQL JOINs and no Cypher MATCH. The only way to express multi-table pattern matching is through nested subqueries with `$parent` record link dereferencing. This turns every query into O(n*m) nested loops. Q1 iterates over 3.1M HAS_TAG edges, and for each one runs a subquery scanning 3.2M HAS_MEMBER edges. We let Q1 run for 15+ minutes before killing it.

**Q3, Q6, Q8, Q9 — Cannot be expressed at all.** SurrealDB does **not support table aliases** (`AS k2`) in subqueries. This makes self-joins impossible. Q3 (triangle enumeration), Q6/Q9 (friends-of-friends), and Q8 (anti-join with self-reference) all require joining the same table with itself — you literally cannot write these queries in SurrealQL.

**Q7 — `math::max()` takes an array**, not two arguments. After fixing the syntax, the query timed out anyway.

---

## Crashes and Stability Issues

- **SurrealDB crashed (exit code 137 — OOM killed)** when we tried to `REMOVE TABLE` on a database with 34M edges while the LSQB data was also loaded. The Docker container just died.
- **Connection reset errors** during schema operations — sending a `REMOVE TABLE` followed by `DEFINE TABLE` in separate HTTP requests caused "Connection reset by peer" errors, leaving transactions in a bad state. Docker logs showed: `"A transaction was dropped without being committed or cancelled"`.
- **`{..+collect}` hangs the server** — the recursive traversal with unlimited depth + collect modifier never returns, even on a 3-node graph. No timeout, no error — it just blocks the connection forever.

---

## The Fundamental Problem

SurrealDB markets itself as a multi-model database with "graph capabilities." In reality:

1. **It's a document database with RELATE syntax** — you can create edges between records, but there's no graph query engine behind it
2. **No graph algorithms** — not PageRank, not BFS, not connected components, not anything
3. **No pattern matching** — no Cypher MATCH, no SQL JOINs, no way to efficiently enumerate variable bindings across multiple tables
4. **Recursive traversal is broken** — `{1..N}` depth range doesn't actually recurse beyond 1 hop
5. **No table aliases** — makes self-joins and any non-trivial graph query impossible

---

## Comparison Table

| Feature | Neo4j | ArcadeDB | Kuzu | Memgraph | ArangoDB | FalkorDB | SurrealDB |
|---------|-------|----------|------|----------|----------|----------|-----------|
| PageRank | Yes | Yes | Yes | Yes | Yes | Yes | No |
| WCC | Yes | Yes | Yes | Yes | Yes | Yes | No |
| BFS | Yes | Yes | Yes | Yes | Yes | Yes | No |
| SSSP | Yes | Yes | — | Yes | Yes | — | No |
| CDLP | Yes | Yes | — | Yes | Yes | Yes | No |
| LCC | Yes | Yes | Yes | Yes | — | — | No |
| Pattern matching | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Multi-hop traversal | Yes | Yes | Yes | Yes | Yes | Yes | No |

**SurrealDB is not a graph database.** It has graph *syntax* but no graph *engine*. Including it in a graph benchmark would be like benchmarking a bicycle in a Formula 1 race — technically it has wheels, but it's not competing in the same category.

---

## How to Reproduce

SurrealDB is included in this repository but excluded from default runs. To run it:

```bash
# Start SurrealDB (Docker)
docker run -d --name surrealdb -p 8000:8000 \
  -e SURREAL_LOG=warn \
  -v /tmp/surrealdb_data:/data \
  surrealdb/surrealdb:v2 start \
  --user root --pass benchmark \
  rocksdb:///data/bench.db

# Graphalytics benchmark (warning: loading takes ~30 minutes)
python3 ldbc-native/benchmark.py surrealdb

# LSQB benchmark (warning: loading takes ~9 minutes, all queries N/A)
python3 lsqb/lsqb_benchmark.py surrealdb
```

*Tested with SurrealDB v2.6.4 (Docker image `surrealdb/surrealdb:v2`) on March 2026.*
