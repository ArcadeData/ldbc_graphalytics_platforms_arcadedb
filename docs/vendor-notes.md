# Vendor notes: SurrealDB, Dgraph, FalkorDB

Systems that need more than a table row: why SurrealDB and Dgraph are excluded from default runs, what Dgraph can still answer, and what the harness has to handle for FalkorDB.

## SurrealDB

SurrealDB is implemented in both benchmark modes but **excluded from default runs** because it scores N/A on every metric — all 6 Graphalytics algorithms and all 9 LSQB queries.

### Why it's excluded

Despite marketing itself as a multi-model database with "graph capabilities," SurrealDB lacks the fundamentals needed for graph benchmarking:

- **No graph algorithms** — zero support for PageRank, WCC, BFS, CDLP, LCC, or SSSP. Every other database in the benchmark ships with at least some of these.
- **Broken recursive traversal** — the `->edge.{1..N}->node` syntax doesn't actually recurse beyond 1 hop. On the real graph, "BFS" found only 34 nodes (direct neighbors) instead of the expected 633K.
- **No pattern matching** — no Cypher MATCH, no SQL JOINs, no table aliases. This makes self-joins and multi-table queries impossible. LSQB queries Q3, Q6, Q8, Q9 cannot be expressed at all. Queries Q1, Q2, Q4, Q5, Q7 are implemented using nested subqueries with `$parent` dereferencing and `array::len()` for cross-product counting, but all timeout at 120s — the O(n*m) nested loop execution without index acceleration is too slow for 3.9M vertices / 17.9M edges.
- **Extremely slow loading** — 34M edges took ~30 minutes via the HTTP API (1MB payload limit forces 3,400 round-trips), compared to seconds for embedded systems.
- **Stability issues** — OOM crashes (exit 137) during cleanup, connection resets during schema operations, and `{..+collect}` hangs the server indefinitely.

For the full analysis, see [SURREALDB.md](../SURREALDB.md).

### How to enable SurrealDB

```bash
# Start SurrealDB (Docker)
docker run -d --name surrealdb -p 8000:8000 \
  -e SURREAL_LOG=warn \
  -v /tmp/surrealdb_data:/data \
  surrealdb/surrealdb:v2 start \
  --user root --pass benchmark \
  rocksdb:///data/bench.db

# Run Graphalytics benchmark (warning: loading takes ~30 minutes)
cd ldbc-native
python3 benchmark.py surrealdb

# Run LSQB benchmark (warning: loading takes ~9 minutes, Q1/Q2/Q4/Q5/Q7 timeout, rest N/A)
cd lsqb
python3 lsqb_benchmark.py surrealdb
```

*Tested with SurrealDB v2.6.4 on March 2026.*


## Dgraph

Dgraph v25.3.0 is implemented in both benchmark modes but **excluded from default runs**. It scores N/A on all 6 Graphalytics algorithms and answers only 3 of 9 LSQB queries.

### Why it's excluded

Dgraph is a distributed graph database with the DQL query language (formerly GraphQL+-). Unlike Cypher or SQL engines, DQL is a hierarchical traversal language that returns nested JSON — it has no `MATCH` clause, no `JOIN`, no table aliases, and no `NOT EXISTS`. This creates fundamental limitations:

- **No graph algorithms** — Dgraph has no built-in PageRank, WCC, BFS (single-source-all-destinations), LCC, SSSP, or CDLP. The only built-in algorithm is `shortest()`, which is point-to-point (requires both source and target UIDs), not single-source-all-destinations as LDBC Graphalytics requires.
- **No pattern matching** — DQL traverses the graph from root nodes outward and cannot express arbitrary join conditions between different parts of a pattern. This makes 6 of 9 LSQB queries impossible.
- **Loading via HTTP mutations** — 34M Graphalytics edges take ~204s via batched RDF N-Quad mutations. LSQB (3.9M vertices, 17.9M edges) takes ~214s.

### What Dgraph CAN do (LSQB Q1, Q4, Q7)

Despite lacking pattern matching, three LSQB queries can be expressed in DQL using creative techniques:

**Q1 (chain traversal)** — DQL value variable propagation. The 8-hop chain Country←City←Person←Forum→Post←Comment→Tag→TagClass is expressed as nested reverse-edge traversals (`~is_part_of`, `~is_located_in`, etc.). At the leaf level, `count(has_type)` counts TagClasses per Tag, then `sum(val())` at each parent level propagates the path count upward — giving the exact Cartesian product count (221,636,419). This works because each level's sum is equivalent to multiplying child path counts, which matches `count(*)` semantics for chain patterns.

**Q4 (star pattern)** — DQL `math()` function. For each Message with tags, likes, and replies, the tuple count equals `tags × likes × replies`. Two `var` blocks compute `math(t * l * r)` separately for Posts (replies via `~reply_of_post`) and Comments (replies via `~reply_of_comment`), then `sum()` aggregates both into the correct total (14,836,038).

**Q7 (optional star)** — Like Q4 but with `OPTIONAL MATCH` semantics. Messages without likes or replies still contribute one row each. Expressed as `math(tags × max(likes, 1) × max(replies, 1))` — the `max(count, 1)` emulates the NULL-becomes-one-row behavior of `OPTIONAL MATCH` (26,190,133).

### Why 6 queries are impossible in DQL

| Query | Limitation |
|-------|-----------|
| **Q2** (diamond) | Requires per-row correlation: "Comment created by Person1 replies to Post created by Person2, AND Person1 KNOWS Person2." DQL `var` blocks produce global UID sets, not per-row bindings. |
| **Q3** (triangle) | Requires self-join on Person (3 different Persons in same Country, all connected by KNOWS). DQL has no self-join. |
| **Q5** (fork) | Requires cross-reference inequality: `tag1 <> tag2` where tag1 is from the message and tag2 is from the reply. DQL cannot compare values across different nesting levels. |
| **Q6** (2-hop KNOWS) | Requires per-row inequality: `person1 <> person3`. DQL has no way to exclude specific nodes per-traversal. |
| **Q8** (anti-pattern) | Requires `NOT EXISTS`: "Comment must NOT have the parent's Tag." DQL has no anti-join operator. |
| **Q9** (anti-pattern) | Requires both `NOT EXISTS` and per-row inequality — combines Q6 and Q8 limitations. |

### Performance comparison (LSQB)

On the 3 queries Dgraph can answer:
- **Q1**: Dgraph 2.52s — faster than Kuzu (5.83s), Neo4j (8.25s), PostgreSQL (6.56s), and Memgraph (60.45s), but 11x slower than ArcadeDB (0.23s) and 17x slower than DuckDB (0.15s).
- **Q4**: Dgraph 8.13s — comparable to Neo4j (7.82s) and PostgreSQL (6.86s), but 400x slower than ArcadeDB (0.02s) and 100x slower than DuckDB (0.08s).
- **Q7**: Dgraph 5.97s — faster than Neo4j (10.45s) and PostgreSQL (11.22s), but 300x slower than ArcadeDB (0.02s) and 75x slower than DuckDB (0.08s).

### How to enable Dgraph

```bash
# Start Dgraph (Docker — requires two containers: Zero + Alpha)
docker network create dgraph-net
docker run -d --name dgraph-zero --network dgraph-net \
  -p 5080:5080 -p 6080:6080 \
  dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080
docker run -d --name dgraph-alpha --network dgraph-net \
  -p 8080:8080 -p 9080:9080 \
  -v /tmp/dgraph_data:/dgraph \
  dgraph/dgraph:latest dgraph alpha \
    --my=dgraph-alpha:7080 \
    --zero=dgraph-zero:5080 \
    --cache size-mb=8192 \
    --badger "compression=none; numgoroutines=8" \
    --security whitelist=0.0.0.0/0 \
    --limit "mutations-nquad=5000000; query-edge=10000000"

# Run Graphalytics benchmark (loading ~204s, all algorithms N/A)
cd ldbc-native
python3 benchmark.py dgraph

# Run LSQB benchmark (loading ~214s, Q1/Q4/Q7 answered, rest N/A)
cd lsqb
python3 lsqb_benchmark.py dgraph
```

*Tested with Dgraph v25.3.0 on March 2026.*


## FalkorDB

FalkorDB 6.0.1 (Redis 8.10.2) is a Redis-based graph database that supports a subset of Cypher. It is included in the default Graphalytics and LSQB runs. With this release it returns **correct counts on all 9 LSQB queries** (v4.16.8, tested in April 2026, returned wrong counts on four of them), but it is slow on the multi-hop patterns (3.5-225 s per query, see the LSQB table) and has the slowest LSQB load (659 s through Cypher `UNWIND`/`CREATE` batches).

### Things the harness has to handle

- **Bulk loading** — the Graphalytics graph (68.4M edge records, both directions) loads in 116 s with `falkordb-bulk-insert` (`pip install falkordb-bulk-loader`); creating the same edges with one Cypher write query per 5000 edges runs at about 25k edges/s and took 53 minutes. The LSQB driver still uses Cypher batches.
- **Snapshots** — with the default Redis save points a background save forks the 20+ GB process during the load, the fork fails and Redis then answers every write with `MISCONF`. The drivers set `save ""` and `stop-writes-on-bgsave-error no` and take one synchronous `SAVE` after the load, so a stopped container comes back with the full graph.
- **Result sets** — `RESULTSET_SIZE` defaults to 10000 rows and silently truncates full per-vertex outputs (the validation then fails); the Graphalytics driver sets it to unlimited.
- **Readiness** — the Redis port opens while the server is still loading its snapshot (-LOADING), so the harness probes with `PING` before the driver connects.
- **Graphalytics** — `algo.pageRank` has no parameters (invalid against the 10-iteration reference), WCC and BFS validate, CDLP finds the reference communities with other label values; no LCC or SSSP.

### How to run FalkorDB (LSQB)

```bash
# Start FalkorDB (Docker)
docker run -d --name falkordb-lsqb -p 6379:6379 \
  -v /tmp/falkordb_lsqb:/var/lib/falkordb/data falkordb/falkordb:latest

# Run LSQB benchmark
cd lsqb
python3 lsqb_benchmark.py falkordb
```

*Tested with FalkorDB 6.0.1 on October 2026.*
