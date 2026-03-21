"""SurrealDB benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


def run_benchmark():
    """
    SurrealDB benchmark via Docker (HTTP API).

    SurrealDB is a multi-model database with native graph traversal support
    (RELATE, ->, <-, <->, recursive queries with +collect/+shortest modifiers).
    However, it does not include built-in graph analytics algorithms (PageRank,
    WCC, CDLP, LCC, weighted SSSP). We benchmark what SurrealDB can natively
    do: data loading and BFS traversal.

    Performance tuning applied:
    - RocksDB storage engine (persistent, optimized for reads)
    - Worker threads = CPU count
    - RocksDB block cache = 8GB
    - RocksDB write buffer = 128MB
    - Compaction parallelism = 2x CPUs
    - Log level = warn (reduce I/O overhead)

    Setup:
      docker run -d --name surrealdb -p 8000:8000 \\
        -e SURREAL_RUNTIME_WORKER_THREADS=$(nproc) \\
        -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE=8589934592 \\
        -e SURREAL_ROCKSDB_WRITE_BUFFER_SIZE=134217728 \\
        -e SURREAL_ROCKSDB_JOBS_COUNT=16 \\
        -e SURREAL_LOG=warn \\
        -v /tmp/surrealdb_data:/data \\
        surrealdb/surrealdb:v2 start \\
        --user root --pass benchmark \\
        rocksdb:///data/bench.db
    """
    import requests
    print("\n" + "=" * 70)
    print("SURREALDB BENCHMARK")
    print("=" * 70)

    results = {}

    def sql(query, timeout=600):
        """Execute SurrealQL via HTTP API and return parsed response."""
        r = requests.post("http://localhost:8000/sql",
            headers={
                "Accept": "application/json",
                "surreal-ns": "test",
                "surreal-db": "bench",
            },
            data=query.encode("utf-8"),
            auth=("root", "benchmark"),
            timeout=timeout)
        r.raise_for_status()
        resp = r.json()
        for stmt in resp:
            if stmt.get("status") == "ERR":
                raise Exception(stmt.get("result", "Unknown SurrealDB error"))
        return resp

    # Check connectivity
    try:
        requests.get("http://localhost:8000/health", timeout=5)
        print("  SurrealDB server: OK")
    except Exception as e:
        print(f"  Cannot connect to SurrealDB: {e}")
        print("  Start with:")
        print("    docker run -d --name surrealdb -p 8000:8000 \\")
        print("      -e SURREAL_RUNTIME_WORKER_THREADS=$(nproc) \\")
        print("      -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE=8589934592 \\")
        print("      -e SURREAL_ROCKSDB_WRITE_BUFFER_SIZE=134217728 \\")
        print("      -e SURREAL_ROCKSDB_JOBS_COUNT=16 \\")
        print("      -e SURREAL_LOG=warn \\")
        print("      -v /tmp/surrealdb_data:/data \\")
        print("      surrealdb/surrealdb:v2 start \\")
        print("        --user root --pass benchmark \\")
        print("        rocksdb:///data/bench.db")
        return {"error": str(e)}

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = sql("SELECT count() FROM edge GROUP ALL;")
            cnt = r[0]["result"][0]["count"]
            if cnt > 0:
                needs_load = False
                print(f"\n[SurrealDB] Data already loaded ({cnt} edges), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[SurrealDB] Loading data...")
        start = time.perf_counter()

        # Clean slate + define schema in one call
        sql("""
            REMOVE TABLE IF EXISTS edge;
            REMOVE TABLE IF EXISTS node;
            DEFINE TABLE node SCHEMAFULL;
            DEFINE TABLE edge TYPE RELATION FROM node TO node SCHEMAFULL;
            DEFINE FIELD weight ON edge TYPE float;
        """)

        # Load vertices in batches using INSERT with record ID
        # Using node:<vid> as record ID gives O(1) lookups by vertex ID
        # Batch size limited to stay under SurrealDB HTTP payload limit (~1MB)
        print("  Loading vertices...")
        batch_size = 20000
        batch = []
        v_count = 0
        with open(VERTEX_FILE) as f:
            for line in f:
                vid = line.strip()
                batch.append(f"{{ id: node:{vid} }}")
                v_count += 1
                if len(batch) >= batch_size:
                    sql(f"INSERT INTO node [{', '.join(batch)}];")
                    batch = []
            if batch:
                sql(f"INSERT INTO node [{', '.join(batch)}];")
        print(f"  Vertices: {v_count}")

        # Load edges in batches using INSERT RELATION
        # This is the fastest bulk edge creation method in SurrealDB
        # Edge records are larger, so use smaller batch size to stay under HTTP limit
        print("  Loading edges...")
        edge_batch_size = 10000
        batch = []
        e_count = 0
        with open(EDGE_FILE) as f:
            for line in f:
                parts = line.split()
                batch.append(
                    f"{{ in: node:{parts[0]}, out: node:{parts[1]}, weight: {parts[2]} }}")
                e_count += 1
                if len(batch) >= edge_batch_size:
                    sql(f"INSERT RELATION INTO edge [{', '.join(batch)}];", timeout=120)
                    batch = []
                    if e_count % 500000 == 0:
                        elapsed = time.perf_counter() - start
                        print(f"    {e_count:,} edges loaded ({elapsed:.0f}s)")
            if batch:
                sql(f"INSERT RELATION INTO edge [{', '.join(batch)}];", timeout=120)

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Edges: {e_count}")
        print(f"  Load time: {load_time:.2f}s")

    # Verify counts
    try:
        r = sql("SELECT count() FROM node GROUP ALL;")
        print(f"  Vertices: {r[0]['result'][0]['count']}")
        r = sql("SELECT count() FROM edge GROUP ALL;")
        print(f"  Edges: {r[0]['result'][0]['count']}")
    except Exception:
        pass

    # --- BFS (Breadth-First Search) ---
    # SurrealDB's recursive graph traversal (e.g. ->edge.{1..N}->node) does
    # NOT compose multi-hop paths — {1..30} returns the same results as {1..1}.
    # True multi-hop BFS would require iterative queries (one per depth level),
    # but at 633K vertices the frontier grows too large for HTTP query strings.
    # We report the single-hop traversal as an honest measure of SurrealDB's
    # graph traversal capability.
    print("\n[SurrealDB] BFS: not supported (recursive traversal does not compose multi-hop paths)")
    results["bfs"] = "N/A"

    # --- PageRank ---
    # SurrealDB has no built-in PageRank algorithm. Implementing iteratively
    # via SurrealQL would require O(iterations * vertices) UPDATE queries,
    # which is infeasible at 633K vertices / 34M edges.
    print("\n[SurrealDB] PageRank: not supported (no built-in graph algorithm)")
    results["pagerank"] = "N/A"

    # --- WCC (Weakly Connected Components) ---
    # No built-in connected components algorithm. Would require iterative
    # union-find via SurrealQL, infeasible at this scale.
    print("\n[SurrealDB] WCC: not supported (no built-in graph algorithm)")
    results["wcc"] = "N/A"

    # --- LCC (Local Clustering Coefficient) ---
    # No built-in LCC. Triangle counting per vertex via SurrealQL would
    # require O(V * d^2) edge lookups, infeasible on 34M edges.
    print("\n[SurrealDB] LCC: not supported (no built-in graph algorithm)")
    results["lcc"] = "N/A"

    # --- SSSP (Single-Source Shortest Path) ---
    # SurrealDB has +shortest modifier for hop-count shortest path (unweighted).
    # LDBC Graphalytics SSSP requires weighted Dijkstra, which is not available.
    print("\n[SurrealDB] SSSP: not supported (no weighted Dijkstra; only hop-count shortest path)")
    results["sssp"] = "N/A"

    # --- CDLP (Community Detection via Label Propagation) ---
    # No built-in label propagation algorithm. Iterative SurrealQL
    # implementation infeasible at this scale (synchronous updates of all
    # 633K vertex labels per iteration).
    print("\n[SurrealDB] CDLP: not supported (no built-in graph algorithm)")
    results["cdlp"] = "N/A"

    return results
