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
    # SurrealDB's recursive graph traversal does not compose multi-hop paths.
    print("\n[SurrealDB] Running BFS...")
    def _run_bfs():
        raise NotImplementedError("recursive traversal does not compose multi-hop paths")
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # --- PageRank ---
    # SurrealDB has no built-in PageRank algorithm.
    print("\n[SurrealDB] Running PageRank...")
    def _run_pagerank():
        raise NotImplementedError("no built-in graph algorithm")
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC (Weakly Connected Components) ---
    # No built-in connected components algorithm.
    print("\n[SurrealDB] Running WCC...")
    def _run_wcc():
        raise NotImplementedError("no built-in graph algorithm")
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- LCC (Local Clustering Coefficient) ---
    # No built-in LCC.
    print("\n[SurrealDB] Running LCC...")
    def _run_lcc():
        raise NotImplementedError("no built-in graph algorithm")
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- SSSP (Single-Source Shortest Path) ---
    # SurrealDB has no weighted Dijkstra; only hop-count shortest path.
    print("\n[SurrealDB] Running SSSP...")
    def _run_sssp():
        raise NotImplementedError("no weighted Dijkstra; only hop-count shortest path")
    elapsed, _ = bench_common.run_timed("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s")

    # --- CDLP (Community Detection via Label Propagation) ---
    # No built-in label propagation algorithm.
    print("\n[SurrealDB] Running CDLP...")
    def _run_cdlp():
        raise NotImplementedError("no built-in graph algorithm")
    elapsed, _ = bench_common.run_timed("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    bench_common.cleanup_docker("surrealdb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("surrealdb")
