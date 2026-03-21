"""Dgraph benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, bench_common


def run_benchmark():
    """
    Dgraph benchmark via Docker (HTTP API).

    Dgraph is a distributed graph database with the DQL query language and
    built-in shortest-path queries. However, it does not include built-in
    graph analytics algorithms (PageRank, WCC, CDLP, LCC, BFS as LDBC
    defines it, or weighted SSSP). We benchmark data loading and report
    N/A for unsupported algorithms.

    Performance tuning applied:
    - Badger cache = 8GB (fast reads)
    - Compression = none (fastest writes)
    - High mutation/query limits
    - Worker goroutines for compaction = 8

    Setup:
      docker network create dgraph-net 2>/dev/null
      docker run -d --name dgraph-zero --network dgraph-net \\
        -p 5080:5080 -p 6080:6080 \\
        dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080
      docker run -d --name dgraph-alpha --network dgraph-net \\
        -p 8080:8080 -p 9080:9080 \\
        -v /tmp/dgraph_data:/dgraph \\
        dgraph/dgraph:latest dgraph alpha \\
          --my=dgraph-alpha:7080 \\
          --zero=dgraph-zero:5080 \\
          --cache size-mb=8192 \\
          --badger "compression=none; numgoroutines=8" \\
          --security whitelist=0.0.0.0/0 \\
          --limit "mutations-nquad=5000000; query-edge=10000000"
    """
    import requests
    print("\n" + "=" * 70)
    print("DGRAPH BENCHMARK")
    print("=" * 70)

    results = {}
    alpha_url = "http://localhost:8080"

    def alter(schema_or_json, timeout=60):
        """Alter schema or drop data."""
        if isinstance(schema_or_json, dict):
            r = requests.post(f"{alpha_url}/alter",
                              json=schema_or_json, timeout=timeout)
        else:
            r = requests.post(f"{alpha_url}/alter",
                              data=schema_or_json.encode("utf-8"), timeout=timeout)
        r.raise_for_status()
        return r.json()

    def mutate(nquads, timeout=600):
        """Execute an RDF N-Quad mutation with immediate commit."""
        payload = f'{{\n  set {{\n{nquads}\n  }}\n}}'
        r = requests.post(f"{alpha_url}/mutate?commitNow=1",
                          data=payload.encode("utf-8"),
                          headers={"Content-Type": "application/rdf"},
                          timeout=timeout)
        r.raise_for_status()
        return r.json()

    def query(dql, timeout=600):
        """Execute a DQL query."""
        r = requests.post(f"{alpha_url}/query",
                          data=dql.encode("utf-8"),
                          headers={"Content-Type": "application/dql"},
                          timeout=timeout)
        r.raise_for_status()
        return r.json()

    # Check connectivity
    try:
        r = requests.get(f"{alpha_url}/health", timeout=5)
        r.raise_for_status()
        print("  Dgraph Alpha server: OK")
    except Exception as e:
        print(f"  Cannot connect to Dgraph Alpha: {e}")
        print("  Start with:")
        print("    docker network create dgraph-net 2>/dev/null")
        print("    docker run -d --name dgraph-zero --network dgraph-net \\")
        print("      -p 5080:5080 -p 6080:6080 \\")
        print("      dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080")
        print("    docker run -d --name dgraph-alpha --network dgraph-net \\")
        print("      -p 8080:8080 -p 9080:9080 \\")
        print("      -v /tmp/dgraph_data:/dgraph \\")
        print("      dgraph/dgraph:latest dgraph alpha \\")
        print("        --my=dgraph-alpha:7080 \\")
        print("        --zero=dgraph-zero:5080 \\")
        print("        --cache size-mb=8192 \\")
        print('        --badger "compression=none; numgoroutines=8" \\')
        print("        --security whitelist=0.0.0.0/0 \\")
        print('        --limit "mutations-nquad=5000000; query-edge=10000000"')
        return {"error": str(e)}

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            resp = query('{ count(func: has(vid)) { total: count(uid) } }')
            cnt = resp["data"]["count"][0]["total"]
            if cnt > 0:
                needs_load = False
                print(f"\n[Dgraph] Data already loaded ({cnt} nodes), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[Dgraph] Loading data...")
        start = time.perf_counter()

        # Drop all existing data and schema
        alter({"drop_all": True})

        # Define schema
        alter("""
            vid: int @index(int) .
            edge: [uid] .
            type Node {
                vid
                edge
            }
        """)

        # Load vertices in batches, collecting blank-node -> UID mappings
        print("  Loading vertices...")
        vid_to_uid = {}
        batch_size = 20000
        batch_lines = []
        v_count = 0
        with open(VERTEX_FILE) as f:
            for line in f:
                vid = line.strip()
                batch_lines.append(f'    _:v{vid} <vid> "{vid}"^^<xs:int> .')
                batch_lines.append(f'    _:v{vid} <dgraph.type> "Node" .')
                v_count += 1
                if len(batch_lines) >= batch_size * 2:
                    resp = mutate("\n".join(batch_lines))
                    uids = resp.get("data", {}).get("uids", {})
                    for key, uid in uids.items():
                        # key is "v{vid}", extract vid
                        vid_to_uid[int(key[1:])] = uid
                    batch_lines = []
            if batch_lines:
                resp = mutate("\n".join(batch_lines))
                uids = resp.get("data", {}).get("uids", {})
                for key, uid in uids.items():
                    vid_to_uid[int(key[1:])] = uid
        print(f"  Vertices: {v_count} (mapped {len(vid_to_uid)} UIDs)")

        # Load edges in batches using resolved UIDs with weight as facet
        print("  Loading edges...")
        edge_batch_size = 50000
        batch_lines = []
        e_count = 0
        with open(EDGE_FILE) as f:
            for line in f:
                parts = line.split()
                src_vid = int(parts[0])
                dst_vid = int(parts[1])
                weight = parts[2]
                src_uid = vid_to_uid.get(src_vid)
                dst_uid = vid_to_uid.get(dst_vid)
                if src_uid and dst_uid:
                    batch_lines.append(
                        f'    <{src_uid}> <edge> <{dst_uid}> (weight={weight}) .')
                e_count += 1
                if len(batch_lines) >= edge_batch_size:
                    mutate("\n".join(batch_lines))
                    batch_lines = []
                    if e_count % 500000 == 0:
                        elapsed = time.perf_counter() - start
                        print(f"    {e_count:,} edges loaded ({elapsed:.0f}s)")
            if batch_lines:
                mutate("\n".join(batch_lines))

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Edges: {e_count}")
        print(f"  Load time: {load_time:.2f}s")

    # Verify counts
    try:
        resp = query('{ count(func: has(vid)) { total: count(uid) } }')
        print(f"  Vertices: {resp['data']['count'][0]['total']}")
        resp = query('{ count(func: has(edge)) { total: count(uid) } }')
        print(f"  Nodes with edges: {resp['data']['count'][0]['total']}")
    except Exception:
        pass

    # --- BFS (Breadth-First Search) ---
    # Dgraph's shortest() function only works point-to-point (requires both
    # source and target UIDs), not single-source-all-destinations as LDBC
    # defines BFS. The @recurse directive could traverse the graph but does
    # not compute distances and would return the entire connected component
    # as a deeply nested JSON structure — infeasible at 633K vertices.
    print("\n[Dgraph] BFS: not supported (shortest() is point-to-point only; no single-source BFS)")
    results["bfs"] = "N/A"

    # --- PageRank ---
    # Dgraph has no built-in PageRank. Implementing iteratively via DQL
    # would require O(iterations * vertices) round-trip mutations, which
    # is infeasible at 633K vertices / 34M edges.
    print("\n[Dgraph] PageRank: not supported (no built-in graph algorithm)")
    results["pagerank"] = "N/A"

    # --- WCC (Weakly Connected Components) ---
    # No built-in connected components. Would require iterative union-find
    # via DQL mutations, infeasible at this scale.
    print("\n[Dgraph] WCC: not supported (no built-in graph algorithm)")
    results["wcc"] = "N/A"

    # --- LCC (Local Clustering Coefficient) ---
    # No built-in LCC. Triangle counting per vertex would require
    # O(V * d^2) edge lookups via DQL, infeasible on 34M edges.
    print("\n[Dgraph] LCC: not supported (no built-in graph algorithm)")
    results["lcc"] = "N/A"

    # --- SSSP (Single-Source Shortest Path) ---
    # Dgraph's shortest() supports weighted edges (via facets) but only
    # between specific source-destination pairs. LDBC SSSP requires
    # single-source-all-destinations weighted Dijkstra, which is not available.
    print("\n[Dgraph] SSSP: not supported (shortest() is point-to-point only; no single-source Dijkstra)")
    results["sssp"] = "N/A"

    # --- CDLP (Community Detection via Label Propagation) ---
    # No built-in label propagation. Iterative DQL implementation infeasible
    # at this scale (synchronous updates of all 633K vertex labels per
    # iteration).
    print("\n[Dgraph] CDLP: not supported (no built-in graph algorithm)")
    results["cdlp"] = "N/A"

    return results
