"""HugeGraph (Vermeer) benchmark for LDBC Graphalytics."""

import time

from ._common import VERTEX_FILE, EDGE_FILE, SOURCE_VERTEX, GRAPHS_DIR, bench_common


def run_benchmark():
    """
    HugeGraph benchmark using Vermeer (the HugeGraph-Computer Go engine).
    Vermeer loads data directly from CSV files and runs all OLAP algorithms.

    Setup (3 containers on a shared Docker network):
      docker network create hugegraph-net
      docker run -d --name vermeer-master --network hugegraph-net \\
        -p 6688:6688 -p 6689:6689 hugegraph/vermeer --env=master
      docker run -d --name vermeer-worker --network hugegraph-net \\
        -p 6788:6788 -p 6789:6789 \\
        -v "$(cd ../datasets && pwd)":/data/graphs:ro \\
        hugegraph/vermeer --env=worker --master_peer=vermeer-master:6689
    Then assign worker to the common pool:
      curl -X POST http://localhost:6688/api/v1/admin/workers/group/\\$/$(
        curl -s http://localhost:6688/api/v1/workers | python3 -c "import sys,json; print(json.load(sys.stdin)['workers'][0]['name'])")
    """
    import requests
    import json as jsonlib
    print("\n" + "=" * 70)
    print("HUGEGRAPH (VERMEER) BENCHMARK")
    print("=" * 70)

    results = {}
    vermeer = "http://localhost:6688/api/v1"

    # Check Vermeer connectivity
    try:
        r = requests.get(f"{vermeer}/workers", timeout=5)
        workers = r.json().get("workers", [])
        if not workers:
            raise Exception("No workers registered")
        worker_ip = workers[0]["ip_addr"]
        worker_name = workers[0]["name"]
        print(f"  Vermeer master: OK ({len(workers)} worker(s))")
    except Exception as e:
        print(f"  Cannot connect to Vermeer: {e}")
        print("  See docstring for setup instructions")
        return {"error": str(e)}

    # Ensure worker is in the common "$" pool (required for task scheduling)
    if workers[0].get("group") != "$":
        requests.post(f"{vermeer}/admin/workers/group/$/{worker_name}")

    # Check if graph already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = requests.get(f"{vermeer}/graphs")
            for g in r.json().get("graphs", []):
                if g["name"] == "bench" and g["state"] == "loaded":
                    needs_load = False
                    print("\n[HugeGraph] Graph already loaded in Vermeer, skipping import")
                    break
        except Exception:
            pass

    if needs_load:
        # Delete existing graph if present
        try:
            requests.delete(f"{vermeer}/graphs/bench")
        except Exception:
            pass

        print("\n[HugeGraph] Loading data into Vermeer...")
        start = time.perf_counter()

        r = requests.post(f"{vermeer}/tasks/create/sync", json={
            "task_type": "load",
            "graph": "bench",
            "params": {
                "load.type": "local",
                "load.parallel": "50",
                "load.delimiter": " ",
                "load.vertex_files": jsonlib.dumps(
                    {worker_ip: VERTEX_FILE.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.edge_files": jsonlib.dumps(
                    {worker_ip: EDGE_FILE.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.use_property": "1",
                "load.vertex_backend": "mem"
            }
        }, timeout=600)

        if r.status_code != 200 or r.json().get("task", {}).get("state") != "loaded":
            print(f"  Load failed: {r.text[:300]}")
            return {"error": "Load failed"}

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Helper to run a Vermeer compute task (raises on failure)
    def run_algo(name, display_name, params):
        algo_params = {"compute.algorithm": name}
        algo_params.update(params)
        r = requests.post(f"{vermeer}/tasks/create/sync", json={
            "task_type": "compute",
            "graph": "bench",
            "params": algo_params
        }, timeout=600)
        if r.status_code != 200 or r.json().get("task", {}).get("state") != "complete":
            raise RuntimeError(f"{display_name} failed: {r.text[:200]}")
        return r.json()

    # --- PageRank ---
    print("\n[HugeGraph] Running PageRank...")
    def _run_pagerank():
        return run_algo("pagerank", "pagerank",
                        {"pagerank.damping": "0.85", "pagerank.diff_threshold": "0.00001"})
    elapsed, _ = bench_common.run_timed("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[HugeGraph] Running WCC...")
    def _run_wcc():
        return run_algo("wcc", "wcc", {})
    elapsed, _ = bench_common.run_timed("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- BFS (SSSP unweighted = hop-count BFS) ---
    print("\n[HugeGraph] Running BFS...")
    def _run_bfs():
        return run_algo("sssp", "bfs", {"sssp.source": str(SOURCE_VERTEX)})
    elapsed, _ = bench_common.run_timed("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # --- LCC (Clustering Coefficient) ---
    print("\n[HugeGraph] Running LCC...")
    def _run_lcc():
        return run_algo("clustering_coefficient", "lcc", {})
    elapsed, _ = bench_common.run_timed("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- SSSP (weighted — Vermeer's sssp is unweighted/hop-count only) ---
    # Vermeer's built-in SSSP computes unweighted shortest paths (hop count).
    # There is no weighted Dijkstra variant available.
    print("\n[HugeGraph] Running SSSP...")
    def _run_sssp():
        raise NotImplementedError("Vermeer sssp is unweighted only")
    elapsed, _ = bench_common.run_timed("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s")

    # --- CDLP (Label Propagation) ---
    print("\n[HugeGraph] Running CDLP...")
    def _run_cdlp():
        return run_algo("lpa", "cdlp", {})
    elapsed, _ = bench_common.run_timed("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    bench_common.cleanup_docker("vermeer-master", "vermeer-worker")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("vermeer-master", "vermeer-worker")
