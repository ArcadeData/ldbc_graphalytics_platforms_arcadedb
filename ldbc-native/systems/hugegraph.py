"""HugeGraph (Vermeer) benchmark for LDBC Graphalytics."""

import os
import time

from ._common import VERTEX_FILE, EDGE_FILE, SOURCE_VERTEX, GRAPHS_DIR, bench_common


# The reference PageRank runs 10 iterations from the uniform vector with damping 0.85 (no convergence test)
PAGERANK_PARAMS = {"pagerank.damping": "0.85", "pagerank.diff_threshold": "0", "compute.max_step": "10"}


def _undirected_edge_file():
    """The datasets store every undirected edge once and Vermeer follows the stored direction: PageRank and BFS run
    on a second graph ("bench_u") loaded from a copy of the edge file that has every edge in both directions."""
    import subprocess
    path = EDGE_FILE[:-len(".e")] + "-undirected.e"
    if not os.path.exists(path) or os.path.getmtime(path) < os.path.getmtime(EDGE_FILE):
        tmp = path + ".tmp"
        with open(tmp, "w") as out:
            subprocess.run(["awk", "{print; print $2\" \"$1\" \"$3}", EDGE_FILE], stdout=out, check=True)
        os.replace(tmp, path)
    return path


def _dump_all(run_algo):
    """Full per-vertex outputs: Vermeer writes "<id>,<value>" lines into a local file of its worker container."""
    if not bench_common.dump_enabled():
        return
    import subprocess as sp
    def vermeer_out(name, tag, params, graph="bench"):
        p = dict(params)
        p.update({"output.type": "local", "output.parallel": "1", "output.file_path": f"/tmp/dump_{tag}"})
        run_algo(name, tag, p, graph=graph)
        txt = sp.run(["docker", "exec", "vermeer-worker", "cat", f"/tmp/dump_{tag}_0"],
                     capture_output=True, text=True, check=True).stdout
        for line in txt.splitlines():
            vid, val = line.split(",", 1)
            yield int(vid), val
    bench_common.dump_safely("hugegraph", "PR", lambda: bench_common.dump_rows("hugegraph", "PR", (
        (v, float(x)) for v, x in vermeer_out("pagerank", "pr", PAGERANK_PARAMS, graph="bench_u"))))
    bench_common.dump_safely("hugegraph", "WCC", lambda: bench_common.dump_rows("hugegraph", "WCC", vermeer_out("wcc", "wcc", {})))
    bench_common.dump_safely("hugegraph", "CDLP", lambda: bench_common.dump_rows("hugegraph", "CDLP", vermeer_out("lpa", "cdlp", {})))
    bench_common.dump_safely("hugegraph", "LCC", lambda: bench_common.dump_rows("hugegraph", "LCC", (
        (v, float(x)) for v, x in vermeer_out("clustering_coefficient", "lcc", {}))))
    def bfs():
        # Vermeer sssp is unweighted (hop count); -1 marks an unreachable vertex
        bench_common.dump_rows("hugegraph", "BFS", (
            (v, bench_common.BFS_UNREACHABLE if int(float(x)) < 0 else int(float(x)))
            for v, x in vermeer_out("sssp", "bfs", {"sssp.source": str(SOURCE_VERTEX)}, graph="bench_u")))
    bench_common.dump_safely("hugegraph", "BFS", bfs)


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

    def is_loaded(graph):
        try:
            for g in requests.get(f"{vermeer}/graphs").json().get("graphs", []):
                if g["name"] == graph and g["state"] == "loaded":
                    return True
        except Exception:
            pass
        return False

    def load(graph, edge_file):
        try:
            requests.delete(f"{vermeer}/graphs/{graph}")
        except Exception:
            pass
        start = time.perf_counter()
        r = requests.post(f"{vermeer}/tasks/create/sync", json={
            "task_type": "load",
            "graph": graph,
            "params": {
                "load.type": "local",
                "load.parallel": "50",
                "load.delimiter": " ",
                "load.vertex_files": jsonlib.dumps(
                    {worker_ip: VERTEX_FILE.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.edge_files": jsonlib.dumps(
                    {worker_ip: edge_file.replace(GRAPHS_DIR, "/data/graphs")}),
                "load.use_property": "1",
                "load.vertex_backend": "mem"
            }
        }, timeout=900)
        if r.status_code != 200 or r.json().get("task", {}).get("state") != "loaded":
            print(f"  Load of {graph} failed: {r.text[:300]}")
            return None
        return time.perf_counter() - start

    # "bench" is the graph as stored (WCC, LCC, CDLP); "bench_u" has every edge in both directions (PageRank, BFS)
    if bench_common.RESET or not is_loaded("bench"):
        print("\n[HugeGraph] Loading data into Vermeer...")
        load_time = load("bench", EDGE_FILE)
        if load_time is None:
            return {"error": "Load failed"}
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
    else:
        print("\n[HugeGraph] Graph already loaded in Vermeer, skipping import")
    if bench_common.RESET or not is_loaded("bench_u"):
        print("\n[HugeGraph] Loading the both-direction copy (bench_u) ...")
        undirected_time = load("bench_u", _undirected_edge_file())
        if undirected_time is None:
            return {"error": "Load of bench_u failed"}
        print(f"  Load time (bench_u): {undirected_time:.2f}s")
        if "load" in results:
            results["load"] += undirected_time

    # Helper to run a Vermeer compute task (raises on failure)
    def run_algo(name, display_name, params, graph="bench"):
        algo_params = {"compute.algorithm": name}
        algo_params.update(params)
        r = requests.post(f"{vermeer}/tasks/create/sync", json={
            "task_type": "compute",
            "graph": graph,
            "params": algo_params
        }, timeout=600)
        if r.status_code != 200 or r.json().get("task", {}).get("state") != "complete":
            raise RuntimeError(f"{display_name} failed: {r.text[:200]}")
        return r.json()

    # --- PageRank ---
    print("\n[HugeGraph] Running PageRank...")
    def _run_pagerank():
        return run_algo("pagerank", "pagerank", PAGERANK_PARAMS, graph="bench_u")
    elapsed, _ = bench_common.run_timed_warm("PageRank", _run_pagerank)
    results["pagerank"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  PageRank time: {elapsed:.2f}s")

    # --- WCC ---
    print("\n[HugeGraph] Running WCC...")
    def _run_wcc():
        return run_algo("wcc", "wcc", {})
    elapsed, _ = bench_common.run_timed_warm("WCC", _run_wcc)
    results["wcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  WCC time: {elapsed:.2f}s")

    # --- BFS (SSSP unweighted = hop-count BFS) ---
    print("\n[HugeGraph] Running BFS...")
    def _run_bfs():
        return run_algo("sssp", "bfs", {"sssp.source": str(SOURCE_VERTEX)}, graph="bench_u")
    elapsed, _ = bench_common.run_timed_warm("BFS", _run_bfs)
    results["bfs"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  BFS time: {elapsed:.2f}s")

    # --- LCC (Clustering Coefficient) ---
    print("\n[HugeGraph] Running LCC...")
    def _run_lcc():
        return run_algo("clustering_coefficient", "lcc", {})
    elapsed, _ = bench_common.run_timed_warm("LCC", _run_lcc)
    results["lcc"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  LCC time: {elapsed:.2f}s")

    # --- SSSP (weighted — Vermeer's sssp is unweighted/hop-count only) ---
    # Vermeer's built-in SSSP computes unweighted shortest paths (hop count).
    # There is no weighted Dijkstra variant available.
    print("\n[HugeGraph] Running SSSP...")
    def _run_sssp():
        raise NotImplementedError("Vermeer sssp is unweighted only")
    elapsed, _ = bench_common.run_timed_warm("SSSP", _run_sssp)
    results["sssp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  SSSP time: {elapsed:.2f}s")

    # --- CDLP (Label Propagation) ---
    print("\n[HugeGraph] Running CDLP...")
    def _run_cdlp():
        return run_algo("lpa", "cdlp", {})
    elapsed, _ = bench_common.run_timed_warm("CDLP", _run_cdlp)
    results["cdlp"] = elapsed
    if isinstance(elapsed, (int, float)):
        print(f"  CDLP time: {elapsed:.2f}s")

    _dump_all(run_algo)
    bench_common.cleanup_docker("vermeer-master", "vermeer-worker")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("vermeer-master", "vermeer-worker")
