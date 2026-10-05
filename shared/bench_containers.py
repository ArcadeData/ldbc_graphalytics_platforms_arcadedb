"""Docker lifecycle for the benchmark vendors, with persistent data.

The orchestrator starts a vendor's container before the benchmark child runs
and stops it afterwards. The data directory is bind-mounted from the state
root, so removing the container keeps the loaded graph: the next run finds the
data already there and skips the load (every system module already checks for
loaded data and skips the import).

The host data directory name contains the image id: a newer image gets a fresh
empty directory (data formats are not portable between versions), an unchanged
image reuses its data.
"""

import os
import socket
import subprocess
import time

import bench_state

HEAP = "12g"  # CLAUDE.md rule: same JVM heap for every JVM-based vendor


class Spec:
    def __init__(self, name, image, ports=(), env=None, volumes=(), args=(),
                 ready_port=None, ready_timeout=240, settle=0, stop_timeout=180, run_args=(),
                 ready_probe=None, max_map_count=None):
        self.name = name
        self.image = image
        self.ports = list(ports)        # ["host:container", ...]
        self.env = dict(env or {})
        self.volumes = list(volumes)    # [("data-subdir", "/container/path"), ...]
        self.args = list(args)          # arguments after the image name
        self.ready_port = ready_port    # host port that must accept TCP connections
        self.ready_timeout = ready_timeout
        self.settle = settle            # extra seconds after the port opens
        self.stop_timeout = stop_timeout
        self.run_args = list(run_args)  # extra `docker run` options, e.g. --user root
        # probe(port) -> bool, a protocol level check run after the port opens: Docker accepts the TCP connection
        # before the engine behind it does (Memgraph while it recovers its data, Postgres during initdb)
        self.ready_probe = ready_probe
        self.max_map_count = max_map_count  # kernel setting of the Docker VM that the engine needs (Memgraph: 524288)


def _docker(*args, check=False, timeout=600):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          check=check, timeout=timeout)


def image_id(image):
    """Short image id; pulls the image when it is not present locally."""
    r = _docker("image", "inspect", "--format", "{{.Id}}", image)
    if r.returncode != 0:
        print(f"  Pulling {image} ...")
        _docker("pull", image, check=True, timeout=1800)
        r = _docker("image", "inspect", "--format", "{{.Id}}", image, check=True)
    return r.stdout.strip().replace("sha256:", "")[:12]


def container_running(name):
    r = _docker("ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}")
    return name in r.stdout.split()


def wait_port(port, timeout, host="127.0.0.1"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    return False


def _exchange(port, payload, expect_len, host="127.0.0.1"):
    """Send payload, return what the server answers (b"" when it closes the connection or does not answer)."""
    try:
        with socket.create_connection((host, port), timeout=5) as s:
            s.settimeout(5)
            s.sendall(payload)
            return s.recv(expect_len)
    except OSError:
        return b""


def bolt_ready(port):
    """Bolt handshake (magic + four proposed versions): the server answers with the 4 byte chosen version."""
    return len(_exchange(port, bytes.fromhex("6060b017") + bytes.fromhex("00000104") + bytes(12), 4)) == 4


def postgres_ready(port):
    """SSLRequest: a running Postgres answers 'S' or 'N'; the Docker proxy just closes while it is not up."""
    return _exchange(port, bytes.fromhex("0000000804d2162f"), 1) in (b"S", b"N")


def redis_ready(port):
    """PING: a Redis (FalkorDB) that is still loading its snapshot answers -LOADING instead of +PONG, and the driver's
    GRAPH.CONFIG commands would silently fail (default 10000 row result sets, 1 s query timeout)."""
    return _exchange(port, b"PING\r\n", 16).startswith(b"+PONG")


def wait_probe(probe, port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe(port):
            return True
        time.sleep(3)
    return False


def ensure_max_map_count(minimum):
    """Raise vm.max_map_count in the Docker VM (Docker Desktop's default, 262144, makes Memgraph's jemalloc fail
    with 'Error in munmap(): Cannot allocate memory' and drop the connection mid-query). Not persistent across
    a Docker restart, so it is applied before every start."""
    r = _docker("run", "--rm", "--privileged", "--pid=host", "alpine", "nsenter", "-t", "1", "-m", "-u", "-n", "-i",
                "sh", "-c", f"[ $(cat /proc/sys/vm/max_map_count) -ge {minimum} ] || sysctl -w vm.max_map_count={minimum}")
    if r.returncode != 0:
        print(f"  Warning: could not raise vm.max_map_count to {minimum}: {r.stderr.strip()[:200]}")


def data_dir(suite, key, image):
    return bench_state.state_path("data", f"{suite}-{key}-{image_id(image)}")


def ensure_running(suite, key, spec, log=print):
    """Start the container if needed. Returns the image id used."""
    iid = image_id(spec.image)
    if container_running(spec.name):
        log(f"  Container {spec.name} already running, reusing it")
        return iid
    _docker("rm", "-f", spec.name)  # stale stopped container with the same name
    if spec.max_map_count:
        ensure_max_map_count(spec.max_map_count)
    cmd = ["run", "-d", "--name", spec.name]
    for p in spec.ports:
        cmd += ["-p", p]
    for k, v in spec.env.items():
        cmd += ["-e", f"{k}={v}"]
    base = bench_state.state_path("data", f"{suite}-{key}-{iid}")
    for sub, cpath in spec.volumes:
        host = os.path.join(base, sub)
        os.makedirs(host, exist_ok=True)
        cmd += ["-v", f"{host}:{cpath}"]
    cmd += [*spec.run_args, spec.image, *spec.args]
    log(f"  Starting {spec.name} ({spec.image}, id {iid}), data in {base}")
    r = _docker(*cmd)
    if r.returncode != 0:
        raise RuntimeError(f"docker run failed for {spec.name}: {r.stderr.strip()[:300]}")
    if spec.ready_port and not wait_port(spec.ready_port, spec.ready_timeout):
        raise RuntimeError(f"{spec.name} did not open port {spec.ready_port} within {spec.ready_timeout}s")
    if spec.ready_probe and not wait_probe(spec.ready_probe, spec.ready_port, spec.ready_timeout):
        raise RuntimeError(f"{spec.name} accepts connections on port {spec.ready_port} but is not ready "
                           f"after {spec.ready_timeout}s")
    if spec.settle:
        time.sleep(spec.settle)
    return iid


def stop(spec, log=print):
    """Graceful stop (so the engine flushes/snapshots), then remove the container."""
    bench_common_save = getattr(__import__("bench_common"), "save_container_log", None)
    if bench_common_save:
        bench_common_save(spec.name)
    if container_running(spec.name):
        _docker("stop", "-t", str(spec.stop_timeout), spec.name, timeout=spec.stop_timeout + 60)
    _docker("rm", "-f", spec.name)
    log(f"  Stopped and removed container {spec.name} (data kept)")


# ------------------------------------------------------------- vendor specs
# key = (suite, vendor key). Vendors without a spec are either embedded or
# manage their own containers (ArcadeDB Docker in Mode 2) or are excluded by
# default (SurrealDB, Dgraph).

NEO4J_ENV = {
    "NEO4J_AUTH": "neo4j/benchmark123",
    "NEO4J_server_memory_heap_initial__size": HEAP,
    "NEO4J_server_memory_heap_max__size": HEAP,
}

MEMGRAPH_ARGS = ["--storage-snapshot-on-exit=true", "--data-recovery-on-startup=true"]
MEMGRAPH_RUN = ["--user", "root"]  # a bind-mounted data dir is root-owned inside the container


def _specs():
    neo4j_image = os.environ.get("NEO4J_IMAGE", "neo4j:2026.09.0-community")
    memgraph_image = os.environ.get("MEMGRAPH_IMAGE", "memgraph/memgraph-mage:latest")
    # ArangoDB is pinned to 3.11.14 on purpose: the Graphalytics driver runs PageRank, WCC, SSSP and CDLP
    # through Pregel, which 3.12 and later no longer provide (only BFS works there). 3.11.14 is the newest
    # release that can run the algorithms.
    arango_image = os.environ.get("ARANGODB_IMAGE", "arangodb/arangodb:3.11.14")
    falkor_image = os.environ.get("FALKORDB_IMAGE", "falkordb/falkordb:latest")
    pg_image = os.environ.get("POSTGRES_IMAGE", "postgres:18")
    arcade_image = os.environ.get("ARCADEDB_IMAGE", "arcadedata/arcadedb:26.10.1")
    return {
        ("graphalytics", "neo4j"): Spec(
            "neo4j-gds", neo4j_image, ["7688:7687", "7476:7474"],
            {**NEO4J_ENV, "NEO4J_PLUGINS": '["graph-data-science"]'},
            [("data", "/data"), ("plugins", "/plugins")], ready_port=7688, settle=5),
        ("graphalytics", "memgraph"): Spec(
            "memgraph", memgraph_image, ["7687:7687"], {},
            [("data", "/var/lib/memgraph")], args=MEMGRAPH_ARGS, ready_port=7687, settle=5,
            stop_timeout=600, run_args=MEMGRAPH_RUN,  # snapshot on exit
            ready_timeout=1800, ready_probe=bolt_ready, max_map_count=1048576),  # recovering the 21 GB snapshot takes ~10 minutes
        ("graphalytics", "arangodb"): Spec(
            "arangodb", arango_image, ["8529:8529"], {"ARANGO_ROOT_PASSWORD": "benchmark"},
            [("data", "/var/lib/arangodb3")], ready_port=8529, settle=10),
        ("graphalytics", "falkordb"): Spec(
            "falkordb", falkor_image, ["6379:6379"], {},
            [("data", "/var/lib/falkordb/data")], ready_port=6379, settle=5,
            stop_timeout=1800, ready_timeout=1800, ready_probe=redis_ready),  # the snapshot of the 97M edge graph is written on shutdown; a kill loses it
        ("lsqb", "neo4j"): Spec(
            "neo4j-lsqb", neo4j_image, ["7688:7687", "7474:7474"], NEO4J_ENV,
            [("data", "/data")], ready_port=7688, settle=15),
        ("lsqb", "memgraph"): Spec(
            "memgraph-lsqb", memgraph_image, ["7689:7687"], {},
            [("data", "/var/lib/memgraph")], args=MEMGRAPH_ARGS, ready_port=7689, settle=5,
            stop_timeout=600, run_args=MEMGRAPH_RUN, ready_timeout=1800, ready_probe=bolt_ready, max_map_count=1048576),
        ("lsqb", "postgresql"): Spec(
            "postgres-lsqb", pg_image, ["5433:5432"], {"POSTGRES_PASSWORD": "benchmark"},
            [("data", "/var/lib/postgresql")], ready_port=5433, settle=8,
            ready_probe=postgres_ready),
        ("lsqb", "falkordb"): Spec(
            "falkordb-lsqb", falkor_image, ["6379:6379"], {},
            [("data", "/var/lib/falkordb/data")], ready_port=6379, settle=5,
            ready_timeout=1800, ready_probe=redis_ready),
        ("lsqb", "arcadedb"): Spec(
            "arcadedb-lsqb", arcade_image, ["2480:2480"],
            {"JAVA_OPTS": "-Darcadedb.server.rootPassword=benchmark "
                          "-Darcadedb.server.httpBodyContentMaxSize=4294967296",
             "ARCADEDB_OPTS_MEMORY": f"-Xms{HEAP} -Xmx{HEAP}"},
            [("data", "/home/arcadedb/databases")], ready_port=2480, settle=10),
    }


def spec_for(suite, key):
    # Non-default Graphalytics datasets use suite names like "graphalytics-graph500-22-w" (their own
    # data directories); the container specs are the same for every dataset.
    return _specs().get((suite.split("-", 1)[0], key))


# ------------------------------------------------------- HugeGraph (Vermeer)
# Two containers on a private network; the worker must be assigned to the "$"
# pool. Vermeer loads the graph from the dataset files every time (about 15 s),
# so there is no persistent data to keep.

def hugegraph_start(datasets_dir, log=print):
    import json
    import urllib.request
    image = os.environ.get("VERMEER_IMAGE", "hugegraph/vermeer:latest")
    _docker("network", "create", "hugegraph-net")
    _docker("rm", "-f", "vermeer-master", "vermeer-worker")
    for cmd in (
        ["run", "-d", "--name", "vermeer-master", "--network", "hugegraph-net",
         "-p", "6688:6688", "-p", "6689:6689", image, "--env=master"],
        ["run", "-d", "--name", "vermeer-worker", "--network", "hugegraph-net",
         "-p", "6788:6788", "-p", "6789:6789", "-v", f"{datasets_dir}:/data/graphs:ro",
         image, "--env=worker", "--master_peer=vermeer-master:6689"],
    ):
        r = _docker(*cmd)
        if r.returncode != 0:
            raise RuntimeError(f"docker {' '.join(cmd[:3])} failed: {r.stderr.strip()[:300]}")
    if not wait_port(6688, 120):
        raise RuntimeError("vermeer-master did not open port 6688")
    worker = None
    for _ in range(60):
        try:
            with urllib.request.urlopen("http://localhost:6688/api/v1/workers", timeout=5) as resp:
                workers = json.load(resp).get("workers", [])
            if workers:
                worker = workers[0]["name"]
                break
        except Exception:
            pass
        time.sleep(2)
    if not worker:
        raise RuntimeError("no vermeer worker registered")
    req = urllib.request.Request(f"http://localhost:6688/api/v1/admin/workers/group/$/{worker}", method="POST")
    urllib.request.urlopen(req, timeout=10).read()
    log(f"  Vermeer worker {worker} assigned to the $ pool")


def hugegraph_stop(log=print):
    _docker("rm", "-f", "vermeer-master", "vermeer-worker")
    _docker("network", "rm", "hugegraph-net")
    log("  Removed vermeer-master, vermeer-worker and network hugegraph-net")
