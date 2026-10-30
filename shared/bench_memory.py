"""Memory sampling of a benchmarked vendor, run by the orchestrator (the parent of the vendor's child process).

Every second the sampler records
  - the resident memory (RSS) of the vendor's whole process group (embedded engines, JVMs, the Python client), and
  - the memory of the vendor's Docker containers (`docker stats`, the working set: usage minus inactive file cache).
Together with the per-operation time windows that `bench_common.run_timed*` writes into the partial-results file, this
gives the memory during each timed operation and the resident memory before the first one.

What the numbers mean (be careful comparing them):
  - Docker vendors: container working set. Process-group RSS is only the small Python client.
  - Embedded C++ engines (Kuzu, DuckDB, LadybugDB): process RSS; they size their buffer pools from the machine RAM by
    default, so the value follows that cap more than the need of the algorithm.
  - JVM engines (ArcadeDB, Neo4j): RSS / container memory is dominated by the fixed 12 GB heap the JVM chose to grow
    into; the embedded ArcadeDB benchmark additionally prints the live heap after a full GC.
"""

import os
import re
import subprocess
import threading
import time

_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_UNITS = {"b": 1 / 1024 ** 2, "kb": 1 / 1024, "kib": 1 / 1024, "mb": 1.0, "mib": 1.0, "gb": 1024.0, "gib": 1024.0}


def _to_mb(text):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not m:
        return None
    unit = _UNITS.get(m.group(2).lower())
    return float(m.group(1)) * unit if unit is not None else None


class MemorySampler:
    def __init__(self, containers=(), interval=1.0):
        self.containers = list(containers)
        self.interval = interval
        self.samples = []          # (epoch, group rss MB, summed container MB or None)
        self._latest = {}          # container -> MB
        self._pgid = None
        self._stop = threading.Event()
        self._thread = None
        self._stats = None

    # ------------------------------------------------------------ lifecycle
    def attach(self, proc):
        """Called with the vendor's child process right after it started (it leads its own process group)."""
        self._pgid = proc.pid
        if self._thread is None:
            self._start_docker_stats()
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._stats is not None:
            try:
                self._stats.terminate()
            except OSError:
                pass

    # ------------------------------------------------------------ sampling
    def _start_docker_stats(self):
        if not self.containers:
            return
        try:
            # no container names: `docker stats` then also reports containers that start later (the ArcadeDB and
            # Vermeer drivers start theirs after the child began); the reader keeps only ours
            self._stats = subprocess.Popen(
                ["docker", "stats", "--format", "{{.Name}}|{{.MemUsage}}"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
        except OSError:
            self._stats = None
            return
        threading.Thread(target=self._read_stats, daemon=True).start()

    def _read_stats(self):
        for line in self._stats.stdout:
            line = _ANSI.sub("", line).strip()
            if "|" not in line:
                continue
            name, usage = line.split("|", 1)
            mb = _to_mb(usage.split("/")[0])
            if mb is not None and name.strip() in self.containers:
                self._latest[name.strip()] = mb

    def _group_rss_mb(self):
        try:
            out = subprocess.run(["ps", "-axo", "pgid=,rss="], capture_output=True, text=True, timeout=10).stdout
        except (OSError, subprocess.SubprocessError):
            return None
        total = 0
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == str(self._pgid):
                total += int(parts[1])
        return total / 1024.0

    def _loop(self):
        while not self._stop.is_set():
            containers = sum(self._latest.values()) if self._latest else None
            self.samples.append((time.time(), self._group_rss_mb(), containers))
            self._stop.wait(self.interval)

    # ------------------------------------------------------------ summary
    def summarize(self, ops):
        """ops: {name: [start_epoch, end_epoch]} from the partial-results file. Returns a JSON-able dict or None."""
        if not self.samples:
            return None
        basis = "container" if self.containers and any(s[2] is not None for s in self.samples) else "process"

        def value(s):
            return s[2] if basis == "container" else s[1]

        def peak(lo, hi):
            vals = [value(s) for s in self.samples if lo <= s[0] <= hi and value(s) is not None]
            return max(vals) if vals else None

        out = {"basis": basis, "samples": len(self.samples)}
        windows = {k: v for k, v in (ops or {}).items() if isinstance(v, (list, tuple)) and len(v) == 2}
        if windows:
            first = min(v[0] for v in windows.values())
            last = max(v[1] for v in windows.values())
            before = [value(s) for s in self.samples if s[0] <= first and value(s) is not None]
            out["resident_before_mb"] = round(before[-1], 1) if before else None
            p = peak(first, last)
            out["peak_mb"] = round(p, 1) if p is not None else None
            out["per_op_mb"] = {k: round(peak(v[0], v[1]), 1) for k, v in windows.items()
                                if peak(v[0], v[1]) is not None}
        allv = [value(s) for s in self.samples if value(s) is not None]
        out["peak_overall_mb"] = round(max(allv), 1) if allv else None
        if basis == "container":
            rss = [s[1] for s in self.samples if s[1] is not None]
            out["client_rss_peak_mb"] = round(max(rss), 1) if rss else None
        return out


# ------------------------------------------------------------ JVM probe
#
# BENCH_JVM_PROBE=1 looks inside the JVM of the Docker vendors that run one (ArcadeDB, Neo4j), so their memory is not just the fixed
# 12 GB heap. The child runs the probe through `docker exec` once before the first timed operation and once after the timed runs of
# every operation (outside the timed window; the next operation's warm-up call absorbs the pause):
#   - live heap: `jcmd GC.class_histogram` forces a full GC on any collector and totals the live objects. `GC.run` is not used because
#     Neo4j starts with -XX:+DisableExplicitGC, which turns it into a no-op;
#   - JVM native memory (`jcmd VM.native_memory summary`, needs -XX:NativeMemoryTracking=summary, see jvm_probe_flags()): everything
#     the JVM accounts for outside the heap (metaspace, threads, code, GC structures, direct and Unsafe buffers);
#   - the container working set at that moment (`docker stats`).
# Two footprints follow. jvm_footprint = live heap + JVM native memory: what the JVM itself accounts for. container_footprint =
# container working set - (committed heap - live heap): the measured container with only the unused part of the fixed heap removed,
# which also counts native memory NMT does not see (Neo4j's page cache, native libraries). Both leave out the headroom a collector
# needs above the live set, so they are the floor of what the engine needs, not a recommended heap size.

JVM_PROBE_ENV = "BENCH_JVM_PROBE"
JVM_CONTAINER_ENV = "BENCH_JVM_CONTAINER"
JVM_VENDORS = ("arcadedb", "neo4j")

_FIND_JVM = "jcmd -l | grep -v JCmd | head -1 | cut -d' ' -f1"


def jvm_probe_enabled():
    return os.environ.get(JVM_PROBE_ENV) == "1"


def jvm_probe_flags():
    """JVM flags the probed containers need (empty when the probe is off)."""
    return ["-XX:NativeMemoryTracking=summary"] if jvm_probe_enabled() else []


def parse_histogram_total(text):
    """Live bytes from the `Total <instances> <bytes>` line of GC.class_histogram, in MB."""
    m = re.search(r"^Total\s+\d+\s+(\d+)\s*$", text, re.M)
    return int(m.group(1)) / 1024 ** 2 if m else None


def parse_nmt(text):
    """(total committed MB, Java heap committed MB) from VM.native_memory summary."""
    total = re.search(r"^Total:\s+reserved=\d+KB,\s+committed=(\d+)KB", text, re.M)
    heap = re.search(r"Java Heap \(reserved=\d+KB,\s+committed=(\d+)KB", text)
    return (int(total.group(1)) / 1024 if total else None, int(heap.group(1)) / 1024 if heap else None)


def _exec(container, script, user=None, timeout=300):
    cmd = ["docker", "exec", *(["-u", user] if user else []), container, "sh", "-c", script]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout


def jvm_probe(container):
    """One probe of the JVM inside `container` (see above). Returns a dict of MB values, or None when it cannot attach."""
    try:
        pid = _exec(container, _FIND_JVM, timeout=30).strip()
        if not pid.isdigit():
            return None
        user = _exec(container, f"stat -c %U /proc/{pid}", timeout=30).strip() or None   # jcmd attaches as the JVM's own user
        t0 = time.time()
        live = parse_histogram_total(_exec(container, f"jcmd {pid} GC.class_histogram", user))
        gc_seconds = time.time() - t0
        nmt_total, heap_committed = parse_nmt(_exec(container, f"jcmd {pid} VM.native_memory summary", user))
        stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container],
                               capture_output=True, text=True, timeout=60).stdout
        container_mb = _to_mb(_ANSI.sub("", stats).split("/")[0]) if stats.strip() else None
    except (OSError, subprocess.SubprocessError):
        return None
    if live is None:
        return None
    out = {"live_heap_mb": round(live, 1), "probe_gc_s": round(gc_seconds, 2)}
    if heap_committed is not None:
        out["heap_committed_mb"] = round(heap_committed, 1)
    if nmt_total is not None and heap_committed is not None:
        out["jvm_native_mb"] = round(nmt_total - heap_committed, 1)
        out["jvm_footprint_mb"] = round(live + nmt_total - heap_committed, 1)
    if container_mb is not None:
        out["container_mb"] = round(container_mb, 1)
        if heap_committed is not None:
            out["container_footprint_mb"] = round(container_mb - (heap_committed - live), 1)
    return out


def describe_jvm(name, probes):
    """One log line per probe: live heap, the two footprints and the container working set."""
    if not probes:
        return []
    gib = lambda mb: f"{mb / 1024:.2f} GiB" if mb is not None else "n/a"  # noqa: E731
    lines = [f"  JVM memory {name} (live heap after a full GC; footprints exclude the unused part of the fixed heap):"]
    for op, p in probes.items():
        lines.append(f"    {op:10} live heap {gib(p.get('live_heap_mb'))}, JVM footprint {gib(p.get('jvm_footprint_mb'))}, "
                     f"container footprint {gib(p.get('container_footprint_mb'))} (container {gib(p.get('container_mb'))})")
    return lines


def describe(name, memory):
    """One log line, also parsed by nobody: for the README tables."""
    if not memory:
        return None
    gib = lambda mb: f"{mb / 1024:.1f} GiB" if mb is not None else "n/a"  # noqa: E731
    parts = [f"peak {gib(memory.get('peak_mb', memory.get('peak_overall_mb')))} during the timed operations",
             f"{gib(memory.get('resident_before_mb'))} resident before the first one ({memory['basis']})"]
    per = memory.get("per_op_mb")
    if per:
        parts.append("per operation: " + ", ".join(f"{k} {v / 1024:.1f}" for k, v in per.items()))
    return f"  Memory {name}: " + "; ".join(parts)
