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
