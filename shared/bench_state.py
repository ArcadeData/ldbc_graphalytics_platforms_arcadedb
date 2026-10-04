"""Persistent state for the benchmark suites: data directories, load markers.

Everything that must survive between runs (Docker data volumes, embedded
databases, recorded load times) lives under one root so that a weekly run can
reuse loaded data instead of reloading 34M edges for every vendor.

Root: $LDBC_BENCH_STATE, default ~/.cache/ldbc-graph-bench
"""

import hashlib
import json
import os
import shutil
import time


def state_root():
    return os.environ.get("LDBC_BENCH_STATE") or os.path.expanduser("~/.cache/ldbc-graph-bench")


def state_path(*parts, create=False):
    p = os.path.join(state_root(), *parts)
    if create:
        os.makedirs(p, exist_ok=True)
    return p


def dataset_signature(paths):
    """Cheap fingerprint of dataset files/directories: names, sizes, mtimes.

    Changing, adding or removing a file changes the signature, which
    invalidates previously loaded data.
    """
    h = hashlib.sha1()
    for p in sorted(paths):
        if os.path.isdir(p):
            entries = sorted(os.listdir(p))
            files = [os.path.join(p, e) for e in entries if os.path.isfile(os.path.join(p, e))]
        else:
            files = [p]
        for f in files:
            try:
                st = os.stat(f)
            except OSError:
                h.update(f"{f}:missing".encode())
                continue
            h.update(f"{os.path.basename(f)}:{st.st_size}:{int(st.st_mtime)}".encode())
    return h.hexdigest()[:16]


def _marker_file(suite, key):
    return state_path("markers", f"{suite}-{key}.json")


def read_marker(suite, key):
    try:
        with open(_marker_file(suite, key)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_marker(suite, key, data):
    path = _marker_file(suite, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def drop_marker(suite, key):
    try:
        os.remove(_marker_file(suite, key))
    except OSError:
        pass


def reset_vendor_data(suite, key):
    """Delete persisted data directories and the load marker of one vendor."""
    data_root = state_path("data")
    prefix = f"{suite}-{key}-"
    if os.path.isdir(data_root):
        for entry in os.listdir(data_root):
            if entry.startswith(prefix):
                shutil.rmtree(os.path.join(data_root, entry), ignore_errors=True)
    drop_marker(suite, key)


def power_source():
    """"ac", "battery" or "unknown". On battery macOS throttles the CPU, so timings taken there are not
    comparable with timings taken on AC power."""
    import subprocess
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return "unknown"
    if "AC Power" in out:
        return "ac"
    if "Battery Power" in out:
        return "battery"
    return "unknown"


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")
