#!/usr/bin/env python3
"""Fake vendors used by test_isolation.py to exercise the orchestrator."""

import os
import signal
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'shared'))

import bench_common

METRICS = ["load", "pagerank", "wcc", "bfs"]


def ok():
    print("fake ok vendor")
    return {"load": 12.5, "pagerank": 0.25, "wcc": 0.5, "bfs": 0.75}


def crash():
    print("about to crash")
    raise RuntimeError("boom")


def hang_ignoring_sigterm():
    """Silent hang that ignores SIGTERM: needs SIGKILL escalation."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    print("started, going silent")
    time.sleep(10_000)


def hang_in_op_blocked_alarm():
    """wcc completes, then pagerank hangs where SIGALRM cannot interrupt it."""
    bench_common.run_timed("wcc", lambda: time.sleep(0.2))
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})  # emulate a blocking C call
    bench_common.run_timed("pagerank", lambda: time.sleep(10_000), timeout=2)
    return {"load": 1.0}


def hang_in_load():
    print("loading ...")
    time.sleep(10_000)


def chatty_forever():
    """Keeps printing (never idle) but never finishes: total limit applies."""
    while True:
        print("working...")
        time.sleep(0.5)


def spawns_stubborn_grandchild():
    """Result is written, but a grandchild that ignores SIGTERM stays alive."""
    import subprocess
    subprocess.Popen([sys.executable, "-c",
                      "import signal,time;signal.signal(signal.SIGTERM, signal.SIG_IGN);time.sleep(10000)"])
    time.sleep(0.5)
    return {"load": 3.0, "pagerank": 1.0, "wcc": 1.0, "bfs": 1.0}


def lingering_thread():
    """Returns a result but leaves a non-daemon thread behind (client library)."""
    threading.Thread(target=lambda: time.sleep(10_000)).start()
    return {"load": 2.0, "pagerank": 0.1, "wcc": 0.1, "bfs": 0.1}


def partial_load_then_hang():
    """Creates (half-written) persisted data, then hangs in the load."""
    d = bench_common.embedded_db_path("fake", "partialload", "db")
    os.makedirs(d, exist_ok=True)
    open(os.path.join(d, "half-written"), "w").write("x")
    print("loading ...")
    time.sleep(10_000)


def load_once():
    """First run loads (5 s reported); later runs reuse the data and report load 0."""
    d = bench_common.embedded_db_path("fake", "loadonce", "db")
    if os.path.isdir(d):
        print("data already loaded, skipping import")
        return {"load": 0.0, "pagerank": 0.2, "wcc": 0.3, "bfs": 0.4}
    os.makedirs(d)
    return {"load": 5.0, "pagerank": 0.2, "wcc": 0.3, "bfs": 0.4}


SYSTEMS = {
    "partialload": ("PartialLoad", partial_load_then_hang),
    "loadonce": ("LoadOnce", load_once),
    "ok": ("Ok", ok),
    "crash": ("Crash", crash),
    "hangterm": ("HangTerm", hang_ignoring_sigterm),
    "hangop": ("HangOp", hang_in_op_blocked_alarm),
    "hangload": ("HangLoad", hang_in_load),
    "chatty": ("Chatty", chatty_forever),
    "grandchild": ("Grandchild", spawns_stubborn_grandchild),
    "linger": ("Linger", lingering_thread),
}

if __name__ == "__main__":
    bench_common.run_benchmarks(
        description="fake suite", available_systems=SYSTEMS, summary_title="fake",
        metrics=METRICS, suite="fake", dataset_paths=lambda: [])
