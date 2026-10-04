"""Orchestrator tests with fake vendors (no real database involved).

Run: python3 -m unittest discover -s tests -v
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.path.join(HERE, "fake_suite.py")


def run_suite(vendors, vendor_timeout=8, idle_timeout=3, grace=2, extra=()):
    state = tempfile.mkdtemp(prefix="bench-state-")
    out = os.path.join(state, "out.json")
    env = dict(os.environ, LDBC_BENCH_STATE=state)
    t0 = time.monotonic()
    p = subprocess.run(
        [sys.executable, SUITE, "--vendor-timeout", str(vendor_timeout),
         "--idle-timeout", str(idle_timeout), "--kill-grace", str(grace),
         "--out", out, *extra, *vendors],
        env=env, capture_output=True, text=True, timeout=120)
    elapsed = time.monotonic() - t0
    with open(out) as f:
        report = json.load(f)
    return report, p.stdout + p.stderr, elapsed


def alive(pattern):
    return subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode == 0


class IsolationTests(unittest.TestCase):
    def test_ok_vendor_returns_result(self):
        r, out, _ = run_suite(["ok"])
        v = r["vendors"]["ok"]
        self.assertEqual(v["status"], "ok")
        self.assertEqual(v["result"]["pagerank"], 0.25)
        self.assertEqual(v["result"]["load"], 12.5)

    def test_crash_is_reported_and_suite_continues(self):
        r, out, _ = run_suite(["crash", "ok"])
        self.assertIn("boom", r["vendors"]["crash"]["status"])
        self.assertEqual(r["vendors"]["ok"]["status"], "ok")

    def test_silent_hang_ignoring_sigterm_is_sigkilled(self):
        r, out, elapsed = run_suite(["hangterm", "ok"], idle_timeout=2, grace=2)
        v = r["vendors"]["hangterm"]
        self.assertIn("idle limit", v["status"])
        self.assertIn("killed", v["status"])          # SIGKILL escalation, not just SIGTERM
        self.assertIn("SIGKILL", out)
        self.assertEqual(r["vendors"]["ok"]["status"], "ok")
        self.assertLess(elapsed, 30)                  # the suite did not stall

    def test_blocked_alarm_keeps_partial_results(self):
        r, out, _ = run_suite(["hangop"], idle_timeout=3)
        res = r["vendors"]["hangop"]["result"]
        self.assertIsInstance(res["wcc"], float)       # finished before the hang
        self.assertEqual(res["pagerank"], "timeout")   # the operation in flight
        self.assertEqual(res["bfs"], "N/A")            # never started
        self.assertIn("partial results kept", r["vendors"]["hangop"]["status"])

    def test_hang_during_load_marks_load_timeout(self):
        r, out, _ = run_suite(["hangload"], idle_timeout=2)
        res = r["vendors"]["hangload"]["result"]
        self.assertEqual(res["load"], "timeout")
        self.assertEqual(res["pagerank"], "N/A")

    def test_total_limit_applies_even_when_chatty(self):
        r, out, elapsed = run_suite(["chatty"], vendor_timeout=3, idle_timeout=60)
        self.assertIn("total limit", r["vendors"]["chatty"]["status"])
        self.assertLess(elapsed, 30)

    def test_lingering_thread_does_not_hang_the_suite(self):
        r, out, elapsed = run_suite(["linger"])
        v = r["vendors"]["linger"]
        self.assertEqual(v["status"], "ok")
        self.assertEqual(v["result"]["load"], 2.0)
        self.assertLess(elapsed, 15)

    def test_grandchild_is_killed_with_the_group(self):
        r, out, _ = run_suite(["grandchild"], idle_timeout=2, grace=1)
        self.assertEqual(r["vendors"]["grandchild"]["result"]["load"], 3.0)
        time.sleep(1)
        self.assertFalse(alive("signal.SIG_IGN\\);time.sleep\\(10000\\)"))

    def test_load_time_is_cached_and_data_reused(self):
        state = tempfile.mkdtemp(prefix="bench-state-")
        env = dict(os.environ, LDBC_BENCH_STATE=state)
        base = [sys.executable, SUITE, "--vendor-timeout", "20", "--idle-timeout", "10"]
        out1, out2, out3 = (os.path.join(state, f"{i}.json") for i in (1, 2, 3))
        subprocess.run([*base, "--out", out1, "loadonce"], env=env, capture_output=True, timeout=60)
        subprocess.run([*base, "--out", out2, "loadonce"], env=env, capture_output=True, timeout=60)
        r1 = json.load(open(out1))["vendors"]["loadonce"]["result"]
        r2 = json.load(open(out2))["vendors"]["loadonce"]["result"]
        self.assertEqual(r1["load"], 5.0)
        self.assertNotIn("_load_cached_from", r1)
        self.assertEqual(r2["load"], 5.0)                 # reused data reports the original load time
        self.assertIn("_load_cached_from", r2)
        # --reset wipes the data: the vendor must load again (fresh load, no cache note)
        subprocess.run([*base, "--reset", "--out", out3, "loadonce"], env=env, capture_output=True, timeout=60)
        r3 = json.load(open(out3))["vendors"]["loadonce"]["result"]
        self.assertEqual(r3["load"], 5.0)
        self.assertNotIn("_load_cached_from", r3)

    def test_half_loaded_data_is_wiped_after_a_kill(self):
        state = tempfile.mkdtemp(prefix="bench-state-")
        env = dict(os.environ, LDBC_BENCH_STATE=state)
        out = os.path.join(state, "o.json")
        subprocess.run([sys.executable, SUITE, "--idle-timeout", "2", "--kill-grace", "1",
                        "--out", out, "partialload"], env=env, capture_output=True, timeout=60)
        self.assertFalse(os.path.exists(os.path.join(state, "data", "fake-partialload-embedded")))


class SpecLookupTests(unittest.TestCase):
    def test_dataset_suffixed_suite_finds_the_container_spec(self):
        sys.path.insert(0, os.path.join(HERE, "..", "shared"))
        import bench_containers
        for suite in ("graphalytics", "graphalytics-graph500-22-w"):
            for key in ("neo4j", "memgraph", "arangodb", "falkordb"):
                self.assertIsNotNone(bench_containers.spec_for(suite, key), (suite, key))
        self.assertIsNone(bench_containers.spec_for("graphalytics-graph500-22-w", "kuzu"))
        self.assertIsNotNone(bench_containers.spec_for("lsqb", "postgresql"))


if __name__ == "__main__":
    unittest.main()
