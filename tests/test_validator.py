"""Unit tests for scripts/validate_outputs.py (tiny made-up reference/output files)."""

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import validate_outputs as v  # noqa: E402


def write(lines):
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt")
    f.write("\n".join(lines) + "\n")
    f.close()
    return f.name


class ValidatorTests(unittest.TestCase):
    def check(self, algo, ref, out, swap=None):
        return v.validate(algo, write(ref), write(out), swap)

    def test_pagerank_within_relative_epsilon(self):
        r = self.check("PR", ["1 0.5", "2 0.25"], ["1 0.50004", "2 0.25"])
        self.assertTrue(r["valid"])

    def test_pagerank_outside_epsilon(self):
        r = self.check("PR", ["1 0.5", "2 0.25"], ["1 0.5002", "2 0.25"])
        self.assertFalse(r["valid"])
        self.assertEqual(r["mismatches"], 1)

    def test_bfs_exact_and_unreachable(self):
        ref = ["1 0", "2 3", "3 9223372036854775807"]
        self.assertTrue(self.check("BFS", ref, ["1 0", "2 3", "3 9223372036854775807"])["valid"])
        self.assertFalse(self.check("BFS", ref, ["1 0", "2 4", "3 9223372036854775807"])["valid"])
        self.assertFalse(self.check("BFS", ref, ["1 0", "2 3", "3 -1"])["valid"])

    def test_wcc_is_partition_equivalence(self):
        ref = ["1 1", "2 1", "3 3", "4 3"]
        self.assertTrue(self.check("WCC", ref, ["1 7", "2 7", "3 9", "4 9"])["valid"])          # relabelled
        self.assertFalse(self.check("WCC", ref, ["1 7", "2 7", "3 7", "4 7"])["valid"])        # merged
        self.assertFalse(self.check("WCC", ref, ["1 7", "2 8", "3 9", "4 9"])["valid"])        # split

    def test_missing_vertex_is_invalid(self):
        r = self.check("CDLP", ["1 5", "2 5"], ["1 5"])
        self.assertFalse(r["valid"])
        self.assertEqual(r["missing"], 1)

    def test_lcc_zero_reference(self):
        self.assertTrue(self.check("LCC", ["1 0.0", "2 0.5"], ["1 0", "2 0.5"])["valid"])
        self.assertFalse(self.check("LCC", ["1 0.0", "2 0.5"], ["1 0.1", "2 0.5"])["valid"])

    def test_bfs_reachable_set_only(self):
        ref = ["1 0", "2 3", "3 9223372036854775807"]
        self.assertTrue(self.check("BFSREACH", ref, ["1 1", "2 1"])["valid"])
        self.assertFalse(self.check("BFSREACH", ref, ["1 1"])["valid"])           # reachable vertex not visited
        self.assertFalse(self.check("BFSREACH", ref, ["1 1", "2 1", "3 1"])["valid"])  # unreachable vertex visited

    def test_swap_maps_ids_back(self):
        ref = ["6 2", "248533 4", "7 1"]
        out = ["248533 2", "6 4", "7 1"]   # ids 6 and 248533 swapped in the derived dataset
        self.assertFalse(self.check("BFS", ref, out)["valid"])
        self.assertTrue(self.check("BFS", ref, out, swap={6: 248533, 248533: 6})["valid"])


class VendorValidationTests(unittest.TestCase):
    def setUp(self):
        self.datasets = tempfile.mkdtemp()
        self.dump = tempfile.mkdtemp()

    def reference(self, graph, algo, lines):
        d = os.path.join(self.datasets, graph)
        os.makedirs(d, exist_ok=True)
        open(os.path.join(d, f"{graph}-{algo}"), "w").write("\n".join(lines) + "\n")

    def dumped(self, vendor, algo, lines):
        open(os.path.join(self.dump, f"{vendor}-{algo}.out"), "w").write("\n".join(lines) + "\n")

    def test_verdict_per_metric(self):
        self.reference("g", "PR", ["1 0.5", "2 0.5"])
        self.reference("g", "WCC", ["1 1", "2 1"])
        self.dumped("acme", "PR", ["1 0.5", "2 0.5"])
        self.dumped("acme", "WCC", ["1 1", "2 2"])           # wrong partition
        r = v.validate_vendor(self.dump, "acme", "g", self.datasets)
        self.assertEqual(r["pagerank"]["verdict"], "valid")
        self.assertEqual(r["wcc"]["verdict"], "invalid")
        self.assertNotIn("lcc", r)                            # nothing dumped: no verdict (shown as unchecked)

    def test_cdlp_renamed_labels_are_equivalent_not_valid(self):
        self.reference("g", "CDLP", ["1 1", "2 1", "3 3"])
        self.dumped("acme", "CDLP", ["1 7", "2 7", "3 9"])                  # same communities, other names
        self.assertEqual(v.validate_vendor(self.dump, "acme", "g", self.datasets)["cdlp"]["verdict"], "equivalent")
        self.dumped("other", "CDLP", ["1 7", "2 8", "3 9"])                 # different communities
        self.assertEqual(v.validate_vendor(self.dump, "other", "g", self.datasets)["cdlp"]["verdict"], "invalid")

    def test_bfs_falls_back_to_reachability(self):
        self.reference("g", "BFS", ["1 0", "2 1", "3 9223372036854775807"])
        self.dumped("acme", "BFSREACH", ["1 1", "2 1"])
        self.assertEqual(v.validate_vendor(self.dump, "acme", "g", self.datasets)["bfs"]["verdict"], "valid")

    def test_derived_dataset_uses_official_reference_with_id_swap(self):
        self.reference("graph500-22", "BFS", ["6 2", "248533 0"])      # official ids
        self.dumped("acme", "BFS", ["248533 2", "6 0"])                # derived dataset swaps the ids
        r = v.validate_vendor(self.dump, "acme", "graph500-22-w", self.datasets)
        self.assertEqual(r["bfs"]["verdict"], "valid")


class LsqbCountTests(unittest.TestCase):
    def test_counts_are_checked_against_expected(self):
        sys.path.insert(0, os.path.join(HERE, "..", "shared"))
        import bench_common
        log = ("  Q1 time: 0.32s  (count=221636419)\n  Q2 time: 0.20s  (count=1085627)\n"
               "  Q4 time: 0.10s  (count=14836000)\n")
        r = bench_common.validate_lsqb_counts(log)
        self.assertEqual(r["q1"]["verdict"], "valid")
        self.assertEqual(r["q2"]["verdict"], "valid")
        self.assertEqual(r["q4"]["verdict"], "invalid")
        self.assertIsNone(bench_common.validate_lsqb_counts(log, "3"))   # no expected counts for other SF


if __name__ == "__main__":
    unittest.main()
