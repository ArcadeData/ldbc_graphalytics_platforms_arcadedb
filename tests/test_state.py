"""reset_vendor_data deletes only the directories of the named vendor."""
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
import bench_state  # noqa: E402


class ResetVendorDataTest(unittest.TestCase):
    def test_reset_keeps_other_vendors_with_the_same_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            names = ["graphalytics-arcadedb-embedded", "graphalytics-arcadedb-native-embedded", "graphalytics-arcadedb-java",
                     "graphalytics-graph500-22-w-arcadedb-embedded", "graphalytics-neo4j-0dfcadbd51e1",
                     "graphalytics-neo4j-gds-0dfcadbd51e1", "lsqb-arcadedb-886731372b6a"]
            for n in names:
                os.makedirs(os.path.join(root, n))
            with mock.patch.object(bench_state, "state_path", side_effect=lambda *a, **k: root if a == ("data",) else os.path.join(root, *a)):
                bench_state.reset_vendor_data("graphalytics", "arcadedb")
                self.assertEqual(sorted(os.listdir(root)), sorted(n for n in names if n != "graphalytics-arcadedb-embedded"))
                bench_state.reset_vendor_data("graphalytics", "neo4j")
                self.assertNotIn("graphalytics-neo4j-0dfcadbd51e1", os.listdir(root))
                self.assertIn("graphalytics-neo4j-gds-0dfcadbd51e1", os.listdir(root))
                bench_state.reset_vendor_data("lsqb", "arcadedb")
                self.assertNotIn("lsqb-arcadedb-886731372b6a", os.listdir(root))
                self.assertIn("graphalytics-arcadedb-native-embedded", os.listdir(root))


if __name__ == "__main__":
    unittest.main()
