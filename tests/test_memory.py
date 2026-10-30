"""JVM probe: parsing of jcmd output and the footprint arithmetic (outputs captured from the ArcadeDB and Neo4j images)."""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

import bench_memory  # noqa: E402

HISTOGRAM = """6:
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        215871       14325112  [B (java.base@25.0.4.1)
6828:             1             16  sun.util.resources.cldr.provider.CLDRLocaleDataMetaInfo (jdk.localedata@25.0.4.1)
Total        763265       51914880
"""

NMT = """6:

Native Memory Tracking:

(Omitting categories weighting less than 1KB)

Total: reserved=2806784KB, committed=1310824KB
       malloc: 23509KB #100167, peak=36740KB #76933
-                 Java Heap (reserved=1048576KB, committed=1048576KB)
                            (mmap: reserved=1048576KB, committed=1048576KB, at peak)
-                     Class (reserved=1049787KB, committed=13755KB)
"""


class ParseTests(unittest.TestCase):
    def test_histogram_total_is_the_live_heap(self):
        self.assertAlmostEqual(bench_memory.parse_histogram_total(HISTOGRAM), 51914880 / 1024 ** 2)

    def test_histogram_without_total(self):
        self.assertIsNone(bench_memory.parse_histogram_total("6:\nAttachNotSupportedException"))

    def test_nmt_total_and_heap(self):
        total, heap = bench_memory.parse_nmt(NMT)
        self.assertAlmostEqual(total, 1310824 / 1024)
        self.assertAlmostEqual(heap, 1024.0)

    def test_nmt_disabled(self):
        self.assertEqual(bench_memory.parse_nmt("Native memory tracking is not enabled"), (None, None))


class ProbeTests(unittest.TestCase):
    def run_probe(self, outputs, stats="1.404GiB / 31.29GiB"):
        def fake_exec(container, script, user=None, timeout=300):
            for needle, out in outputs.items():
                if needle in script:
                    return out
            return ""
        with mock.patch.object(bench_memory, "_exec", side_effect=fake_exec), \
                mock.patch("subprocess.run", return_value=mock.Mock(stdout=stats)):
            return bench_memory.jvm_probe("neo4j-gds")

    def test_footprints_remove_only_the_unused_heap(self):
        p = self.run_probe({"jcmd -l": "6\n", "stat": "neo4j\n", "class_histogram": HISTOGRAM, "native_memory": NMT})
        live = 51914880 / 1024 ** 2
        self.assertAlmostEqual(p["live_heap_mb"], round(live, 1))
        self.assertAlmostEqual(p["jvm_native_mb"], round(1310824 / 1024 - 1024, 1))
        self.assertAlmostEqual(p["jvm_footprint_mb"], round(live + 1310824 / 1024 - 1024, 1))
        self.assertAlmostEqual(p["container_footprint_mb"], round(1.404 * 1024 - (1024 - live), 1))

    def test_no_jvm_in_the_container(self):
        self.assertIsNone(self.run_probe({"jcmd -l": ""}))

    def test_flags_only_when_enabled(self):
        with mock.patch.dict(os.environ, {"BENCH_JVM_PROBE": "1"}):
            self.assertEqual(bench_memory.jvm_probe_flags(), ["-XX:NativeMemoryTracking=summary"])
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(bench_memory.jvm_probe_flags(), [])


if __name__ == "__main__":
    unittest.main()
