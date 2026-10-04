"""Power source detection: timings taken on battery are throttled and must be flagged."""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

import bench_state  # noqa: E402


class PowerSourceTests(unittest.TestCase):
    def source_for(self, stdout):
        completed = mock.Mock(stdout=stdout)
        with mock.patch("subprocess.run", return_value=completed):
            return bench_state.power_source()

    def test_ac_power(self):
        self.assertEqual(self.source_for("Now drawing from 'AC Power'\n -InternalBattery-0 100%; charged"), "ac")

    def test_battery_power(self):
        self.assertEqual(self.source_for("Now drawing from 'Battery Power'\n -InternalBattery-0 64%; discharging"), "battery")

    def test_unknown_when_pmset_is_missing_or_unrecognised(self):
        self.assertEqual(self.source_for("something else"), "unknown")
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            self.assertEqual(bench_state.power_source(), "unknown")


if __name__ == "__main__":
    unittest.main()
