import unittest

from mode1_to_bench_json import convert


class ConvertTests(unittest.TestCase):
    def test_two_algorithms_sorted_by_timestamp(self):
        data = {
            "result": {
                "jobs": {
                    "job-pr": {"algorithm": "PR", "runs": ["run-pr"]},
                    "job-wcc": {"algorithm": "WCC", "runs": ["run-wcc"]},
                },
                "runs": {
                    "run-pr": {"timestamp": "2", "load_time": "1.500", "processing_time": "4.250"},
                    "run-wcc": {"timestamp": "1", "load_time": "0.900", "processing_time": "2.100"},
                },
            }
        }
        self.assertEqual(
            convert(data),
            [
                {"name": "WCC load", "unit": "s", "value": 0.9},
                {"name": "WCC processing", "unit": "s", "value": 2.1},
                {"name": "PR load", "unit": "s", "value": 1.5},
                {"name": "PR processing", "unit": "s", "value": 4.25},
            ],
        )

    def test_skips_nan_values(self):
        data = {
            "result": {
                "jobs": {"job-bfs": {"algorithm": "BFS", "runs": ["run-bfs"]}},
                "runs": {
                    "run-bfs": {"timestamp": "1", "load_time": "nan", "processing_time": "3.0"}
                },
            }
        }
        self.assertEqual(
            convert(data),
            [{"name": "BFS processing", "unit": "s", "value": 3.0}],
        )


if __name__ == "__main__":
    unittest.main()
