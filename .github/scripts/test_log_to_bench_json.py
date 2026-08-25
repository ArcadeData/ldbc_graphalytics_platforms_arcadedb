import unittest

from log_to_bench_json import convert


class ConvertTests(unittest.TestCase):
    def test_mode2_style_log(self):
        log = (
            "[ArcadeDB] Running PageRank...\n"
            "Algorithm  ArcadeDB\n"
            "----------------------\n"
            "LOAD            1.23s\n"
            "PR               4.56s\n"
            "WCC               0.78s\n"
        )
        names = ["LOAD", "PR", "WCC", "BFS", "LCC", "SSSP", "CDLP"]
        self.assertEqual(
            convert(log, names),
            [
                {"name": "LOAD", "unit": "s", "value": 1.23},
                {"name": "PR", "unit": "s", "value": 4.56},
                {"name": "WCC", "unit": "s", "value": 0.78},
            ],
        )

    def test_ignores_non_matching_lines(self):
        log = "Vertices: 12345\nLOAD              2.00s\n"
        self.assertEqual(
            convert(log, ["LOAD"]),
            [{"name": "LOAD", "unit": "s", "value": 2.0}],
        )

    def test_mode3_style_query_names(self):
        log = "Q1                0.05s\nQ2                0.11s\n"
        self.assertEqual(
            convert(log, ["LOAD"] + [f"Q{i}" for i in range(1, 10)]),
            [
                {"name": "Q1", "unit": "s", "value": 0.05},
                {"name": "Q2", "unit": "s", "value": 0.11},
            ],
        )

    def test_empty_log_returns_empty_list(self):
        self.assertEqual(convert("", ["LOAD", "PR"]), [])


if __name__ == "__main__":
    unittest.main()
