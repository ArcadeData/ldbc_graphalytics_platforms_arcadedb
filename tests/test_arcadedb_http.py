"""NDJSON result streams of the ArcadeDB HTTP API: rows, the stats trailer, errors after the stream began, truncation."""

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))

import arcadedb_http  # noqa: E402


def response(lines, status=200):
    r = mock.MagicMock(status_code=status, text="refused")
    r.iter_lines.return_value = [json.dumps(x).encode() if isinstance(x, dict) else x for x in lines]
    r.__enter__.return_value = r
    return r


def rows(lines, status=200):
    with mock.patch("requests.post", return_value=response(lines, status)) as post:
        out = list(arcadedb_http.stream_rows("http://h/api/v1", "bench", ("root", "x"), "MATCH (v) RETURN v.VID AS id"))
    return out, post


class StreamTests(unittest.TestCase):
    def test_rows_until_the_stats_line(self):
        out, post = rows([{"record": {"id": 1}}, b"", {"record": {"id": 2}},
                          {"stats": {"limit": -1, "returned": 2, "truncated": False}}])
        self.assertEqual(out, [{"id": 1}, {"id": 2}])
        self.assertEqual(post.call_args.kwargs["headers"], {"Accept": "application/x-ndjson"})
        self.assertTrue(post.call_args.kwargs["stream"])

    def test_a_stream_cut_before_the_stats_line_fails(self):
        with self.assertRaisesRegex(RuntimeError, "without its stats line"):
            rows([{"record": {"id": 1}}])

    def test_an_error_after_the_stream_began_fails(self):
        with self.assertRaisesRegex(RuntimeError, "OutOfMemoryError"):
            rows([{"record": {"id": 1}}, {"error": {"exception": "java.lang.OutOfMemoryError", "message": "Java heap space"}}])

    def test_a_truncated_result_fails(self):
        with self.assertRaisesRegex(RuntimeError, "truncated"):
            rows([{"record": {"id": 1}}, {"stats": {"limit": 1, "returned": 1, "truncated": True}}])

    def test_a_refused_request_fails(self):
        with self.assertRaisesRegex(RuntimeError, "HTTP 400"):
            rows([], status=400)


if __name__ == "__main__":
    unittest.main()
