"""Row-returning ArcadeDB calls over HTTP as NDJSON (Accept: application/x-ndjson, ArcadeDB issue #7306, server 26.3.2 and later).

The buffered application/json response builds the whole result in the server heap before sending it (about 1 GB for the 2.4M-row
outputs of graph500-22); the NDJSON encoding streams one row per line and never holds more than one in memory. Every line is one JSON
object with one key: {"record": {...}} per row, {"stats": {"limit", "returned", "truncated"}} as the last line of a complete stream,
or {"error": {...}} when the stream failed after it began. A bare newline is a keep-alive. A stream without the stats line, or one the
server truncated, is an error: the rows are not the whole result.

Only read-only statements stream; PROFILE / EXPLAIN return a plan, not rows, and stay on application/json.
"""

import json

import requests

CONTENT_TYPE = "application/x-ndjson"


def stream_rows(base, db, auth, command, language="sql", timeout=600, params=None):
    """Yield the result rows (dicts) of one read-only command, streamed. base = http://host:port/api/v1."""
    body = {"language": language, "command": command, "limit": -1, "serializer": "record"}
    if params is not None:
        body["params"] = params
    with requests.post(f"{base}/command/{db}", json=body, auth=auth, timeout=timeout, stream=True,
                       headers={"Accept": CONTENT_TYPE}) as r:
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} {r.text[:300]}")
        complete = False
        for line in r.iter_lines():
            if not line:
                continue
            event = json.loads(line)
            if "record" in event:
                yield event["record"]
            elif "stats" in event:
                if event["stats"].get("truncated"):
                    raise RuntimeError(f"the server truncated the result: {event['stats']}")
                complete = True
            elif "error" in event:
                err = event["error"]
                raise RuntimeError(f"{err.get('exception', 'error')}: {err.get('message', '')} {err.get('detail', '')}".strip())
        if not complete:
            raise RuntimeError("the result stream ended without its stats line (connection cut, the rows are incomplete)")
