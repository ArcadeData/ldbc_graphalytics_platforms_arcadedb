#!/usr/bin/env python3
"""Collect the wall-clock and engine-reported compute times of vendor logs (ldbc-native/benchmark.py output).

  python3 scripts/collect_results.py weekly-results/20261007-server-time/dg-kuzu.log ...

Prints one line per log: algorithm -> server-reported seconds / wall-clock seconds (or timeout / failed / N/A).
"""
import re
import sys

ALGOS = ["PR", "WCC", "BFS", "LCC", "SSSP", "CDLP"]
NAMES = {"pagerank": "PR", "pr": "PR", "wcc": "WCC", "bfs": "BFS", "lcc": "LCC", "sssp": "SSSP", "cdlp": "CDLP"}


def parse(path):
    wall, server, status = {}, {}, {}
    for line in open(path, errors="replace"):
        m = re.match(r"^\s+(PageRank|pagerank|WCC|wcc|BFS|bfs|LCC|lcc|SSSP|sssp|CDLP|cdlp)\b.*? time: ([0-9.]+)s", line)
        if m:
            wall[NAMES[m.group(1).lower()]] = float(m.group(2))
        m = re.match(r"^\s+\[server-time\] \S+ (\w+): ([0-9.]+)s", line)
        if m:
            server[NAMES.get(m.group(1).lower(), m.group(1))] = float(m.group(2))
        m = re.match(r"^(PAGERANK|PageRank|WCC|BFS|LCC|SSSP|CDLP)\s+(timeout|N/A|OOM)", line)
        if m:
            status[NAMES[m.group(1).lower()]] = m.group(2)
        m = re.match(r"^\s+(PageRank|WCC|BFS|LCC|SSSP|CDLP)\b.* failed", line)
        if m:
            status.setdefault(NAMES[m.group(1).lower()], "failed")
    mem = re.search(r"Memory [A-Za-z0-9-]+: peak ([0-9.]+) GiB", open(path, errors="replace").read())
    return wall, server, status, (float(mem.group(1)) if mem else None)


if __name__ == "__main__":
    for path in sys.argv[1:]:
        wall, server, status, mem = parse(path)
        cells = []
        for a in ALGOS:
            if a in wall or a in server:
                cells.append(f"{a} srv={server.get(a, '-')} wall={wall.get(a, '-')}")
            elif a in status:
                cells.append(f"{a} {status[a]}")
        print(path.split("weekly-results/")[-1], "| mem", mem, "|", "; ".join(cells))
