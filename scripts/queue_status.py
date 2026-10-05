#!/usr/bin/env python3
"""ASCII picture of a benchmark queue and where it is now (the standard way to report a running queue).

    python3 scripts/queue_status.py [plan.json]        # default plan: ~/.cache/ldbc-graph-bench/queue-plan.json

A plan is a small JSON file written when a queue is started:

    {"title": "chain9, warm only",
     "progress": "/tmp/chain9.progress",          # one line "<step name> done HH:MM" appended after every step
     "steps": [{"name": "neo4j", "log": "/tmp/c9-neo4j.log", "minutes": 8, "what": "Graphalytics"}, ...],
     "after": [{"name": "docs+commit", "minutes": 20, "what": "README, results"}]}

Steps are shown in order: finished (with their finish time), the running one (progress bar from its expected
minutes, its last log activity) and the waiting ones, then an estimated finish time, the machine state (power source,
free memory, swap, containers) and what is running. It only reads files and `ps`; it never touches a benchmark.
"""

import datetime
import json
import os
import re
import subprocess
import sys

DEFAULT_PLAN = os.path.expanduser("~/.cache/ldbc-graph-bench/queue-plan.json")
WIDTH = 76


def run(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=20).stdout
    except Exception:  # noqa: BLE001
        return ""


def done_times(progress):
    out = {}
    try:
        for line in open(progress):
            m = re.match(r"(\S+) done (\d\d):(\d\d)", line)
            if m:
                out[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    except OSError:
        pass
    return out


def last_activity(log):
    try:
        lines = [l.rstrip() for l in open(log, errors="replace").read().splitlines() if l.strip()]
    except OSError:
        return "(not started)"
    for l in reversed(lines):
        if re.search(r"timed run|\[dump\]|Correctness|Running|Loading|Memory|failed|TIMEOUT|Q\d time", l):
            return re.sub(r"\s+", " ", l.strip())[:62]
    return re.sub(r"\s+", " ", lines[-1].strip())[:62] if lines else "(started)"


def bar(frac, width=20):
    n = int(round(frac * width))
    return "#" * n + "." * (width - n)


def main():
    plan = json.load(open(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAN))
    steps, after = plan["steps"], plan.get("after", [])
    now = datetime.datetime.now()
    done = done_times(plan["progress"])
    current = next((i for i, s in enumerate(steps) if s["name"] not in done), None)
    name_w = max(13, max(len(s["name"]) for s in steps + after))

    def row(text):
        print("   │ " + text[:WIDTH - 2].ljust(WIDTH - 2) + " │")

    power = "AC" if "AC Power" in run("pmset -g batt") else "BATTERY (!)"
    print(f"            BENCHMARK QUEUE   {now:%a %H:%M}   ({plan.get('title', 'queue')}, {power} power)")
    print("   ┌" + "─" * WIDTH + "┐")
    remaining = 0.0
    for i, s in enumerate(steps):
        what = s.get("what", "")
        if s["name"] in done:
            h, m = done[s["name"]]
            row(f"✔ {s['name']:<{name_w}} [{bar(1)}] DONE {h:02d}:{m:02d}   {what}")
        elif i == current:
            started = now
            try:
                started = datetime.datetime.fromtimestamp(os.stat(s["log"]).st_birthtime)
            except (OSError, KeyError):
                pass
            if i > 0 and steps[i - 1]["name"] in done:   # a step starts when the previous one finished
                h, m = done[steps[i - 1]["name"]]
                started = datetime.datetime(now.year, now.month, now.day, h, m)
            el = max(0.0, (now - started).total_seconds() / 60)
            frac = min(0.95, el / s["minutes"])
            row(f"▶ {s['name']:<{name_w}} [{bar(frac)}] RUNNING {el:3.0f}/{s['minutes']} min   {what}")
            row(f"     └─ now: {last_activity(s.get('log', ''))}")
            remaining += max(2, s["minutes"] - el)
        else:
            row(f"· {s['name']:<{name_w}} [{bar(0)}] waiting ~{s['minutes']} min   {what}")
            remaining += s["minutes"]
        row("      ↓")
    for j, s in enumerate(after):
        remaining += s["minutes"]
        row(f"· {s['name']:<{name_w}} [{bar(0)}] waiting ~{s['minutes']} min   {s.get('what', '')}")
        if j < len(after) - 1:
            row("      ↓")
    print("   └" + "─" * WIDTH + "┘")
    if current is None and not after:
        print("   queue finished")
    else:
        eta = now + datetime.timedelta(minutes=remaining)
        print(f"   estimated remaining: ~{remaining:.0f} min  ->  finish about {eta:%H:%M}")

    free = re.search(r"free percentage:\s*(\d+)", run("memory_pressure"))
    swap = re.search(r"used = ([\d.]+)M", run("sysctl vm.swapusage"))
    running = sorted({re.sub(r".*(benchmark\.py|weekend\.py|lsqb_benchmark\.py)", r"\1", l.split(None, 1)[1])[:40]
                      for l in run("pgrep -fl 'benchmark.py --vendor|lsqb_benchmark|weekend.py'").splitlines()
                      if l.strip() and len(l.split(None, 1)) > 1})
    containers = [c for c in run("docker ps --format '{{.Names}}'").split() if c != "buildx_buildkit_maven0"]
    print(f"   machine: power {power} | free memory {free.group(1) + '%' if free else '?'} | swap used "
          f"{float(swap.group(1)) / 1024:.1f} GB | containers: {', '.join(containers) or 'none'}")
    short = sorted({re.sub(r"\s.*", "", r) for r in running}) if len(", ".join(running)) > 70 else running
    print(f"   running: {', '.join(short) or 'nothing'}")


if __name__ == "__main__":
    main()
