#!/usr/bin/env python3
"""Convert a Mode 1 (official LDBC Graphalytics) results.json into
github-action-benchmark's customSmallerIsBetter JSON format."""
import json
import sys


def convert(data):
    result = data.get("result", data.get("experiments", {}))
    runs = result.get("runs", {})
    jobs = result.get("jobs", {})

    entries = []
    for run_id, run in sorted(runs.items(), key=lambda item: item[1]["timestamp"]):
        algorithm = next(j["algorithm"] for j in jobs.values() if run_id in j["runs"])
        for metric, label in (("load_time", "load"), ("processing_time", "processing")):
            raw = run.get(metric)
            if raw is None or raw == "nan":
                print(f"skipping {algorithm} {label}: no value ({raw!r})", file=sys.stderr)
                continue
            entries.append({"name": f"{algorithm} {label}", "unit": "s", "value": float(raw)})
    return entries


def main():
    with open(sys.argv[1]) as f:
        data = json.load(f)
    json.dump(convert(data), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
