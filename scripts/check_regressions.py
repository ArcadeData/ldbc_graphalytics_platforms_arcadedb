#!/usr/bin/env python3
"""Flag ArcadeDB numbers that got much slower than in the previous weekly run (recurrence guard for #8660 and friends).

  python3 scripts/check_regressions.py weekly-results/<new>/weekly.json                  # previous = newest older run
  python3 scripts/check_regressions.py new/weekly.json --previous old/weekly.json --factor 2

Compared (ArcadeDB only, lower is better): the embedded Graphalytics and LSQB medians (`java-m2`, `java-lsqb`), the Mode 1
processing times in the order the framework ran them (`mode1`), and every bulk UPDATE of the #8660 reproducer
(`bulk-update`). A value is a regression when new > factor * old AND new - old > --min-abs seconds (sub-second numbers
jump around by more than 2x between runs, so the absolute floor keeps them from raising false alarms). OLTP numbers are
noisy (about 2x run to run, see ArcadeDB-release-progress.md), so use a factor of 2 only with the median of several reps
and read an OLTP flag as "look at it", not as a verdict. Load times are one-off and not compared.
A metric that was valid before and is missing or invalid now is reported as well.
Exit code 1 when anything is flagged.
"""

import argparse
import glob
import json
import os
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def metrics(report):
    """{label: seconds} for every comparable ArcadeDB value of one weekly.json."""
    out = {}
    for suite in ("java-m2", "java-lsqb"):
        for variant, d in (report.get(suite) or {}).items():
            for k, v in (d.get("median") or {}).items():
                if k != "LOAD":
                    out[f"{suite}/{variant}/{k}"] = v
    for mode, d in ((report.get("mode1") or {}).get("summary") or {}).items():
        for algo, v in (d.get("algorithms") or {}).items():
            if v.get("success", True):
                out[f"mode1/{mode}/{algo}"] = v["processing_time"]
    for i, run in enumerate((report.get("bulk-update") or {}).get("runs") or [], 1):
        for algo, v in (run.get("updates") or {}).items():
            out[f"bulk-update/order{i}/{algo}"] = v
    return out


def invalid(report):
    """Labels of ArcadeDB results whose correctness check failed."""
    bad = set()
    for suite in ("java-m2", "java-lsqb"):
        for variant, d in (report.get(suite) or {}).items():
            for k, verdict in (d.get("validation") or {}).items():
                if isinstance(verdict, dict) and verdict.get("verdict") == "invalid":
                    bad.add(f"{suite}/{variant}/{k}")
    return bad


def previous_run(current_path):
    here = os.path.abspath(current_path)
    runs = [p for p in glob.glob(os.path.join(ROOT, "weekly-results", "*", "weekly.json"))
            if os.path.abspath(p) != here and not json.load(open(p)).get("dry_run")]
    runs.sort(key=os.path.getmtime)
    older = [p for p in runs if os.path.getmtime(p) < os.path.getmtime(here)] if os.path.exists(here) else runs
    return older[-1] if older else None


def compare(new, old, factor=2.0, min_abs=0.25):
    """[(label, old, new, note)] for every regression."""
    n, o = metrics(new), metrics(old)
    ran = {label.split("/")[0] for label in n}   # a suite that did not run in this (partial) run is not "missing"
    flagged = []
    for label, was in sorted(o.items()):
        now = n.get(label)
        if now is None:
            if label.split("/")[0] in ran:
                flagged.append((label, was, None, "missing"))
        elif now > factor * was and now - was > min_abs:
            flagged.append((label, was, now, f"{now / was:.1f}x slower"))
    now_invalid = invalid(new) - invalid(old)
    flagged += [(label, o.get(label), n.get(label), "INVALID output") for label in sorted(now_invalid)]
    return flagged


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("current", help="weekly.json of the run to check")
    ap.add_argument("--previous", help="weekly.json to compare with (default: the newest older run)")
    ap.add_argument("--factor", type=float, default=2.0)
    ap.add_argument("--min-abs", type=float, default=0.25, help="ignore slowdowns smaller than this many seconds")
    args = ap.parse_args()
    prev = args.previous or previous_run(args.current)
    if not prev:
        print("No previous weekly run to compare with.")
        return 0
    flagged = compare(json.load(open(args.current)), json.load(open(prev)), args.factor, args.min_abs)
    print(f"Compared {args.current} with {prev} (factor {args.factor}, floor {args.min_abs}s)")
    for label, was, now, note in flagged:
        print(f"  REGRESSION {label}: {was} -> {now}  ({note})")
    if not flagged:
        print("  no regressions")
    return 1 if flagged else 0


if __name__ == "__main__":
    sys.exit(main())
