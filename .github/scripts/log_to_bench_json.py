#!/usr/bin/env python3
"""Convert a Mode 2/3 embedded-benchmark stdout log into
github-action-benchmark's customSmallerIsBetter JSON format.

Matches lines printed as "%-10s %9.2fs%n" (e.g. "PR             1.23s") for
each of the given metric names, in the order they appear in the log.
"""
import json
import re
import sys


def convert(log_text, names):
    pattern = re.compile(r"^(" + "|".join(re.escape(n) for n in names) + r")\s+(\S+)")
    entries = []
    for line in log_text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        name, raw_value = match.group(1), match.group(2)
        entries.append({"name": name, "unit": "s", "value": float(raw_value.rstrip("s"))})
    return entries


def main():
    log_path, names = sys.argv[1], sys.argv[2:]
    with open(log_path) as f:
        log_text = f.read()
    json.dump(convert(log_text, names), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
