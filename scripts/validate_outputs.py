#!/usr/bin/env python3
"""Validate per-vertex algorithm outputs against the official LDBC Graphalytics reference outputs.

A vendor output is a text file with one `<vertex id> <value>` pair per line (every vertex of the graph).
Reference outputs live next to the dataset: datasets/<graph>/<graph>-<ALGO> (BFS, CDLP, LCC, PR, SSSP, WCC).

Rules (Graphalytics validation):
  BFS, CDLP   exact match
  WCC         same partition into components (component labels may differ)
  PR, LCC, SSSP  numeric match within a relative error of 1e-4 (reference 0 or infinity must match exactly)

  python3 scripts/validate_outputs.py --graph datagen-7_5-fb --algo PR --output neo4j-PR.out
  python3 scripts/validate_outputs.py --graph graph500-22 --swap 6:248533 --dir outputs/ --vendor neo4j

--swap A:B maps ids A and B back to each other in the vendor output: the derived dataset graph500-22-w
swaps vertex ids 6 and 248533 so that the official BFS source is vertex 6.
"""

import argparse
import glob
import math
import os
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
EPSILON = 1e-4
INF_TOKENS = {"infinity", "inf", "+inf", "9223372036854775807", "nan"}


def read_pairs(path, swap=None):
    out = {}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 2:
                continue
            vid = int(parts[0])
            if swap:
                vid = swap.get(vid, vid)
            out[vid] = parts[1]
    return out


def _num(s):
    try:
        return float(s)
    except ValueError:
        return None


def _is_inf(s):
    return s.strip().lower() in INF_TOKENS


def _close(ref, out):
    if _is_inf(ref) or _is_inf(out):
        return _is_inf(ref) and _is_inf(out)
    a, b = _num(ref), _num(out)
    if a is None or b is None:
        return False
    if a == b:
        return True
    if a == 0.0:
        return abs(b) < 1e-14
    return abs(a - b) <= EPSILON * abs(a)


def validate(algo, ref_path, out_path, swap=None, max_examples=5):
    algo = algo.upper()
    if algo == "BFSREACH":
        return validate_reach(ref_path, out_path, swap, max_examples)
    if algo == "PRNORM":
        return validate_pr_normalized(ref_path, out_path, swap, max_examples)
    ref = read_pairs(ref_path)
    out = read_pairs(out_path, swap)
    res = {"algo": algo, "reference": len(ref), "output": len(out), "missing": 0, "extra": 0,
           "mismatches": 0, "examples": [], "valid": False}
    res["missing"] = sum(1 for v in ref if v not in out)
    res["extra"] = sum(1 for v in out if v not in ref)
    if algo == "WCC":
        # same partition: the mapping reference label -> output label must be a bijection
        fwd, bwd = {}, {}
        for v, rl in ref.items():
            ol = out.get(v)
            if ol is None:
                continue
            ok = fwd.setdefault(rl, ol) == ol and bwd.setdefault(ol, rl) == rl
            if not ok:
                res["mismatches"] += 1
                if len(res["examples"]) < max_examples:
                    res["examples"].append((v, rl, ol))
    else:
        eq = (lambda r, o: r == o) if algo in ("BFS", "CDLP") else _close
        for v, rv in ref.items():
            ov = out.get(v)
            if ov is None:
                continue
            if algo in ("BFS", "CDLP") and _num(rv) is not None and _num(ov) is not None:
                same = int(float(rv)) == int(float(ov))  # integers written as 3 or 3.0
            else:
                same = eq(rv, ov)
            if not same:
                res["mismatches"] += 1
                if len(res["examples"]) < max_examples:
                    res["examples"].append((v, rv, ov))
    res["valid"] = res["missing"] == 0 and res["mismatches"] == 0
    return res


def validate_pr_normalized(ref_path, out_path, swap=None, max_examples=5):
    """Diagnostic: PageRank after scaling the output to the reference total. A match here means the
    engine only differs in scale/normalisation convention; a mismatch means different semantics
    (for example direction handling or dangling vertices)."""
    ref = read_pairs(ref_path)
    out = read_pairs(out_path, swap)
    rs = sum(float(v) for v in ref.values())
    os_ = sum(float(v) for k, v in out.items() if k in ref) or 1.0
    k = rs / os_
    res = {"algo": "PRNORM", "reference": len(ref), "output": len(out), "missing": 0, "extra": 0,
           "mismatches": 0, "examples": [], "valid": False, "scale": k}
    for v, rv in ref.items():
        ov = out.get(v)
        if ov is None:
            res["missing"] += 1
            continue
        if not _close(rv, repr(float(ov) * k)):
            res["mismatches"] += 1
            if len(res["examples"]) < max_examples:
                res["examples"].append((v, rv, float(ov) * k))
    res["valid"] = res["missing"] == 0 and res["mismatches"] == 0
    return res


def validate_reach(ref_path, out_path, swap=None, max_examples=5):
    """BFS reachability only: the output lists the visited vertices (any value); the reference
    distances say which vertices are reachable (everything except the unreachable sentinel)."""
    ref = read_pairs(ref_path)
    reached = set(read_pairs(out_path, swap))
    res = {"algo": "BFSREACH", "reference": len(ref), "output": len(reached), "missing": 0, "extra": 0,
           "mismatches": 0, "examples": [], "valid": False}
    for v, rv in ref.items():
        want = not _is_inf(rv)
        if want != (v in reached):
            res["mismatches"] += 1
            if len(res["examples"]) < max_examples:
                res["examples"].append((v, "reachable" if want else "unreachable",
                                        "visited" if v in reached else "not visited"))
    res["valid"] = res["mismatches"] == 0
    return res


def describe(r):
    pct = 100.0 * (r["reference"] - r["missing"] - r["mismatches"]) / max(1, r["reference"])
    status = "VALID" if r["valid"] else "INVALID"
    line = (f"{r['algo']:5} {status:8} {pct:6.2f}% of {r['reference']} vertices match"
            f" (mismatches {r['mismatches']}, missing {r['missing']}, extra {r['extra']})")
    if "scale" in r:
        line += f"  (output scaled by {r['scale']:.6g})"
    if r["examples"]:
        line += "  e.g. (vertex, reference, output): " + "; ".join(str(e) for e in r["examples"][:3])
    return line


# metric name used in the result dictionaries -> (dump/reference algorithm, alternative dump names)
METRIC_ALGO = {"pagerank": "PR", "wcc": "WCC", "lcc": "LCC", "bfs": "BFS", "sssp": "SSSP", "cdlp": "CDLP"}
# derived datasets that are renamed/permuted copies of an official one: (official name, id swap)
DERIVED = {"graph500-22-w": ("graph500-22", {6: 248533, 248533: 6})}


def validate_vendor(dump_dir, vendor, dataset, datasets_dir):
    """Validate every dumped output of one vendor. Returns {metric: {"verdict", "pct", ...}}.

    verdict: "valid", "invalid" (output differs from the reference), "unchecked" (no output was dumped
    for a metric, so its timing cannot be trusted either).
    """
    graph, swap = DERIVED.get(dataset, (dataset, None))
    refdir = os.path.join(datasets_dir, graph)
    out = {}
    for metric, algo in METRIC_ALGO.items():
        candidates = [algo] + (["BFSREACH"] if algo == "BFS" else [])
        path = next((os.path.join(dump_dir, f"{vendor}-{c}.out") for c in candidates
                     if os.path.exists(os.path.join(dump_dir, f"{vendor}-{c}.out"))), None)
        ref = os.path.join(refdir, f"{graph}-{algo}")
        if path is None or not os.path.exists(ref):
            continue
        used = os.path.basename(path)[len(vendor) + 1:-4]
        r = validate(used, ref, path, swap)
        total = max(1, r["reference"])
        verdict = "valid" if r["valid"] else "invalid"
        if verdict == "invalid" and used == "CDLP":
            # identical communities under different label names: the result is right, the labels are not
            # the vertex ids the official validator compares byte for byte
            if validate("WCC", ref, path, swap)["valid"]:
                verdict = "equivalent"
        out[metric] = {"verdict": verdict, "algo": used,
                       "pct": round(100.0 * (r["reference"] - r["missing"] - r["mismatches"]) / total, 2),
                       "mismatches": r["mismatches"], "missing": r["missing"]}
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--graph", required=True, help="reference dataset, e.g. datagen-7_5-fb or graph500-22")
    ap.add_argument("--datasets", default=os.path.join(ROOT, "datasets"))
    ap.add_argument("--algo", help="BFS, CDLP, LCC, PR, SSSP or WCC (with --output)")
    ap.add_argument("--output", help="vendor output file for --algo")
    ap.add_argument("--dir", help="directory with <vendor>-<ALGO>.out files (with --vendor)")
    ap.add_argument("--vendor")
    ap.add_argument("--swap", help="A:B ids to map back to each other in the vendor output")
    args = ap.parse_args()
    swap = None
    if args.swap:
        a, b = (int(x) for x in args.swap.split(":"))
        swap = {a: b, b: a}
    refdir = os.path.join(args.datasets, args.graph)
    jobs = []
    if args.algo:
        jobs.append((args.algo, args.output))
    elif args.dir and args.vendor:
        for path in sorted(glob.glob(os.path.join(args.dir, f"{args.vendor}-*.out"))):
            jobs.append((os.path.basename(path).rsplit("-", 1)[1].split(".")[0], path))
    else:
        ap.error("give --algo/--output or --dir/--vendor")
    ok = True
    for algo, path in jobs:
        base = {"BFSREACH": "BFS", "PRNORM": "PR"}.get(algo.upper(), algo.upper())
        ref = os.path.join(refdir, f"{args.graph}-{base}")
        if not os.path.exists(ref):
            print(f"{algo}: no reference output for {args.graph}")
            continue
        r = validate(algo, ref, path, swap)
        print(describe(r))
        ok &= r["valid"]
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
