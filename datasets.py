#!/usr/bin/env python3
"""
Dataset manager for LDBC benchmarks.

Downloads, extracts, and inspects datasets from the official LDBC repository
(https://ldbcouncil.org) into the datasets/ directory.

Usage:
  python3 datasets.py                         # Show downloaded datasets
  python3 datasets.py list                    # Same as above
  python3 datasets.py available               # Show all downloadable datasets
  python3 datasets.py download datagen-7_5-fb # Download a Graphalytics dataset
  python3 datasets.py download lsqb-sf1       # Download LSQB SF1 (both formats)
  python3 datasets.py download lsqb-sf1 --format merged-fk  # Only merged-fk
"""

import argparse
import os
import subprocess
import sys

DATASETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets")

GRAPHALYTICS_BASE = "https://datasets.ldbcouncil.org/graphalytics"
LSQB_BASE = "https://datasets.ldbcouncil.org/lsqb"

# -------------------------------------------------------------------------
# Graphalytics dataset catalog
# (name, vertices, edges, scale, compressed_size_bytes)
# -------------------------------------------------------------------------
GRAPHALYTICS_DATASETS = [
    # Test datasets
    ("example-directed",      10,      17,       "test",  None),
    ("example-undirected",    9,       12,       "test",  None),
    # 2XS
    ("wiki-Talk",             2_394_385,  5_021_410,   "2XS",  34_900_000),
    # XS
    ("kgs",                   832_247,   17_891_698,   "XS",   65_700_000),
    ("cit-Patents",           3_774_768,  16_518_948,   "XS",  119_100_000),
    # S
    ("dota-league",           61_170,    50_870_313,   "S",   114_300_000),
    ("datagen-7_5-fb",        633_432,   34_185_747,   "S",   162_300_000),
    ("datagen-7_6-fb",        754_147,   42_162_988,   "S",   200_000_000),
    ("graph500-22",           2_393_285,  64_155_735,   "S",   202_400_000),
    ("datagen-7_9-fb",        1_387_587,  85_670_523,   "S",   401_200_000),
    ("datagen-7_7-zf",        13_180_508, 32_791_267,   "S",   434_500_000),
    ("datagen-7_8-zf",        16_521_886, 41_025_146,   "S",   544_300_000),
    # M
    ("graph500-23",           4_610_222, 129_333_677,   "M",   410_600_000),
    ("datagen-8_0-fb",        1_706_561, 107_507_376,   "M",   502_500_000),
    ("datagen-8_1-fb",        2_072_117, 134_267_822,   "M",   625_400_000),
    ("graph500-24",           8_870_942, 260_379_520,   "M",   847_700_000),
    ("datagen-8_4-fb",        3_809_084, 269_479_177,   "M", 1_200_000_000),
    ("datagen-8_2-zf",        43_734_497, 106_440_188,  "M", 1_400_000_000),
    ("datagen-8_3-zf",        53_525_014, 130_579_909,  "M", 1_700_000_000),
    # L
    ("datagen-8_5-fb",        4_599_739, 332_026_902,   "L", 1_500_000_000),
    ("graph500-25",           17_062_472, 523_602_831,  "L", 1_700_000_000),
    ("datagen-8_6-fb",        5_667_674, 421_988_071,   "L", 1_900_000_000),
    ("datagen-8_9-fb",        10_572_901, 848_681_908,  "L", 3_700_000_000),
    ("datagen-8_7-zf",        145_050_709, 340_157_363, "L", 4_600_000_000),
    ("datagen-8_8-zf",        168_308_893, 413_354_288, "L", 5_300_000_000),
    # XL
    ("graph500-26",           32_804_978, 1_051_922_853, "XL",  3_400_000_000),
    ("datagen-9_0-fb",        12_857_671, 1_049_527_225, "XL",  4_600_000_000),
    ("twitter_mpi",           52_579_682, 1_963_263_508, "XL",  5_700_000_000),
    ("datagen-9_1-fb",        16_087_483, 1_342_158_397, "XL",  5_800_000_000),
    ("com-friendster",        65_608_366, 1_806_067_135, "XL",  6_700_000_000),
    ("graph500-27",           63_561_252, 2_111_642_032, "XL",  7_100_000_000),
    ("datagen-sf3k-fb",       33_553_323, 2_477_375_030, "XL", 12_700_000_000),
    ("datagen-9_2-zf",        434_943_376, 1_042_340_732,"XL", 13_700_000_000),
    ("datagen-9_4-fb",        29_310_565, 2_588_948_669, "XL", 14_000_000_000),
    ("datagen-9_3-zf",        555_270_053, 1_309_815_510,"XL", 17_400_000_000),
    # 2XL+
    ("graph500-28",           121_242_388, 4_236_163_958,  "2XL", 14_400_000_000),
    ("graph500-29",           232_999_630, 8_493_569_115,  "2XL", 29_600_000_000),
    ("datagen-sf10k-fb",      100_218_750, 9_072_519_178,  "2XL", 40_500_000_000),
    ("graph500-30",           447_797_986, 17_042_315_453, "3XL", 60_800_000_000),
]

# -------------------------------------------------------------------------
# LSQB dataset catalog
# (scale_factor, approx_vertices, approx_edges)
# -------------------------------------------------------------------------
LSQB_SCALE_FACTORS = [
    ("0.1",    400_000,     1_800_000),
    ("0.3",  1_200_000,     5_400_000),
    ("1",    3_900_000,    17_900_000),
    ("3",   11_000_000,    53_000_000),
    ("10",  36_000_000,   177_000_000),
    ("30",  105_000_000,   530_000_000),
    ("100", 350_000_000, 1_775_000_000),
    ("300", 1_050_000_000, 5_300_000_000),
    ("1000", 3_500_000_000, 17_700_000_000),
]

LSQB_FORMATS = ["merged-fk", "projected-fk"]


# =========================================================================
# Helpers
# =========================================================================
def fmt_size(n):
    """Format byte count as human-readable string."""
    if n is None:
        return "  -"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fmt_count(n):
    """Format large numbers with K/M/B suffix."""
    if n >= 1_000_000_000:
        return f"{n / 1e9:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    if n >= 1_000:
        return f"{n / 1e3:.0f}K"
    return str(n)


def dir_size(path):
    """Total size of a directory in bytes."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total


def count_lines(filepath):
    """Count lines in a text file (fast)."""
    count = 0
    try:
        with open(filepath, "rb") as f:
            for _ in f:
                count += 1
    except Exception:
        return None
    return count


def detect_dataset_info(name, path):
    """Detect vertex/edge counts for a downloaded dataset."""
    # Graphalytics: look for .v and .e files
    v_file = os.path.join(path, f"{name}.v")
    e_file = os.path.join(path, f"{name}.e")
    if os.path.isfile(v_file):
        vertices = count_lines(v_file)
        edges = count_lines(e_file) if os.path.isfile(e_file) else None
        return vertices, edges

    # LSQB: look for Person.csv
    person_csv = os.path.join(path, "Person.csv")
    if os.path.isfile(person_csv):
        # Count total vertices/edges from CSV files
        vertices = 0
        edges = 0
        for f in os.listdir(path):
            if not f.endswith(".csv"):
                continue
            fp = os.path.join(path, f)
            lines = count_lines(fp)
            if lines is None:
                continue
            lines -= 1  # subtract header
            if "_" in f and not f[0].isupper():
                continue
            # Edge tables have two entity names separated by underscore
            parts = f.replace(".csv", "").split("_")
            if len(parts) >= 3 and parts[1] in ("knows", "likes", "hasTag",
                    "hasMember", "hasInterest", "hasCreator", "containerOf",
                    "replyOf", "isPartOf", "isLocatedIn", "hasType"):
                edges += lines
            else:
                vertices += lines
        return vertices if vertices > 0 else None, edges if edges > 0 else None

    return None, None


# =========================================================================
# Commands
# =========================================================================
def cmd_list():
    """Show downloaded datasets."""
    entries = []
    for name in sorted(os.listdir(DATASETS_DIR)):
        path = os.path.join(DATASETS_DIR, name)
        if not os.path.isdir(path) or name.startswith("."):
            continue
        size = dir_size(path)
        vertices, edges = detect_dataset_info(name, path)
        entries.append((name, size, vertices, edges))

    if not entries:
        print("No datasets downloaded yet.\n")
        print("Download one with:")
        print("  python3 datasets.py download datagen-7_5-fb")
        print("  python3 datasets.py download lsqb-sf1")
        print("\nSee all available datasets:")
        print("  python3 datasets.py available")
        return

    print(f"\nDownloaded datasets in {DATASETS_DIR}/\n")
    print(f"  {'Dataset':<45} {'Size':>10}  {'Vertices':>12}  {'Edges':>12}")
    print(f"  {'-'*45} {'-'*10}  {'-'*12}  {'-'*12}")
    for name, size, vertices, edges in entries:
        v_str = fmt_count(vertices) if vertices else "-"
        e_str = fmt_count(edges) if edges else "-"
        print(f"  {name:<45} {fmt_size(size):>10}  {v_str:>12}  {e_str:>12}")
    print()


def cmd_available():
    """Show all downloadable datasets."""
    # Graphalytics
    print("\n" + "=" * 78)
    print("LDBC Graphalytics Datasets")
    print("  Graph algorithm benchmarks (BFS, PageRank, WCC, LCC, SSSP, CDLP)")
    print("  Source: https://ldbcouncil.org/benchmarks/graphalytics/")
    print("=" * 78)
    print(f"\n  {'Dataset':<25} {'Scale':>5}  {'Vertices':>12}  {'Edges':>12}  {'Download':>10}  {'Status'}")
    print(f"  {'-'*25} {'-'*5}  {'-'*12}  {'-'*12}  {'-'*10}  {'-'*12}")

    for name, vertices, edges, scale, comp_size in GRAPHALYTICS_DATASETS:
        path = os.path.join(DATASETS_DIR, name)
        status = "downloaded" if os.path.isdir(path) else ""
        cs = fmt_size(comp_size) if comp_size else "-"
        print(f"  {name:<25} {scale:>5}  {fmt_count(vertices):>12}  {fmt_count(edges):>12}  {cs:>10}  {status}")

    # LSQB
    print("\n" + "=" * 78)
    print("LSQB Datasets (Labelled Subgraph Query Benchmark)")
    print("  Subgraph pattern matching queries on LDBC Social Network")
    print("  Source: https://github.com/ldbc/lsqb")
    print("=" * 78)
    print()
    print("  Two format variants are available for each scale factor:")
    print()
    print("    merged-fk     Entity CSVs contain FK columns (e.g. City.csv has")
    print("                  ispartof_country). Natural fit for SQL databases.")
    print("                  Used by: ArcadeDB, DuckDB, PostgreSQL, Neo4j")
    print()
    print("    projected-fk  Entity CSVs have only the ID. Every relationship is a")
    print("                  separate edge CSV (e.g. City_isPartOf_Country.csv).")
    print("                  Natural fit for graph database bulk loaders.")
    print("                  Used by: Kuzu")
    print()
    print(f"  {'Dataset':<25} {'Vertices':>12}  {'Edges':>12}  {'Formats':>28}  {'Status'}")
    print(f"  {'-'*25} {'-'*12}  {'-'*12}  {'-'*28}  {'-'*12}")

    for sf, vertices, edges in LSQB_SCALE_FACTORS:
        name = f"lsqb-sf{sf}"
        merged_dir = os.path.join(DATASETS_DIR, f"social-network-sf{sf}-merged-fk")
        projected_dir = os.path.join(DATASETS_DIR, f"social-network-sf{sf}-projected-fk")
        formats = []
        if os.path.isdir(merged_dir):
            formats.append("merged-fk")
        if os.path.isdir(projected_dir):
            formats.append("projected-fk")
        status = ", ".join(formats) if formats else ""
        print(f"  {name:<25} {fmt_count(vertices):>12}  {fmt_count(edges):>12}  {'merged-fk, projected-fk':>28}  {status}")

    print(f"\nDownload:  python3 datasets.py download <name>")
    print(f"Examples:  python3 datasets.py download datagen-7_5-fb")
    print(f"           python3 datasets.py download lsqb-sf1")
    print(f"           python3 datasets.py download lsqb-sf1 --format merged-fk\n")


def cmd_download(name, fmt_filter=None):
    """Download and extract a dataset."""
    # Check for zstd
    if not _has_command("zstd") and not _has_command("unzstd"):
        print("Error: 'zstd' is required to extract datasets.")
        print("  macOS:  brew install zstd")
        print("  Linux:  apt install zstd  (or yum install zstd)")
        sys.exit(1)

    # LSQB dataset?
    if name.startswith("lsqb-sf"):
        sf = name.replace("lsqb-sf", "")
        valid_sfs = [s for s, _, _ in LSQB_SCALE_FACTORS]
        if sf not in valid_sfs:
            print(f"Error: Unknown scale factor '{sf}'. Available: {', '.join(valid_sfs)}")
            sys.exit(1)
        formats = [fmt_filter] if fmt_filter else LSQB_FORMATS
        for fmt in formats:
            archive_name = f"social-network-sf{sf}-{fmt}"
            url = f"{LSQB_BASE}/{archive_name}.tar.zst"
            dest_dir = os.path.join(DATASETS_DIR, archive_name)
            if os.path.isdir(dest_dir):
                print(f"  Already downloaded: {archive_name}/")
                continue
            _download_and_extract(url, f"{archive_name}.tar.zst")
        return

    # Graphalytics dataset?
    known = {d[0] for d in GRAPHALYTICS_DATASETS}
    if name not in known:
        print(f"Error: Unknown dataset '{name}'.")
        print(f"Run 'python3 datasets.py available' to see all datasets.")
        sys.exit(1)

    dest_dir = os.path.join(DATASETS_DIR, name)
    if os.path.isdir(dest_dir):
        print(f"  Already downloaded: {name}/")
        return
    url = f"{GRAPHALYTICS_BASE}/{name}.tar.zst"
    _download_and_extract(url, f"{name}.tar.zst", subdir=name)


def _download_and_extract(url, archive_name, subdir=None):
    """Download a .tar.zst archive and extract it.

    Args:
        subdir: If set, extract into DATASETS_DIR/subdir/ (for archives that
                extract flat without a top-level directory).
                If None, extract directly into DATASETS_DIR (for archives
                that already contain a top-level directory).
    """
    archive_path = os.path.join(DATASETS_DIR, archive_name)

    # Download
    print(f"  Downloading {archive_name}...")
    if _has_command("curl"):
        subprocess.run(
            ["curl", "-L", "--progress-bar", "-o", archive_path, url],
            check=True)
    elif _has_command("wget"):
        subprocess.run(
            ["wget", "-q", "--show-progress", "-O", archive_path, url],
            check=True)
    else:
        print("Error: neither 'curl' nor 'wget' found.")
        sys.exit(1)

    # Extract
    print(f"  Extracting {archive_name}...")
    if subdir:
        # Graphalytics archives extract flat — put them in a named subdirectory
        extract_dir = os.path.join(DATASETS_DIR, subdir)
        os.makedirs(extract_dir, exist_ok=True)
        subprocess.run(
            ["tar", "--use-compress-program=unzstd", "-xf", archive_path],
            cwd=extract_dir, check=True)
    else:
        # LSQB archives already contain a top-level directory
        subprocess.run(
            ["tar", "--use-compress-program=unzstd", "-xf", archive_path],
            cwd=DATASETS_DIR, check=True)

    # Remove archive
    os.remove(archive_path)
    print(f"  Done.\n")


def _has_command(cmd):
    """Check if a command exists on PATH."""
    from shutil import which
    return which(cmd) is not None


# =========================================================================
# Main
# =========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="LDBC dataset manager — download and inspect benchmark datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  python3 datasets.py                          # show downloaded datasets
  python3 datasets.py available                # show all downloadable datasets
  python3 datasets.py download datagen-7_5-fb  # download Graphalytics dataset
  python3 datasets.py download lsqb-sf1        # download LSQB SF1 (both formats)
  python3 datasets.py download lsqb-sf1 --format merged-fk""")

    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="Show downloaded datasets")
    sub.add_parser("available", help="Show all downloadable datasets")

    dl = sub.add_parser("download", help="Download a dataset")
    dl.add_argument("name", help="Dataset name (e.g., datagen-7_5-fb, lsqb-sf1)")
    dl.add_argument("--format", dest="fmt", choices=LSQB_FORMATS,
                    help="LSQB only: download only this format (default: both)")

    args = parser.parse_args()

    if args.command is None or args.command == "list":
        cmd_list()
    elif args.command == "available":
        cmd_available()
    elif args.command == "download":
        cmd_download(args.name, args.fmt)


if __name__ == "__main__":
    main()
