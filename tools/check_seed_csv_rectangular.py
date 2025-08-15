#!/usr/bin/env python3
"""
check_seed_csv_rectangular.py

CI guard: ensure every CSV matching data/*_seed.csv has a consistent column
count across all non-empty rows (i.e., no ragged rows). Exits non-zero on error.

Usage:
  python tools/check_seed_csv_rectangular.py
  python tools/check_seed_csv_rectangular.py --glob 'data/**/*_seed.csv'
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from typing import List

def is_blank_row(row: List[str]) -> bool:
    """Treat a row as blank if all cells are empty/whitespace."""
    return all((c is None) or (str(c).strip() == "") for c in row)

def check_file(path: str) -> List[str]:
    """
    Check one CSV file for rectangularity. Returns a list of error messages.
    Uses utf-8-sig to transparently strip BOM if present.
    """
    errors: List[str] = []
    header: List[str] | None = None
    expected_cols = None

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        # Find the first non-empty header row
        for row in reader:
            if not is_blank_row(row):
                header = row
                expected_cols = len(header)
                break

        if header is None:
            # Totally empty file; treat as error to avoid silent failures.
            errors.append(f"{path}: empty file (no header found)")
            return errors

        # Walk remaining rows
        for row in reader:
            # csv.reader counts line numbers internally (1-based) across the stream
            line_no = reader.line_num
            if is_blank_row(row):
                continue

            got = len(row)
            if got != expected_cols:
                preview_cells = " | ".join((row + [''])[:min(6, got)])
                hint = ""
                # Common hint: trailing comma creates an extra empty column
                if got > expected_cols and row[-1] == "":
                    hint = " (possible trailing comma)"
                errors.append(
                    f"{path}:{line_no}: expected {expected_cols} cols, got {got}{hint}\n"
                    f"  header: {expected_cols} -> {', '.join(header[:6])}"
                    f"{' ...' if len(header) > 6 else ''}\n"
                    f"  row[0:6]: {preview_cells}"
                )

    return errors

def main() -> int:
    ap = argparse.ArgumentParser(description="Fail if any *_seed.csv has ragged rows.")
    ap.add_argument(
        "--glob",
        default="data/*_seed.csv",
        help="Glob pattern to scan (default: data/*_seed.csv)",
    )
    args = ap.parse_args()

    matches = sorted(glob.glob(args.glob, recursive=True))
    if not matches:
        print(f"[check-seed] No files matched pattern: {args.glob}")
        # Not an error; repository may not have seed files yet.
        return 0

    all_errors: List[str] = []
    for path in matches:
        if not os.path.isfile(path):
            continue
        errs = check_file(path)
        if errs:
            all_errors.extend(errs)
        else:
            # Friendly OK line for logs
            print(f"[check-seed] OK: {path}")

    if all_errors:
        print("\n[check-seed] FAIL: ragged rows detected\n", file=sys.stderr)
        print("\n".join(all_errors), file=sys.stderr)
        return 1

    print("\n[check-seed] All seed CSVs rectangular ✅")
    return 0

if __name__ == "__main__":
    sys.exit(main())
