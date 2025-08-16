#!/usr/bin/env python3
import csv, json, os, sys

PATH = os.environ.get("DATA_DIR", "data")
CSV  = os.path.join(PATH, "methods_seed.csv")

REQUIRED_COLS = ["Name","Parameters Schema"]

def main():
    if not os.path.exists(CSV):
        print(f"[check-methods] {CSV} not found (skip).")
        return
    bad = 0
    with open(CSV, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for col in REQUIRED_COLS:
            if col not in rdr.fieldnames:
                print(f"[check-methods] ERROR: missing column '{col}'")
                sys.exit(1)
        for i, row in enumerate(rdr, start=2):  # header is row 1
            name = (row.get("Name") or "").strip()
            js   = (row.get("Parameters Schema") or "").strip()
            if not name:
                print(f"[check-methods] WARN: row {i} has empty Name (skipping JSON lint).")
                continue
            if not js:
                print(f"[check-methods] ERROR: row {i} ({name}) has empty Parameters Schema")
                bad += 1; continue
            try:
                json.loads(js)
            except json.JSONDecodeError as e:
                print(f"[check-methods] ERROR: row {i} ({name}) bad JSON: {e}")
                bad += 1
    if bad:
        print(f"[check-methods] ❌ {bad} schema error(s).")
        sys.exit(1)
    print("[check-methods] ✅ all method schemas valid JSON.")

if __name__ == "__main__":
    main()
