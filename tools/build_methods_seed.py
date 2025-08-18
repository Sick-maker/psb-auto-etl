#!/usr/bin/env python3
import csv, glob, json, os, sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "data" / "methods"
OUT_CSV = ROOT / "data" / "methods_seed.csv"

RESERVED = ["name", "description", "schema", "version", "status", "notes"]
CSV_BASE = ["Name", "Description", "Parameters Schema", "Version", "Status", "Notes"]

# Fill for missing extras (default empty to avoid type conflicts in Notion).
FILL_NA = os.getenv("FILL_NA", "")

def load_jsons() -> List[Dict[str, Any]]:
    rows = []
    for p in sorted(SRC_DIR.glob("MTH-*.json")):
        with p.open(encoding="utf-8") as f:
            obj = json.load(f)
        # sanity
        for k in ["name", "description", "schema", "version", "status"]:
            if k not in obj:
                raise SystemExit(f"[methods] {p.name}: missing required field '{k}'")
        # filename/name consistency
        stem = p.stem
        if obj["name"] != stem:
            raise SystemExit(f"[methods] {p.name}: name '{obj['name']}' must match filename stem '{stem}'")
        rows.append(obj)
    return rows

def union_extra_keys(objs: List[Dict[str, Any]]) -> List[str]:
    keys = set()
    for o in objs:
        x = o.get("x", {})
        if isinstance(x, dict):
            keys.update(x.keys())
        elif x is not None:
            raise SystemExit(f"[methods] {o.get('name','<unknown>')}: 'x' must be an object if present")
    return sorted(keys)

def norm_scalar(v: Any) -> str:
    # Keep CSV friendly: lists become comma-joined; booleans/numbers → str; None → FILL_NA/empty
    if v is None:
        return FILL_NA
    if isinstance(v, (list, tuple)):
        return ",".join(str(x) for x in v)
    return str(v)

def main() -> None:
    objs = load_jsons()
    if not objs:
        print("[methods] No JSON methods found; nothing to do.")
        return

    extra_cols = [f"X.{k}" for k in union_extra_keys(objs)]
    fieldnames = CSV_BASE + extra_cols

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for o in objs:
            row = {
                "Name": o["name"],
                "Description": o["description"],
                "Parameters Schema": json.dumps(o["schema"], ensure_ascii=False),
                "Version": o["version"],
                "Status": o["status"],
                "Notes": o.get("notes", ""),
            }
            x = o.get("x", {}) or {}
            for col in extra_cols:
                key = col[2:]  # strip "X."
                row[col] = norm_scalar(x.get(key, FILL_NA))
            w.writerow(row)

    print(f"[methods] Built {OUT_CSV} with {len(objs)} methods from {len(list(SRC_DIR.glob('MTH-*.json')))} JSON files.")

if __name__ == "__main__":
    main()
