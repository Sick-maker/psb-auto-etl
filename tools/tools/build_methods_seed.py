#!/usr/bin/env python3
import csv, json, os, re, sys
from typing import Any, Dict, List

METHODS_DIR = os.environ.get("METHODS_DIR", "data/methods")
OUT_CSV     = os.environ.get("METHODS_OUT", "data/methods_seed.csv")
SCORING_CSV = os.environ.get("SCORING_CSV", "data/scoring_seed.csv")  # optional check

NAME_RE = re.compile(r"^MTH-[a-z0-9-]+-v\d+\.\d+$")

REQUIRED_KEYS = ["name","description","parameters_schema","version","status","notes"]

def load_scoring_ids(path: str) -> set:
    if not os.path.exists(path):
        return set()
    ids = set()
    with open(path, newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            name = (row.get("Name") or row.get("name") or "").strip()
            if name:
                ids.add(name)
    return ids

def validate_method(m: Dict[str, Any], scoring_ids: set, seen_names: set, path: str):
    missing = [k for k in REQUIRED_KEYS if k not in m]
    if missing:
        raise ValueError(f"{path}: missing keys {missing}")
    name = str(m["name"])
    if not NAME_RE.match(name):
        raise ValueError(f"{path}: name '{name}' must match {NAME_RE.pattern}")
    if name in seen_names:
        raise ValueError(f"{path}: duplicate method name '{name}'")
    seen_names.add(name)

    # light checks
    ver = str(m["version"])
    if not ver.startswith("v"):
        raise ValueError(f"{path}: version should start with 'v' (e.g., v1.0) got '{ver}'")

    params = m.get("parameters_schema")
    if not isinstance(params, dict):
        raise ValueError(f"{path}: parameters_schema must be an object")

    # optional: if a scoring_id is present, ensure it exists in scoring_seed.csv
    scoring_id = params.get("scoring_id")
    if scoring_id and scoring_ids and scoring_id not in scoring_ids:
        raise ValueError(f"{path}: parameters_schema.scoring_id '{scoring_id}' "
                         f"not found in scoring_seed.csv")

def iter_method_jsons() -> List[str]:
    files = []
    if os.path.isdir(METHODS_DIR):
        for fn in os.listdir(METHODS_DIR):
            if fn.lower().endswith(".json"):
                files.append(os.path.join(METHODS_DIR, fn))
    return sorted(files)

def compile_csv(method_files: List[str], scoring_ids: set):
    rows = []
    seen = set()
    for fp in method_files:
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        validate_method(data, scoring_ids, seen, fp)
        rows.append({
            "Name": data["name"],
            "Description": data["description"],
            "Parameters Schema": json.dumps(data["parameters_schema"], ensure_ascii=True),
            "Version": data["version"],
            "Status": data["status"],
            "Notes": data.get("notes",""),
        })
    # deterministic order: by Name
    rows.sort(key=lambda r: r["Name"])
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "Name","Description","Parameters Schema","Version","Status","Notes"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return rows

def main():
    method_files = iter_method_jsons()
    if not method_files:
        print(f"[methods] No JSON files found in {METHODS_DIR}", file=sys.stderr)
        sys.exit(1)
    scoring_ids = load_scoring_ids(SCORING_CSV)
    rows = compile_csv(method_files, scoring_ids)
    print(f"[methods] Built {OUT_CSV} with {len(rows)} methods from {len(method_files)} JSON files.")

if __name__ == "__main__":
    main()
