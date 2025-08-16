#!/usr/bin/env python3
"""
Rebuild data/corpora/registry.csv from the files present.

- Scans data/corpora for *.csv and *.txt (excl: registry.csv, README.md)
- Reads optional '# id: ...' on the first non-empty, non-header comment line
- Infers id from filename if not present (en_unigram_v2.1.csv -> CORP-en-unigram-v2.1)
- Writes registry with: id,type,filename,bytes,sha256,source
"""
import csv, hashlib, os, sys, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORP_DIR = ROOT / "data" / "corpora"
REG_PATH = CORP_DIR / "registry.csv"

TYPES = ["unigram", "quadgram", "lexicon"]

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def guess_id_from_filename(name: str) -> str:
    # en_unigram_v2.1.csv -> CORP-en-unigram-v2.1
    base = re.sub(r"\.csv$|\.txt$", "", name, flags=re.I)
    return f"CORP-{base.replace('_','-')}"

def read_declared_id(p: Path) -> str | None:
    try:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                if s.startswith("#"):
                    m = re.match(r"#\s*id\s*:\s*(.+)$", s, flags=re.I)
                    if m:
                        return m.group(1).strip()
                    else:
                        # other comments allowed; keep scanning until first non-comment
                        continue
                # first non-comment line reached; stop
                break
    except Exception:
        pass
    return None

def classify_type(fid: str, filename: str) -> str:
    key = f"{fid} {filename}".lower()
    if "unigram" in key:
        return "unigram"
    if "quadgram" in key:
        return "quadgram"
    if "function_words" in key or "lexicon" in key:
        return "lexicon"
    return "other"

def main() -> None:
    if not CORP_DIR.exists():
        print("No data/corpora/ directory found; nothing to do.")
        sys.exit(0)

    rows = []
    for p in sorted(CORP_DIR.glob("*")):
        if not p.is_file():
            continue
        if p.name.lower() in {"registry.csv", "readme.md"}:
            continue
        if not re.search(r"\.(csv|txt)$", p.name, flags=re.I):
            continue

        fid = read_declared_id(p) or guess_id_from_filename(p.name)
        ftype = classify_type(fid, p.name)
        sz = p.stat().st_size
        digest = sha256_file(p)
        rows.append({
            "id": fid,
            "type": ftype,
            "filename": p.name,
            "bytes": str(sz),
            "sha256": digest,
            "source": "manual",
        })

    # ensure dir exists
    CORP_DIR.mkdir(parents=True, exist_ok=True)
    # write registry
    with REG_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","type","filename","bytes","sha256","source"])
        w.writeheader()
        for r in sorted(rows, key=lambda r: r["id"].lower()):
            w.writerow(r)

    print(f"[corpora] wrote registry with {len(rows)} entries at {REG_PATH}")

if __name__ == "__main__":
    main()
