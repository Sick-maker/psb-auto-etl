#!/usr/bin/env python3
import argparse, csv, hashlib, pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw"
NORM_TAG = "upper_strip_v1.0"

def upper_strip(s: str) -> str:
    return "".join(ch for ch in s.upper() if "A" <= ch <= "Z")

def letters_only(path: pathlib.Path) -> str:
    return upper_strip(path.read_text(encoding="utf-8"))

def sha256_letters(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def build_csvs():
    DATA.mkdir(exist_ok=True)
    ctexts = {}
    for sec in ("k1","k2","k3","k4"):
        p = RAW / f"{sec}_cipher.txt"
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr); sys.exit(1)
        s = letters_only(p)
        if sec == "k4" and len(s) != 97:
            print(f"ERROR: K4 letters-only length must be 97, got {len(s)}", file=sys.stderr); sys.exit(1)
        ctexts[sec.upper()] = s
    plains = {}
    for sec in ("k1","k2","k3"):
        p = RAW / f"{sec}_plain.txt"
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr); sys.exit(1)
        s = letters_only(p); plains[sec.upper()] = s
    for sec in ("K1","K2","K3"):
        if len(ctexts[sec]) != len(plains[sec]):
            print(f"ERROR: {sec} letters-only length mismatch: cipher={len(ctexts[sec])} plain={len(plains[sec])}", file=sys.stderr); sys.exit(1)
    with (DATA / "ciphertexts.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["Title","Section","Normalization","Ciphertext (A–Z)","Length","Checksum","Version","Notes"])
        for sec in ("K1","K2","K3","K4"):
            s = ctexts[sec]; w.writerow([f"{sec} — base", sec, NORM_TAG, s, len(s), sha256_letters(s), 1, ""])
    with (DATA / "baselines.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["Title","Section","Plaintext Canon (A–Z)","Length","Checksum","Version","Notes"])
        for sec in ("K1","K2","K3"):
            s = plains[sec]; w.writerow([f"{sec} — baseline", sec, s, len(s), sha256_letters(s), 1, "Misspellings preserved canon"])
    print("Wrote data/ciphertexts.csv and data/baselines.csv")

def main():
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build-csvs"); sub.add_parser("normalize")
    args = ap.parse_args()
    if args.cmd == "build-csvs": build_csvs()
    else:
        import sys; print(upper_strip(sys.stdin.read()))

if __name__ == "__main__":
    main()
