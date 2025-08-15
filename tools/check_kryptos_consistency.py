#!/usr/bin/env python3
import csv, hashlib, sys, pathlib, re

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw"

AZ = re.compile(r"[A-Z]")

def letters_only(s: str) -> str:
    return "".join(ch for ch in s.upper() if AZ.match(ch))

def sha256_letters(s: str) -> str:
    return hashlib.sha256(letters_only(s).encode("utf-8")).hexdigest()

def read(path: pathlib.Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()

def load_csv(path: pathlib.Path, key_field: str) -> dict:
    out = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            out[row[key_field]] = row
    return out

def fail(msg: str):
    print(f"CONSISTENCY ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    # Expect these raw sources
    raw_cipher = {
        "K1": RAW / "k1_cipher.txt",
        "K2": RAW / "k2_cipher.txt",
        "K3": RAW / "k3_cipher.txt",
        "K4": RAW / "k4_cipher.txt",
    }
    raw_plain = {
        "K1": RAW / "k1_plain.txt",
        "K2": RAW / "k2_plain.txt",
        "K3": RAW / "k3_plain.txt",
    }

    # 1) Check files exist
    missing = [p for p in [*raw_cipher.values(), *raw_plain.values()] if not p.exists()]
    if missing:
        fail("Missing raw files: " + ", ".join(str(p) for p in missing))

    # 2) Recompute lengths + hashes from raw
    cipher_facts = {}
    for k, path in raw_cipher.items():
        s = read(path)
        ls = letters_only(s)
        cipher_facts[k] = dict(length=len(ls), checksum=sha256_letters(s))

    plain_facts = {}
    for k, path in raw_plain.items():
        s = read(path)
        ls = letters_only(s)
        plain_facts[k] = dict(length=len(ls), checksum=sha256_letters(s))

    # 3) Load built CSVs
    ctxt_csv = DATA / "ciphertexts.csv"
    base_csv = DATA / "baselines.csv"
    if not ctxt_csv.exists() or not base_csv.exists():
        fail("Expected data/ciphertexts.csv and data/baselines.csv to exist (run kryptos_normalize.py build-csvs first).")

    ctxt = load_csv(ctxt_csv, "Section")      # Section ∈ {K1,K2,K3,K4}
    base = load_csv(base_csv, "Section")      # Section ∈ {K1,K2,K3}

    # 4) Compare lengths + checksums for ciphertexts
    for k, facts in cipher_facts.items():
        if k not in ctxt:
            fail(f"{k} missing in ciphertexts.csv")
        row = ctxt[k]
        got_len = int(row["Length"])
        got_sum = row["Checksum"]
        if got_len != facts["length"]:
            fail(f"{k} ciphertext length mismatch: csv={got_len}, raw={facts['length']}")
        if got_sum != facts["checksum"]:
            fail(f"{k} ciphertext checksum mismatch: csv={got_sum}, raw={facts['checksum']}")

    # 5) Compare lengths + checksums for baselines (K1–K3)
    for k, facts in plain_facts.items():
        if k not in base:
            fail(f"{k} missing in baselines.csv")
        row = base[k]
        got_len = int(row["Length"])
        got_sum = row["Checksum"]
        if got_len != facts["length"]:
            fail(f"{k} baseline length mismatch: csv={got_len}, raw={facts['length']}")
        if got_sum != facts["checksum"]:
            fail(f"{k} baseline checksum mismatch: csv={got_sum}, raw={facts['checksum']}")

    # 6) Sanity: K1–K3 letter counts equal between cipher and baseline
    for k in ["K1", "K2", "K3"]:
        if cipher_facts[k]["length"] != plain_facts[k]["length"]:
            fail(f"{k} letters-only length differs between ciphertext({cipher_facts[k]['length']}) and baseline({plain_facts[k]['length']})")

    print("KRYPTOS CONSISTENCY OK ✅")

if __name__ == "__main__":
    main()
