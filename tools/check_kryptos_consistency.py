# tools/check_kryptos_consistency.py
from pathlib import Path
import csv, sys

CIPH = Path("data/ciphertexts.csv")
BASE = Path("data/baselines.csv")

def die(msg: str) -> None:
    print(f"::error::{msg}")
    sys.exit(1)

def load_csv(p: Path) -> list[dict]:
    if not p.exists():
        die(f"Required file missing: {p}")
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    c = load_csv(CIPH)
    b = load_csv(BASE)

    # Index by section
    c_by = {r["section"]: r for r in c}
    b_by = {r["section"]: r for r in b}

    # Must have K1–K4 in ciphertexts, and K1–K3 in baselines
    for s in ["K1","K2","K3","K4"]:
        if s not in c_by: die(f"ciphertexts.csv missing section {s}")
    for s in ["K1","K2","K3"]:
        if s not in b_by: die(f"baselines.csv missing section {s}")

    # Length parity K1–K3 (letters only)
    for s in ["K1","K2","K3"]:
        lc = int(c_by[s]["length"])
        lb = int(b_by[s]["length"])
        if lc != lb:
            die(f"Length mismatch {s}: ciphertext={lc} vs baseline={lb}")

    print("Kryptos consistency OK: baselines present, lengths match.")

if __name__ == "__main__":
    main()
