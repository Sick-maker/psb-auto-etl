# tools/kryptos_normalize.py
from pathlib import Path
import csv, hashlib, sys

RAW = Path("data/raw")
OUT_CIPHER = Path("data/ciphertexts.csv")
OUT_BASE = Path("data/baselines.csv")

def letters_only(s: str) -> str:
    return "".join(ch for ch in s.upper() if "A" <= ch <= "Z")

def sha256_letters(s: str) -> str:
    return hashlib.sha256(letters_only(s).encode("utf-8")).hexdigest()

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="strict")

def build_csvs() -> None:
    # --- Ciphertexts (K1–K4) ---
    rows_c = []
    for sec in ("K1", "K2", "K3", "K4"):
        txt = read_text(RAW / f"{sec.lower()}_cipher.txt")
        norm = letters_only(txt)
        rows_c.append({
            "ctx_id": f"CTX-{sec}-base-v1.0",
            "section": sec,
            "letters": norm,
            "length": len(norm),
            "checksum": sha256_letters(txt),
        })

    OUT_CIPHER.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CIPHER.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ctx_id","section","letters","length","checksum"])
        w.writeheader(); w.writerows(rows_c)

    # --- Baselines (K1–K3 only) ---
    rows_b = []
    for sec in ("K1", "K2", "K3"):
        txt = read_text(RAW / f"{sec.lower()}_plain.txt")
        norm = letters_only(txt)
        rows_b.append({
            "ctx_id": f"CTX-{sec}-base-v1.0",
            "section": sec,
            "plaintext": norm,
            "length": len(norm),
            "checksum": sha256_letters(txt),
        })

    with OUT_BASE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ctx_id","section","plaintext","length","checksum"])
        w.writeheader(); w.writerows(rows_b)

    # Helpful console proof for the workflow log
    print(f"WROTE {OUT_CIPHER.resolve()}")
    print(f"WROTE {OUT_BASE.resolve()}")

if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "build-csvs":
        build_csvs()
    else:
        print("Usage: python tools/kryptos_normalize.py build-csvs")
        sys.exit(2)
