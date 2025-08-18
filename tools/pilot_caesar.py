#!/usr/bin/env python3
"""
pilot_caesar.py
Runs a minimal Caesar sweep over selected sections, scores with sfx_eval, writes artifacts,
optionally syncs a RUN + summary rows to Notion (schema-adaptive).
"""

from __future__ import annotations
import argparse, csv, json, os, time
from typing import Dict, List, Tuple
import requests  # only needed if --notion is used

from sfx_eval import composite_default

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")

CIPHERTEXTS_CSV = os.path.join(DATA_DIR, "ciphertexts.csv")

# ---------- helpers ----------
def now_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())

def caesar_shift(ch: str, k: int) -> str:
    if "A" <= ch <= "Z":
        return chr((ord(ch) - 65 - k) % 26 + 65)
    return ch

def decrypt_caesar(text: str, k: int) -> str:
    t = "".join(c for c in text.upper() if "A" <= c <= "Z")
    return "".join(caesar_shift(c, k) for c in t)

def load_ciphertexts() -> List[Dict[str, str]]:
    if not os.path.exists(CIPHERTEXTS_CSV):
        raise SystemExit(f"Missing {CIPHERTEXTS_CSV}")
    with open(CIPHERTEXTS_CSV, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def ensure_dir(d: str):
    os.makedirs(d, exist_ok=True)

# ---------- Notion (schema-adaptive, optional) ----------
NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.environ.get("NOTION_VERSION", "2022-06-28")

def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VER, "Content-Type": "application/json"}

def notion_upsert_run(token: str, db_id: str, title: str, props: Dict[str, str]) -> str:
    # Find title prop name
    r = requests.get(f"{NOTION_API}/databases/{db_id}", headers=headers(token)); r.raise_for_status()
    schema = r.json()
    title_prop = None
    for pname, pdef in schema.get("properties", {}).items():
        if pdef.get("type") == "title":
            title_prop = pname
            break
    if not title_prop:
        raise RuntimeError("No title property in RUN DB")

    # Query by exact title
    q = {"filter": {"property": title_prop, "title": {"equals": title}}, "page_size": 1}
    r = requests.post(f"{NOTION_API}/databases/{db_id}/query", headers=headers(token), json=q); r.raise_for_status()
    results = r.json().get("results", [])
    shaped = {}
    for k, v in props.items():
        if k not in schema["properties"]:
            continue
        t = schema["properties"][k]["type"]
        if t == "rich_text":
            shaped[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}
        elif t == "number":
            try: shaped[k] = {"number": float(v)}
            except Exception: pass
        elif t == "select":
            shaped[k] = {"select": {"name": str(v)}}
        elif t == "status":
            shaped[k] = {"status": {"name": str(v)}}
        elif t in ("url","email","phone_number"):
            shaped[k] = {t: str(v)}
        elif t == "checkbox":
            shaped[k] = {"checkbox": (str(v).lower() in ("1","true","yes"))}
        # relations/multi_select omitted for brevity

    if results:
        pid = results[0]["id"]
        if shaped:
            r = requests.patch(f"{NOTION_API}/pages/{pid}", headers=headers(token), json={"properties": shaped}); r.raise_for_status()
        return pid
    else:
        payload = {"parent": {"database_id": db_id},
                   "properties": {**shaped, title_prop: {"title": [{"type": "text", "text": {"content": title}}]}}}
        r = requests.post(f"{NOTION_API}/pages", headers=headers(token), json=payload); r.raise_for_status()
        return r.json()["id"]

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", default="K1,K2,K3,K4", help="Comma list, e.g., K1,K2")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--notion", action="store_true", help="Attempt to write a RUN row to Notion")
    args = ap.parse_args()

    rows = load_ciphertexts()
    want = set(s.strip().upper() for s in args.sections.split(",") if s.strip())
    rows = [r for r in rows if (r.get("section") or "").upper() in want]

    stamp = now_stamp()
    run_id = f"RUN-EXP-{stamp}-CAESAR"
    out_dir = os.path.join(ROOT, run_id)
    ensure_dir(out_dir)

    trials: List[Dict[str, str]] = []
    for r in rows:
        section = (r.get("section") or "").upper()
        letters = r.get("letters") or ""
        for k in range(26):
            pt = decrypt_caesar(letters, k)
            comp, x2, qn = composite_default(pt)
            trials.append({
                "run_id": run_id,
                "section": section,
                "shift": k,
                "composite": f"{comp:.6f}",
                "chi2": f"{x2:.6f}",
                "neg_logp4": f"{qn:.6f}",
                "plaintext": pt[:120]  # preview
            })

    # rank by composite ascending (lower is better)
    trials.sort(key=lambda d: float(d["composite"]))

    # write artifacts
    manifest = {
        "run_id": run_id,
        "method": "MTH-caesar-scan-v1.0",
        "sfx_id": "SFX-chi2-quadgram-v2.1",
        "sections": sorted(list(want)),
        "n_trials": len(trials),
        "created_utc": stamp
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    with open(os.path.join(out_dir, "results.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(trials[0].keys()) if trials else ["run_id"])
        w.writeheader()
        for t in trials:
            w.writerow(t)

    topN = trials[: args.top]
    with open(os.path.join(out_dir, "top.json"), "w", encoding="utf-8") as f:
        json.dump(topN, f, indent=2)
    with open(os.path.join(out_dir, "top.txt"), "w", encoding="utf-8") as f:
        for t in topN:
            f.write(f"[{t['section']}] shift={t['shift']} composite={t['composite']}  {t['plaintext']}\n")

    print(f"[pilot] wrote {len(trials)} trials → {out_dir}")

    # Optional Notion sync (single RUN summary row)
    if args.notion:
        token = os.environ.get("NOTION_TOKEN", "")
        db_run = os.environ.get("NOTION_DB_RUN", "")
        if not token or not db_run:
            print("[pilot] NOTION_TOKEN/NOTION_DB_RUN not set; skipping Notion.")
            return
        title = run_id
        props = {
            "Method": "MTH-caesar-scan-v1.0",
            "Status": "Done",
            "Trials": len(trials),
            "Best Score": float(topN[0]["composite"]) if topN else "",
            "Notes": f"Sections={','.join(sorted(list(want)))}; SFX=SFX-chi2-quadgram-v2.1"
        }
        pid = notion_upsert_run(token, db_run, title, props)
        print(f"[pilot] Notion RUN upserted: {pid}")

if __name__ == "__main__":
    main()
