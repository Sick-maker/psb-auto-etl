#!/usr/bin/env python3
"""
notion_sync_kryptos.py
Push Kryptos CSVs from /data into Notion databases.

Env vars required:
  NOTION_TOKEN          - a Notion internal integration token
  DB_CIPHERTEXTS        - Notion database id for "ciphertexts"
  DB_BASELINES          - Notion database id for "baselines"
  DB_CLUES              - Notion database id for "clues"
  DB_CONTROLS           - Notion database id for "controls"

Usage:
  python tools/notion_sync_kryptos.py [--dry-run]

Notes:
- We infer page titles based on section:
    ciphertexts: "{section} — base"         (e.g., "K2 — base")
    baselines:   "{section} — baseline"     (e.g., "K2 — baseline")
- We link CTX relations (Baselines/Clues/Controls → Ciphertexts) by section.
- We adapt to your property names by reading the DB schema and only setting
  properties that exist.
"""

import csv
import os
import sys
import argparse
import time
import typing as t

import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.environ.get("NOTION_VERSION", "2022-06-28")

DATA_DIR = os.environ.get("DATA_DIR", "data")
CIPHERTEXTS_CSV = os.path.join(DATA_DIR, "ciphertexts.csv")
BASELINES_CSV   = os.path.join(DATA_DIR, "baselines.csv")
CLUES_CSV       = os.path.join(DATA_DIR, "clues_seed.csv")
CONTROLS_CSV    = os.path.join(DATA_DIR, "controls_seed.csv")

# ---------- helpers ----------

def env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Missing required env var: {name}")
    return v

def headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def get_db_schema(token: str, db_id: str) -> dict:
    r = requests.get(f"{NOTION_API}/databases/{db_id}", headers=headers(token))
    r.raise_for_status()
    return r.json()

def title_prop_name(db_schema: dict) -> str:
    """Return the property name that is type 'title'."""
    for pname, pdef in db_schema.get("properties", {}).items():
        if pdef.get("type") == "title":
            return pname
    raise RuntimeError("No title property found in database.")

def notion_rich_text(s: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": s}}]}

def ensure_select(value: str) -> dict:
    return {"select": {"name": value}}

def ensure_status(value: str) -> dict:
    return {"status": {"name": value}}

def ensure_relation(page_id: str) -> dict:
    return {"relation": [{"id": page_id}]}

def build_props_from_schema(schema: dict, desired: dict) -> dict:
    """
    Given DB schema and desired raw values {prop_name: raw_value},
    shape them to the correct Notion property payloads when possible.
    Unknown props are ignored (safe no-op).
    """
    out = {}
    props = schema.get("properties", {})
    for pname, raw in desired.items():
        if pname not in props:
            continue
        ptype = props[pname]["type"]
        if raw is None:
            continue
        if ptype == "rich_text":
            out[pname] = notion_rich_text(str(raw))
        elif ptype == "title":
            # here 'raw' must be already built as title payload elsewhere
            # (we set title separately in upsert)
            pass
        elif ptype == "number":
            try:
                out[pname] = {"number": float(raw)}
            except Exception:
                # best effort skip
                continue
        elif ptype == "select":
            out[pname] = ensure_select(str(raw))
        elif ptype == "status":
            out[pname] = ensure_status(str(raw))
        elif ptype == "multi_select":
            if isinstance(raw, (list, tuple)):
                out[pname] = {"multi_select": [{"name": str(x)} for x in raw]}
            else:
                out[pname] = {"multi_select": [{"name": str(raw)}]}
        elif ptype == "relation":
            # raw must be a Notion page ID (or list). Allow str or list[str].
            if isinstance(raw, str):
                out[pname] = ensure_relation(raw)
            elif isinstance(raw, (list, tuple)):
                out[pname] = {"relation": [{"id": pid} for pid in raw if pid]}
        elif ptype in ("url", "email", "phone_number"):
            out[pname] = {ptype: str(raw)}
        elif ptype == "checkbox":
            out[pname] = {"checkbox": bool(raw)}
        else:
            # Unsupported types are skipped gracefully
            continue
    return out

def query_page_by_title(token: str, db_id: str, title_prop: str, name: str) -> t.Optional[str]:
    url = f"{NOTION_API}/databases/{db_id}/query"
    payload = {
        "filter": {
            "property": title_prop,
            "title": {"equals": name}
        },
        "page_size": 1
    }
    r = requests.post(url, headers=headers(token), json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    if results:
        return results[0]["id"]
    return None

def create_page(token: str, db_id: str, title_prop: str, name: str, props: dict) -> str:
    payload = {
        "parent": {"database_id": db_id},
        "properties": {**props, title_prop: {"title": [{"type": "text", "text": {"content": name}}]}}
    }
    r = requests.post(f"{NOTION_API}/pages", headers=headers(token), json=payload)
    r.raise_for_status()
    return r.json()["id"]

def update_page(token: str, page_id: str, props: dict) -> None:
    payload = {"properties": props}
    r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=headers(token), json=payload)
    r.raise_for_status()

def upsert(token: str, db_schema: dict, db_id: str, name: str, props: dict, dry: bool=False) -> str:
    title_prop = title_prop_name(db_schema)
    page_id = query_page_by_title(token, db_id, title_prop, name)
    shaped = build_props_from_schema(db_schema, props)
    if page_id:
        if dry:
            print(f"DRY: update '{name}' ({page_id}) with {list(shaped.keys())}")
            return page_id
        update_page(token, page_id, shaped)
        print(f"Updated: {name}")
        return page_id
    else:
        if dry:
            print(f"DRY: create '{name}' with {list(shaped.keys())}")
            return "dry-created"
        new_id = create_page(token, db_id, title_prop, name, shaped)
        print(f"Created: {name}")
        return new_id

def load_csv(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, newline='', encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [ {k.strip(): (v or "").strip() for k,v in row.items()} for row in rdr ]

def section_from_ctx(ctx_id: str) -> str:
    # e.g., CTX-K4-base-v1.0 -> K4
    if not ctx_id:
        return ""
    parts = ctx_id.split("-")
    for p in parts:
        if p in ("K1","K2","K3","K4"):
            return p
    return ""

# ---------- main sync ----------

def sync(dry_run: bool=False) -> None:
    token = env("NOTION_TOKEN")
    db_cipher = env("DB_CIPHERTEXTS")
    db_base   = env("DB_BASELINES")
    db_clues  = env("DB_CLUES")
    db_ctrls  = env("DB_CONTROLS")

    schema_cipher = get_db_schema(token, db_cipher)
    schema_base   = get_db_schema(token, db_base)
    schema_clues  = get_db_schema(token, db_clues)
    schema_ctrls  = get_db_schema(token, db_ctrls)

    # 1) Ciphertexts
    sec_to_cipher_id: dict[str, str] = {}
    rows_c = load_csv(CIPHERTEXTS_CSV)
    for r in rows_c:
        section = r.get("section") or section_from_ctx(r.get("ctx_id",""))
        if not section:
            continue
        name = f"{section} — base"
        props = {
            "Section": section,
            "Ciphertext (A–Z)": r.get("letters",""),
            "Checksum": r.get("checksum",""),
            "Normalization": "upper_strip_v1.0",
            "Version": 1,
        }
        pid = upsert(token, schema_cipher, db_cipher, name, props, dry_run)
        sec_to_cipher_id[section] = pid

    # 2) Baselines
    rows_b = load_csv(BASELINES_CSV)
    for r in rows_b:
        section = r.get("section") or section_from_ctx(r.get("ctx_id",""))
        if not section:
            continue
        name = f"{section} — baseline"
        ctx_pid = sec_to_cipher_id.get(section)
        props = {
            "Section": section,
            "Plaintext Canon (A–Z)": r.get("plaintext",""),
            "Version": 1,
            "CTX": ctx_pid,
        }
        upsert(token, schema_base, db_base, name, props, dry_run)

    # 3) Clues (seed)
    rows_cl = load_csv(CLUES_CSV)
    for r in rows_cl:
        token_word = r.get("token") or r.get("Token") or ""
        ctx_id = r.get("ctx_id") or r.get("CTX") or ""
        section = r.get("section") or section_from_ctx(ctx_id) or "K4"
        name = r.get("title") or f"{section}: {token_word}".strip()
        ctx_pid = sec_to_cipher_id.get(section)
        props = {
            "Token": token_word,
            "Expected Start": r.get("expected_start") or r.get("Expected Start"),
            "Length": r.get("length") or r.get("Length"),
            "Notes": r.get("notes") or r.get("Notes") or "",
            "Version": r.get("version") or 1,
            "CTX": ctx_pid,
        }
        upsert(token, schema_clues, db_clues, name, props, dry_run)

    # 4) Controls (seed)
    rows_ct = load_csv(CONTROLS_CSV)
    for r in rows_ct:
        title = r.get("title") or r.get("Title") or ""
        ctx_id = r.get("ctx_id") or r.get("CTX") or ""
        section = r.get("section") or section_from_ctx(ctx_id)
        ctx_pid = sec_to_cipher_id.get(section) if section else None
        props = {
            "Type": r.get("type") or r.get("Type"),
            "Method/Scoring": r.get("method_scoring") or r.get("Method/Scoring"),
            "Recipe": r.get("recipe") or r.get("Recipe"),
            "Thresholds": r.get("thresholds") or r.get("Thresholds"),
            "Status": r.get("status") or r.get("Status"),
            "Notes": r.get("notes") or r.get("Notes") or "",
            "Version": r.get("version") or r.get("Version") or 1,
            "CTX": ctx_pid,
        }
        if not title:
            # Make a reasonable title if missing
            title = f"CTL-{section or 'K?' }-auto"
        upsert(token, schema_ctrls, db_ctrls, title, props, dry_run)

    print("Done.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing to Notion")
    args = ap.parse_args()
    sync(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
