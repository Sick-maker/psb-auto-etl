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

Extras:
- Schema guard: raises if a DB is missing required columns.
- No-op optimization: skip PATCH when outgoing props match what’s on the page.
- Template safety: skip any page whose title begins with "template".
"""

import csv
import os
import sys
import argparse
import typing as t
import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.environ.get("NOTION_VERSION", "2022-06-28")

DATA_DIR = os.environ.get("DATA_DIR", "data")
CIPHERTEXTS_CSV = os.path.join(DATA_DIR, "ciphertexts.csv")
BASELINES_CSV   = os.path.join(DATA_DIR, "baselines.csv")
CLUES_CSV       = os.path.join(DATA_DIR, "clues_seed.csv")
CONTROLS_CSV    = os.path.join(DATA_DIR, "controls_seed.csv")

# --- required column guard (as named in your Notion DBs) ---
REQ_CIPHERTEXTS = ["Section", "Ciphertext (A–Z)", "Checksum", "Normalization", "Version"]
REQ_BASELINES   = ["Section", "Plaintext Canon (A–Z)", "Version", "CTX"]
REQ_CLUES       = ["Token", "Notes", "Version", "CTX"]      # Optional: Expected Start, Length
REQ_CONTROLS    = ["Type", "Method/Scoring", "Recipe", "Thresholds", "Status", "Notes", "Version", "CTX"]

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
    for pname, pdef in db_schema.get("properties", {}).items():
        if pdef.get("type") == "title":
            return pname
    raise RuntimeError("No title property found in database.")

def guard_required_props(db_schema: dict, required: list[str], db_label: str) -> None:
    present = set(db_schema.get("properties", {}).keys())
    missing = [p for p in required if p not in present]
    if missing:
        raise SystemExit(f"[schema-guard] '{db_label}' is missing required columns: {', '.join(missing)}")

def notion_rich_text(s: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": s}}]}

def ensure_select(value: str) -> dict:
    return {"select": {"name": value}}

def ensure_status(value: str) -> dict:
    return {"status": {"name": value}}

def ensure_relation(page_id: str) -> dict:
    return {"relation": [{"id": page_id}]}

def build_props_from_schema(schema: dict, desired: dict) -> dict:
    out = {}
    props = schema.get("properties", {})
    for pname, raw in desired.items():
        if pname not in props or raw is None:
            continue
        ptype = props[pname]["type"]
        if ptype == "rich_text":
            out[pname] = notion_rich_text(str(raw))
        elif ptype == "title":
            pass  # set in create payload
        elif ptype == "number":
            try:
                out[pname] = {"number": float(raw)}
            except Exception:
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
            if isinstance(raw, str):
                out[pname] = ensure_relation(raw)
            elif isinstance(raw, (list, tuple)):
                out[pname] = {"relation": [{"id": pid} for pid in raw if pid]}
        elif ptype in ("url", "email", "phone_number"):
            out[pname] = {ptype: str(raw)}
        elif ptype == "checkbox":
            out[pname] = {"checkbox": bool(raw)}
    return out

def query_page_by_title(token: str, db_id: str, title_prop: str, name: str) -> t.Optional[str]:
    url = f"{NOTION_API}/databases/{db_id}/query"
    payload = {"filter": {"property": title_prop, "title": {"equals": name}}, "page_size": 1}
    r = requests.post(url, headers=headers(token), json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])
    if results:
        return results[0]["id"]
    return None

def get_page(token: str, page_id: str) -> dict:
    r = requests.get(f"{NOTION_API}/pages/{page_id}", headers=headers(token))
    r.raise_for_status()
    return r.json()

def create_page(token: str, db_id: str, title_prop: str, name: str, props: dict) -> str:
    payload = {
        "parent": {"database_id": db_id},
        "properties": {**props, title_prop: {"title": [{"type": "text", "text": {"content": name}}]}},
    }
    r = requests.post(f"{NOTION_API}/pages", headers=headers(token), json=payload)
    r.raise_for_status()
    return r.json()["id"]

def update_page(token: str, page_id: str, props: dict) -> None:
    payload = {"properties": props}
    r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=headers(token), json=payload)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # Surface Notion's message (super handy when a property validation fails)
        dbg = None
        try:
            dbg = r.json()
        except Exception:
            dbg = r.text
        raise requests.HTTPError(
            f"{e} :: while updating page {page_id} with props {list(props.keys())} :: {dbg}",
            response=r
        )

# ---------- normalization for "no-op" diff ----------

def _norm_payload_value_from_schema(ptype: str, value: dict):
    if ptype == "rich_text":
        arr = value.get("rich_text", [])
        return "".join((x.get("text", {}) or {}).get("content", "") for x in arr if isinstance(x, dict))
    if ptype == "select":
        return (value.get("select") or {}).get("name")
    if ptype == "status":
        return (value.get("status") or {}).get("name")
    if ptype == "number":
        return value.get("number")
    if ptype == "checkbox":
        return bool(value.get("checkbox"))
    if ptype == "multi_select":
        names = [x.get("name") for x in value.get("multi_select", []) if isinstance(x, dict)]
        return tuple(sorted([n for n in names if n is not None]))
    if ptype == "relation":
        ids = [x.get("id") for x in value.get("relation", []) if isinstance(x, dict)]
        return tuple(sorted([i for i in ids if i]))
    if ptype in ("url", "email", "phone_number"):
        return value.get(ptype)
    return value

def _norm_page_value_from_schema(ptype: str, value: dict):
    if ptype == "rich_text":
        arr = value.get("rich_text", [])
        return "".join(x.get("plain_text", "") for x in arr if isinstance(x, dict))
    if ptype == "select":
        sel = value.get("select")
        return sel.get("name") if isinstance(sel, dict) else None
    if ptype == "status":
        st = value.get("status")
        return st.get("name") if isinstance(st, dict) else None
    if ptype == "number":
        return value.get("number")
    if ptype == "checkbox":
        return bool(value.get("checkbox"))
    if ptype == "multi_select":
        names = [x.get("name") for x in value.get("multi_select", []) if isinstance(x, dict)]
        return tuple(sorted([n for n in names if n is not None]))
    if ptype == "relation":
        ids = [x.get("id") for x in value.get("relation", []) if isinstance(x, dict)]
        return tuple(sorted([i for i in ids if i]))
    if ptype in ("url", "email", "phone_number"):
        return value.get(ptype)
    return value

def _normalized_subset_equal(schema: dict, page_props: dict, shaped_payload: dict) -> bool:
    props_def = schema.get("properties", {})
    for pname, payload_val in shaped_payload.items():
        if pname not in props_def:
            continue
        ptype = props_def[pname]["type"]
        new_norm = _norm_payload_value_from_schema(ptype, payload_val)
        page_val = page_props.get(pname, {})
        cur_norm = _norm_page_value_from_schema(ptype, page_val if isinstance(page_val, dict) else {})
        if new_norm != cur_norm:
            return False
    return True

def _page_title_text(schema: dict, page: dict) -> str:
    tprop = title_prop_name(schema)
    arr = (page.get("properties", {}).get(tprop, {}) or {}).get("title", []) or []
    return "".join(x.get("plain_text", "") for x in arr if isinstance(x, dict)).strip()

# ---------- upsert with guards + no-op + template skip ----------

def upsert(token: str, db_schema: dict, db_id: str, name: str, props: dict, dry: bool=False) -> str:
    # Hard skip if the *requested* name looks like a template
    if (name or "").strip().lower().startswith("template"):
        print(f"Skip template page by name: {name}")
        return "skipped-template"

    title_prop = title_prop_name(db_schema)
    page_id = query_page_by_title(token, db_id, title_prop, name)
    shaped = build_props_from_schema(db_schema, props)

    if page_id:
        if dry:
            print(f"DRY: update '{name}' ({page_id}) with {list(shaped.keys())}")
            return page_id
        page = get_page(token, page_id)
        # Safety: if the matched page is actually a template, skip
        if _page_title_text(db_schema, page).lower().startswith("template"):
            print(f"Skip template page: {name} ({page_id})")
            return page_id
        page_props = page.get("properties", {})
        if _normalized_subset_equal(db_schema, page_props, shaped):
            print(f"No change: {name}")
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

# ---------- CSV + section helpers ----------

def load_csv(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, newline='', encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [{k.strip(): (v or "").strip() for k, v in row.items()} for row in rdr]

def section_from_ctx(ctx_id: str) -> str:
    if not ctx_id:
        return ""
    parts = ctx_id.split("-")
    for p in parts:
        if p in ("K1", "K2", "K3", "K4"):
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

    # --- schema guard ---
    guard_required_props(schema_cipher, REQ_CIPHERTEXTS, "ciphertexts")
    guard_required_props(schema_base,   REQ_BASELINES,   "baselines")
    guard_required_props(schema_clues,  REQ_CLUES,       "clues")
    guard_required_props(schema_ctrls,  REQ_CONTROLS,    "controls")

    # 1) Ciphertexts
    sec_to_cipher_id: dict[str, str] = {}
    for r in load_csv(CIPHERTEXTS_CSV):
        section = r.get("section") or section_from_ctx(r.get("ctx_id", ""))
        if not section:
            continue
        name = f"{section} — base"
        props = {
            "Section": section,
            "Ciphertext (A–Z)": r.get("letters", ""),
            "Checksum": r.get("checksum", ""),
            "Normalization": "upper_strip_v1.0",
            "Version": 1,
        }
        pid = upsert(token, schema_cipher, db_cipher, name, props, dry_run)
        sec_to_cipher_id[section] = pid

    # 2) Baselines
    for r in load_csv(BASELINES_CSV):
        section = r.get("section") or section_from_ctx(r.get("ctx_id", ""))
        if not section:
            continue
        name = f"{section} — baseline"
        ctx_pid = sec_to_cipher_id.get(section)
        props = {
            "Section": section,
            "Plaintext Canon (A–Z)": r.get("plaintext", ""),
            "Version": 1,
            "CTX": ctx_pid,
        }
        upsert(token, schema_base, db_base, name, props, dry_run)

    # 3) Clues (seed) — hard-skip template-like titles
    for r in load_csv(CLUES_CSV):
        token_word = r.get("token") or r.get("Token") or ""
        ctx_id = r.get("ctx_id") or r.get("CTX") or ""
        section = r.get("section") or section_from_ctx(ctx_id) or "K4"
        name = (r.get("title") or f"{section}: {token_word}").strip()
        if name.lower().startswith("template"):
            print(f"Skip template-like clue row: {name}")
            continue
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

    # 4) Controls (seed) — hard-skip template-like titles
    for r in load_csv(CONTROLS_CSV):
        title = (r.get("title") or r.get("Title") or "").strip()
        if title.lower().startswith("template"):
            print(f"Skip template-like control row: {title}")
            continue
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
            title = f"CTL-{section or 'K?'}-auto"
        upsert(token, schema_ctrls, db_ctrls, title, props, dry_run)

    print("Done.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing to Notion")
    args = ap.parse_args()
    sync(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
