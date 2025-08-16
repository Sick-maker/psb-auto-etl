#!/usr/bin/env python3
"""
Push MTH (methods) and SFX (scoring) seed rows into Notion.

Env:
  NOTION_TOKEN   - Notion internal integration token
  DB_MTH         - Notion database id for MTH
  DB_SFX         - Notion database id for SFX

CSV:
  data/methods_seed.csv    (name, description, parameters_schema, version, status)
  data/scoring_seed.csv    (name, formula, conventions, version, status)

Usage:
  python tools/notion_sync_methods_scoring.py [--dry-run]
"""

import argparse, csv, os, typing as t, requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.environ.get("NOTION_VERSION", "2022-06-28")
DATA_DIR   = os.environ.get("DATA_DIR", "data")
MTH_CSV    = os.path.join(DATA_DIR, "methods_seed.csv")
SFX_CSV    = os.path.join(DATA_DIR, "scoring_seed.csv")

# ---------- helpers ----------
def env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Missing required env var: {name}")
    return v

def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VER, "Content-Type": "application/json"}

def get_db_schema(token: str, db_id: str) -> dict:
    r = requests.get(f"{NOTION_API}/databases/{db_id}", headers=headers(token)); r.raise_for_status(); return r.json()

def title_prop_name(db_schema: dict) -> str:
    for pname, pdef in db_schema.get("properties", {}).items():
        if pdef.get("type") == "title": return pname
    raise RuntimeError("No title property found in database.")

def notion_rich_text(s: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": s}}]}

def ensure_select(value: str) -> dict:
    return {"select": {"name": value}}

def build_props_from_schema(schema: dict, desired: dict) -> dict:
    out, props = {}, schema.get("properties", {})
    for pname, raw in desired.items():
        if raw is None or pname not in props: continue
        ptype = props[pname]["type"]
        if ptype == "rich_text": out[pname] = notion_rich_text(str(raw))
        elif ptype == "number":
            try: out[pname] = {"number": float(raw)}
            except Exception: pass
        elif ptype == "select": out[pname] = ensure_select(str(raw))
        elif ptype in ("url","email","phone_number"): out[pname] = {ptype: str(raw)}
        elif ptype == "checkbox": out[pname] = {"checkbox": bool(raw)}
        # title handled separately; unknowns skipped
    return out

def query_page_by_title(token: str, db_id: str, title_prop: str, name: str) -> t.Optional[str]:
    r = requests.post(f"{NOTION_API}/databases/{db_id}/query", headers=headers(token),
                      json={"filter":{"property": title_prop,"title":{"equals": name}}, "page_size":1})
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None

def create_page(token: str, db_id: str, title_prop: str, name: str, props: dict) -> str:
    payload = {"parent":{"database_id": db_id}, "properties": {**props, title_prop: {"title":[{"type":"text","text":{"content":name}}]}}}
    r = requests.post(f"{NOTION_API}/pages", headers=headers(token), json=payload); r.raise_for_status(); return r.json()["id"]

def update_page(token: str, page_id: str, props: dict) -> None:
    r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=headers(token), json={"properties": props}); r.raise_for_status()

def upsert(token: str, db_schema: dict, db_id: str, name: str, props: dict, dry: bool=False) -> str:
    title_prop = title_prop_name(db_schema)
    page_id = query_page_by_title(token, db_id, title_prop, name)
    shaped  = build_props_from_schema(db_schema, props)
    if page_id:
        if dry:
            print(f"DRY: update '{name}' ({page_id}) with {list(shaped.keys())}")
            return page_id
        update_page(token, page_id, shaped); print(f"Updated: {name}"); return page_id
    else:
        if dry:
            print(f"DRY: create '{name}' with {list(shaped.keys())}")
            return "dry-created"
        nid = create_page(token, db_id, title_prop, name, shaped); print(f"Created: {name}"); return nid

def load_csv(path: str) -> list[dict]:
    if not os.path.exists(path): return []
    with open(path, newline='', encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [{k.strip(): (v or "").strip() for k,v in row.items()} for row in rdr]

def assert_required(schema: dict, required: list[str], label: str) -> None:
    have = set(schema.get("properties", {}).keys())
    missing = [p for p in required if p not in have]
    if missing:
        raise SystemExit(f"[{label}] Missing required properties in Notion DB: {', '.join(missing)}")

# ---------- sync ----------
def sync(dry_run: bool=False):
    token = env("NOTION_TOKEN")
    db_mth = env("DB_MTH")
    db_sfx = env("DB_SFX")

    schema_mth = get_db_schema(token, db_mth)
    schema_sfx = get_db_schema(token, db_sfx)

    # Guards (so we fail loudly if someone renamed props)
    assert_required(schema_mth, ["Description", "Parameters Schema", "Version", "Status"], "MTH")
    assert_required(schema_sfx, ["Formula", "Conventions", "Version", "Status"], "SFX")

    # MTH
    for r in load_csv(MTH_CSV):
        name = r.get("name") or r.get("Name"); if not name: continue
        props = {
            "Description": r.get("description") or r.get("Description"),
            "Parameters Schema": r.get("parameters_schema") or r.get("Parameters Schema"),
            "Version": r.get("version") or r.get("Version") or 1,
            "Status": r.get("status") or r.get("Status") or "Ready",
        }
        upsert(token, schema_mth, db_mth, name, props, dry_run)

    # SFX
    for r in load_csv(SFX_CSV):
        name = r.get("name") or r.get("Name"); if not name: continue
        props = {
            "Formula": r.get("formula") or r.get("Formula"),
            "Conventions": r.get("conventions") or r.get("Conventions"),
            "Version": r.get("version") or r.get("Version") or 1,
            "Status": r.get("status") or r.get("Status") or "Ready",
        }
        upsert(token, schema_sfx, db_sfx, name, props, dry_run)

    print("Done.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing to Notion")
    args = ap.parse_args()
    sync(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
