#!/usr/bin/env python3
"""
notion_sync_methods_scoring.py
Sync /data/methods_seed.csv and /data/scoring_seed.csv into Notion DBs.

Env vars:
  NOTION_TOKEN  - Notion internal integration token
  DB_MTH        - Notion database id for methods (MTH)
  DB_SFX        - Notion database id for scoring/score-functions (SFX)

Usage:
  python tools/notion_sync_methods_scoring.py [--dry-run]
"""

import os
import csv
import argparse
import typing as t

import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.environ.get("NOTION_VERSION", "2022-06-28")

DATA_DIR = os.environ.get("DATA_DIR", "data")
METHODS_CSV  = os.path.join(DATA_DIR, "methods_seed.csv")
SCORING_CSV  = os.path.join(DATA_DIR, "scoring_seed.csv")


# ----------------- helpers -----------------

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


def load_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in rdr]


def get_db_schema(token: str, db_id: str) -> dict:
    r = requests.get(f"{NOTION_API}/databases/{db_id}", headers=headers(token))
    r.raise_for_status()
    return r.json()


def title_prop_name(db_schema: dict) -> str:
    for pname, pdef in db_schema.get("properties", {}).items():
        if pdef.get("type") == "title":
            return pname
    raise RuntimeError("No title property found in database.")


def rich_text(s: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": s}}]}


def ensure_select(value: str) -> dict:
    return {"select": {"name": value}}


def ensure_status(value: str) -> dict:
    return {"status": {"name": value}}


def build_props_from_schema(schema: dict, desired: dict) -> dict:
    """
    Shape desired {prop: raw} into Notion property payloads, skipping unknown props.
    """
    out = {}
    props = schema.get("properties", {})
    for pname, raw in desired.items():
        if raw is None or pname not in props:
            continue
        ptype = props[pname]["type"]

        if ptype == "title":
            # set in create payload; ignore here
            continue
        elif ptype == "rich_text":
            out[pname] = rich_text(str(raw))
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
            vals = raw if isinstance(raw, (list, tuple)) else [raw]
            out[pname] = {"multi_select": [{"name": str(x)} for x in vals]}
        elif ptype in ("url", "email", "phone_number"):
            out[pname] = {ptype: str(raw)}
        elif ptype == "checkbox":
            out[pname] = {"checkbox": str(raw).lower() in ("1", "true", "yes")}
        # other types are no-op
    return out


def query_page_by_title(token: str, db_id: str, title_prop: str, name: str) -> t.Optional[str]:
    url = f"{NOTION_API}/databases/{db_id}/query"
    payload = {
        "filter": {"property": title_prop, "title": {"equals": name}},
        "page_size": 1,
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
        "properties": {**props, title_prop: {"title": [{"type": "text", "text": {"content": name}}]}},
    }
    r = requests.post(f"{NOTION_API}/pages", headers=headers(token), json=payload)
    r.raise_for_status()
    return r.json()["id"]


def update_page(token: str, page_id: str, props: dict) -> None:
    payload = {"properties": props}
    r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=headers(token), json=payload)
    r.raise_for_status()


def upsert(token: str, db_schema: dict, db_id: str, name: str, props: dict, dry: bool = False) -> str:
    """
    Create or update page by its title.
    """
    title_prop = title_prop_name(db_schema)
    shaped = build_props_from_schema(db_schema, props)
    page_id = query_page_by_title(token, db_id, title_prop, name)

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


# ----------------- main sync -----------------

def sync(dry_run: bool = False) -> None:
    token   = env("NOTION_TOKEN")
    db_mth  = env("DB_MTH")
    db_sfx  = env("DB_SFX")

    schema_mth = get_db_schema(token, db_mth)
    schema_sfx = get_db_schema(token, db_sfx)

    # 1) Methods
    for r in load_csv(METHODS_CSV):
        name = r.get("name") or r.get("Name")
        if not name:
            continue

        props = {
            # These keys are “friendly” and will be filtered to what exists in your DB.
            "Description": r.get("description") or r.get("Description"),
            "Parameters Schema": r.get("parameters_schema") or r.get("Parameters Schema"),
            "Version": r.get("version") or r.get("Version") or 1,
            "Status": r.get("status") or r.get("Status") or "Ready",
            "Notes": r.get("notes") or r.get("Notes"),
        }
        upsert(token, schema_mth, db_mth, name, props, dry_run)

    # 2) Scoring
    for r in load_csv(SCORING_CSV):
        title = r.get("title") or r.get("Title")
        if not title:
            continue

        props = {
            "Formula": r.get("formula") or r.get("Formula"),
            "Conventions": r.get("conventions") or r.get("Conventions"),
            "Notes": r.get("notes") or r.get("Notes"),
            "Version": r.get("version") or r.get("Version") or "v2.1",
        }
        upsert(token, schema_sfx, db_sfx, title, props, dry_run)

    print("Done.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing to Notion")
    args = ap.parse_args()
    sync(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
