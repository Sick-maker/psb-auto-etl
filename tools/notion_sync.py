#!/usr/bin/env python3
import os, sys, csv, json, time, requests

API = "https://api.notion.com/v1"
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

DB_RUNS = os.getenv("NOTION_DB_RUNS")
DB_RESULTS = os.getenv("NOTION_DB_RESULTS")
DB_ARTIFACTS = os.getenv("NOTION_DB_ARTIFACTS")
DB_BRIEFINGS = os.getenv("NOTION_DB_BRIEFINGS")
DRY = os.getenv("PSB_DRY_RUN","0") == "1"

def get_db(db_id):
    r = requests.get(f"{API}/databases/{db_id}", headers=HEADERS, timeout=60)
    if r.status_code != 200:
        print(f"ERROR: cannot read DB {db_id}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    return r.json()

def make_filter(prop_name, prop_type, value):
    if prop_type == "title":
        return {"filter": {"property": prop_name, "title": {"equals": value}}}
    if prop_type == "number":
        try: num = float(value)
        except: num = None
        return {"filter": {"property": prop_name, "number": {"equals": num}}}
    # status/select do not support equals in DB queries; key props for us are title/rich_text/number.
    return {"filter": {"property": prop_name, "rich_text": {"equals": value}}}

def v_title(v): return {"title":[{"type":"text","text":{"content": v or ""}}]}
def v_text(v):  return {"rich_text":[{"type":"text","text":{"content": v or ""}}]}
def v_number(v):
    try: return {"number": float(v)} if str(v).strip() != "" else {"number": None}
    except: return {"number": None}
def v_select(v): return {"select": {"name": v}} if v else {"select": None}
def v_status(v): return {"status": {"name": v}} if v else {"status": None}

def adapt(prop_type, value):
    if prop_type == "title":   return v_title(value)
    if prop_type == "number":  return v_number(value)
    if prop_type == "status":  return v_status(value)
    if prop_type == "select":  return v_select(value)
    # fallback
    return v_text(value)

def build_props(db_kind, row, types):
    # Only include properties that exist; adapt value to the DB's property type
    props = {}
    def add(name):
        if name in types:
            props[name] = adapt(types[name], row.get(name, ""))
        else:
            print(f"SKIP: '{name}' not in schema for {db_kind}")
    if db_kind == "runs":
        for name in ["Title","RUN ID","Experiment","Hypothesis","Ciphertext","Method","Scoring",
                     "PRNG","Seed","Env Hash","Code Commit","Stop Condition","Status",
                     "CPUh","Wall Minutes","Peak Mem MB","Iterations","Candidates/sec"]:
            add(name)
    elif db_kind == "results":
        for name in ["Title","RUN","Best Score","Avg Score","Median","p10","p90",
                     "Composite Z","Chi2 Z","Quadgram Z","WordRate Z",
                     "TopN Table","Score Histogram","Param Sweep"]:
            add(name)
    elif db_kind == "artifacts":
        for name in ["Title","RUN","Type","Path/URL","Checksum","Mime","Size Bytes"]:
            add(name)
    elif db_kind == "briefings":
        for name in ["Title","RUN","Version","Header","Technical","Broad"]:
            add(name)
    else:
        raise ValueError("Unknown db kind")
    return props

def upsert(db_id, db_kind, csv_path, key_prop, db_json):
    if not os.path.exists(csv_path): return
    types = {k:v.get("type") for k,v in db_json.get("properties", {}).items()}
    key_type = types.get(key_prop, "rich_text")

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_prop, "")
            q = requests.post(f"{API}/databases/{db_id}/query",
                              headers=HEADERS,
                              json=make_filter(key_prop, key_type, key),
                              timeout=60)
            if q.status_code != 200:
                print(f"ERROR: query failed ({db_kind}/{key_prop}): {q.status_code} {q.text[:300]}")
                q.raise_for_status()
            results = q.json().get("results", [])
            props = build_props(db_kind, row, types)
            if DRY:
                print(f"DRY: would {'update' if results else 'create'} {db_kind} key={key} with props={list(props.keys())}")
                continue
            if results:
                page_id = results[0]["id"]
                r = requests.patch(f"{API}/pages/{page_id}", headers=HEADERS, json={"properties": props}, timeout=60)
                if r.status_code != 200:
                    print(f"ERROR: update failed ({db_kind}/{key}): {r.status_code} {r.text[:300]}"); r.raise_for_status()
                print(f"Updated {db_kind} page for {key}")
            else:
                r = requests.post(f"{API}/pages", headers=HEADERS, json={"parent":{"database_id": db_id},"properties": props}, timeout=60)
                if r.status_code != 200:
                    print(f"ERROR: create failed ({db_kind}/{key}): {r.status_code} {r.text[:300]}"); r.raise_for_status()
                print(f"Created {db_kind} page for {key}")
            time.sleep(0.3)

def main(out_dir):
    required = [NOTION_TOKEN, DB_RUNS, DB_RESULTS, DB_ARTIFACTS, DB_BRIEFINGS]
    if not all(required):
        print("Missing Notion secrets. Skipping sync."); return
    runs_db     = get_db(DB_RUNS)
    results_db  = get_db(DB_RESULTS)
    artifacts_db= get_db(DB_ARTIFACTS)
    briefings_db= get_db(DB_BRIEFINGS)
    upsert(DB_RUNS, "runs",     os.path.join(out_dir, "runs.csv"),              "RUN ID", runs_db)
    upsert(DB_RESULTS, "results", os.path.join(out_dir, "results_summaries.csv"), "RUN", results_db)
    upsert(DB_ARTIFACTS,"artifacts",os.path.join(out_dir, "artifacts.csv"),      "Title", artifacts_db)
    upsert(DB_BRIEFINGS,"briefings",os.path.join(out_dir, "briefings.csv"),      "RUN", briefings_db)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/notion_sync.py <OUT_DIR>"); sys.exit(1)
    main(sys.argv[1])
