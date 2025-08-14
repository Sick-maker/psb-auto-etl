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

def find_title_prop_name(db_json):
    for name, meta in db_json.get("properties", {}).items():
        if meta.get("type") == "title":
            return name
    return "Name"  # default fallback in many workspaces

def prop_type_map(db_json):
    return {name: meta.get("type") for name, meta in db_json.get("properties", {}).items()}

def filter_payload(prop_name, prop_type, key):
    if prop_type == "title":
        return {"filter": {"property": prop_name, "title": {"equals": key}}}
    if prop_type == "number":
        try: num = float(key)
        except: num = None
        return {"filter": {"property": prop_name, "number": {"equals": num}}}
    return {"filter": {"property": prop_name, "rich_text": {"equals": key}}}

def mk_title(v): return {"title":[{"type":"text","text":{"content": v or ""}}]}
def mk_text(v):  return {"rich_text":[{"type":"text","text":{"content": v or ""}}]}
def mk_number(v):
    try: return {"number": float(v)} if str(v).strip() != "" else {"number": None}
    except: return {"number": None}
def mk_select(v): return {"select": {"name": v}} if v else {"select": None}

def build_props(db_kind, row, db_schema):
    # Only include properties that actually exist in the DB schema
    props = {}
    def add(name, val):
        if name in db_schema:
            props[name] = val
        else:
            print(f"SKIP: property '{name}' not in DB schema; ignoring for {db_kind}")
    if db_kind == "runs":
        add("Title", mk_title(row.get("Title","")))
        add("RUN ID", mk_text(row.get("RUN ID","")))
        add("Experiment", mk_text(row.get("Experiment","")))
        add("Hypothesis", mk_text(row.get("Hypothesis","")))
        add("Ciphertext", mk_text(row.get("Ciphertext","")))
        add("Method", mk_text(row.get("Method","")))
        add("Scoring", mk_text(row.get("Scoring","")))
        add("PRNG", mk_text(row.get("PRNG","")))
        add("Seed", mk_text(row.get("Seed","")))
        add("Env Hash", mk_text(row.get("Env Hash","")))
        add("Code Commit", mk_text(row.get("Code Commit","")))
        add("Stop Condition", mk_text(row.get("Stop Condition","")))
        add("Status", mk_select(row.get("Status","")))
        add("CPUh", mk_number(row.get("CPUh","")))
        add("Wall Minutes", mk_number(row.get("Wall Minutes","")))
        add("Peak Mem MB", mk_number(row.get("Peak Mem MB","")))
        add("Iterations", mk_number(row.get("Iterations","")))
        add("Candidates/sec", mk_number(row.get("Candidates/sec","")))
        return props
    if db_kind == "results":
        add("Title", mk_title(row.get("Title","")))
        add("RUN", mk_text(row.get("RUN","")))
        add("Best Score", mk_number(row.get("Best Score","")))
        add("Avg Score", mk_number(row.get("Avg Score","")))
        add("Median", mk_number(row.get("Median","")))
        add("p10", mk_number(row.get("p10","")))
        add("p90", mk_number(row.get("p90","")))
        add("Composite Z", mk_number(row.get("Composite Z","")))
        add("Chi2 Z", mk_number(row.get("Chi2 Z","")))
        add("Quadgram Z", mk_number(row.get("Quadgram Z","")))
        add("WordRate Z", mk_number(row.get("WordRate Z","")))
        add("TopN Table", mk_text(row.get("TopN Table","")))
        add("Score Histogram", mk_text(row.get("Score Histogram","")))
        add("Param Sweep", mk_text(row.get("Param Sweep","")))
        return props
    if db_kind == "artifacts":
        add("Title", mk_title(row.get("Title","")))
        add("RUN", mk_text(row.get("RUN","")))
        add("Type", mk_select(row.get("Type","")))
        add("Path/URL", mk_text(row.get("Path/URL","")))
        add("Checksum", mk_text(row.get("Checksum","")))
        add("Mime", mk_text(row.get("Mime","")))
        add("Size Bytes", mk_number(row.get("Size Bytes","")))
        return props
    if db_kind == "briefings":
        add("Title", mk_title(row.get("Title","")))
        add("RUN", mk_text(row.get("RUN","")))
        add("Version", mk_text(row.get("Version","")))
        add("Header", mk_text(row.get("Header","")))
        add("Technical", mk_text(row.get("Technical","")))
        add("Broad", mk_text(row.get("Broad","")))
        return props
    raise ValueError("Unknown db kind")

def upsert(db_id, db_kind, csv_path, key_prop, db_json):
    if not os.path.exists(csv_path): return
    schema = db_json.get("properties", {})
    types = {k:v.get("type") for k,v in schema.items()}
    key_type = types.get(key_prop, "rich_text")
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_prop, "")
            payload = filter_payload(key_prop, key_type, key)
            q = requests.post(f"{API}/databases/{db_id}/query", headers=HEADERS, json=payload, timeout=60)
            if q.status_code != 200:
                print(f"ERROR: query failed ({db_kind}/{key_prop}): {q.status_code} {q.text[:300]}")
                q.raise_for_status()
            results = q.json().get("results", [])
            props = build_props(db_kind, row, types)
            if DRY:
                print(f"DRY: would {'update' if results else 'create'} {db_kind} for key={key}; props={list(props.keys())}")
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
        print("Missing Notion secrets. Skipping sync.")
        return
    # Get schemas
    runs_db = get_db(DB_RUNS)
    results_db = get_db(DB_RESULTS)
    artifacts_db = get_db(DB_ARTIFACTS)
    briefings_db = get_db(DB_BRIEFINGS)
    # Upsert
    upsert(DB_RUNS, "runs", os.path.join(out_dir, "runs.csv"), "RUN ID", runs_db)
    upsert(DB_RESULTS, "results", os.path.join(out_dir, "results_summaries.csv"), "RUN", results_db)
    upsert(DB_ARTIFACTS, "artifacts", os.path.join(out_dir, "artifacts.csv"), "Title", artifacts_db)
    upsert(DB_BRIEFINGS, "briefings", os.path.join(out_dir, "briefings.csv"), "RUN", briefings_db)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/notion_sync.py <OUT_DIR>"); sys.exit(1)
    main(sys.argv[1])
