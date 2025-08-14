#!/usr/bin/env python3
import os, sys, csv, json, time, requests

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DB_RUNS = os.getenv("NOTION_DB_RUNS")
DB_RESULTS = os.getenv("NOTION_DB_RESULTS")
DB_ARTIFACTS = os.getenv("NOTION_DB_ARTIFACTS")
DB_BRIEFINGS = os.getenv("NOTION_DB_BRIEFINGS")

API = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def need_env():
    if not (NOTION_TOKEN and DB_RUNS and DB_RESULTS and DB_ARTIFACTS and DB_BRIEFINGS):
        print("Missing Notion secrets. Skipping sync.")
        return False
    return True

def get_db_props(db_id):
    url = f"{API}/databases/{db_id}"
    r = requests.get(url, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        print("WARN: Could not fetch DB schema:", db_id, r.status_code, r.text[:300])
        return {}
    data = r.json()
    return {k: v.get("type") for k, v in data.get("properties", {}).items()}

def notion_query(db_id, prop_name, prop_type, value):
    url = f"{API}/databases/{db_id}/query"
    if prop_type == "title":
        filt = {"property": prop_name, "title": {"equals": value}}
    elif prop_type == "number":
        try:
            value = float(value)
        except:
            value = None
        filt = {"property": prop_name, "number": {"equals": value}}
    else:
        # default to rich_text
        filt = {"property": prop_name, "rich_text": {"equals": value}}
    payload = {"filter": filt}
    r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    if r.status_code != 200:
        print("ERROR: Query failed", db_id, prop_name, prop_type, r.status_code, r.text[:300])
        r.raise_for_status()
    return r.json().get("results", [])

def page_props_from_map(db_kind, row):
    # Map CSV columns to Notion properties (title/rich_text/number/select)
    def title(v): return {"title":[{"type":"text","text":{"content": v or ""}}]}
    def text(v):  return {"rich_text":[{"type":"text","text":{"content": v or ""}}]}
    def number(v):
        try: return {"number": float(v)} if str(v).strip() != "" else {"number": None}
        except: return {"number": None}
    def sel(v): return {"select": {"name": v}} if v else {"select": None}

    if db_kind == "runs":
        return {
            "Title": title(row.get("Title","")),
            "RUN ID": text(row.get("RUN ID","")),
            "Experiment": text(row.get("Experiment","")),
            "Hypothesis": text(row.get("Hypothesis","")),
            "Ciphertext": text(row.get("Ciphertext","")),
            "Method": text(row.get("Method","")),
            "Scoring": text(row.get("Scoring","")),
            "PRNG": text(row.get("PRNG","")),
            "Seed": text(row.get("Seed","")),
            "Env Hash": text(row.get("Env Hash","")),
            "Code Commit": text(row.get("Code Commit","")),
            "Stop Condition": text(row.get("Stop Condition","")),
            "Status": sel(row.get("Status","")),
            "CPUh": number(row.get("CPUh","")),
            "Wall Minutes": number(row.get("Wall Minutes","")),
            "Peak Mem MB": number(row.get("Peak Mem MB","")),
            "Iterations": number(row.get("Iterations","")),
            "Candidates/sec": number(row.get("Candidates/sec","")),
        }
    if db_kind == "results":
        return {
            "Title": title(row.get("Title","")),
            "RUN": text(row.get("RUN","")),
            "Best Score": number(row.get("Best Score","")),
            "Avg Score": number(row.get("Avg Score","")),
            "Median": number(row.get("Median","")),
            "p10": number(row.get("p10","")),
            "p90": number(row.get("p90","")),
            "Composite Z": number(row.get("Composite Z","")),
            "Chi2 Z": number(row.get("Chi2 Z","")),
            "Quadgram Z": number(row.get("Quadgram Z","")),
            "WordRate Z": number(row.get("WordRate Z","")),
            "TopN Table": text(row.get("TopN Table","")),
            "Score Histogram": text(row.get("Score Histogram","")),
            "Param Sweep": text(row.get("Param Sweep","")),
        }
    if db_kind == "artifacts":
        return {
            "Title": title(row.get("Title","")),
            "RUN": text(row.get("RUN","")),
            "Type": sel(row.get("Type","")),
            "Path/URL": text(row.get("Path/URL","")),
            "Checksum": text(row.get("Checksum","")),
            "Mime": text(row.get("Mime","")),
            "Size Bytes": number(row.get("Size Bytes","")),
        }
    if db_kind == "briefings":
        return {
            "Title": title(row.get("Title","")),
            "RUN": text(row.get("RUN","")),
            "Version": text(row.get("Version","")),
            "Header": text(row.get("Header","")),
            "Technical": text(row.get("Technical","")),
            "Broad": text(row.get("Broad","")),
        }
    raise ValueError("Unknown db kind")

def upsert(db_id, db_kind, csv_path, key_prop, prop_types):
    if not os.path.exists(csv_path): return
    key_type = prop_types.get(key_prop, "rich_text")
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row[key_prop]
            results = notion_query(db_id, key_prop, key_type, key)
            props = page_props_from_map(db_kind, row)
            if results:
                page_id = results[0]["id"]
                r = requests.patch(f"{API}/pages/{page_id}", headers=HEADERS, json={"properties": props}, timeout=60)
                if r.status_code != 200:
                    print("ERROR: Update failed", db_kind, key, r.status_code, r.text[:300]); r.raise_for_status()
                print(f"Updated {db_kind} page for {key}")
            else:
                r = requests.post(f"{API}/pages", headers=HEADERS, json={"parent":{"database_id": db_id},"properties": props}, timeout=60)
                if r.status_code != 200:
                    print("ERROR: Create failed", db_kind, key, r.status_code, r.text[:300]); r.raise_for_status()
                print(f"Created {db_kind} page for {key}")
            time.sleep(0.3)  # rate limiting

def main(out_dir):
    if not need_env(): return
    dbs = {
        "runs": (DB_RUNS, "RUN ID"),
        "results": (DB_RESULTS, "RUN"),
        "artifacts": (DB_ARTIFACTS, "Title"),   # Title is a title-type property
        "briefings": (DB_BRIEFINGS, "RUN"),
    }
    # Pull schema once for each DB to pick correct filter type
    types = {kind: get_db_props(db_id) for kind,(db_id,_) in dbs.items()}
    upsert(DB_RUNS, "runs", os.path.join(out_dir, "runs.csv"), "RUN ID", types["runs"])
    upsert(DB_RESULTS, "results", os.path.join(out_dir, "results_summaries.csv"), "RUN", types["results"])
    upsert(DB_ARTIFACTS, "artifacts", os.path.join(out_dir, "artifacts.csv"), "Title", types["artifacts"])
    upsert(DB_BRIEFINGS, "briefings", os.path.join(out_dir, "briefings.csv"), "RUN", types["briefings"])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/notion_sync.py <OUT_DIR>"); sys.exit(1)
    main(sys.argv[1])
