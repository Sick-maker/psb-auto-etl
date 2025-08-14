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

def notion_query(db_id, prop_name, value):
    url = f"{API}/databases/{db_id}/query"
    payload = {"filter": {"property": prop_name, "rich_text": {"equals": value}}}
    r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json().get("results", [])

def page_props_from_map(db_kind, row):
    # Map CSV columns to Notion properties (text/rich_text/title/number/select)
    def title(v): return {"title":[{"type":"text","text":{"content": v or ""}}]}
    def text(v):  return {"rich_text":[{"type":"text","text":{"content": v or ""}}]}
    def number(v):
        try: return {"number": float(v)} if str(v).strip() != "" else {"number": None}
        except: return {"number": None}
    def sel(v): return {"select": {"name": v}} if v else {"select": None}

    if db_kind == "runs":
        return {
            "Title": title(row["Title"]),
            "RUN ID": text(row["RUN ID"]),
            "Experiment": text(row["Experiment"]),
            "Hypothesis": text(row["Hypothesis"]),
            "Ciphertext": text(row["Ciphertext"]),
            "Method": text(row["Method"]),
            "Scoring": text(row["Scoring"]),
            "PRNG": text(row["PRNG"]),
            "Seed": text(row["Seed"]),
            "Env Hash": text(row["Env Hash"]),
            "Code Commit": text(row["Code Commit"]),
            "Stop Condition": text(row["Stop Condition"]),
            "Status": sel(row["Status"]),
            "CPUh": number(row["CPUh"]),
            "Wall Minutes": number(row["Wall Minutes"]),
            "Peak Mem MB": number(row["Peak Mem MB"]),
            "Iterations": number(row["Iterations"]),
            "Candidates/sec": number(row["Candidates/sec"]),
        }
    if db_kind == "results":
        return {
            "Title": title(row["Title"]),
            "RUN": text(row["RUN"]),
            "Best Score": number(row["Best Score"]),
            "Avg Score": number(row["Avg Score"]),
            "Median": number(row["Median"]),
            "p10": number(row["p10"]),
            "p90": number(row["p90"]),
            "Composite Z": number(row["Composite Z"]),
            "Chi2 Z": number(row["Chi2 Z"]),
            "Quadgram Z": number(row["Quadgram Z"]),
            "WordRate Z": number(row["WordRate Z"]),
            "TopN Table": text(row["TopN Table"]),
            "Score Histogram": text(row["Score Histogram"]),
            "Param Sweep": text(row["Param Sweep"]),
        }
    if db_kind == "artifacts":
        return {
            "Title": title(row["Title"]),
            "RUN": text(row["RUN"]),
            "Type": sel(row["Type"]),
            "Path/URL": text(row["Path/URL"]),
            "Checksum": text(row["Checksum"]),
            "Mime": text(row["Mime"]),
            "Size Bytes": number(row["Size Bytes"]),
        }
    if db_kind == "briefings":
        return {
            "Title": title(row["Title"]),
            "RUN": text(row["RUN"]),
            "Version": text(row["Version"]),
            "Header": text(row["Header"]),
            "Technical": text(row["Technical"]),
            "Broad": text(row["Broad"]),
        }
    raise ValueError("Unknown db kind")

def upsert(db_id, db_kind, csv_path, key_prop):
    if not os.path.exists(csv_path): return
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row[key_prop]
            # query
            results = notion_query(db_id, key_prop, key)
            props = page_props_from_map(db_kind, row)
            if results:
                page_id = results[0]["id"]
                r = requests.patch(f"{API}/pages/{page_id}", headers=HEADERS, json={"properties": props}, timeout=60)
                r.raise_for_status()
                print(f"Updated {db_kind} page for {key}")
            else:
                r = requests.post(f"{API}/pages", headers=HEADERS, json={"parent":{"database_id": db_id},"properties": props}, timeout=60)
                r.raise_for_status()
                print(f"Created {db_kind} page for {key}")
            time.sleep(0.3)  # be gentle

def main(out_dir):
    if not need_env(): return
    upsert(DB_RUNS, "runs", os.path.join(out_dir, "runs.csv"), "RUN ID")
    upsert(DB_RESULTS, "results", os.path.join(out_dir, "results_summaries.csv"), "RUN")
    upsert(DB_ARTIFACTS, "artifacts", os.path.join(out_dir, "artifacts.csv"), "Title")
    upsert(DB_BRIEFINGS, "briefings", os.path.join(out_dir, "briefings.csv"), "RUN")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/notion_sync.py <OUT_DIR>"); sys.exit(1)
    main(sys.argv[1])
