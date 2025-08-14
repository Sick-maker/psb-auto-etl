#!/usr/bin/env python3
import os, requests, json, sys

API = "https://api.notion.com/v1"
TOKEN = os.getenv("NOTION_TOKEN")
DBS = {
    "runs": os.getenv("NOTION_DB_RUNS",""),
    "results": os.getenv("NOTION_DB_RESULTS",""),
    "artifacts": os.getenv("NOTION_DB_ARTIFACTS",""),
    "briefings": os.getenv("NOTION_DB_BRIEFINGS",""),
}
HEADERS = {"Authorization": f"Bearer {TOKEN}" if TOKEN else "", "Notion-Version":"2022-06-28"}

def main():
    print("Secrets present:", {k: bool(os.getenv(k)) for k in ["NOTION_TOKEN","NOTION_DB_RUNS","NOTION_DB_RESULTS","NOTION_DB_ARTIFACTS","NOTION_DB_BRIEFINGS"]})
    if not TOKEN:
        print("No NOTION_TOKEN; aborting."); return 1
    for kind, db in DBS.items():
        if not db:
            print(f"[{kind}] missing DB id"); continue
        r = requests.get(f"{API}/databases/{db}", headers=HEADERS, timeout=60)
        print(f"[{kind}] GET /databases/{db} → {r.status_code}")
        if r.status_code != 200:
            print(r.text[:500])
            continue
        data = r.json()
        props = {k:v.get("type") for k,v in data.get("properties",{}).items()}
        title_name = next((k for k,v in data.get("properties",{}).items() if v.get("type")=="title"), "Name")
        print(f"  title property: {title_name}")
        print(f"  properties: {props}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
