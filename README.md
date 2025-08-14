# Project Sand Born — GitHub → Notion Auto-ETL (v0.6)

This repository lets you **drop PSB-RUN bundles into `bundles/`**, and a GitHub Action will:

1) Validate the bundle against the **Bible v0.6** schemas,  
2) Run ETL to produce CSVs (Runs / Results / Artifacts / Briefings),  
3) **Upsert** those rows into your **Notion** databases via the Notion API.

No local coding is required after setup.

## Quick Setup (once)

### 0) Create a Notion integration
- In Notion, go to **Settings → Connections → Develop or manage integrations**.
- Create a new **Internal Integration** (name: `PSB-ETL`), copy the **Secret**.

### 1) Create four Notion databases (viewer role)
Create these databases in your Notion workspace with **exact property names**:
- **Runs** — Properties: `Title (title)`, `RUN ID (text)`, `Experiment (text)`, `Hypothesis (text)`, `Ciphertext (text)`, `Method (text)`, `Scoring (text)`, `PRNG (text)`, `Seed (number or text)`, `Env Hash (text)`, `Code Commit (text)`, `Stop Condition (text)`, `Status (select)`, `CPUh (number)`, `Wall Minutes (number)`, `Peak Mem MB (number)`, `Iterations (number)`, `Candidates/sec (number)`
- **Results Summaries** — Properties: `Title (title)`, `RUN (text)`, `Best Score (number)`, `Avg Score (number)`, `Median (number)`, `p10 (number)`, `p90 (number)`, `Composite Z (number)`, `Chi2 Z (number)`, `Quadgram Z (number)`, `WordRate Z (number)`, `TopN Table (text)`, `Score Histogram (text)`, `Param Sweep (text)`
- **Artifacts** — Properties: `Title (title)`, `RUN (text)`, `Type (select)`, `Path/URL (text)`, `Checksum (text)`, `Mime (text)`, `Size Bytes (number)`
- **Briefings** — Properties: `Title (title)`, `RUN (text)`, `Version (text)`, `Header (rich_text)`, `Technical (rich_text)`, `Broad (rich_text)`

> Tip: You can create each DB by **Importing** the CSV templates in `templates/notion/` once, then adjust property types.

Share each database with your `PSB-ETL` integration (top-right **Share → Invite → PSB-ETL**).

### 2) Get your database IDs
Open a database as a full page in the browser. The URL contains a UUID (with or without hyphens). Keep the hyphenated UUID form — that is your **database ID**.

### 3) Configure GitHub Secrets
Go to **Settings → Secrets and variables → Actions**, and add:
- `NOTION_TOKEN` — the integration secret from step 0
- `NOTION_DB_RUNS`
- `NOTION_DB_RESULTS`
- `NOTION_DB_ARTIFACTS`
- `NOTION_DB_BRIEFINGS`

### 4) Push this repo to GitHub

```
git init
git add .
git commit -m "PSB GitHub→Notion Auto-ETL v0.6"
git branch -M main
git remote add origin <YOUR_REPO_URL>
git push -u origin main
```

## Daily use
- Drop each PSB-RUN bundle under `bundles/<RUN_ID>/` (it should contain `manifest.json`, `results_summary.json`, `briefing.md`, etc.).
- Commit and push. The workflow `psb-etl.yml` will run automatically. After success, check your Notion DBs.

If you ever want to bypass Notion and just keep CSVs, disable the “Sync to Notion” step in the workflow; the CSVs remain under `out/` in the repo artifacts.

---
