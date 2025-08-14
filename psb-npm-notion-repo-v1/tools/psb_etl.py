#!/usr/bin/env python3
import os, sys, json, csv, mimetypes

def read_json(path):
    with open(path, 'r', encoding='utf-8') as f: return json.load(f)
def read_text(path):
    with open(path, 'r', encoding='utf-8') as f: return f.read()
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def main(run_dir, out_dir):
    ensure_dir(out_dir)
    man_path = os.path.join(run_dir, 'manifest.json')
    if not os.path.exists(man_path):
        print('ERROR: manifest.json not found'); sys.exit(2)
    manifest = read_json(man_path)
    rs_path = os.path.join(run_dir, 'results_summary.json')
    rs = read_json(rs_path) if os.path.exists(rs_path) else None
    briefing_path = os.path.join(run_dir, 'briefing.md')
    briefing = read_text(briefing_path) if os.path.exists(briefing_path) else ''

    run_id = manifest.get('run_id',''); exp_id = manifest.get('experiment_id','')
    hyp_id = manifest.get('hypothesis_id',''); ctx_id = manifest.get('ciphertext',{}).get('id','')
    mth_id = manifest.get('method',{}).get('id',''); sfx_id = manifest.get('scoring',{}).get('id','')
    prng = manifest.get('prng',{}).get('name',''); seed = manifest.get('prng',{}).get('seed','')
    env_hash = manifest.get('env',{}).get('env_hash',''); code_commit = manifest.get('code_recipe',{}).get('commit','')
    limits = manifest.get('limits', {}); stop_cond = ','.join(sorted(limits.keys())) if limits else ''
    # Runs
    runs_csv = os.path.join(out_dir, 'runs.csv'); hdr = not os.path.exists(runs_csv)
    with open(runs_csv, 'a', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        if hdr: w.writerow(["Title","RUN ID","Experiment","Hypothesis","Ciphertext","Method","Scoring","PRNG","Seed","Env Hash","Code Commit","Stop Condition","Status","CPUh","Wall Minutes","Peak Mem MB","Iterations","Candidates/sec"])
        cpu=wall=mem=iters=cps=""
        if rs:
            rsrc = rs.get('resources',{})
            cpu, wall, mem, iters, cps = rsrc.get('cpu_hours',''), rsrc.get('wall_minutes',''), rsrc.get('peak_mem_mb',''), rsrc.get('iterations',''), rsrc.get('candidates_per_sec','')
        w.writerow([f"{run_id}", run_id, exp_id, hyp_id, ctx_id, mth_id, sfx_id, prng, seed, env_hash, code_commit, stop_cond, "Completed" if rs else "Paused", cpu, wall, mem, iters, cps])
    # Results Summaries
    if rs:
        rs_csv = os.path.join(out_dir, 'results_summaries.csv'); hdr = not os.path.exists(rs_csv)
        with open(rs_csv, 'a', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            if hdr: w.writerow(["Title","RUN","Best Score","Avg Score","Median","p10","p90","Composite Z","Chi2 Z","Quadgram Z","WordRate Z","TopN Table","Score Histogram","Param Sweep"])
            s, z = rs.get('scores',{}), rs.get('z_scores',{}); arts = rs.get('artifacts',{})
            w.writerow([f"{run_id} — RS", run_id, s.get('best',''), s.get('avg',''), s.get('median',''), s.get('p10',''), s.get('p90',''),
                        z.get('composite',''), z.get('chi2',''), z.get('quadgram',''), z.get('wordrate',''),
                        arts.get('topn_table',''), arts.get('score_histogram',''), arts.get('param_sweep','')])
    # Artifacts
    arts_csv = os.path.join(out_dir, 'artifacts.csv'); hdr = not os.path.exists(arts_csv)
    with open(arts_csv, 'a', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        if hdr: w.writerow(["Title","RUN","Type","Path/URL","Checksum","Mime","Size Bytes"])
        for name in sorted(os.listdir(run_dir)):
            if name in ('manifest.json','results_summary.json','briefing.md','checksums.txt'): continue
            path = os.path.join(run_dir, name)
            if os.path.isdir(path): continue
            mime = ('' if (os.path.getsize(path)==0) else (mimetypes.guess_type(path)[0] or ""))
            size = os.path.getsize(path)
            atype = "CSV" if name.endswith(".csv") else ("Plot" if name.endswith(".png") else ("Text" if name.endswith(".md") else "Other"))
            w.writerow([f"{run_id} — {name}", run_id, atype, name, "", mime, size])
    # Briefings
    if briefing:
        brf_csv = os.path.join(out_dir, 'briefings.csv'); hdr = not os.path.exists(brf_csv)
        with open(brf_csv, 'a', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            if hdr: w.writerow(["Title","RUN","Version","Header","Technical","Broad"])
            w.writerow([f"{run_id} — Briefing", run_id, "1.0", "", briefing, ""])
    print("ETL complete:", out_dir)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python tools/psb_etl.py <RUN_DIR> <OUT_DIR>'); sys.exit(1)
    main(sys.argv[1], sys.argv[2])
