#!/usr/bin/env python3
import os, sys, subprocess, json

def run(cmd):
    print("+", " ".join(cmd)); subprocess.check_call(cmd)

def main():
    bundles_root = "bundles"
    out_root = "out"
    os.makedirs(out_root, exist_ok=True)
    if not os.path.exists(bundles_root):
        print("No bundles/ directory found."); return
    for name in sorted(os.listdir(bundles_root)):
        run_dir = os.path.join(bundles_root, name)
        if not os.path.isdir(run_dir): continue
        # 1) validate
        run(["python", "tools/validate.py", run_dir])
        # 2) etl
        out_dir = os.path.join(out_root, name)
        run(["python", "tools/psb_etl.py", run_dir, out_dir])
        # 3) notion sync (uses secrets if present)
        run(["python", "tools/notion_sync.py", out_dir])

if __name__ == "__main__":
    main()
