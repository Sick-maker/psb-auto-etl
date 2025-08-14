#!/usr/bin/env python3
import json, sys, os
from jsonschema import validate, Draft7Validator

def load(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main(run_dir):
    man = load(os.path.join(run_dir, 'manifest.json'))
    rs_path = os.path.join(run_dir, 'results_summary.json')
    rs  = load(rs_path) if os.path.exists(rs_path) else None
    ms = load(os.path.join('schemas','manifest.schema.json'))
    rs_s = load(os.path.join('schemas','results_summary.schema.json'))
    Draft7Validator.check_schema(ms); Draft7Validator.check_schema(rs_s)
    validate(man, ms)
    if rs: validate(rs, rs_s)
    print('OK: manifest.json' + (' and results_summary.json' if rs else '') + ' validate against v0.6 schemas.')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python tools/validate.py <RUN_DIR>'); sys.exit(1)
    main(sys.argv[1])
