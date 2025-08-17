# METHODS (source-of-truth)

Add **one JSON per method** in this folder; CI compiles them into `data/methods_seed.csv`
via `tools/build_methods_seed.py`. Do **not** hand-edit `methods_seed.csv`.

## File naming
`MTH-<kebab-case-name>-v<major>.<minor>.json`
- Example: `MTH-caesar-scan-v1.0.json`
- The `"name"` field inside the JSON must match the filename stem.

## JSON schema (fields map 1:1 to CSV columns)

```json
{
  "name": "MTH-example-v1.0",
  "description": "Short human description.",
  "parameters_schema": { "any": "JSON object with parameters" },
  "version": "v1.0",
  "status": "Ready",
  "notes": "Optional notes"
}
