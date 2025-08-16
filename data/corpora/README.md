# Corpora Pack (Demo) — 20250816T014752Z

This folder contains **demo** corpora for wiring SFX/MTH. Replace with authoritative resources later.

## Files
- resources/unigram_en_demo.txt — English letter frequencies (demo)
- resources/quadgram_en_demo.txt — few common quadgrams with log-10-ish weights (demo)
- resources/function_words_demo.txt — common function words (demo)
- registry.csv — IDs, types, file paths, SHA256 checksums.

## Registry semantics
- `id` — stable corpora ID you’ll reference from SFX, e.g. `COR-quadgram-english-demo-v0`.
- `type` — Unigram|Quadgram|Wordlist|Other
- `path` — path relative to repo root (recommended), e.g. `data/corpora/resources/quadgram_en_demo.txt`
- `checksum` — SHA256 of the file content
- `source` — provenance/URL or note
- `license` — license snippet or SPDX id

> When you swap in real corpora, keep the `id` stable if semantics match; otherwise **bump the ID** and recalibrate nulls per the Bible.
