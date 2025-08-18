#!/usr/bin/env python3
"""
sfx_eval.py
Simple scoring helpers: chi-square vs unigram frequency and quadgram log-likelihood.

Inputs: data/corpora/en_unigram_v2.1.csv (gram,prob)
        data/corpora/en_quadgram_v2.1.csv (gram,log10_weight)
"""

from __future__ import annotations
import csv, math, os
from collections import Counter
from typing import Dict, Tuple

ROOT = os.path.dirname(os.path.dirname(__file__))
CORP_DIR = os.path.join(ROOT, "data", "corpora")
UNI_PATH = os.path.join(CORP_DIR, "en_unigram_v2.1.csv")
QUAD_PATH = os.path.join(CORP_DIR, "en_quadgram_v2.1.csv")

def load_unigrams(path: str = UNI_PATH) -> Dict[str, float]:
    uni: Dict[str, float] = {}
    with open(path, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            g = (r.get("gram") or "").strip().upper()
            p = float(r.get("prob") or 0.0)
            if len(g) == 1 and p > 0:
                uni[g] = p
    # ensure full A–Z
    for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        uni.setdefault(c, 1e-9)
    # normalize to 1.0
    s = sum(uni.values())
    return {k: v / s for k, v in uni.items()}

def load_quadgrams(path: str = QUAD_PATH) -> Dict[str, float]:
    q: Dict[str, float] = {}
    with open(path, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            g = (r.get("gram") or "").strip().upper()
            w = float(r.get("log10_weight") or 0.0)
            if len(g) == 4:
                q[g] = w
    return q

_UNI = None
_QUAD = None
def _ensure_loaded():
    global _UNI, _QUAD
    if _UNI is None: _UNI = load_unigrams()
    if _QUAD is None: _QUAD = load_quadgrams()

def chi2_unigram(text: str) -> float:
    """Chi-square against English unigrams. Lower is “more English-like”."""
    _ensure_loaded()
    letters = [c for c in text.upper() if "A" <= c <= "Z"]
    n = len(letters) or 1
    cnt = Counter(letters)
    x2 = 0.0
    for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        obs = cnt.get(c, 0)
        exp = _UNI[c] * n
        x2 += (obs - exp) ** 2 / (exp if exp > 0 else 1e-9)
    return x2

def neg_logp_quadgram(text: str, floor: float = -5.0) -> float:
    """Sum of -log10 probs over quadgrams (lower is better)."""
    _ensure_loaded()
    t = "".join([c for c in text.upper() if "A" <= c <= "Z"])
    if len(t) < 4:
        return -floor  # tiny penalty
    s = 0.0
    for i in range(len(t) - 3):
        g = t[i:i+4]
        w = _QUAD.get(g, floor)  # log10 weight (typically negative)
        s += -w
    return s

def composite_default(text: str, w_uni: float = 0.5, w_quad: float = 0.5) -> Tuple[float, float, float]:
    x2 = chi2_unigram(text)
    qn = neg_logp_quadgram(text)
    comp = w_uni * x2 + w_quad * qn
    return comp, x2, qn
