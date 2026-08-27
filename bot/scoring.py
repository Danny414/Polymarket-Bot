"""
Signal scoring engine — Corrections 2–8.

Each correction is an isolated function so they can be tested and
adjusted independently without touching any other module.
"""

import json


# ------------------------------------------------------------------ #
#  Correction 2 — Liquidity classification                            #
# ------------------------------------------------------------------ #

def score_liquidity(volume24h: float) -> int:
    """
    0 pts — below hard filter (should not reach here)
    1 pt  — $2,500–$5,000   → IGNORE tier
    2 pts — $5,000–$25,000  → WATCH tier
    3 pts — >$25,000        → TRADEABLE eligible
    """
    if volume24h >= 25_000:
        return 3
    if volume24h >= 5_000:
        return 2
    if volume24h >= 2_500:
        return 1
    return 0


def liquidity_label(volume24h: float) -> str:
    if volume24h >= 25_000:
        return "TRADEABLE"
    if volume24h >= 5_000:
        return "WATCH"
    return "IGNORE"


# ------------------------------------------------------------------ #
#  Correction 3 — Price movement confirmation                         #
# ------------------------------------------------------------------ #

def score_price_movement(prob_change: float) -> int:
    """
    prob_change is expressed as a fraction (0.05 = 5 pp).
    0 pts — change < 2%
    1 pt  — change 2–5%
    2 pts — change > 5%
    """
    if prob_change > 0.05:
        return 2
    if prob_change >= 0.02:
        return 1
    return 0


def price_movement_adjustment(prob_change: float) -> int:
    """Returns +1 / 0 / -1 adjustment for corrections 3."""
    if prob_change > 0.05:
        return 1
    if prob_change < 0.02:
        return -1
    return 0


# ------------------------------------------------------------------ #
#  Correction 4 — Volume spike strength                               #
# ------------------------------------------------------------------ #

def score_spike_strength(spike_ratio: float) -> int:
    """
    spike_ratio = current_24h / rolling_avg_24h
    0 pts — < 2x (downgrade)
    1 pt  — 2x–5x (neutral)
    2 pts — > 5x  (upgrade)
    """
    if spike_ratio > 5.0:
        return 2
    if spike_ratio >= 2.0:
        return 1
    return 0


def spike_adjustment(spike_ratio: float, has_history: bool) -> int:
    """Returns +1 / 0 / -1 adjustment for correction 4."""
    if not has_history:
        return 0   # no data → neutral
    if spike_ratio > 5.0:
        return 1
    if spike_ratio < 2.0:
        return -1
    return 0


# ------------------------------------------------------------------ #
#  Correction 5 — Market risk / type                                  #
# ------------------------------------------------------------------ #

_POLITICAL_KEYWORDS = {
    "election", "president", "congress", "senate", "vote", "poll",
    "fed ", "gdp", "inflation", "interest rate", "central bank",
    "minister", "chancellor", "parliament", "referendum", "ballot",
    "approval rating", "tariff", "policy",
}

_SPORTS_FIRST_SET_TYPES = {
    "first_set", "first_half", "first_quarter", "first_map",
    "first_blood", "first_kill", "first_game",
}


def detect_market_type(market: dict) -> str:
    """
    Returns one of: 'sports_first_set', 'political', 'binary_fixed', 'general'
    """
    # Sports first-set / first-period type
    sports_type = (market.get("sportsMarketType") or "").lower()
    if any(t in sports_type for t in _SPORTS_FIRST_SET_TYPES):
        return "sports_first_set"

    question = (market.get("question") or "").lower()

    # Political / macro
    if any(kw in question for kw in _POLITICAL_KEYWORDS):
        return "political"

    # Binary outcome with fixed deadline
    try:
        outcomes = market.get("outcomes", "[]")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if len(outcomes) == 2:
            lower_set = {o.strip().lower() for o in outcomes}
            if lower_set in ({"yes", "no"}, {"true", "false"}, {"over", "under"}):
                return "binary_fixed"
    except Exception:
        pass

    return "general"


def score_market_reliability(market_type: str) -> int:
    """
    2 pts — binary_fixed (clearest resolution criteria)
    1 pt  — political / macro / general
    0 pts — sports_first_set (highest noise / variance)
    """
    if market_type == "binary_fixed":
        return 2
    if market_type == "sports_first_set":
        return 0
    return 1


# ------------------------------------------------------------------ #
#  Correction 6 — Timing / news catalyst                              #
# ------------------------------------------------------------------ #

def score_timing_catalyst(prob_change: float, spike_ratio: float) -> int:
    """
    1 pt if meaningful price or volume movement (suggests a news catalyst).
    0 pts otherwise.
    """
    if prob_change >= 0.02 or spike_ratio >= 2.0:
        return 1
    return 0


# ------------------------------------------------------------------ #
#  Correction 7 — Final classification                                #
# ------------------------------------------------------------------ #

def classify_score(final_score: int) -> tuple[str, str]:
    """Returns (classification_label, confidence_label)."""
    if final_score >= 8:
        return "🟢 TRADEABLE", "High"
    if final_score >= 5:
        return "🟡 WATCH", "Medium"
    return "🔴 IGNORE", "Low"


# ------------------------------------------------------------------ #
#  Master function                                                     #
# ------------------------------------------------------------------ #

def compute_signal_score(market: dict) -> dict:
    """
    Compute the full signal score for a market dict.
    Expects the following keys already set by scanner.py:
      - volume24hr      (float)
      - prob_change     (float, fraction 0–1)
      - spike_ratio     (float, ≥ 0)
      - spike_has_history (bool)
      - market_type     (str)

    Returns a dict with score, classification, confidence, breakdown.
    """
    volume24h       = float(market.get("volume24hr", 0) or 0)
    prob_change     = float(market.get("prob_change", 0) or 0)
    spike_ratio     = float(market.get("spike_ratio", 1.0) or 1.0)
    has_history     = bool(market.get("spike_has_history", False))
    market_type     = market.get("market_type", "general")

    liq   = score_liquidity(volume24h)
    price = score_price_movement(prob_change)
    spike = score_spike_strength(spike_ratio)
    rel   = score_market_reliability(market_type)
    time_ = score_timing_catalyst(prob_change, spike_ratio)

    raw = liq + price + spike + rel + time_

    # Apply Correction 3 & 4 adjustments
    adj = price_movement_adjustment(prob_change) + spike_adjustment(spike_ratio, has_history)
    final = max(1, min(10, raw + adj))

    classification, confidence = classify_score(final)

    return {
        "signal_score_v2":  final,
        "classification":   classification,
        "confidence":       confidence,
        "liquidity_class":  liquidity_label(volume24h),
        "score_breakdown": {
            "liquidity":         liq,
            "price_movement":    price,
            "spike_strength":    spike,
            "market_reliability": rel,
            "timing_catalyst":   time_,
            "adjustment":        adj,
        },
    }
