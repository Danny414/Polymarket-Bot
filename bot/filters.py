from datetime import datetime, timezone
from typing import Optional
from bot.config import (
    MIN_PROBABILITY,
    TIER1_MIN_DAYS,
    TIER1_MAX_DAYS,
    TIER2_MIN_DAYS,
    TIER2_MAX_DAYS,
    HARD_VOLUME_24H_MIN,
)


def days_until_expiry(end_date_str: str) -> Optional[float]:
    if not end_date_str:
        return None
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                end_dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                delta = (end_dt - now).total_seconds() / 86400
                return delta
            except ValueError:
                continue
        return None
    except Exception:
        return None


def get_market_tier(days: float) -> Optional[int]:
    if TIER1_MIN_DAYS <= days <= TIER1_MAX_DAYS:
        return 1
    if TIER2_MIN_DAYS < days <= TIER2_MAX_DAYS:
        return 2
    return None


def filter_markets(markets: list) -> dict:
    tier1 = []
    tier2 = []

    for market in markets:
        try:
            # Correction 1 — Hard 24h volume filter: suppress entirely, no log
            volume24h = float(market.get("volume24hr", 0) or 0)
            if volume24h < HARD_VOLUME_24H_MIN:
                continue

            prob = float(market.get("best_probability", 0))
            if prob < MIN_PROBABILITY or prob > 0.99:
                continue

            end_date = market.get("end_date_iso") or market.get("endDate") or market.get("end_date")
            days = days_until_expiry(end_date)
            if days is None or days <= 0:
                continue

            market["days_remaining"] = round(days, 1)
            market["probability"] = prob

            tier = get_market_tier(days)
            if tier == 1:
                tier1.append(market)
            elif tier == 2:
                tier2.append(market)
        except Exception:
            continue

    tier1_sorted = sorted(tier1, key=lambda x: x["probability"], reverse=True)
    tier2_sorted = sorted(tier2, key=lambda x: x["probability"], reverse=True)

    return {"tier1": tier1_sorted, "tier2": tier2_sorted}
