import asyncio
import json
import os
import time
import aiohttp
from bot.config import (
    POLYMARKET_API_BASE,
    VOLUME_SPIKE_THRESHOLD,
    VOLUME_SNAPSHOTS_PATH,
    PROB_SNAPSHOTS_PATH,
    SIGNALS_PER_TIER,
)
from bot.filters import filter_markets
from bot.scoring import compute_signal_score, detect_market_type
from bot.logger import logger


def load_volume_snapshots() -> dict:
    os.makedirs(os.path.dirname(VOLUME_SNAPSHOTS_PATH), exist_ok=True)
    if os.path.exists(VOLUME_SNAPSHOTS_PATH):
        try:
            with open(VOLUME_SNAPSHOTS_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_volume_snapshots(data: dict):
    os.makedirs(os.path.dirname(VOLUME_SNAPSHOTS_PATH), exist_ok=True)
    with open(VOLUME_SNAPSHOTS_PATH, "w") as f:
        json.dump(data, f)


def load_prob_snapshots() -> dict:
    os.makedirs(os.path.dirname(PROB_SNAPSHOTS_PATH), exist_ok=True)
    if os.path.exists(PROB_SNAPSHOTS_PATH):
        try:
            with open(PROB_SNAPSHOTS_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_prob_snapshots(data: dict):
    os.makedirs(os.path.dirname(PROB_SNAPSHOTS_PATH), exist_ok=True)
    with open(PROB_SNAPSHOTS_PATH, "w") as f:
        json.dump(data, f)


def get_prob_change(market_id: str, current_prob: float, snapshots: dict) -> float:
    """Return absolute probability change vs the last recorded value (fraction 0–1)."""
    history = snapshots.get(market_id, [])
    if not history:
        return 0.0
    return abs(current_prob - history[-1])


def update_prob_snapshot(market_id: str, current_prob: float, snapshots: dict, max_history: int = 12):
    if market_id not in snapshots:
        snapshots[market_id] = []
    snapshots[market_id].append(current_prob)
    if len(snapshots[market_id]) > max_history:
        snapshots[market_id] = snapshots[market_id][-max_history:]


async def fetch_markets(session: aiohttp.ClientSession, offset: int = 0, limit: int = 100) -> list:
    url = f"{POLYMARKET_API_BASE}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "limit": limit,
        "offset": offset,
        "order": "volume",
        "ascending": "false",
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get("markets", data.get("results", []))
            else:
                logger.warning(f"Polymarket API returned status {resp.status}")
            return []
    except asyncio.TimeoutError:
        logger.error("Timeout fetching markets from Polymarket")
        return []
    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        return []


def extract_best_outcome(market: dict) -> tuple:
    """Returns (best_probability, best_outcome_label)"""
    try:
        outcomes = market.get("outcomes")
        prices = market.get("outcomePrices") or market.get("outcome_prices")
        if outcomes and prices:
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)
            pairs = list(zip(outcomes, [float(p) for p in prices]))
            if pairs:
                best = max(pairs, key=lambda x: x[1])
                return best[1], best[0]
    except Exception:
        pass

    for key in ("probability", "best_bid", "lastTradePrice", "price"):
        val = market.get(key)
        if val is not None:
            try:
                return float(val), "YES"
            except Exception:
                pass
    return 0.0, "YES"


def extract_best_probability(market: dict) -> float:
    prob, _ = extract_best_outcome(market)
    return prob


def get_spike_ratio(market_id: str, current_volume: float, snapshots: dict) -> tuple[float, bool]:
    """Return (spike_ratio, has_history). spike_ratio = current / rolling_avg."""
    history = snapshots.get(market_id, [])
    if len(history) < 2:
        return 1.0, False
    avg_volume = sum(history) / len(history)
    if avg_volume <= 0:
        return 1.0, True
    return current_volume / avg_volume, True


def detect_volume_spike(market_id: str, current_volume: float, snapshots: dict) -> bool:
    ratio, _ = get_spike_ratio(market_id, current_volume, snapshots)
    return ratio >= VOLUME_SPIKE_THRESHOLD


def update_volume_snapshot(market_id: str, current_volume: float, snapshots: dict, max_history: int = 12):
    if market_id not in snapshots:
        snapshots[market_id] = []
    snapshots[market_id].append(current_volume)
    if len(snapshots[market_id]) > max_history:
        snapshots[market_id] = snapshots[market_id][-max_history:]


async def scan_markets() -> dict:
    logger.info("Starting Polymarket scan...")
    vol_snapshots  = load_volume_snapshots()
    prob_snapshots = load_prob_snapshots()
    all_markets = []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_markets(session, offset=i * 100, limit=100) for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_markets.extend(r)

    logger.info(f"Fetched {len(all_markets)} total markets")

    enriched = []
    spike_markets = []

    for market in all_markets:
        market_id = str(market.get("id") or market.get("conditionId") or "")
        if not market_id:
            continue

        prob = extract_best_probability(market)
        market["best_probability"] = prob

        volume24h = float(market.get("volume24hr", 0) or 0)

        # --- Spike ratio (Corrections 4) ---
        spike_ratio, has_history = get_spike_ratio(market_id, volume24h, vol_snapshots)
        market["spike_ratio"]        = round(spike_ratio, 2)
        market["spike_has_history"]  = has_history
        if spike_ratio >= VOLUME_SPIKE_THRESHOLD:
            market["volume_spike"] = True
            spike_markets.append(market)

        # --- Probability change (Correction 3) ---
        prob_change = get_prob_change(market_id, prob, prob_snapshots)
        market["prob_change"] = round(prob_change, 4)

        # --- Market type (Correction 5) ---
        market["market_type"] = detect_market_type(market)

        # --- Signal score (Corrections 2–7) ---
        score_data = compute_signal_score(market)
        market.update(score_data)

        # --- Best outcome label ---
        _, best_label = extract_best_outcome(market)
        market["best_outcome_label"] = best_label

        # --- Polymarket URL from events array ---
        events = market.get("events")
        if events and isinstance(events, list) and len(events) > 0:
            event_slug  = events[0].get("slug", "")
            market_slug = market.get("slug", "")
            if event_slug and market_slug:
                market["polymarket_url"] = f"https://polymarket.com/event/{event_slug}/{market_slug}"
            elif event_slug:
                market["polymarket_url"] = f"https://polymarket.com/event/{event_slug}"
            else:
                market["polymarket_url"] = f"https://polymarket.com/event/{market_slug}"
        else:
            market_slug = market.get("slug", "")
            market["polymarket_url"] = f"https://polymarket.com/event/{market_slug}" if market_slug else ""

        update_volume_snapshot(market_id, volume24h, vol_snapshots)
        update_prob_snapshot(market_id, prob, prob_snapshots)
        enriched.append(market)

    save_volume_snapshots(vol_snapshots)
    save_prob_snapshots(prob_snapshots)

    filtered = filter_markets(enriched)
    tier1 = filtered["tier1"][:SIGNALS_PER_TIER]
    tier2 = filtered["tier2"][:SIGNALS_PER_TIER]

    logger.info(f"Scan complete. Tier1: {len(tier1)}, Tier2: {len(tier2)}, Spikes: {len(spike_markets)}")
    return {
        "tier1": tier1,
        "tier2": tier2,
        "spikes": spike_markets[:5],
        "timestamp": time.time(),
    }
