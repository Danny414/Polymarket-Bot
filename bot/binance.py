"""
Binance Stealth Scanner — finds USDT pairs with flat price action
(-1.5% to +2%) combined with heavy 24h volume (>$5M), signalling
quiet accumulation before a potential breakout.
"""

import json
import os
import time
import aiohttp

from bot.logger import logger

BINANCE_API          = "https://api.binance.com/api/v3/ticker/24hr"
BINANCE_COOLDOWN_PATH = "bot/data/binance_cooldowns.json"
BINANCE_COOLDOWN_SECS = 45 * 60   # 45-minute cooldown per symbol
STEALTH_CHANGE_MIN    = -1.5       # % lower bound (flat / slight dip)
STEALTH_CHANGE_MAX    =  2.0       # % upper bound
STEALTH_VOL_MIN       = 5_000_000  # $5M minimum 24h quote volume


# ------------------------------------------------------------------ #
#  Cooldown helpers                                                    #
# ------------------------------------------------------------------ #

def _load_cooldowns() -> dict:
    os.makedirs(os.path.dirname(BINANCE_COOLDOWN_PATH), exist_ok=True)
    if os.path.exists(BINANCE_COOLDOWN_PATH):
        try:
            with open(BINANCE_COOLDOWN_PATH) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_cooldowns(data: dict):
    os.makedirs(os.path.dirname(BINANCE_COOLDOWN_PATH), exist_ok=True)
    with open(BINANCE_COOLDOWN_PATH, "w") as f:
        json.dump(data, f)


def _on_cooldown(symbol: str, cooldowns: dict) -> bool:
    return (time.time() - cooldowns.get(symbol, 0)) < BINANCE_COOLDOWN_SECS


# ------------------------------------------------------------------ #
#  Main scan                                                           #
# ------------------------------------------------------------------ #

async def binance_stealth_scan() -> list[dict]:
    """
    Fetch all Binance 24hr tickers and return a list of stealth-gem
    dicts for symbols that pass the flat-price + heavy-volume filter
    and are not on cooldown.
    """
    logger.info("[binance] Starting stealth scan...")
    gems = []
    cooldowns = _load_cooldowns()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                BINANCE_API, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"[binance] API returned HTTP {resp.status}")
                    return []
                tickers = await resp.json()
    except Exception as e:
        logger.error(f"[binance] Fetch error: {e}")
        return []

    for ticker in tickers:
        symbol = ticker.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        try:
            change = float(ticker["priceChangePercent"])
            vol    = float(ticker["quoteVolume"])
            price  = float(ticker["lastPrice"])
        except (KeyError, ValueError):
            continue

        if not (STEALTH_CHANGE_MIN < change < STEALTH_CHANGE_MAX):
            continue
        if vol < STEALTH_VOL_MIN:
            continue
        if _on_cooldown(symbol, cooldowns):
            continue

        base = symbol.replace("USDT", "")
        gems.append({
            "symbol": symbol,
            "base":   base,
            "change": change,
            "volume": vol,
            "price":  price,
        })

    logger.info(f"[binance] Stealth scan complete. {len(gems)} new gem(s) found.")

    # Stamp cooldowns for everything we're about to alert
    for g in gems:
        cooldowns[g["symbol"]] = time.time()
    _save_cooldowns(cooldowns)

    return gems


def format_stealth_alert(gem: dict) -> str:
    symbol = gem["symbol"]
    base   = gem["base"]
    change = gem["change"]
    vol_m  = gem["volume"] / 1_000_000
    return (
        f"💎 *BINANCE STEALTH GEM: {symbol}*\n"
        f"📈 Change (24h): {change:+.2f}%\n"
        f"📊 Volume (24h): ${vol_m:.1f}M\n"
        f"🐳 Signal: HIGH ACCUMULATION\n"
        f"🔗 [Whale Audit](https://platform.arkhamintelligence.com/explorer/search?q={base}&type=token)"
    )
