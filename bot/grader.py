"""
Grader: checks expired signals against the Polymarket API,
determines whether the predicted outcome was correct, updates
the database, and sends a Telegram notification for each result.
"""

import asyncio
import json
import sqlite3
import time
import aiohttp

from bot.config import DB_PATH, POLYMARKET_API_BASE
from bot.logger import logger


# --------------------------------------------------------------------- #
#  Database helpers                                                       #
# --------------------------------------------------------------------- #

def get_ungraded_expired_signals() -> list[dict]:
    """
    Return signals that:
      - were classified as TRADEABLE at send time
      - have no grade yet (was_correct IS NULL)
      - were sent far enough in the past that the market should have expired
        (sent_at + days_remaining * 86400 < now - 3600 buffer)

    WATCH and IGNORE signals are deliberately excluded — they were not
    actionable recommendations and should not skew performance stats.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        now = time.time()
        cur.execute(
            """
            SELECT * FROM signals
            WHERE was_correct IS NULL
              AND (sent_at + days_remaining * 86400) < ?
              AND classification LIKE '%TRADEABLE%'
            ORDER BY sent_at ASC
            """,
            (now - 3600,),  # 1-hour grace buffer
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        logger.info(f"[grader] {len(rows)} TRADEABLE signal(s) eligible for grading.")
        return rows
    except Exception as e:
        logger.error(f"[grader] DB read error: {e}")
        return []


def update_signal_grade(
    signal_id: int,
    was_correct: int,
    actual_outcome: str,
    signal_score: float,
):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE signals
            SET was_correct = ?, outcome = ?, signal_score = ?, resolved_at = ?
            WHERE id = ?
            """,
            (was_correct, actual_outcome, signal_score, time.time(), signal_id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[grader] DB write error for signal {signal_id}: {e}")


# --------------------------------------------------------------------- #
#  Polymarket resolution lookup                                           #
# --------------------------------------------------------------------- #

async def fetch_market_resolution(
    session: aiohttp.ClientSession, market_id: str
) -> dict | None:
    """Fetch a single market by ID and return its resolution data."""
    url = f"{POLYMARKET_API_BASE}/markets/{market_id}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return await resp.json()
            logger.warning(f"[grader] Market {market_id} returned HTTP {resp.status}")
    except Exception as e:
        logger.error(f"[grader] Error fetching market {market_id}: {e}")
    return None


def resolve_winner(market_data: dict) -> str | None:
    """
    Return the name of the winning outcome, or None if not yet resolved.
    A resolved market has outcomePrices where exactly one outcome = 1.0.
    """
    if not market_data:
        return None

    closed = market_data.get("closed", False)
    resolution_status = market_data.get("umaResolutionStatus", "")
    # Accept 'resolved' or market simply being closed with final prices
    if not closed:
        return None

    try:
        outcomes = market_data.get("outcomes", "[]")
        prices = market_data.get("outcomePrices", "[]")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)

        float_prices = [float(p) for p in prices]
        # Winner is the outcome whose price settled at 1.0 (or closest to 1.0)
        best_idx = max(range(len(float_prices)), key=lambda i: float_prices[i])
        if float_prices[best_idx] >= 0.95:  # allow slight float imprecision
            return outcomes[best_idx]
    except Exception as e:
        logger.error(f"[grader] resolve_winner parse error: {e}")

    return None


def brier_score(predicted_prob: float, was_correct: int) -> float:
    """
    Brier score component for a single binary prediction.
    Lower is better (0 = perfect, 1 = worst).
    We store as a 0-100 'signal score' where higher = better.
    """
    outcome = 1.0 if was_correct else 0.0
    brier = (predicted_prob - outcome) ** 2
    return round((1 - brier) * 100, 2)


# --------------------------------------------------------------------- #
#  Main grading loop                                                      #
# --------------------------------------------------------------------- #

async def grade_expired_signals(send_fn) -> list[dict]:
    """
    Grade all expired ungraded signals. Sends a Telegram message for each
    newly resolved signal. Returns a list of graded result dicts.
    """
    signals = get_ungraded_expired_signals()
    if not signals:
        logger.info("[grader] No expired ungraded signals to grade.")
        return []

    logger.info(f"[grader] Found {len(signals)} signal(s) to grade.")
    graded = []

    async with aiohttp.ClientSession() as session:
        for sig in signals:
            market_id = sig["market_id"]
            predicted = (sig.get("outcome") or "").strip().upper()
            probability = sig.get("probability", 0.5)
            question = sig.get("question", "Unknown")
            tier = sig.get("tier", 0)

            market_data = await fetch_market_resolution(session, market_id)
            if market_data is None:
                logger.warning(f"[grader] Could not fetch market {market_id}, skipping.")
                continue

            actual_winner = resolve_winner(market_data)
            if actual_winner is None:
                logger.info(f"[grader] Market {market_id} not yet resolved, skipping.")
                continue

            actual_upper = actual_winner.strip().upper()
            was_correct = 1 if actual_upper == predicted else 0
            score = brier_score(probability, was_correct)

            update_signal_grade(
                signal_id=sig["id"],
                was_correct=was_correct,
                actual_outcome=actual_winner,
                signal_score=score,
            )

            result = {
                "question": question,
                "predicted": predicted,
                "actual": actual_winner,
                "was_correct": was_correct,
                "probability": probability,
                "tier": tier,
                "score": score,
            }
            graded.append(result)
            logger.info(
                f"[grader] Graded: market={market_id} predicted={predicted} "
                f"actual={actual_winner} correct={was_correct} score={score}"
            )

            await _send_grade_notification(send_fn, result)
            await asyncio.sleep(0.5)  # gentle rate limit

    logger.info(f"[grader] Grading complete. {len(graded)} signal(s) resolved.")
    return graded


async def _send_grade_notification(send_fn, result: dict):
    emoji = "✅" if result["was_correct"] else "❌"
    q = result["question"]
    if len(q) > 75:
        q = q[:72] + "..."
    tier_label = f"Tier {result['tier']}" if result["tier"] else "—"
    msg = (
        f"{emoji} *SIGNAL GRADED* 🟢 TRADEABLE\n\n"
        f"📋 {q}\n\n"
        f"🎯 Predicted: *{result['predicted']}*\n"
        f"🏁 Actual result: *{result['actual']}*\n"
        f"📊 Signal probability was: {result['probability']*100:.1f}%\n"
        f"⭐ Signal score: {result['score']}/100\n"
        f"📅 {tier_label}\n"
    )
    try:
        await send_fn(msg)
    except Exception as e:
        logger.error(f"[grader] Failed to send grade notification: {e}")
