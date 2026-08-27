import json
import os
import time
import aiohttp
import asyncio
from bot.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    COOLDOWN_MINUTES,
    COOLDOWN_PATH,
    SIGNALS_PER_TIER,
)
from bot.performance import record_signal, get_performance_stats, get_24h_stats
from bot.logger import logger


def load_cooldowns() -> dict:
    os.makedirs(os.path.dirname(COOLDOWN_PATH), exist_ok=True)
    if os.path.exists(COOLDOWN_PATH):
        try:
            with open(COOLDOWN_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cooldowns(data: dict):
    os.makedirs(os.path.dirname(COOLDOWN_PATH), exist_ok=True)
    with open(COOLDOWN_PATH, "w") as f:
        json.dump(data, f)


def is_on_cooldown(market_id: str, cooldowns: dict) -> bool:
    last_sent = cooldowns.get(market_id, 0)
    return (time.time() - last_sent) < COOLDOWN_MINUTES * 60


def set_cooldown(market_id: str, cooldowns: dict):
    cooldowns[market_id] = time.time()


def format_prob_bar(prob: float) -> str:
    filled = int(prob * 10)
    return "█" * filled + "░" * (10 - filled)


def format_tier_signal(market: dict, rank: int, tier: int) -> str:
    question = market.get("question") or market.get("title") or "Unknown Market"
    if len(question) > 80:
        question = question[:77] + "..."

    prob = market.get("probability", 0)
    days = market.get("days_remaining", 0)
    volume = float(market.get("volume", 0) or 0)
    volume24h = float(market.get("volume24hr", 0) or 0)
    bar = format_prob_bar(prob)
    spike_tag = " 🔥 SPIKE" if market.get("volume_spike") else ""

    # Bet signal label from actual outcome names
    best_label = (market.get("best_outcome_label") or "YES").upper()
    signal_emoji = "🟢" if best_label in ("YES", "OVER", "TRUE") else "🔴"

    market_url = market.get("polymarket_url", "")
    url_line = f"\n   🔗 {market_url}" if market_url else ""

    # Correction 8 — Append scoring fields (do not change anything above)
    score        = market.get("signal_score_v2", "–")
    classif      = market.get("classification", "–")
    confidence   = market.get("confidence", "–")

    return (
        f"#{rank} | {question}\n"
        f"   {signal_emoji} BET SIGNAL: *{best_label}*\n"
        f"   📊 Probability: {prob*100:.1f}% {bar}\n"
        f"   ⏱ Expires in: {days}d | 💰 Vol: ${volume:,.0f} | 24h: ${volume24h:,.0f}{spike_tag}{url_line}\n"
        f"   Score: {score}/10 | {classif} | Confidence: {confidence}"
    )


def build_alert_message(tier1: list, tier2: list) -> str:
    lines = ["🚨 *POLYMARKET SIGNAL ALERT*", ""]

    lines.append(f"📅 *TIER 1 — 5 to 7 Day Expiry* ({len(tier1)} signals)")
    lines.append("─" * 35)
    for i, m in enumerate(tier1[:SIGNALS_PER_TIER], 1):
        lines.append(format_tier_signal(m, i, 1))
        lines.append("")

    lines.append(f"📅 *TIER 2 — 7 to 14 Day Expiry* ({len(tier2)} signals)")
    lines.append("─" * 35)
    for i, m in enumerate(tier2[:SIGNALS_PER_TIER], 1):
        lines.append(format_tier_signal(m, i, 2))
        lines.append("")

    lines.append("━" * 35)
    lines.append("_Sorted highest → lowest probability_")
    lines.append("_Min probability: 50% | Anti-spam: 45min cooldown_")
    return "\n".join(lines)


def build_spike_message(spikes: list) -> str:
    lines = ["⚡ *VOLUME SPIKE ALERT*", ""]
    for i, m in enumerate(spikes[:5], 1):
        question = m.get("question") or m.get("title") or "Unknown"
        if len(question) > 70:
            question = question[:67] + "..."
        prob = m.get("probability", 0)
        volume24h = float(m.get("volume24hr", 0) or 0)
        best_label = (m.get("best_outcome_label") or "YES").upper()
        signal_emoji = "🟢" if best_label in ("YES", "OVER", "TRUE") else "🔴"
        market_url = m.get("polymarket_url", "")
        url_line = f"\n   🔗 {market_url}" if market_url else ""
        lines.append(f"#{i} {question}")
        lines.append(f"   {signal_emoji} BET: *{best_label}* | 📊 {prob*100:.1f}% | 24h Vol: ${volume24h:,.0f}{url_line}")
        lines.append("")
    lines.append("_Unusual trading activity detected_")
    return "\n".join(lines)


async def send_telegram_message(text: str, parse_mode: str = "Markdown") -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        return True
                    logger.warning(f"Telegram API error: {data}")
                    return False
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt+1}): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return False


async def process_and_send_alerts(scan_result: dict):
    tier1 = scan_result.get("tier1", [])
    tier2 = scan_result.get("tier2", [])
    spikes = scan_result.get("spikes", [])

    cooldowns = load_cooldowns()

    filtered_tier1 = []
    for m in tier1:
        mid = str(m.get("id") or m.get("conditionId") or "")
        if mid and not is_on_cooldown(mid, cooldowns):
            filtered_tier1.append(m)

    filtered_tier2 = []
    for m in tier2:
        mid = str(m.get("id") or m.get("conditionId") or "")
        if mid and not is_on_cooldown(mid, cooldowns):
            filtered_tier2.append(m)

    if not filtered_tier1 and not filtered_tier2:
        logger.info("All markets on cooldown. No new alerts to send.")
        return

    msg = build_alert_message(filtered_tier1, filtered_tier2)
    success = await send_telegram_message(msg)

    if success:
        logger.info("Main signal alert sent successfully")
        for m in filtered_tier1 + filtered_tier2:
            mid = str(m.get("id") or m.get("conditionId") or "")
            if mid:
                set_cooldown(mid, cooldowns)
                tier = 1 if m in filtered_tier1 else 2
                record_signal(
                    market_id=mid,
                    question=m.get("question") or m.get("title") or "",
                    probability=m.get("probability", 0),
                    days_remaining=m.get("days_remaining", 0),
                    tier=tier,
                    volume=float(m.get("volume", 0) or 0),
                    predicted_outcome=(m.get("best_outcome_label") or "YES"),
                    classification=m.get("classification", ""),
                )
        save_cooldowns(cooldowns)
    else:
        logger.error("Failed to send main signal alert")

    if spikes:
        filtered_spikes = [
            s for s in spikes
            if not is_on_cooldown(f"spike_{s.get('id','')}", cooldowns)
        ]
        if filtered_spikes:
            spike_msg = build_spike_message(filtered_spikes)
            spike_ok = await send_telegram_message(spike_msg)
            if spike_ok:
                logger.info("Spike alert sent successfully")
                for s in filtered_spikes:
                    set_cooldown(f"spike_{s.get('id','')}", cooldowns)
                save_cooldowns(cooldowns)


def _format_success_rate(rate) -> str:
    """Return a coloured success-rate string with a performance label."""
    if rate is None:
        return "— (no graded signals yet)"
    if rate >= 70:
        label = "🟢 Strong"
    elif rate >= 50:
        label = "🟡 Moderate"
    else:
        label = "🔴 Below target"
    return f"*{rate}%* — {label}"


async def send_performance_report():
    h24  = get_24h_stats()
    stats = get_performance_stats()
    if not stats and not h24:
        return

    # ── 24-hour self-evaluation block ──────────────────────────────────── #
    sent_24h   = h24.get("sent", 0)
    graded_24h = h24.get("graded", 0)
    rate_24h   = h24.get("success_rate")
    t1_24h     = h24.get("tier1", {})
    t2_24h     = h24.get("tier2", {})

    lines = [
        "🤖 *BOT SELF-EVALUATION — Last 24 Hours*",
        "_(TRADEABLE signals only)_\n",
        f"📤 Signals sent:       {sent_24h}",
        f"✅ Resolved & graded:  {graded_24h}",
        f"🎯 Success rate:       {_format_success_rate(rate_24h)}",
    ]

    # Tier breakdown (only if at least one tier has graded signals)
    if t1_24h.get("graded", 0) > 0 or t2_24h.get("graded", 0) > 0:
        lines.append("")
        if t1_24h.get("graded", 0) > 0:
            lines.append(
                f"   📅 Tier 1: {t1_24h['correct']}/{t1_24h['graded']} correct "
                f"({_format_success_rate(t1_24h['success_rate'])})"
            )
        if t2_24h.get("graded", 0) > 0:
            lines.append(
                f"   📅 Tier 2: {t2_24h['correct']}/{t2_24h['graded']} correct "
                f"({_format_success_rate(t2_24h['success_rate'])})"
            )

    # Individual results from the last 24h
    graded_signals = h24.get("graded_signals", [])
    if graded_signals:
        lines.append("")
        lines.append("📋 *Graded this window:*")
        for r in graded_signals:
            icon  = "✅" if r.get("was_correct") else "❌"
            q     = (r.get("question") or "")[:52]
            prob  = round((r.get("probability") or 0) * 100, 1)
            score = r.get("signal_score") or 0
            tier  = r.get("tier", "?")
            lines.append(f"  {icon} [T{tier}] {q} | {prob}% → {score:.0f}/100")
    elif graded_24h == 0 and sent_24h > 0:
        lines.append("\n_Markets are still open — check back once they resolve._")

    lines += ["", "━" * 35, ""]

    # ── All-time stats block ───────────────────────────────────────────── #
    t1 = stats.get("tier1", {})
    t2 = stats.get("tier2", {})

    lines += [
        "📈 *ALL-TIME PERFORMANCE (TRADEABLE)*\n",
        f"📊 Total Signals Sent: {stats.get('total', 0)}",
        f"⏳ Pending Grading:    {stats.get('pending', 0)}",
        f"✅ Graded Signals:     {stats.get('resolved', 0)}",
        f"🎯 Overall Accuracy:  {_format_success_rate(stats.get('accuracy') or None if stats.get('resolved',0) > 0 else None)}",
        f"⭐ Avg Signal Score:  {stats.get('avg_score', 0)}/100",
        f"🔢 Avg Probability:   {stats.get('avg_probability', 0)}%",
        "",
        f"📅 *Tier 1 (5–7 days)*",
        f"   Graded: {t1.get('total',0)} | Correct: {t1.get('correct',0)} | Accuracy: {t1.get('accuracy',0)}% | Avg Score: {t1.get('avg_score',0)}/100",
        "",
        f"📅 *Tier 2 (7–14 days)*",
        f"   Graded: {t2.get('total',0)} | Correct: {t2.get('correct',0)} | Accuracy: {t2.get('accuracy',0)}% | Avg Score: {t2.get('avg_score',0)}/100",
    ]

    recent = stats.get("recent_graded", [])
    if recent:
        lines += ["", "🕓 *Last 5 Graded Signals*"]
        for r in recent:
            icon = "✅" if r.get("was_correct") else "❌"
            q = (r.get("question") or "")[:55]
            prob = round((r.get("probability") or 0) * 100, 1)
            score = r.get("signal_score") or 0
            lines.append(f"  {icon} {q} | {prob}% → score {score:.0f}/100")

    await send_telegram_message("\n".join(lines))
