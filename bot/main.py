import asyncio
import time
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import SCAN_INTERVAL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from bot.logger import logger
from bot.scanner import scan_markets
from bot.alerts import process_and_send_alerts, send_telegram_message, send_performance_report
from bot.performance import init_db
from bot.grader import grade_expired_signals
from bot.binance import binance_stealth_scan, format_stealth_alert


PERFORMANCE_REPORT_INTERVAL = 24 * 60 * 60   # daily
GRADING_INTERVAL            = 60 * 60         # every hour
BINANCE_SCAN_INTERVAL       = 5  * 60         # every 5 minutes
HEARTBEAT_INTERVAL          = 10 * 60         # every 10 minutes


async def startup_message():
    msg = (
        "🚀 *SharpFlow Master System Online*\n\n"
        "✅ Polymarket Scanner: Active\n"
        "💎 Binance Stealth Scanner: Active\n"
        f"⏱ Scan interval: every {SCAN_INTERVAL // 60} minutes\n"
        "🎯 Tiers:\n"
        "   • Tier 1 — 5 to 7 days expiry (5 signals)\n"
        "   • Tier 2 — 7 to 14 days expiry (5 signals)\n"
        "⚡ Volume spike detection: Active\n"
        "🛡 Anti-spam cooldown: 45 minutes\n"
        "📈 Signal grading: Active (checked hourly)\n"
        "💓 Heartbeat: every 10 minutes\n\n"
        "_Scanning Polymarket + Binance live data..._"
    )
    await send_telegram_message(msg)


async def scan_loop():
    """Runs the Polymarket market scan every SCAN_INTERVAL seconds."""
    scan_count = 0
    while True:
        try:
            logger.info(f"=== Scan #{scan_count + 1} starting ===")
            scan_result = await scan_markets()
            await process_and_send_alerts(scan_result)
            scan_count += 1
            logger.info(f"Scan #{scan_count} done. Sleeping {SCAN_INTERVAL}s...")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[scan_loop] Unexpected error: {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)


async def binance_loop():
    """Scans Binance for stealth accumulation gems every BINANCE_SCAN_INTERVAL seconds."""
    # Slight stagger so it doesn't fire at the exact same time as the Polymarket scan
    await asyncio.sleep(60)
    while True:
        try:
            gems = await binance_stealth_scan()
            for gem in gems:
                await send_telegram_message(format_stealth_alert(gem))
                await asyncio.sleep(1)   # gentle rate limit between messages
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[binance_loop] Unexpected error: {e}", exc_info=True)
        await asyncio.sleep(BINANCE_SCAN_INTERVAL)


async def heartbeat_loop():
    """Sends a brief status ping every HEARTBEAT_INTERVAL seconds."""
    await asyncio.sleep(HEARTBEAT_INTERVAL)
    while True:
        try:
            now = datetime.now().strftime("%H:%M")
            msg = f"💓 *HEARTBEAT* | {now}\nSystem fully operational."
            await send_telegram_message(msg)
            logger.info("[heartbeat] Ping sent.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[heartbeat_loop] Unexpected error: {e}", exc_info=True)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def grading_loop():
    """Checks and grades expired signals every GRADING_INTERVAL seconds."""
    await asyncio.sleep(120)
    while True:
        try:
            logger.info("[grader] Running grading pass...")
            graded = await grade_expired_signals(send_telegram_message)
            if graded:
                logger.info(f"[grader] Graded {len(graded)} signal(s) this pass.")
            else:
                logger.info("[grader] No signals resolved yet.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[grading_loop] Unexpected error: {e}", exc_info=True)
        await asyncio.sleep(GRADING_INTERVAL)


async def report_loop():
    """Sends a daily performance summary."""
    await asyncio.sleep(PERFORMANCE_REPORT_INTERVAL)
    while True:
        try:
            logger.info("Sending daily performance report...")
            await send_performance_report()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[report_loop] Unexpected error: {e}", exc_info=True)
        await asyncio.sleep(PERFORMANCE_REPORT_INTERVAL)


async def main():
    logger.info("Initialising database...")
    init_db()

    logger.info("Sending startup message...")
    await startup_message()

    await asyncio.gather(
        scan_loop(),
        binance_loop(),
        heartbeat_loop(),
        grading_loop(),
        report_loop(),
    )


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
        sys.exit(1)
    logger.info(f"Starting Polymarket Signal Bot (chat_id={TELEGRAM_CHAT_ID})")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
