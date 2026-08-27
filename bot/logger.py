import logging
import sys
import os

os.makedirs("bot/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot/logs/bot.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger("polymarket_bot")
