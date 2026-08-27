import os

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "867434065")

SCAN_INTERVAL = int(os.environ.get("BOT_SCAN_INTERVAL", "300"))
VOLUME_SPIKE_THRESHOLD = float(os.environ.get("VOLUME_SPIKE_THRESHOLD", "2.5"))
COOLDOWN_MINUTES = int(os.environ.get("COOLDOWN_MINUTES", "45"))

MIN_PROBABILITY = 0.50

TIER1_MIN_DAYS = 5
TIER1_MAX_DAYS = 7
TIER2_MIN_DAYS = 7
TIER2_MAX_DAYS = 14

SIGNALS_PER_TIER = 5

POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

DB_PATH = "bot/data/performance.db"
VOLUME_SNAPSHOTS_PATH = "bot/data/volume_snapshots.json"
PROB_SNAPSHOTS_PATH = "bot/data/prob_snapshots.json"
COOLDOWN_PATH = "bot/data/cooldowns.json"

HARD_VOLUME_24H_MIN = 2500.0
