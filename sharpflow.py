# ============================================================
#  SHARPFLOW — Single-file Polymarket bot
#  Run: python3 sharpflow.py
#  Requirements: pip install aiohttp
# ============================================================

import asyncio, json, logging, os, re, sqlite3, sys, time
from datetime import datetime, timezone

import aiohttp

# ── CONFIG ───────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "867434065")
POLYGON_RPC_URL     = os.environ.get("POLYGON_RPC_URL", "")   # optional, for whale tracking

SCAN_INTERVAL       = 300     # 5 min
HEARTBEAT_INTERVAL  = 21600   # 6 hours
GRADING_INTERVAL    = 3600    # 1 hour
REPORT_INTERVAL     = 86400   # 24 hours

# Stats only count signals sent on or after this date.
# Reset to Aug 2 2026 when Tier-1-only, 55-80% prob, and paper trading were deployed.
import calendar
STATS_SINCE = calendar.timegm((2026, 8, 2, 0, 0, 0, 0, 0, 0))

MIN_PROBABILITY        = 0.55   # Min 55%
MAX_PROBABILITY        = 0.80   # Max 80%
MAX_SIGNALS_PER_MARKET = 2      # Max 2 signals per market; 3rd only on reversal
TIER1_MIN_DAYS      = 1         # sports games resolve in 1-4 days
TIER1_MAX_DAYS      = 7
SIGNALS_PER_TIER    = 5
COOLDOWN_MINUTES    = 45
VOLUME_SPIKE_THRESH = 2.5
HARD_VOL_24H_MIN    = 2500.0
WHALE_MIN_USD       = 1000.0

POLYMARKET_API      = "https://gamma-api.polymarket.com"
CLOB_API            = "https://clob.polymarket.com"
CTF_CONTRACT        = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
TRANSFER_SINGLE_SIG = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ZERO_ADDR_PAD       = "0x0000000000000000000000000000000000000000000000000000000000000000"

os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

DB_PATH           = "data/performance.db"
VOL_SNAP_PATH     = "data/volume_snapshots.json"
PROB_SNAP_PATH    = "data/prob_snapshots.json"
COOLDOWN_PATH     = "data/cooldowns.json"
ALLOWED_USERS_PATH= "data/allowed_users.json"

# Admin is the chat_id in TELEGRAM_CHAT_ID — only they can /enable & /disable users
ADMIN_CHAT_ID     = str(os.environ.get("TELEGRAM_CHAT_ID", "867434065"))

# ── LOGGER ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("sharpflow")

# ── DATABASE ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id        TEXT NOT NULL,
            question         TEXT,
            probability      REAL,
            days_remaining   REAL,
            tier             INTEGER,
            volume           REAL,
            sent_at          REAL,
            resolved_at      REAL,
            outcome          TEXT,
            actual_outcome   TEXT,
            was_correct      INTEGER,
            signal_score     REAL,
            classification   TEXT
        )
    """)
    # safe migrations for old schemas
    for sql in [
        "ALTER TABLE signals ADD COLUMN actual_outcome TEXT",
        "ALTER TABLE signals ADD COLUMN classification TEXT",
        "ALTER TABLE signals ADD COLUMN sport TEXT",
        "ALTER TABLE signals ADD COLUMN signal_type TEXT",
    ]:
        try: cur.execute(sql); conn.commit()
        except Exception: pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS whale_trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id    TEXT,
            wallet       TEXT,
            outcome      TEXT,
            usd_amount   REAL,
            side         TEXT,
            tx_hash      TEXT,
            recorded_at  REAL
        )
    """)
    conn.commit(); conn.close()
    log.info("DB initialised.")


def record_signal(market_id, question, probability, days_remaining,
                  tier, volume, predicted_outcome="", classification="", sport="",
                  signal_type="NEW"):
    try:
        conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
        cur.execute(
            """INSERT INTO signals
               (market_id, question, probability, days_remaining,
                tier, volume, sent_at, outcome, classification, sport, signal_type)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (market_id, question, probability, days_remaining, tier, volume,
             time.time(), predicted_outcome.strip().upper(), classification, sport,
             signal_type))
        conn.commit(); conn.close()
    except Exception as e: log.error(f"record_signal: {e}")


def get_ungraded_tradeable() -> list:
    """
    Returns only NEW (first-send) TRADEABLE signals that have not been graded yet.
    UPDATE and REVERSAL signals are excluded — grading each game only once
    prevents the same outcome from inflating win/loss counts.
    """
    try:
        conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; cur = conn.cursor()
        cur.execute(
            """SELECT * FROM signals
               WHERE was_correct IS NULL
                 AND classification LIKE '%TRADEABLE%'
                 AND (signal_type = 'NEW' OR signal_type IS NULL)
               ORDER BY sent_at ASC""")
        rows = [dict(r) for r in cur.fetchall()]; conn.close()
        log.info(f"[grader] {len(rows)} ungraded NEW signal(s) to check.")
        return rows
    except Exception as e: log.error(f"get_ungraded_tradeable: {e}"); return []


def update_grade(sig_id, was_correct, actual_outcome, score):
    try:
        conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
        cur.execute(
            "UPDATE signals SET was_correct=?, actual_outcome=?, signal_score=?, resolved_at=? WHERE id=?",
            (was_correct, actual_outcome, score, time.time(), sig_id))
        conn.commit(); conn.close()
    except Exception as e: log.error(f"update_grade id={sig_id}: {e}")


def record_whale_trade(market_id, wallet, outcome, usd_amount, side, tx_hash=""):
    try:
        conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
        cur.execute(
            "INSERT INTO whale_trades (market_id,wallet,outcome,usd_amount,side,tx_hash,recorded_at) VALUES (?,?,?,?,?,?,?)",
            (market_id, wallet, outcome, usd_amount, side, tx_hash, time.time()))
        conn.commit(); conn.close()
    except Exception as e: log.error(f"record_whale_trade: {e}")


def get_wallet_win_rate(wallet: str) -> str:
    """Return win rate string for a wallet based on past graded signals we saw it bet on."""
    try:
        conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; cur = conn.cursor()
        cur.execute("SELECT COUNT(*) c FROM whale_trades WHERE wallet=?", (wallet,))
        total = cur.fetchone()["c"]; conn.close()
        if total == 0: return ""
        return f" ({total} tracked bets)"
    except Exception: return ""


def get_24h_stats() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        cutoff = max(time.time() - 86400, STATS_SINCE)   # never go before stats epoch

        def cnt(sql, *a): cur.execute(sql, a); return cur.fetchone()["c"]

        sent    = cnt("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?", cutoff)
        graded  = cnt("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct IN (0,1)", cutoff)
        correct = cnt("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct=1", cutoff)

        tier_rows = {}
        for t in (1, 2):
            cur.execute(
                """SELECT COUNT(*) total,
                          SUM(CASE WHEN was_correct=1 THEN 1 ELSE 0 END) correct
                   FROM signals
                   WHERE classification LIKE '%TRADEABLE%'
                     AND sent_at>=? AND tier=? AND was_correct IN (0,1)""", (cutoff, t))
            r = cur.fetchone(); tt = r["total"] or 0; cc = r["correct"] or 0
            tier_rows[t] = {"graded": tt, "correct": cc,
                            "success_rate": round(cc/tt*100, 1) if tt else None}

        cur.execute(
            """SELECT question, actual_outcome, was_correct, probability, signal_score, tier
               FROM signals WHERE classification LIKE '%TRADEABLE%'
                 AND sent_at>=? AND was_correct IN (0,1) ORDER BY resolved_at DESC""", (cutoff,))
        graded_signals = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"sent": sent, "graded": graded, "correct": correct,
                "success_rate": round(correct/graded*100, 1) if graded else None,
                "tier1": tier_rows[1], "tier2": tier_rows[2],
                "graded_signals": graded_signals}
    except Exception as e: log.error(f"get_24h_stats: {e}"); return {}


def get_all_time_stats() -> dict:
    """
    Stats for TRADEABLE signals sent on or after STATS_SINCE.
    Voids (was_correct=-1) are excluded — they are abandoned Polymarket markets
    that do not reflect signal accuracy.
    """
    try:
        conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; cur = conn.cursor()
        s = STATS_SINCE  # epoch cutoff

        def q(sql, *a): cur.execute(sql, a); return cur.fetchone()

        total    = q("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?", s)["c"]
        resolved = q("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct IN (0,1)", s)["c"]
        correct  = q("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct=1", s)["c"]
        pending  = q("SELECT COUNT(*) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct IS NULL", s)["c"]
        avg_prob = q("SELECT AVG(probability) a FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?", s)["a"] or 0.0
        avg_sc   = q("SELECT AVG(signal_score) a FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct IN (0,1) AND signal_score IS NOT NULL", s)["a"] or 0.0

        # Unique market stats — deduplicated by market_id
        unique_total   = q("SELECT COUNT(DISTINCT market_id) c FROM signals WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?", s)["c"]
        unique_correct = q("""SELECT COUNT(DISTINCT market_id) c FROM signals
                              WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct=1""", s)["c"]
        unique_graded  = q("""SELECT COUNT(DISTINCT market_id) c FROM signals
                              WHERE classification LIKE '%TRADEABLE%' AND sent_at>=? AND was_correct IN (0,1)""", s)["c"]
        unique_wrong   = unique_graded - unique_correct
        unique_win_rate= round(unique_correct / unique_graded * 100, 1) if unique_graded else 0.0
        avg_updates    = round(total / unique_total, 2) if unique_total else 0.0

        # Paper trading: $100 per signal at signaled probability
        cur.execute("""
            SELECT probability, was_correct FROM signals
            WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?
              AND was_correct IN (0,1) AND probability > 0""", (s,))
        paper_rows    = cur.fetchall()
        paper_n       = len(paper_rows)
        paper_wins    = sum(1 for r in paper_rows if r["was_correct"] == 1)
        paper_losses  = paper_n - paper_wins
        paper_invested= paper_n * 100.0
        paper_profit  = sum(
            (100.0 * (1.0 - r["probability"]) / r["probability"]) if r["was_correct"] == 1
            else -100.0
            for r in paper_rows
        )
        paper_roi     = round(paper_profit / paper_invested * 100, 1) if paper_invested else 0.0
        paper_net     = round(paper_profit, 2)
        # Average probability on graded signals = break-even win rate
        paper_avg_prob= round(sum(r["probability"] for r in paper_rows) / paper_n * 100, 1) if paper_n else 0.0
        paper_edge    = round((paper_wins / paper_n * 100) - paper_avg_prob, 1) if paper_n else 0.0

        # Per-sport breakdown
        cur.execute(
            """SELECT sport,
                      COUNT(*) total,
                      SUM(CASE WHEN was_correct=1 THEN 1 ELSE 0 END) wins
               FROM signals
               WHERE classification LIKE '%TRADEABLE%' AND sent_at>=?
                 AND was_correct IN (0,1)
                 AND sport IS NOT NULL AND sport != '' AND sport != 'Other'
               GROUP BY sport ORDER BY total DESC""", (s,))
        sport_stats = [dict(r) for r in cur.fetchall()]

        conn.close()
        accuracy = round(correct/resolved*100, 1) if resolved else 0.0
        return {"total": total, "resolved": resolved, "pending": pending, "correct": correct,
                "accuracy": accuracy, "avg_probability": round(avg_prob*100, 1),
                "avg_score": round(avg_sc, 1),
                "unique_total": unique_total, "unique_correct": unique_correct,
                "unique_wrong": unique_wrong, "unique_graded": unique_graded,
                "unique_win_rate": unique_win_rate, "avg_updates": avg_updates,
                "paper_n": paper_n, "paper_wins": paper_wins, "paper_losses": paper_losses,
                "paper_net": paper_net, "paper_roi": paper_roi,
                "paper_avg_prob": paper_avg_prob, "paper_edge": paper_edge,
                "sport_stats": sport_stats}
    except Exception as e: log.error(f"get_all_time_stats: {e}"); return {}

# ── JSON HELPERS ──────────────────────────────────────────────
def _jload(path) -> dict:
    if os.path.exists(path):
        try:
            with open(path) as f: return json.load(f)
        except Exception: pass
    return {}

def _jsave(path, data):
    with open(path, "w") as f: json.dump(data, f)

# ── TELEGRAM ──────────────────────────────────────────────────
async def send_to(text: str, chat_id: str, parse_mode="Markdown") -> bool:
    """Send a message to a specific chat_id."""
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text,
               "parse_mode": parse_mode, "disable_web_page_preview": True}
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    d = await r.json()
                    if d.get("ok"): return True
                    log.warning(f"Telegram error: {d}"); return False
        except Exception as e:
            log.error(f"send_to attempt {attempt+1}: {e}")
            if attempt < 2: await asyncio.sleep(2 ** attempt)
    return False

async def send_msg(text: str, parse_mode="Markdown") -> bool:
    """Send a message to the admin/alert chat (TELEGRAM_CHAT_ID)."""
    return await send_to(text, TELEGRAM_CHAT_ID, parse_mode)

# ── USER MANAGEMENT ───────────────────────────────────────────
# Storage format: {"users": {"john": {"chat_id": "123456"}, ...}}
# Admin is always included in broadcasts via ADMIN_CHAT_ID.

def _load_user_db() -> dict:
    """Return the full user dict {username: {chat_id: ...}}."""
    return _jload(ALLOWED_USERS_PATH).get("users", {})

def _save_user_db(users: dict):
    _jsave(ALLOWED_USERS_PATH, {"users": users})

def load_allowed_users() -> set:
    """Return the set of enabled usernames (lowercase, no @)."""
    return set(_load_user_db().keys())

def save_allowed_users(users: set):
    """Add new usernames to the DB, preserving existing chat_id data."""
    db = _load_user_db()
    # Remove users no longer in the set
    for u in list(db.keys()):
        if u not in users:
            del db[u]
    # Add new users
    for u in users:
        if u not in db:
            db[u] = {"chat_id": None}
    _save_user_db(db)

def register_chat_id(username: str, chat_id: str):
    """Store the chat_id for an enabled user so we can broadcast to them."""
    if not username: return
    db = _load_user_db()
    key = username.lower().lstrip("@")
    if key in db and db[key].get("chat_id") != chat_id:
        db[key]["chat_id"] = chat_id
        _save_user_db(db)

def is_allowed_user(username: str, chat_id: str) -> bool:
    """Return True if this user may use the bot (admin always allowed)."""
    if chat_id == ADMIN_CHAT_ID: return True
    if not username: return False
    return username.lower().lstrip("@") in _load_user_db()

def get_broadcast_ids() -> list[str]:
    """Return all chat_ids to broadcast alerts to: admin + enabled users."""
    ids = [ADMIN_CHAT_ID]
    for info in _load_user_db().values():
        cid = info.get("chat_id")
        if cid and str(cid) != ADMIN_CHAT_ID:
            ids.append(str(cid))
    return ids

async def broadcast_alert(text: str, parse_mode="Markdown"):
    """Send a market alert to admin and all enabled users who have interacted with the bot."""
    for cid in get_broadcast_ids():
        await send_to(text, cid, parse_mode)

# ── SCORING ───────────────────────────────────────────────────
_POLITICAL_KW = {
    "election", "president", "congress", "senate", "vote", "poll", "fed ",
    "gdp", "inflation", "interest rate", "central bank", "minister", "chancellor",
    "parliament", "referendum", "ballot", "approval rating", "tariff", "policy",
}
_SPORTS_FIRST = {
    "first_set", "first_half", "first_quarter", "first_map",
    "first_blood", "first_kill", "first_game",
}

# ── SPORT DETECTION ───────────────────────────────────────────
# Only these sports will ever generate alerts.
ALLOWED_SPORTS = {"Baseball", "Tennis", "Basketball", "American Football", "Cricket"}

# Markets with these prefixes/substrings are ESPORTS regardless of sportsMarketType tag
_ESPORTS_BLOCK = {
    "lol:", "dota 2:", "dota:", "counter-strike:", "valorant:", "overwatch:",
    "rocket league:", "starcraft", "pubg:", "apex legends:", "esport",
    "king of glory", "mobile legends", "rainbow six:", "hearthstone",
    "teamfight tactics", "smite:", "heroes of the storm",
}

def is_esports(m: dict) -> bool:
    """Return True if the market is an esports market (block even if sportsMarketType set)."""
    q  = (m.get("question") or m.get("title") or "").lower()
    ev = (m.get("events") or [{}])[0] if m.get("events") else {}
    ev_title = (ev.get("title") or "").lower()
    text = q + " " + ev_title
    return any(kw in text for kw in _ESPORTS_BLOCK)

_SPORT_KEYWORDS: dict[str, list[str]] = {
    "Basketball": [
        "nba", "basketball", " lakers", " celtics", " warriors", " bucks",
        " heat ", " nets ", " knicks", "76ers", " bulls", " nuggets", " suns",
        " clippers", " spurs", " wolves", " thunder", " magic ", " hawks",
        " grizzlies", " pistons", " raptors", " pacers", " cavaliers",
        " jazz ", " pelicans", " rockets", " timberwolves",
        # WNBA teams
        "wnba", " lynx", " tempo ", " sparks", " sky ", " mystics",
        " dream ", " fever ", " mercury ", " storm ", " liberty",
        " wings ", " aces ", " sun ", " solar",
    ],
    "Baseball": [
        "mlb", "baseball", " yankees", " dodgers", " astros", "red sox",
        " cubs", " braves", " giants", " mets ", " phillies", " cardinals",
        " orioles", " mariners", " padres", " guardians", " tigers", " royals",
        " rays", " twins", " athletics", " brewers", " pirates", " nationals",
        " reds ", " rangers", " angels", " white sox", " blue jays",
    ],
    # Tennis: Polymarket titles use "[Tournament] Open: P1 vs P2" or "[City]: P1 vs P2"
    "Tennis": [
        " open:", " open,",
        "atp ", " itf ", "wimbledon", "french open", "australian open",
        "roland garros", "grand slam", "tennis",
        # ATP 250/500/1000 named tournaments
        "los cabos", "dc open", "citi dc", "mubadala", "estoril",
        "hamburg open", " umag", "bastad", "kitzbuhel", "hall of fame",
        "atlanta open", "canadian open", "cincinnati open", "winston-salem",
        "eastbourne", "queens club", "halle open", "nottingham open",
        "geneva open", "lyon open", "madrid open", "rome open",
        "monte carlo", "miami open", "indian wells", "dubai open",
        "doha open", "rotterdam", "marseille", "montpellier",
        "barcelona open", "acapulco", "buenos aires", "santiago open",
        "munich open", "gstaad", "newport open", "washington open",
        "cleveland open", "chengdu", "hangzhou", "zhuhai", "vienna open",
        "stockholm open", "basel open", "paris masters", "bercy",
        # ITF/Challenger city names (common Polymarket pattern: "City: P1 vs P2")
        "liberec", "bonn ", " bonn:", "targu mures", "mures",
        "prostejov", "hammamet", "shymkent", "fergana", "trnava",
        "bratislava", "kosice", "houston open", "orlando open",
        # Top ATP players (men only — WTA players are in _WOMENS_TENNIS_KW)
        "djokovic", "alcaraz", "sinner", "nadal", "federer",
        "medvedev", "zverev", "rublev", "tsitsipas", "fritz",
        "de minaur", "draper", "shelton ", "musetti", "hurkacz",
        "rune ", "ruud ", "dimitrov", "norrie ", "khachanov",
        "bublik", "cerundolo", "arnaldi", "navone", "zhang j",
        "zheng j", "landaluce", "van assche", "blockx",
        "shapovalov", "auger-aliassime", "tiafoe", "davidovich",
        "popyrin", "darderi", "svrcina", "pacheco", "gea ",
        "barton ", "bueno ", "roncadelli", "gombos",
    ],
    "American Football": [
        "nfl", "super bowl", "quarterback", "touchdown", "nfl draft",
        "patriots", "chiefs", "eagles", "cowboys", "49ers", "packers",
        "bills ", "ravens", "bengals", "browns", "steelers",
        "texans", "colts", "jaguars", "titans", "broncos",
        "raiders", "chargers", "seahawks", "rams ", "cardinals",
        "falcons", "panthers", "saints", "buccaneers", "bears",
        "lions ", "vikings", "giants ", "jets ", "commanders",
    ],
    "Cricket": [
        "cricket", " icc ", " odi ", " t20", "test match", " ipl",
        " bbl ", " ashes", "innings", "wicket", "t20 series",
        "test series", "one day", "world cup cricket",
    ],
}

# ITF/Challenger title pattern: "CityName: Firstname Lastname vs Firstname Lastname"
# Used as last-resort tennis detection when keyword lookup returns "Other" but
# sportsMarketType is set and there's no esports signal.
_CITY_TENNIS_RE = re.compile(
    r'^[A-Z][A-Za-z\s,]+:\s+[A-Z][a-z]+ [A-Z][a-z]+ vs\.? [A-Z][a-z]+ [A-Z][a-z]'
)

def _is_itf_tennis_pattern(question: str) -> bool:
    """Return True if question looks like '[City]: Firstname Lastname vs Firstname Lastname'."""
    return bool(_CITY_TENNIS_RE.match(question.strip()))

# WTA players and women's-tennis markers — block all women's tennis
_WOMENS_TENNIS_KW = [
    " wta", "women's tennis", "ladies'",
    # Top WTA players (2025-26 season)
    "swiatek", "sabalenka", "gauff", "rybakina", "pegula",
    "kasatkina", "jabeur", "ostapenko", "kvitova", "andreescu",
    "pliskova", "bencic", "azarenka", "krejcikova", "vondrousova",
    "haddad maia", " garcia ", "fernandez", "badosa", "samsonova",
    "kontaveit", "yastremska", "alexandrova", "potapova",
    "fruhvirtova", "tauson", "linette", "heather watson",
    "kudermetova", "lepchenko", "pareja", "keys ", " stephens",
    "anisimova", "navarro ", "kenin ", "panova ", "sherif",
    "cornet", "bouzkova", "sorribes", "noskova", "muchova",
    "sramkova", "andreeva", "shnaider", "rakhimova", "zarazua",
    "collins d", "danielle collins", "jessica pegula",
]

def is_womens_tennis(m: dict) -> bool:
    """Return True if the market is a women's tennis market."""
    q = " " + (m.get("question") or m.get("title") or "").lower() + " "
    return any(kw in q for kw in _WOMENS_TENNIS_KW)

def detect_sport(question: str) -> str:
    """Return the sport name detected from the market question, or 'Other'."""
    q = " " + question.lower() + " "
    for sport, keywords in _SPORT_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return sport
    return "Other"

def is_sports_market(m: dict) -> bool:
    """Returns True only if the market is a sport in ALLOWED_SPORTS."""
    if m.get("sportsMarketType"):
        return True
    q = m.get("question") or m.get("title") or ""
    return detect_sport(q) != "Other"

def detect_market_type(m: dict) -> str:
    st = (m.get("sportsMarketType") or "").lower()
    if any(t in st for t in _SPORTS_FIRST): return "sports_first_set"
    q = (m.get("question") or "").lower()
    if any(kw in q for kw in _POLITICAL_KW): return "political"
    try:
        outs = m.get("outcomes", "[]")
        if isinstance(outs, str): outs = json.loads(outs)
        if len(outs) == 2 and {o.strip().lower() for o in outs} in (
                {"yes","no"}, {"true","false"}, {"over","under"}):
            return "binary_fixed"
    except Exception: pass
    return "general"

def compute_score(m: dict) -> dict:
    vol   = float(m.get("volume24hr", 0) or 0)
    prob_c= float(m.get("prob_change", 0) or 0)
    ratio = float(m.get("spike_ratio", 1) or 1)
    hist  = bool(m.get("spike_has_history", False))
    mtype = m.get("market_type", "general")

    liq   = 3 if vol >= 25000 else 2 if vol >= 5000 else 1 if vol >= 2500 else 0
    price = 2 if prob_c > 0.05 else 1 if prob_c >= 0.02 else 0
    spike = 2 if ratio > 5 else 1 if ratio >= 2 else 0
    rel   = 2 if mtype == "binary_fixed" else 0 if mtype == "sports_first_set" else 1
    cat   = 1 if (prob_c >= 0.02 or ratio >= 2) else 0
    p_adj = 1 if prob_c > 0.05 else -1 if prob_c < 0.02 else 0
    s_adj = (1 if ratio > 5 else -1 if ratio < 2 else 0) if hist else 0
    # Sports bonus: Polymarket-tagged sports markets have no probe history on
    # first scan, so raw score understates their quality. +2 corrects for this.
    sports_bonus = 2 if m.get("sportsMarketType") else 0
    final = max(1, min(10, liq + price + spike + rel + cat + p_adj + s_adj + sports_bonus))

    if final >= 8:   classif, conf = "🟢 TRADEABLE", "High"
    elif final >= 5: classif, conf = "🟡 WATCH",     "Medium"
    else:            classif, conf = "🔴 IGNORE",    "Low"

    return {"signal_score_v2": final, "classification": classif, "confidence": conf}

# ── SCANNER ───────────────────────────────────────────────────
def extract_best_outcome(m: dict) -> tuple:
    try:
        outs   = m.get("outcomes")
        prices = m.get("outcomePrices") or m.get("outcome_prices")
        if outs and prices:
            if isinstance(outs,   str): outs   = json.loads(outs)
            if isinstance(prices, str): prices = json.loads(prices)
            pairs = list(zip(outs, [float(p) for p in prices]))
            if pairs: best = max(pairs, key=lambda x: x[1]); return best[1], best[0]
    except Exception: pass
    for key in ("probability", "best_bid", "lastTradePrice", "price"):
        v = m.get(key)
        if v is not None:
            try: return float(v), "YES"
            except Exception: pass
    return 0.0, "YES"

async def fetch_markets_page(session, offset=0, limit=100) -> list:
    try:
        async with session.get(
            f"{POLYMARKET_API}/markets",
            params={"active":"true","closed":"false","limit":limit,"offset":offset,
                    "order":"volume24hr","ascending":"false"},
            timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                d = await r.json()
                return d if isinstance(d, list) else d.get("markets", d.get("results", []))
    except Exception as e: log.error(f"fetch_markets_page offset={offset}: {e}")
    return []

def days_until(end_date_str) -> float | None:
    if not end_date_str: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
            return (dt - datetime.now(timezone.utc)).total_seconds() / 86400
        except ValueError: continue
    return None

async def scan_markets() -> dict:
    log.info("Starting Polymarket scan...")
    vol_snaps  = _jload(VOL_SNAP_PATH)
    prob_snaps = _jload(PROB_SNAP_PATH)
    all_markets = []

    async with aiohttp.ClientSession() as session:
        pages = await asyncio.gather(
            *[fetch_markets_page(session, i*100, 100) for i in range(5)],
            return_exceptions=True)
        for p in pages:
            if isinstance(p, list): all_markets.extend(p)

    log.info(f"Fetched {len(all_markets)} markets")
    enriched, spikes = [], []

    for m in all_markets:
        mid = str(m.get("id") or m.get("conditionId") or "")
        if not mid: continue

        prob, best_label = extract_best_outcome(m)
        m["best_probability"]    = prob
        m["best_outcome_label"]  = best_label
        vol24 = float(m.get("volume24hr", 0) or 0)

        hist  = vol_snaps.get(mid, [])
        ratio = (vol24 / (sum(hist)/len(hist))) if len(hist) >= 2 and sum(hist) > 0 else 1.0
        has_h = len(hist) >= 2
        m["spike_ratio"]        = round(ratio, 2)
        m["spike_has_history"]  = has_h
        if ratio >= VOLUME_SPIKE_THRESH:
            m["volume_spike"] = True; spikes.append(m)

        ph = prob_snaps.get(mid, [])
        m["prob_change"]  = round(abs(prob - ph[-1]), 4) if ph else 0.0
        m["market_type"]  = detect_market_type(m)
        m.update(compute_score(m))

        # Polymarket-tagged sports markets bypass the scoring system —
        # they have no prob-history yet but are legitimate signals.
        if m.get("sportsMarketType"):
            m["classification"] = "🟢 TRADEABLE"
            m["confidence"]     = "High"

        # build URL
        events = m.get("events")
        if events and isinstance(events, list) and events:
            es = events[0].get("slug", ""); ms = m.get("slug", "")
            m["polymarket_url"] = (f"https://polymarket.com/event/{es}/{ms}" if es and ms
                                   else f"https://polymarket.com/event/{es or ms}")
        else:
            ms = m.get("slug", "")
            m["polymarket_url"] = f"https://polymarket.com/event/{ms}" if ms else ""

        vol_snaps.setdefault(mid, []).append(vol24)
        vol_snaps[mid] = vol_snaps[mid][-12:]
        prob_snaps.setdefault(mid, []).append(prob)
        prob_snaps[mid] = prob_snaps[mid][-12:]
        enriched.append(m)

    _jsave(VOL_SNAP_PATH,  vol_snaps)
    _jsave(PROB_SNAP_PATH, prob_snaps)

    tier1 = []
    for m in enriched:
        if float(m.get("volume24hr", 0) or 0) < HARD_VOL_24H_MIN: continue
        # Block esports even when sportsMarketType is set
        if is_esports(m): continue
        if not is_sports_market(m): continue
        prob = m["best_probability"]
        if prob < MIN_PROBABILITY or prob > MAX_PROBABILITY: continue
        end  = m.get("end_date_iso") or m.get("endDate") or m.get("end_date")
        days = days_until(end)
        if not days or days <= 0: continue
        # Score gate — must be at least 8/10 (below 8 = no stake recommendation)
        if m.get("signal_score_v2", 0) < 8: continue
        # Detect sport; for sportsMarketType markets with city-pattern titles
        # (e.g. "Liberec: Barton vs Bueno") fall back to tennis pattern detection
        q     = m.get("question") or m.get("title") or ""
        sport = detect_sport(q)
        if sport == "Other" and m.get("sportsMarketType"):
            if _is_itf_tennis_pattern(q):
                sport = "Tennis"
        if sport not in ALLOWED_SPORTS: continue
        # Block women's tennis; relabel men's tennis to "Tennis (ATP)" for stats
        if sport == "Tennis" and is_womens_tennis(m): continue
        if sport == "Tennis": sport = "Tennis (ATP)"
        m["days_remaining"] = round(days, 1)
        m["probability"]    = prob
        m["sport"]          = sport
        if TIER1_MIN_DAYS <= days <= TIER1_MAX_DAYS: tier1.append(m)

    tier1 = sorted(tier1, key=lambda x: x["probability"], reverse=True)[:SIGNALS_PER_TIER]
    log.info(f"Scan done — Tier1:{len(tier1)} Spikes:{len(spikes)}")
    return {"tier1": tier1, "spikes": spikes[:10], "timestamp": time.time()}

# ── WHALE TRACKING ────────────────────────────────────────────
def _trunc(addr: str) -> str:
    """0x1234...5678"""
    if not addr or len(addr) < 12: return addr
    return f"{addr[:6]}...{addr[-4:]}"

async def _clob_recent_trades(session, condition_id: str, since_seconds=3600) -> list:
    """
    Fetch recent trades from Polymarket CLOB for a given condition_id.
    Returns list of {wallet, outcome, usd_amount, side, tx_hash}.
    """
    try:
        async with session.get(
            f"{CLOB_API}/trades",
            params={"market": condition_id, "limit": "100"},
            timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200: return []
            data = await r.json()
    except Exception as e:
        log.debug(f"[whale/clob] {condition_id}: {e}"); return []

    if not isinstance(data, list): data = data.get("data", [])
    cutoff = time.time() - since_seconds
    trades = []
    for t in data:
        try:
            # parse match_time
            mt = t.get("match_time") or ""
            if mt:
                for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        ts = datetime.strptime(mt, fmt).replace(tzinfo=timezone.utc).timestamp(); break
                    except ValueError: ts = 0
            else: ts = 0
            if ts < cutoff: continue

            price  = float(t.get("price", 0) or 0)
            size   = float(t.get("size",  0) or 0)
            usd    = round(price * size, 2)
            if usd < WHALE_MIN_USD: continue

            side   = (t.get("side") or "BUY").upper()
            # taker is the active party (market order); use maker for large passive fills
            wallet = t.get("taker_address") or t.get("maker_address") or ""
            if not wallet: continue
            outcome= (t.get("outcome") or "").upper() or "YES"
            trades.append({
                "wallet":     wallet,
                "outcome":    outcome,
                "usd_amount": usd,
                "side":       side,
                "tx_hash":    t.get("transaction_hash", ""),
            })
        except Exception: continue
    return trades


async def _polygon_recent_buys(condition_id: str, token_ids: list, since_blocks=150) -> list:
    """
    Use Polygon RPC eth_getLogs to find TransferSingle mints (from=0x0) on CTF contract.
    Returns list of {wallet, outcome, usd_amount, side, tx_hash}.
    Only runs when POLYGON_RPC_URL is configured.
    """
    if not POLYGON_RPC_URL: return []
    rpc = POLYGON_RPC_URL

    # token_id → outcome label
    tid_map = {}
    for i, tid in enumerate(token_ids or []):
        tid_map[str(tid).lower()] = "YES" if i == 0 else "NO"

    async def rpc_call(method, params):
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc,
                json={"jsonrpc":"2.0","id":1,"method":method,"params":params},
                timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json(); return d.get("result")

    try:
        hex_block = await rpc_call("eth_blockNumber", [])
        current   = int(hex_block, 16)
        from_blk  = hex(max(0, current - since_blocks))

        logs = await rpc_call("eth_getLogs", [{
            "address":   CTF_CONTRACT,
            "fromBlock": from_blk,
            "toBlock":   "latest",
            "topics":    [TRANSFER_SINGLE_SIG, None, ZERO_ADDR_PAD],  # from = 0x0 = mint = buy
        }])
    except Exception as e:
        log.debug(f"[whale/rpc] eth_getLogs: {e}"); return []

    if not logs: return []
    trades = []
    for lg in logs:
        try:
            topics = lg.get("topics", [])
            if len(topics) < 4: continue
            # operator = topics[1], from = topics[2]=0, to = topics[3]
            to_addr  = "0x" + topics[3][-40:]      # buyer
            data_hex = lg.get("data", "0x")[2:]     # 64 hex chars per param
            if len(data_hex) < 128: continue
            token_id_hex = data_hex[:64]
            value_hex    = data_hex[64:128]
            token_id_int = int(token_id_hex, 16)
            value        = int(value_hex, 16) / 1e6   # USDC 6 decimals

            if value < WHALE_MIN_USD: continue
            outcome = tid_map.get(str(token_id_int).lower(), "")
            if not outcome: continue   # token not from this market

            trades.append({
                "wallet":     to_addr,
                "outcome":    outcome,
                "usd_amount": round(value, 2),
                "side":       "BUY",
                "tx_hash":    lg.get("transactionHash", ""),
            })
        except Exception: continue
    return trades


async def get_whale_trades(session, market: dict) -> list:
    """
    Get top whale bets for a market. Uses CLOB API first;
    if Polygon RPC configured, merges results.
    Returns top 3 by usd_amount with same-wallet aggregation.
    """
    condition_id = str(market.get("id") or market.get("conditionId") or "")
    if not condition_id: return []

    clob_trades  = await _clob_recent_trades(session, condition_id)

    rpc_trades = []
    if POLYGON_RPC_URL:
        token_ids = market.get("clobTokenIds") or []
        if isinstance(token_ids, str):
            try: token_ids = json.loads(token_ids)
            except Exception: token_ids = []
        rpc_trades = await _polygon_recent_buys(condition_id, token_ids)

    all_trades = clob_trades + rpc_trades

    # aggregate by wallet
    wallet_totals: dict[str, dict] = {}
    for t in all_trades:
        w = t["wallet"].lower()
        if w not in wallet_totals:
            wallet_totals[w] = {
                "wallet":     t["wallet"],
                "outcome":    t["outcome"],
                "usd_amount": 0.0,
                "side":       t["side"],
                "tx_hash":    t["tx_hash"],
            }
        wallet_totals[w]["usd_amount"] += t["usd_amount"]
        # store largest tx hash
        if t["usd_amount"] > wallet_totals[w]["usd_amount"] * 0.8:
            wallet_totals[w]["tx_hash"] = t["tx_hash"]

    # record to DB and return top 3
    top3 = sorted(wallet_totals.values(), key=lambda x: x["usd_amount"], reverse=True)[:3]
    cid  = str(market.get("id") or market.get("conditionId") or "")
    for tr in top3:
        record_whale_trade(cid, tr["wallet"], tr["outcome"], tr["usd_amount"], tr["side"], tr["tx_hash"])
    return top3

# ── GRADER ────────────────────────────────────────────────────
VOID_SENTINEL = "__VOID__"  # returned when a market is permanently unresolvable

def resolve_winner(data, expired_at: float = 0) -> str | None:
    """
    Returns the winning outcome label from a resolved market API response,
    VOID_SENTINEL if the market is permanently stuck (closed >14 days, equal prices),
    or None if the market hasn't resolved yet.
    """
    if not data: return None
    closed   = data.get("closed") or data.get("resolved")
    if not closed: return None
    try:
        outs   = data.get("outcomes", "[]")
        prices = data.get("outcomePrices", "[]")
        if isinstance(outs,   str): outs   = json.loads(outs)
        if isinstance(prices, str): prices = json.loads(prices)
        fp = [float(p) for p in prices]
        if not fp: return None
        bi = max(range(len(fp)), key=lambda i: fp[i])
        if fp[bi] >= 0.95:
            return outs[bi]
        # Prices stuck near equal — check if it's been >14 days since expiry
        days_since_expiry = (time.time() - expired_at) / 86400 if expired_at else 0
        if days_since_expiry > 14:
            return VOID_SENTINEL  # permanently unresolvable, mark as void
        return None
    except Exception as e: log.error(f"resolve_winner: {e}"); return None


async def _fetch_market(session, mid: str) -> dict | None:
    try:
        async with session.get(
            f"{POLYMARKET_API}/markets/{mid}",
            timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.json() if r.status == 200 else None
    except Exception as e:
        log.error(f"[grader] fetch market {mid}: {e}"); return None


async def grade_signals():
    sigs = get_ungraded_tradeable()
    if not sigs:
        log.info("[grader] Nothing to grade."); return

    graded_count = 0
    void_count   = 0
    pending_count = 0
    BATCH = 20  # concurrent API calls per batch

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(sigs), BATCH):
            batch = sigs[i:i+BATCH]
            results = await asyncio.gather(
                *[_fetch_market(session, s["market_id"]) for s in batch])

            for sig, data in zip(batch, results):
                if not data: continue
                mid = sig["market_id"]
                expired_at = sig.get("sent_at", 0) + sig.get("days_remaining", 0) * 86400
                winner = resolve_winner(data, expired_at)

                if winner == VOID_SENTINEL:
                    # Permanently unresolvable — mark void so it leaves the queue
                    update_grade(sig["id"], -1, "VOID", 0)
                    void_count += 1
                    log.info(f"[grader] id={sig['id']} market={mid} VOID (closed, prices stuck)")
                    continue

                if not winner:
                    pending_count += 1
                    continue  # genuinely not resolved yet

                predicted   = (sig.get("outcome") or "").strip().upper()
                was_correct = 1 if winner.strip().upper() == predicted else 0

                # Brier score → 0–100
                prob  = sig.get("probability", 0.5)
                brier = (prob - (1.0 if was_correct else 0.0)) ** 2
                score = round((1 - brier) * 100, 2)

                update_grade(sig["id"], was_correct, winner, score)
                graded_count += 1
                log.info(f"[grader] id={sig['id']} predicted={predicted} actual={winner} correct={was_correct} score={score}")

                # Notify all users on every TRADEABLE grade
                emoji  = "✅" if was_correct else "❌"
                q      = sig.get("question", "?")
                q_trunc= q[:72] + "..." if len(q) > 75 else q
                cl     = sig.get("classification") or "TRADEABLE"
                sport  = sig.get("sport", "")
                s_em   = _SPORT_EMOJI.get(sport, "🏟️") if sport else "📋"
                await broadcast_alert(
                    f"{emoji} *SIGNAL GRADED*\n\n"
                    f"{s_em} {q_trunc}\n\n"
                    f"🎯 Predicted: *{predicted}*\n"
                    f"🏁 Actual result: *{winner}*\n"
                    f"📊 Probability at signal: {prob*100:.1f}%\n"
                    f"⭐ Brier score: {score}/100\n"
                    f"📅 Tier {sig.get('tier','?')} | {cl}\n")
                await asyncio.sleep(0.5)

            await asyncio.sleep(0.2)  # brief pause between batches

    log.info(f"[grader] Pass complete — graded={graded_count} void={void_count} pending={pending_count}")

# ── ALERTS ────────────────────────────────────────────────────
def prob_bar(p): return "█" * int(p * 10) + "░" * (10 - int(p * 10))

_SPORT_EMOJI = {
    "Baseball":          "⚾",
    "Tennis":            "🎾",
    "Tennis (ATP)":      "🎾",
    "Tennis (WTA)":      "🎾",
    "Basketball":        "🏀",
    "American Football": "🏈",
    "Cricket":           "🏏",
}

def compute_stake(sport: str, score) -> str:
    """
    Return the recommended stake for a signal.
    Tier 1A: Baseball or Tennis (ATP) + score ≥9  → $200
    Tier 1B: Baseball or Tennis (ATP) + score = 8 → $100
    Tier 2:  All other sports + score ≥8          → $50
    Pass:    score < 8                             → not sent (gate enforces this)
    """
    sc = float(score) if score and score != "–" else 0.0
    premium = sport in ("Baseball", "Tennis (ATP)")
    if sc >= 9 and premium: return "$200"
    if sc >= 8 and premium: return "$100"
    if sc >= 8:             return "$50"
    return "PASS"

_SIGNAL_HEADERS = {
    "NEW":      "🟢 NEW SIGNAL",
    "UPDATE":   "🔄 CONFIDENCE UPDATE",
    "REVERSAL": "🔴 REVERSAL",
}

def fmt_signal(m, rank, signal_type="NEW"):
    q     = (m.get("question") or m.get("title") or "Unknown")[:80]
    prob  = m.get("probability", 0)
    days  = m.get("days_remaining", 0)
    vol24 = float(m.get("volume24hr", 0) or 0)
    lbl   = (m.get("best_outcome_label") or "YES").upper()
    emo   = "🟢" if lbl in ("YES","OVER","TRUE") else "🔴"
    url   = m.get("polymarket_url", "")
    ul    = f"\n   🔗 {url}" if url else ""
    sc    = m.get("signal_score_v2", "–")
    sport = m.get("sport", "")
    s_em  = _SPORT_EMOJI.get(sport, "🏟️")
    hdr   = _SIGNAL_HEADERS.get(signal_type, "🟢 NEW SIGNAL")
    stake = compute_stake(sport, sc)
    st_em = "💰" if stake != "PASS" else "🚫"
    return (
        f"*{hdr}*\n"
        f"#{rank} | {s_em} {sport} | {q}\n"
        f"   {emo} BET: *{lbl}*\n"
        f"   📊 Prob: {prob*100:.1f}%  {prob_bar(prob)}\n"
        f"   ⏱ Expires: {days}d  |  💰 24h Vol: ${vol24:,.0f}\n"
        f"   🎯 Score: {sc}/10  |  {st_em} Stake: *{stake}*{ul}"
    )

def build_tier1_alert(markets):
    lines = ["🚨 *POLYMARKET SIGNAL ALERT*", ""]
    for i, m in enumerate(markets, 1):
        stype = m.get("signal_type", "NEW")
        lines += [fmt_signal(m, i, stype), ""]
    lines += ["━" * 35,
              "_Min 55% · Max 80% · 45-min cooldown · Max 2 updates per market_"]
    return "\n".join(lines)

def build_spike_alert(spikes, whale_data: dict):
    lines = ["⚡ *VOLUME SPIKE ALERT*", ""]
    for i, m in enumerate(spikes, 1):
        q    = (m.get("question") or m.get("title") or "Unknown")[:70]
        prob = m.get("probability", 0)
        vol24= float(m.get("volume24hr", 0) or 0)
        lbl  = (m.get("best_outcome_label") or "YES").upper()
        emo  = "🟢" if lbl in ("YES","OVER","TRUE") else "🔴"
        url  = m.get("polymarket_url", "")
        ul   = f"\n   🔗 {url}" if url else ""
        lines += [f"#{i} {q}",
                  f"   {emo} BET: *{lbl}* | 📊 {prob*100:.1f}% | 24h Vol: ${vol24:,.0f}{ul}"]

        mid    = str(m.get("id") or m.get("conditionId") or "")
        whales = whale_data.get(mid, [])
        if whales:
            lines.append("   🐋 *Whale Activity (last 60min):*")
            for w in whales:
                hist = get_wallet_win_rate(w["wallet"])
                lines.append(
                    f"      • {_trunc(w['wallet'])} bet *${w['usd_amount']:,.0f}* on {w['outcome']}{hist}")
        lines.append("")
    lines.append("_Unusual trading activity detected_")
    return "\n".join(lines)

def fmt_rate(rate):
    if rate is None: return "— (no graded signals yet)"
    if rate >= 70: return f"*{rate}%* — 🟢 Strong"
    if rate >= 50: return f"*{rate}%* — 🟡 Moderate"
    return f"*{rate}%* — 🔴 Below target"

async def send_report():
    h24 = get_24h_stats(); ats = get_all_time_stats()
    if not ats and not h24: return
    t1h = h24.get("tier1", {}); t2h = h24.get("tier2", {})
    lines = [
        "🤖 *BOT SELF-EVALUATION — Last 24 Hours*", "_(TRADEABLE signals only)_\n",
        f"📤 Signals sent:      {h24.get('sent', 0)}",
        f"✅ Resolved & graded: {h24.get('graded', 0)}",
        f"🎯 Success rate:      {fmt_rate(h24.get('success_rate'))}",
    ]
    if t1h.get("graded", 0) > 0 or t2h.get("graded", 0) > 0:
        lines.append("")
        if t1h.get("graded", 0) > 0:
            lines.append(f"   📅 Tier 1: {t1h['correct']}/{t1h['graded']} correct — {fmt_rate(t1h['success_rate'])}")
        if t2h.get("graded", 0) > 0:
            lines.append(f"   📅 Tier 2: {t2h['correct']}/{t2h['graded']} correct — {fmt_rate(t2h['success_rate'])}")
    gs = h24.get("graded_signals", [])
    if gs:
        lines += ["", "📋 *Graded this window:*"]
        for r in gs:
            ico = "✅" if r.get("was_correct") else "❌"
            lines.append(
                f"  {ico} [T{r.get('tier','?')}] {(r.get('question') or '')[:52]} | "
                f"{round((r.get('probability') or 0)*100,1)}% → {r.get('signal_score') or 0:.0f}/100")
    elif h24.get("graded", 0) == 0 and h24.get("sent", 0) > 0:
        lines.append("\n_Markets still open — check back once they resolve._")

    lines += ["", "━" * 35, ""]
    paper_net = ats.get("paper_net", 0.0); paper_roi = ats.get("paper_roi", 0.0)
    net_str = f"+${paper_net:,.2f}" if paper_net >= 0 else f"-${abs(paper_net):,.2f}"
    lines += [
        "📈 *ALL-TIME PERFORMANCE (TRADEABLE)*\n",
        f"📊 Total Signals Sent: {ats.get('total', 0)}",
        f"⏳ Pending Grading:    {ats.get('pending', 0)}",
        f"✅ Graded Signals:     {ats.get('resolved', 0)}",
        f"🎯 Overall Accuracy:  {fmt_rate(ats.get('accuracy') if ats.get('resolved', 0) > 0 else None)}",
        f"💰 Paper ROI:         {net_str}  ({paper_roi:+.1f}%)",
        f"📌 Unique Markets:    {ats.get('unique_total', 0)} | Win Rate: {ats.get('unique_win_rate', 0)}%",
    ]
    rec = ats.get("recent", [])
    if rec:
        lines += ["", "🕓 *Last 5 Graded Signals*"]
        for r in rec:
            ico = "✅" if r.get("was_correct") else "❌"
            lines.append(
                f"  {ico} {(r.get('question') or '')[:55]} | "
                f"{round((r.get('probability') or 0)*100,1)}% → {r.get('signal_score') or 0:.0f}/100")
    await send_msg("\n".join(lines))


def _is_visible(m: dict) -> bool:
    """Only TRADEABLE sports signals are sent. WATCH, IGNORE and non-sports are suppressed."""
    cl = m.get("classification", "")
    return "TRADEABLE" in cl


def _market_state(cds: dict, mid: str) -> dict:
    """Read per-market alert state. Handles migration from old float-timestamp format."""
    entry = cds.get(mid, {})
    if isinstance(entry, (int, float)):
        return {"count": 1, "last_outcome": "", "last_at": float(entry)}
    return entry or {"count": 0, "last_outcome": "", "last_at": 0}


async def process_alerts(scan_result: dict):
    t1     = scan_result.get("tier1", [])
    spikes = scan_result.get("spikes", [])
    cds    = _jload(COOLDOWN_PATH)
    now    = time.time()

    filtered = []
    for m in t1:
        if not _is_visible(m): continue
        mid = str(m.get("id") or m.get("conditionId") or "")
        if not mid: continue

        state = _market_state(cds, mid)
        age   = now - state.get("last_at", 0)

        # Respect 45-min cooldown between any two updates for the same market
        if age < COOLDOWN_MINUTES * 60: continue

        count        = state.get("count", 0)
        prev_outcome = state.get("last_outcome", "")
        cur_outcome  = (m.get("best_outcome_label") or "YES").upper()
        is_reversal  = bool(prev_outcome) and cur_outcome != prev_outcome

        # Max 2 signals per market; a 3rd only fires when the edge reverses
        if count >= MAX_SIGNALS_PER_MARKET and not is_reversal: continue

        if count == 0:
            stype = "NEW"
        elif is_reversal:
            stype = "REVERSAL"
        else:
            stype = "UPDATE"

        m["signal_type"] = stype
        filtered.append(m)

    if not filtered:
        log.info("All markets on cooldown or IGNORE — no new alerts."); return

    alert_text = build_tier1_alert(filtered)
    await broadcast_alert(alert_text)
    log.info(f"Main alert broadcast ({len(filtered)} signal(s))")

    for m in filtered:
        mid         = str(m.get("id") or m.get("conditionId") or "")
        if not mid: continue
        state       = _market_state(cds, mid)
        cur_outcome = (m.get("best_outcome_label") or "YES").upper()
        stype       = m.get("signal_type", "NEW")
        # Reversal resets count to 1 so the new direction can get one UPDATE
        new_count   = 1 if stype == "REVERSAL" else state.get("count", 0) + 1
        cds[mid]    = {"count": new_count, "last_outcome": cur_outcome, "last_at": now}
        record_signal(
            mid,
            m.get("question") or m.get("title") or "",
            m.get("probability", 0),
            m.get("days_remaining", 0),
            1,
            float(m.get("volume", 0) or 0),
            m.get("best_outcome_label") or "YES",
            m.get("classification", ""),
            m.get("sport", "Other"),
            stype,
        )
    _jsave(COOLDOWN_PATH, cds)

# ── COMMANDS ──────────────────────────────────────────────────
async def send_stats_msg(target_chat_id: str | None = None):
    """Send all-time grading stats to Telegram (used by /stats command and daily report).
    If target_chat_id is provided, sends to that user; otherwise sends to admin."""
    ats = get_all_time_stats()
    if not ats:
        await send_to("⚠️ No stats available yet.", target_chat_id or TELEGRAM_CHAT_ID)
        return

    resolved = ats.get("resolved", 0)
    correct  = ats.get("correct", 0)
    pending  = ats.get("pending", 0)
    total    = ats.get("total", 0)
    losses   = resolved - correct
    accuracy = ats.get("accuracy", 0.0)

    u_total   = ats.get("unique_total", 0)
    u_correct = ats.get("unique_correct", 0)
    u_wrong   = ats.get("unique_wrong", 0)
    u_rate    = ats.get("unique_win_rate", 0.0)
    avg_upd   = ats.get("avg_updates", 0.0)

    paper_n       = ats.get("paper_n", 0)
    paper_wins    = ats.get("paper_wins", 0)
    paper_losses  = ats.get("paper_losses", 0)
    paper_net     = ats.get("paper_net", 0.0)
    paper_roi     = ats.get("paper_roi", 0.0)
    paper_avg_prob= ats.get("paper_avg_prob", 0.0)
    paper_edge    = ats.get("paper_edge", 0.0)

    if accuracy >= 70:   acc_emo = "🟢"
    elif accuracy >= 50: acc_emo = "🟡"
    else:                acc_emo = "🔴"
    if paper_roi >= 10:  roi_emo = "🟢"
    elif paper_roi >= 0: roi_emo = "🟡"
    else:                roi_emo = "🔴"
    if u_rate >= 70:     ur_emo  = "🟢"
    elif u_rate >= 50:   ur_emo  = "🟡"
    else:                ur_emo  = "🔴"

    lines = [
        "━" * 30,
        "📊  *SHARPFLOW — ALL-TIME STATS*",
        "        _(Sports TRADEABLE signals only)_",
        "━" * 30,
        "",
        f"  📤  Total Signals:    {total}",
        f"  ✅  Graded:           {resolved}",
        f"  ⏳  Pending:          {pending}",
        "",
        f"  🏆  Wins:             {correct}",
        f"  ❌  Losses:           {losses}",
        f"  {acc_emo}  Win Rate:         *{accuracy}%*  ({correct}/{resolved})",
        "",
        "─" * 30,
        "  🏟️  *UNIQUE MARKET STATS*",
        "─" * 30,
        "",
        f"  📌  Unique Markets:   {u_total}",
        f"  ✅  Correct:          {u_correct}",
        f"  ❌  Wrong:            {u_wrong}",
        f"  {ur_emo}  Market Win Rate:  *{u_rate}%*",
        f"  🔄  Avg Updates/Mkt:  {avg_upd}",
    ]

    if paper_n > 0:
        net_str  = f"+${paper_net:,.2f}" if paper_net >= 0 else f"-${abs(paper_net):,.2f}"
        edge_str = f"{paper_edge:+.1f}%"
        if paper_edge >= 2:   edge_emo = "🟢"
        elif paper_edge >= 0: edge_emo = "🟡"
        else:                 edge_emo = "🔴"
        lines += [
            "",
            "─" * 30,
            "  💰  *PAPER TRADING ($100/signal)*",
            "─" * 30,
            "",
            f"  📊  Signals graded:  {paper_n}  ({paper_wins}W / {paper_losses}L)",
            f"  💵  Total invested:  ${paper_n * 100:,}",
            f"  {roi_emo}  Net P&L:          *{net_str}*",
            f"  {roi_emo}  ROI:              *{paper_roi:+.1f}%*",
            "",
            f"  🔢  Avg signal prob:  {paper_avg_prob}%  (= break-even win rate)",
            f"  🎯  Actual win rate:  {round(paper_wins/paper_n*100,1) if paper_n else 0}%",
            f"  {edge_emo}  Edge:             *{edge_str}*",
            f"        _(+edge = beating the market)_",
        ]

    sport_rows = ats.get("sport_stats", [])
    if sport_rows:
        lines += ["", "─" * 30, "  🏅  *WIN RATE BY SPORT*", "─" * 30, ""]
        for row in sport_rows:
            tot  = row.get("total", 0)
            wins = row.get("wins", 0)
            loss = tot - wins
            rate = round(wins / tot * 100, 1) if tot else 0.0
            if   rate >= 70: emo = "🟢"
            elif rate >= 50: emo = "🟡"
            else:            emo = "🔴"
            lines.append(f"  {emo}  *{row['sport']}*")
            lines.append(f"       {wins}W  /  {loss}L  —  {rate}%")
            lines.append("")

    lines += ["━" * 30, "  _/stats — refresh anytime_"]
    await send_to("\n".join(lines), target_chat_id or TELEGRAM_CHAT_ID)


async def commands_loop():
    """Poll Telegram for user commands. Admin can /enable & /disable users."""
    offset = 0
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    log.info("[commands] Command listener started.")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{tg_url}/getUpdates",
                    params={"offset": offset, "timeout": 30, "allowed_updates": '["message"]'},
                    timeout=aiohttp.ClientTimeout(total=40),
                ) as r:
                    data = await r.json()

            if not data.get("ok"):
                await asyncio.sleep(5); continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg      = update.get("message", {})
                raw_text = (msg.get("text") or "").strip()
                text     = raw_text.lower()
                chat     = str(msg.get("chat", {}).get("id", ""))
                username = (msg.get("from", {}).get("username") or "").lower().lstrip("@")

                is_admin = (chat == ADMIN_CHAT_ID)

                # ── Admin-only: enable / disable users ────────────────
                if is_admin and text.startswith("/enable "):
                    target = raw_text.split(None, 1)[1].strip().lower().lstrip("@")
                    users  = load_allowed_users()
                    users.add(target)
                    save_allowed_users(users)
                    log.info(f"[commands] enabled user: {target}")
                    await send_to(f"✅ *@{target}* has been granted access to SharpFlow.", chat)
                    continue

                if is_admin and text.startswith("/disable "):
                    target = raw_text.split(None, 1)[1].strip().lower().lstrip("@")
                    users  = load_allowed_users()
                    users.discard(target)
                    save_allowed_users(users)
                    log.info(f"[commands] disabled user: {target}")
                    await send_to(f"🚫 *@{target}* has been removed from SharpFlow.", chat)
                    continue

                if is_admin and text.startswith("/broadcast "):
                    body = raw_text.split(None, 1)[1].strip()
                    blast = (
                        "👑 MESSAGE FROM OWNER\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"{body}\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    )
                    ids = get_broadcast_ids()
                    sent_to = 0
                    for cid in ids:
                        if await send_to(blast, cid, parse_mode=""): sent_to += 1
                    # Warn admin about users who haven't activated the bot yet
                    db = _load_user_db()
                    no_start = [u for u, info in db.items() if not info.get("chat_id")]
                    note = ""
                    if no_start:
                        note = f"\n⚠️ {len(no_start)} user(s) haven't sent /start yet: " + ", ".join(f"@{u}" for u in no_start)
                    log.info(f"[commands] broadcast sent to {sent_to}/{len(ids)} user(s)")
                    await send_to(f"✅ Broadcast delivered to *{sent_to}* user(s).{note}", chat)
                    continue

                if is_admin and text in ("/users", "users"):
                    users = load_allowed_users()
                    if users:
                        ul = "\n".join(f"  • @{u}" for u in sorted(users))
                        await send_to(f"👥 *Allowed Users*\n\n{ul}", chat)
                    else:
                        await send_to("👥 No extra users enabled yet.\nUse /enable username to add one.", chat)
                    continue

                # ── /start — register chat_id and welcome ─────────────
                if text == "/start":
                    if is_allowed_user(username, chat):
                        if username and chat != ADMIN_CHAT_ID:
                            register_chat_id(username, chat)
                        await send_to(
                            "✅ SharpFlow activated!\n\n"
                            "You'll now receive live bet signals, grading results, and "
                            "owner broadcasts directly here.\n\n"
                            "Use /stats to check the win rate, or /help for all commands.",
                            chat, parse_mode="")
                        log.info(f"[commands] /start registered {username or chat}")
                    else:
                        await send_to(
                            "🔒 You don't have access to SharpFlow.\n"
                            "Ask the owner to add you.", chat, parse_mode="")
                    continue

                # ── General commands: admin + allowed users ────────────
                if not is_allowed_user(username, chat):
                    continue  # silently ignore unknown users

                # Record chat_id so this user receives broadcast alerts
                if username and chat != ADMIN_CHAT_ID:
                    register_chat_id(username, chat)

                if text in ("/stats", "/stat", "stats", "stat"):
                    log.info(f"[commands] /stats requested by {username or chat}")
                    await send_stats_msg(target_chat_id=chat)

                elif text in ("/help", "help"):
                    help_text = (
                        "━" * 28 + "\n"
                        "🤖  *SharpFlow Commands*\n"
                        "━" * 28 + "\n\n"
                        "  /stats  —  Win rate & sport breakdown\n"
                        "  /help   —  This message\n"
                    )
                    if is_admin:
                        help_text += (
                            "\n─" + "─" * 27 + "\n"
                            "  🔑  *Admin Commands*\n\n"
                            "  /enable \\<username\\>      — Grant access\n"
                            "  /disable \\<username\\>     — Revoke access\n"
                            "  /users                  — List allowed users\n"
                            "  /broadcast \\<message\\>   — Send message to all users\n"
                        )
                    await send_to(help_text, chat)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[commands] {e}", exc_info=True)
            await asyncio.sleep(10)


# ── MAIN LOOPS ────────────────────────────────────────────────
async def scan_loop():
    count = 0
    while True:
        try:
            log.info(f"=== Scan #{count+1} ===")
            result = await scan_markets()
            await process_alerts(result)
            count += 1
        except asyncio.CancelledError: raise
        except Exception as e: log.error(f"[scan_loop] {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)


async def heartbeat_loop():
    await asyncio.sleep(HEARTBEAT_INTERVAL)
    while True:
        try:
            await send_msg(
                f"💓 *HEARTBEAT* | {datetime.now().strftime('%H:%M')}\n"
                "System fully operational.")
        except asyncio.CancelledError: raise
        except Exception as e: log.error(f"[heartbeat] {e}", exc_info=True)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def grading_loop():
    await asyncio.sleep(120)  # give DB time to accumulate signals
    while True:
        try:
            log.info("[grader] Running pass...")
            await grade_signals()
        except asyncio.CancelledError: raise
        except Exception as e: log.error(f"[grading_loop] {e}", exc_info=True)
        await asyncio.sleep(GRADING_INTERVAL)


async def report_loop():
    await asyncio.sleep(REPORT_INTERVAL)
    while True:
        try:
            log.info("Sending daily report...")
            await send_report()
        except asyncio.CancelledError: raise
        except Exception as e: log.error(f"[report_loop] {e}", exc_info=True)
        await asyncio.sleep(REPORT_INTERVAL)


async def main():
    init_db()
    whale_note = " | Polygon RPC: Active" if POLYGON_RPC_URL else ""
    await send_msg(
        "🚀 *SharpFlow Online*\n\n"
        "✅ Polymarket Scanner: Active\n"
        f"🐋 Whale Tracking: Active{whale_note}\n"
        f"⏱ Scan interval: every {SCAN_INTERVAL//60} minutes\n"
        "🎯 Tier 1 only — 1–7 day expiry\n"
        "📊 Prob range: 55–80% | Max 2 signals/market\n"
        "⚽ Sports markets only (TRADEABLE signals)\n"
        "🛡 Cooldown: 45 minutes | Reversal alerts enabled\n"
        "📈 Signal grading + paper trading ROI: Active\n"
        "📊 Commands: /stats | /help | /enable | /disable\n\n"
        "_Scanning Polymarket live data..._")
    await asyncio.gather(
        scan_loop(),
        heartbeat_loop(),
        grading_loop(),
        report_loop(),
        commands_loop(),
    )


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: Set TELEGRAM_BOT_TOKEN environment variable.")
        sys.exit(1)
    log.info(f"Starting SharpFlow (chat_id={TELEGRAM_CHAT_ID})")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
