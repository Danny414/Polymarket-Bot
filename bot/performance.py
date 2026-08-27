import sqlite3
import os
import time
from bot.config import DB_PATH
from bot.logger import logger


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            question TEXT,
            probability REAL,
            days_remaining REAL,
            tier INTEGER,
            volume REAL,
            sent_at REAL,
            resolved_at REAL,
            outcome TEXT,
            was_correct INTEGER,
            signal_score REAL,
            classification TEXT
        )
    """)
    # Migrate existing DBs — ignore errors if column already exists
    for col_sql in [
        "ALTER TABLE signals ADD COLUMN outcome TEXT",
        "ALTER TABLE signals ADD COLUMN classification TEXT",
    ]:
        try:
            cur.execute(col_sql)
            conn.commit()
        except Exception:
            pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS performance_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at REAL,
            total_signals INTEGER,
            correct_signals INTEGER,
            accuracy REAL,
            avg_probability REAL,
            tier1_accuracy REAL,
            tier2_accuracy REAL
        )
    """)
    conn.commit()
    conn.close()


def record_signal(
    market_id: str,
    question: str,
    probability: float,
    days_remaining: float,
    tier: int,
    volume: float,
    predicted_outcome: str = "",
    classification: str = "",
):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO signals
               (market_id, question, probability, days_remaining, tier, volume,
                sent_at, outcome, classification)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                market_id,
                question,
                probability,
                days_remaining,
                tier,
                volume,
                time.time(),
                predicted_outcome.strip().upper(),
                classification,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error recording signal: {e}")


def get_24h_stats() -> dict:
    """
    Success-rate self-evaluation scoped to TRADEABLE signals sent in the
    last 24 hours.  Only signals that have already been graded count toward
    the success rate so the number is never inflated by pending markets.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cutoff = time.time() - 86400  # 24 hours ago

        cur.execute(
            """SELECT COUNT(*) as c FROM signals
               WHERE classification LIKE '%TRADEABLE%'
                 AND sent_at >= ?""",
            (cutoff,),
        )
        sent = cur.fetchone()["c"]

        cur.execute(
            """SELECT COUNT(*) as c FROM signals
               WHERE classification LIKE '%TRADEABLE%'
                 AND sent_at >= ?
                 AND was_correct IS NOT NULL""",
            (cutoff,),
        )
        graded = cur.fetchone()["c"]

        cur.execute(
            """SELECT COUNT(*) as c FROM signals
               WHERE classification LIKE '%TRADEABLE%'
                 AND sent_at >= ?
                 AND was_correct = 1""",
            (cutoff,),
        )
        correct = cur.fetchone()["c"]

        # Per-tier breakdown (graded only)
        tier_rows = {}
        for tier in (1, 2):
            cur.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN was_correct=1 THEN 1 ELSE 0 END) as correct
                   FROM signals
                   WHERE classification LIKE '%TRADEABLE%'
                     AND sent_at >= ?
                     AND tier = ?
                     AND was_correct IS NOT NULL""",
                (cutoff, tier),
            )
            row = cur.fetchone()
            t = row["total"] or 0
            c = row["correct"] or 0
            tier_rows[tier] = {
                "graded": t,
                "correct": c,
                "success_rate": round(c / t * 100, 1) if t > 0 else None,
            }

        # Individual graded signals from the last 24h for the detail list
        cur.execute(
            """SELECT question, outcome, was_correct, probability, signal_score, tier
               FROM signals
               WHERE classification LIKE '%TRADEABLE%'
                 AND sent_at >= ?
                 AND was_correct IS NOT NULL
               ORDER BY resolved_at DESC""",
            (cutoff,),
        )
        graded_signals = [dict(r) for r in cur.fetchall()]

        conn.close()
        success_rate = round(correct / graded * 100, 1) if graded > 0 else None
        return {
            "sent": sent,
            "graded": graded,
            "correct": correct,
            "success_rate": success_rate,
            "tier1": tier_rows[1],
            "tier2": tier_rows[2],
            "graded_signals": graded_signals,
        }
    except Exception as e:
        logger.error(f"Error getting 24h stats: {e}")
        return {}


def get_performance_stats() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Only count TRADEABLE signals — WATCH/IGNORE are never graded
        cur.execute("SELECT COUNT(*) as c FROM signals WHERE classification LIKE '%TRADEABLE%'")
        total = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM signals WHERE classification LIKE '%TRADEABLE%' AND was_correct IS NOT NULL")
        resolved = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM signals WHERE classification LIKE '%TRADEABLE%' AND was_correct = 1")
        correct = cur.fetchone()["c"]

        cur.execute("SELECT AVG(probability) as a FROM signals WHERE classification LIKE '%TRADEABLE%'")
        row = cur.fetchone()
        avg_prob = row["a"] if row["a"] else 0.0

        cur.execute("SELECT AVG(signal_score) as a FROM signals WHERE classification LIKE '%TRADEABLE%' AND signal_score IS NOT NULL")
        row = cur.fetchone()
        avg_score = row["a"] if row["a"] else 0.0

        tier1_stats = _tier_stats(cur, 1)
        tier2_stats = _tier_stats(cur, 2)

        # Last 5 graded signals
        cur.execute(
            """SELECT question, outcome, was_correct, probability, signal_score, resolved_at
               FROM signals
               WHERE was_correct IS NOT NULL
               ORDER BY resolved_at DESC LIMIT 5"""
        )
        recent_graded = [dict(r) for r in cur.fetchall()]

        # Signals pending grading
        cur.execute("SELECT COUNT(*) as c FROM signals WHERE was_correct IS NULL")
        pending = cur.fetchone()["c"]

        conn.close()
        accuracy = (correct / resolved * 100) if resolved > 0 else 0.0
        return {
            "total": total,
            "resolved": resolved,
            "pending": pending,
            "correct": correct,
            "accuracy": round(accuracy, 1),
            "avg_probability": round(avg_prob * 100, 1),
            "avg_score": round(avg_score, 1),
            "tier1": tier1_stats,
            "tier2": tier2_stats,
            "recent_graded": recent_graded,
            # legacy flat keys kept for backwards compat
            "tier1_accuracy": tier1_stats["accuracy"],
            "tier2_accuracy": tier2_stats["accuracy"],
        }
    except Exception as e:
        logger.error(f"Error getting performance stats: {e}")
        return {}


def _tier_stats(cur, tier: int) -> dict:
    cur.execute(
        """SELECT COUNT(*) as c FROM signals
           WHERE tier=? AND classification LIKE '%TRADEABLE%' AND was_correct IS NOT NULL""",
        (tier,),
    )
    total = cur.fetchone()["c"]
    if total == 0:
        return {"total": 0, "correct": 0, "accuracy": 0.0, "avg_score": 0.0}

    cur.execute(
        "SELECT COUNT(*) as c FROM signals WHERE tier=? AND classification LIKE '%TRADEABLE%' AND was_correct=1",
        (tier,),
    )
    correct = cur.fetchone()["c"]

    cur.execute(
        """SELECT AVG(signal_score) as a FROM signals
           WHERE tier=? AND classification LIKE '%TRADEABLE%' AND signal_score IS NOT NULL""",
        (tier,),
    )
    row = cur.fetchone()
    avg_score = row["a"] if row["a"] else 0.0

    return {
        "total": total,
        "correct": correct,
        "accuracy": round(correct / total * 100, 1),
        "avg_score": round(avg_score, 1),
    }
