"""
ARISTOTLE: SUI LOGOS
Scheduler — runs bot.py at 07:00 and 19:00 UTC daily
Run this file to keep the bot alive.
"""

import schedule
import time
import logging
import sqlite3
from datetime import datetime, timezone
from bot import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("scheduler")

def job():
    log.info("Scheduled run triggered.")
    try:
        run()
    except Exception as e:
        log.error(f"Pipeline error: {e}")

def hours_since_last_run() -> float:
    """Returns hours since last DB snapshot, or 99 if no data."""
    try:
        conn = sqlite3.connect("aristotle.db")
        row = conn.execute(
            "SELECT timestamp FROM snapshots_v3 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return 99
        last = datetime.fromisoformat(row[0])
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last).total_seconds() / 3600
    except Exception:
        return 99

# Schedule twice daily at UTC
schedule.every().day.at("07:00").do(job)
schedule.every().day.at("19:00").do(job)

log.info("Scheduler started — running at 07:00 and 19:00 UTC.")

# Fire immediately on startup if last run was more than 10 hours ago
hours_since = hours_since_last_run()
if hours_since > 10:
    log.info(f"Last run was {hours_since:.1f}h ago — firing missed report now.")
    job()
else:
    log.info(f"Last run was {hours_since:.1f}h ago — on schedule, no catch-up needed.")

while True:
    schedule.run_pending()
    time.sleep(30)
