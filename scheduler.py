"""
ARISTOTLE: SUI LOGOS
Scheduler — runs bot.py at 07:00 and 21:00 UTC daily
Run this file to keep the bot alive.
"""

import schedule
import time
import logging
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

# Schedule twice daily at UTC
schedule.every().day.at("07:00").do(job)
schedule.every().day.at("21:00").do(job)

log.info("Scheduler started — running at 07:00 and 21:00 UTC.")

while True:
    schedule.run_pending()
    time.sleep(30)
