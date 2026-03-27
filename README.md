# ARISTOTLE: SUI LOGOS — Setup Guide

## What this is
A Python bot that posts twice-daily Sui blockchain briefs to two Telegram channels.
- **Sui Update** (free channel) — Price, DEX Volume, Best Performing Token
- **Sui Logos** (paid channel) — Full brief + Logos Index

---

## Files
| File | Purpose |
|---|---|
| `bot.py` | Main pipeline — fetches data, calculates, formats, posts |
| `scheduler.py` | Keeps bot running, triggers at 08:00 + 20:00 UTC |
| `requirements.txt` | Python dependencies |
| `aristotle.db` | Auto-created SQLite database (stores snapshots) |

---

## Step 1 — Add your credentials to bot.py

Open `bot.py` and fill in these three lines at the top:

```python
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
FREE_CHANNEL_ID    = "YOUR_FREE_CHANNEL_ID_HERE"
PAID_CHANNEL_ID    = "YOUR_PAID_CHANNEL_ID_HERE"
```

**Bot token**: from BotFather on Telegram (looks like `7743920831:AAFxxx...`)
**Channel IDs**: your channel username (e.g. `@suiupdate`) or numeric ID (e.g. `-1001234567890`)

> ⚠️ Never share your bot token publicly or commit it to GitHub.

---

## Step 2 — Install Python dependencies

In your terminal, navigate to this folder and run:

```bash
pip install -r requirements.txt
```

---

## Step 3 — Test the bot manually

Run a single brief (no scheduler):

```bash
python bot.py
```

You'll see the brief printed in your terminal and posted to both channels.

---

## Step 4 — Run the scheduler

To keep the bot running continuously (twice daily):

```bash
python scheduler.py
```

Leave this running. It will post at 08:00 and 20:00 UTC every day.

---

## Deploying to Railway (recommended)

1. Create a free account at [railway.app](https://railway.app)
2. Create a new project → Deploy from GitHub
3. Push this folder to a GitHub repo
4. Set environment variables in Railway dashboard (instead of hardcoding credentials)
5. Set start command to: `python scheduler.py`

---

## Notes

- **Mean reversion** requires at least 5 historical snapshots to calculate. Until then it defaults to 0.0.
- **Active addresses** is currently a proxy from Sui RPC transaction data. Replace with a Dune query in v2 for accuracy.
- **DeepBook liquidity** pulls from the Mysten Labs DeepBook indexer. If the endpoint changes, update the URL in `fetch_deepbook()`.
- **Logos Index ranges** in `RANGES` should be reviewed monthly as Sui grows.
