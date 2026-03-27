"""
ARISTOTLE: SUI LOGOS
Twice-daily Sui blockchain intelligence brief
Pipeline: Fetch → Calculate → Format → Post → Store
"""

import os
import requests
import sqlite3
import json
import logging
from datetime import datetime, timezone
from statistics import mean, stdev

# ─────────────────────────────────────────
# CONFIGURATION — fill in your values
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
FREE_CHANNEL_ID    = os.environ.get("FREE_CHANNEL_ID")
PAID_CHANNEL_ID    = os.environ.get("PAID_CHANNEL_ID")
DB_PATH = "aristotle.db"

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("aristotle")

# ─────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────

def init_db():
    """Create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            sui_price REAL,
            sui_price_change_24h REAL,
            dex_volume REAL,
            tvl REAL,
            tvl_change_24h REAL,
            active_addresses INTEGER,
            deepbook_liquidity REAL,
            mean_reversion REAL,
            logos_index REAL,
            best_token_symbol TEXT,
            best_token_change REAL
        )
    """)
    conn.commit()
    conn.close()
    log.info("Database ready.")

def save_snapshot(data: dict):
    """Save a data snapshot to SQLite."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO snapshots (
            timestamp, sui_price, sui_price_change_24h, dex_volume,
            tvl, tvl_change_24h, active_addresses, deepbook_liquidity,
            mean_reversion, logos_index, best_token_symbol, best_token_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("timestamp"),
        data.get("sui_price"),
        data.get("sui_price_change_24h"),
        data.get("dex_volume"),
        data.get("tvl"),
        data.get("tvl_change_24h"),
        data.get("active_addresses"),
        data.get("deepbook_liquidity"),
        data.get("mean_reversion"),
        data.get("logos_index"),
        data.get("best_token_symbol"),
        data.get("best_token_change"),
    ))
    conn.commit()
    conn.close()
    log.info("Snapshot saved.")

def get_price_history(days: int = 20) -> list:
    """Fetch last N logos_index snapshots for mean reversion calculation."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT sui_price FROM snapshots
        ORDER BY id DESC LIMIT ?
    """, (days,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0] is not None]

# ─────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────

def fetch_coingecko() -> dict:
    """
    Fetch SUI price, 24h change, 24h volume, and top Sui ecosystem token.
    Uses CoinGecko free API (no key required).
    """
    log.info("Fetching CoinGecko data...")
    result = {
        "sui_price": None,
        "sui_price_change_24h": None,
        "dex_volume": None,
        "best_token_symbol": None,
        "best_token_change": None,
    }

    try:
        # SUI price + volume
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "sui",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_24hr_vol": "true",
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()["sui"]
        result["sui_price"] = data.get("usd")
        result["sui_price_change_24h"] = data.get("usd_24h_change")
        result["dex_volume"] = data.get("usd_24h_vol")

        # Best performing Sui ecosystem token (by 24h change)
        cat_url = "https://api.coingecko.com/api/v3/coins/markets"
        cat_params = {
            "vs_currency": "usd",
            "category": "sui-ecosystem",
            "order": "price_change_percentage_24h_desc",
            "per_page": 10,
            "page": 1,
        }
        r2 = requests.get(cat_url, params=cat_params, timeout=10)
        r2.raise_for_status()
        tokens = r2.json()

        # Filter out SUI itself, pick top performer
        for token in tokens:
            symbol = token.get("symbol", "").upper()
            change = token.get("price_change_percentage_24h")
            if symbol != "SUI" and change is not None:
                result["best_token_symbol"] = symbol
                result["best_token_change"] = change
                break

        log.info(f"CoinGecko: SUI=${result['sui_price']} ({result['sui_price_change_24h']:+.2f}%)")

    except Exception as e:
        log.error(f"CoinGecko fetch failed: {e}")

    return result


def fetch_defillama() -> dict:
    """
    Fetch Sui TVL from DeFiLlama.
    """
    log.info("Fetching DeFiLlama TVL...")
    result = {"tvl": None, "tvl_change_24h": None}

    try:
        url = "https://api.llama.fi/v2/chains"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        chains = r.json()

        for chain in chains:
            if chain.get("name", "").lower() == "sui":
                result["tvl"] = chain.get("tvl")
                result["tvl_change_24h"] = chain.get("change_1d")
                break

        log.info(f"DeFiLlama: TVL=${result['tvl']:,.0f}" if result["tvl"] else "DeFiLlama: TVL not found")

    except Exception as e:
        log.error(f"DeFiLlama fetch failed: {e}")

    return result


def fetch_sui_rpc() -> dict:
    """
    Fetch active addresses approximation from Sui RPC.
    Uses the getLatestSuiSystemState endpoint.
    Active addresses are approximated via recent transaction count from checkpoints.
    """
    log.info("Fetching Sui RPC data...")
    result = {"active_addresses": None}

    try:
        # Get latest checkpoint for tx count proxy
        url = "https://fullnode.mainnet.sui.io:443"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_getLatestCheckpointSequenceNumber",
            "params": []
        }
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        latest_checkpoint = int(r.json().get("result", 0))

        # Get checkpoint details to extract tx count
        payload2 = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "sui_getCheckpoint",
            "params": [str(latest_checkpoint)]
        }
        r2 = requests.post(url, json=payload2, timeout=10)
        r2.raise_for_status()
        checkpoint_data = r2.json().get("result", {})

        # Use network total transactions as active address proxy
        # In production, replace with a Dune query for true active addresses
        network_total_tx = int(checkpoint_data.get("networkTotalTransactions", 0))

        # Rough proxy: divide by average tx per address (tunable constant)
        # This will be replaced by Dune data in v2
        result["active_addresses"] = network_total_tx // 50 if network_total_tx > 0 else None

        log.info(f"Sui RPC: ~{result['active_addresses']:,} active addresses (proxy)" if result["active_addresses"] else "Sui RPC: address data unavailable")

    except Exception as e:
        log.error(f"Sui RPC fetch failed: {e}")

    return result


def fetch_deepbook() -> dict:
    """
    Fetch DeepBook liquidity from Sui RPC.
    Queries the SUI/USDC pool total base and quote amounts.
    DeepBook V3 pool object ID for SUI/USDC on mainnet.
    """
    log.info("Fetching DeepBook liquidity...")
    result = {"deepbook_liquidity": None}

    try:
        # DeepBook V3 SUI/USDC pool
        POOL_ID = "0x4405b50d791fd3346754e8171aaab6bc2ed26c2c46efdd033c14b30ae507ac33"

        url = "https://fullnode.mainnet.sui.io:443"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_getDynamicFields",
            "params": [POOL_ID, None, 10]
        }
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()

        # Parse pool fields for liquidity depth
        fields = data.get("result", {}).get("data", [])

        # Fallback: use DeepBook API if available
        db_api = "https://deepbook-indexer.mainnet.mystenlabs.com/get_pools"
        r2 = requests.get(db_api, timeout=10)
        if r2.status_code == 200:
            pools = r2.json()
            sui_usdc = None
            for pool in pools:
                if "SUI" in pool.get("base_asset_name", "") and "USDC" in pool.get("quote_asset_name", ""):
                    sui_usdc = pool
                    break
            if sui_usdc:
                base = float(sui_usdc.get("base_asset_trading_volume", 0))
                result["deepbook_liquidity"] = base
                log.info(f"DeepBook: SUI/USDC liquidity={base:,.0f}")
        else:
            # Set a reasonable placeholder if API unavailable
            result["deepbook_liquidity"] = 0
            log.warning("DeepBook API unavailable, set to 0")

    except Exception as e:
        log.error(f"DeepBook fetch failed: {e}")
        result["deepbook_liquidity"] = 0

    return result


# ─────────────────────────────────────────
# LOGOS INDEX CALCULATION
# ─────────────────────────────────────────

# Weights (must sum to 1.0)
WEIGHTS = {
    "active_addresses": 0.30,
    "tvl":              0.28,
    "deepbook":         0.20,
    "mean_reversion":   0.12,
    "sui_price":        0.10,
}

# Normalisation reference ranges (update periodically as Sui grows)
RANGES = {
    "active_addresses": {"min": 50_000,    "max": 500_000},
    "tvl":              {"min": 200_000_000, "max": 2_000_000_000},
    "deepbook":         {"min": 0,         "max": 10_000_000},
    "sui_price":        {"min": 0.5,       "max": 10.0},
    # mean_reversion handled separately (z-score → 0-100)
}

# Dampening: cap single-period change to ±10 points
DAMPENING_CAP = 10.0


def normalise(value, min_val, max_val) -> float:
    """Normalise a value to 0–100 range."""
    if value is None:
        return 50.0  # neutral fallback
    clamped = max(min_val, min(max_val, value))
    return ((clamped - min_val) / (max_val - min_val)) * 100


def mean_reversion_zscore(current_price: float, history: list) -> float:
    """
    Calculate Z-score mean reversion.
    Z = (price - 20d MA) / 20d SD
    Returns z-score (typically -3 to +3).
    """
    if len(history) < 5:
        return 0.0  # not enough history

    prices = history[-20:] if len(history) >= 20 else history
    ma = mean(prices)
    sd = stdev(prices) if len(prices) > 1 else 1.0
    if sd == 0:
        return 0.0
    return (current_price - ma) / sd


def zscore_to_score(z: float) -> float:
    """
    Convert z-score to 0–100 score for Logos Index.
    Z=0 (at mean) → 50
    Z=-2 (oversold) → 80 (network health: cheap = opportunity)
    Z=+2 (overbought) → 20 (stretched = risk)
    Inverted: lower z = higher structural health score
    """
    # Invert: negative z (below mean) → higher score
    score = 50 - (z * 15)
    return max(0, min(100, score))


def calculate_logos_index(data: dict, previous_index: float = None) -> float:
    """
    Calculate composite Logos Index (1–100).
    Applies dampening to prevent single-metric spikes.
    """
    scores = {}

    # Active addresses (30%)
    scores["active_addresses"] = normalise(
        data.get("active_addresses"),
        RANGES["active_addresses"]["min"],
        RANGES["active_addresses"]["max"]
    )

    # TVL (28%)
    scores["tvl"] = normalise(
        data.get("tvl"),
        RANGES["tvl"]["min"],
        RANGES["tvl"]["max"]
    )

    # DeepBook liquidity (20%)
    scores["deepbook"] = normalise(
        data.get("deepbook_liquidity"),
        RANGES["deepbook"]["min"],
        RANGES["deepbook"]["max"]
    )

    # Mean reversion (12%) — z-score converted to 0-100
    z = data.get("mean_reversion", 0.0) or 0.0
    scores["mean_reversion"] = zscore_to_score(z)

    # SUI price (10%)
    scores["sui_price"] = normalise(
        data.get("sui_price"),
        RANGES["sui_price"]["min"],
        RANGES["sui_price"]["max"]
    )

    # Weighted composite
    raw_index = sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)

    # Clamp to 1–100
    raw_index = max(1, min(100, raw_index))

    # Apply dampening if we have a previous value
    if previous_index is not None:
        delta = raw_index - previous_index
        if abs(delta) > DAMPENING_CAP:
            raw_index = previous_index + (DAMPENING_CAP if delta > 0 else -DAMPENING_CAP)

    return round(raw_index, 1)


def get_previous_logos_index() -> float:
    """Retrieve the most recent Logos Index from DB."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT logos_index FROM snapshots ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def get_previous_active_addresses() -> int:
    """Retrieve the most recent active addresses from DB."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT active_addresses FROM snapshots ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


# ─────────────────────────────────────────
# BRIEF FORMATTERS
# ─────────────────────────────────────────

def fmt_price(value, decimals=2):
    if value is None:
        return "—"
    return f"${value:,.{decimals}f}"

def fmt_pct(value):
    if value is None:
        return "—"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"

def fmt_large(value):
    """Format large USD values: $1.23B, $456.7M"""
    if value is None:
        return "—"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    return f"${value:,.0f}"

def fmt_num(value):
    if value is None:
        return "—"
    return f"{value:,}"

def fmt_addr(value):
    """Format address count as compact: 142.0K or 1.4M"""
    if value is None:
        return "—"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return str(value)


def format_free_brief(data: dict) -> str:
    """Format the Sui Update free brief."""
    now = datetime.now(timezone.utc)
    session = "08:00" if now.hour < 14 else "20:00"

    best_str = "—"
    if data.get("best_token_symbol") and data.get("best_token_change") is not None:
        best_str = f"{data['best_token_symbol']} {fmt_pct(data['best_token_change'])}"

    lines = [
        f"ARISTOTLE · SUI UPDATE",
        f"{now.strftime('%d %b %Y')} · {session} UTC",
        f"─────────────────────",
        f"PRICE   {fmt_price(data.get('sui_price'))} {fmt_pct(data.get('sui_price_change_24h'))}",
        f"TVL     {fmt_large(data.get('tvl'))} {fmt_pct(data.get('tvl_change_24h'))}",
        f"LEADER  {best_str}",
        f"─────────────────────",
        f"@aristotlesuiupdate",
    ]
    return "\n".join(lines)


def format_paid_brief(data: dict) -> str:
    """Format the Sui Logos paid brief."""
    now = datetime.now(timezone.utc)
    session = "08:00" if now.hour < 14 else "20:00"

    z = data.get("mean_reversion")
    z_str = f"{z:+.2f}" if z is not None else "—"

    logos = data.get("logos_index")
    logos_str = f"{logos:.1f}" if logos is not None else "—"

    # Active addresses with change vs previous snapshot
    prev_addr = get_previous_active_addresses()
    curr_addr = data.get("active_addresses")
    addr_str = fmt_addr(curr_addr)
    if prev_addr and curr_addr:
        addr_change = ((curr_addr - prev_addr) / prev_addr) * 100
        addr_str += f"  {fmt_pct(addr_change)}"

    lines = [
        f"ARISTOTLE · SUI LOGOS",
        f"{now.strftime('%d %b %Y')} · {session} UTC",
        f"─────────────────────────────────",
        f"PRICE          {fmt_price(data.get('sui_price'))}  {fmt_pct(data.get('sui_price_change_24h'))}",
        f"TVL            {fmt_large(data.get('tvl'))}  {fmt_pct(data.get('tvl_change_24h'))}",
        f"ACTIVE ADDR    {addr_str}",
        f"DEEPBOOK LIQ   {fmt_large(data.get('deepbook_liquidity'))}",
        f"MEAN REVERSION {z_str}",
        f"─────────────────────────────────",
        f"LOGOS INDEX    {logos_str} / 100",
        f"─────────────────────────────────",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────
# TELEGRAM POSTING
# ─────────────────────────────────────────

def post_to_telegram(channel_id: str, message: str) -> bool:
    """Post a message to a Telegram channel using plain monospace text."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": channel_id,
        "text": f"<pre>{message}</pre>",
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        log.info(f"Posted to {channel_id}")
        return True
    except Exception as e:
        log.error(f"Telegram post failed for {channel_id}: {e}")
        return False


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run():
    log.info("═══ ARISTOTLE PIPELINE START ═══")
    init_db()

    # 1. Fetch all data
    cg   = fetch_coingecko()
    dl   = fetch_defillama()
    rpc  = fetch_sui_rpc()
    db   = fetch_deepbook()

    # 2. Merge into single data dict
    data = {**cg, **dl, **rpc, **db}
    data["timestamp"] = datetime.now(timezone.utc).isoformat()

    # 3. Mean reversion z-score
    price_history = get_price_history(days=20)
    if data.get("sui_price") and price_history:
        data["mean_reversion"] = round(
            mean_reversion_zscore(data["sui_price"], price_history), 4
        )
    else:
        data["mean_reversion"] = 0.0
        log.warning("Not enough price history for mean reversion — using 0.0")

    # 4. Logos Index
    prev_index = get_previous_logos_index()
    data["logos_index"] = calculate_logos_index(data, previous_index=prev_index)
    log.info(f"Logos Index: {data['logos_index']}")

    # 5. Format briefs
    free_brief = format_free_brief(data)
    paid_brief = format_paid_brief(data)

    log.info("\n" + "─"*40)
    log.info("FREE BRIEF PREVIEW:\n" + free_brief)
    log.info("─"*40)
    log.info("PAID BRIEF PREVIEW:\n" + paid_brief)
    log.info("─"*40)

    # 6. Post to Telegram
    post_to_telegram(FREE_CHANNEL_ID, free_brief)
    post_to_telegram(PAID_CHANNEL_ID, paid_brief)

    # 7. Save snapshot
    save_snapshot(data)

    log.info("═══ PIPELINE COMPLETE ═══")


if __name__ == "__main__":
    run()
