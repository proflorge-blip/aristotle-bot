"""
ARISTOTLE: SUI LOGOS
Twice-daily Sui blockchain intelligence brief
Pipeline: Fetch → Calculate → Format → Post → Store
v3: Final formatting, locked metrics, arrows on Logos Index
"""

import os
import requests
import sqlite3
import logging
import time
from datetime import datetime, timezone
from statistics import mean, stdev

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
FREE_CHANNEL_ID    = os.environ.get("FREE_CHANNEL_ID")
PAID_CHANNEL_ID    = os.environ.get("PAID_CHANNEL_ID")

DB_PATH = "aristotle.db"

STABLECOINS = {"USDC", "USDT", "USDE", "DAI", "BUCK", "SUIUSD", "AUSD", "FDUSD"}

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("aristotle")

# ─────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots_v3 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            sui_price REAL,
            sui_price_change_24h REAL,
            dex_volume REAL,
            tvl REAL,
            tvl_change_24h REAL,
            active_addresses INTEGER,
            deepbook_liquidity REAL,
            deepbook_change REAL,
            mean_reversion REAL,
            mean_reversion_prev REAL,
            logos_index REAL,
            best_token_symbol TEXT,
            best_token_change REAL
        )
    """)
    conn.commit()
    conn.close()
    log.info("Database ready.")

def save_snapshot(data: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO snapshots_v3 (
            timestamp, sui_price, sui_price_change_24h, dex_volume,
            tvl, tvl_change_24h, active_addresses, deepbook_liquidity,
            deepbook_change, mean_reversion, mean_reversion_prev,
            logos_index, best_token_symbol, best_token_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("timestamp"),
        data.get("sui_price"),
        data.get("sui_price_change_24h"),
        data.get("dex_volume"),
        data.get("tvl"),
        data.get("tvl_change_24h"),
        data.get("active_addresses"),
        data.get("deepbook_liquidity"),
        data.get("deepbook_change"),
        data.get("mean_reversion"),
        data.get("mean_reversion_prev"),
        data.get("logos_index"),
        data.get("best_token_symbol"),
        data.get("best_token_change"),
    ))
    conn.commit()
    conn.close()

def get_price_history(days: int = 20) -> list:
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT sui_price FROM snapshots_v3 ORDER BY id DESC LIMIT ?", (days,))
        rows = c.fetchall()
        conn.close()
        return [r[0] for r in rows if r[0] is not None]
    except Exception:
        return []

def get_previous_value(column: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(f"SELECT {column} FROM snapshots_v3 ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

# ─────────────────────────────────────────
# DATA FETCHERS
# ─────────────────────────────────────────

def fetch_coingecko() -> dict:
    log.info("Fetching CoinGecko data...")
    result = {
        "sui_price": None,
        "sui_price_change_24h": None,
        "dex_volume": None,
        "best_token_symbol": None,
        "best_token_change": None,
    }
    headers = {"accept": "application/json"}

    for attempt in range(3):
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                "ids": "sui",
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_24hr_vol": "true",
            }
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"CoinGecko rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json().get("sui", {})
            result["sui_price"] = data.get("usd")
            result["sui_price_change_24h"] = data.get("usd_24h_change")
            result["dex_volume"] = data.get("usd_24h_vol")
            log.info(f"CoinGecko: SUI=${result['sui_price']} ({result['sui_price_change_24h']:+.2f}%)")
            break
        except Exception as e:
            log.error(f"CoinGecko price attempt {attempt+1} failed: {e}")
            time.sleep(10)

    # Top Sui ecosystem token (filter stablecoins + SUI)
    try:
        time.sleep(3)
        cat_url = "https://api.coingecko.com/api/v3/coins/markets"
        cat_params = {
            "vs_currency": "usd",
            "category": "sui-ecosystem",
            "order": "price_change_percentage_24h_desc",
            "per_page": 20,
            "page": 1,
        }
        r2 = requests.get(cat_url, params=cat_params, headers=headers, timeout=15)
        if r2.status_code == 200:
            tokens = r2.json()
            for token in tokens:
                symbol = token.get("symbol", "").upper()
                change = token.get("price_change_percentage_24h")
                if symbol not in STABLECOINS and symbol != "SUI" and change is not None:
                    result["best_token_symbol"] = symbol
                    result["best_token_change"] = change
                    log.info(f"Leader: {symbol} {change:+.2f}%")
                    break
        else:
            log.warning(f"CoinGecko markets: {r2.status_code}")
    except Exception as e:
        log.error(f"CoinGecko leader fetch failed: {e}")

    return result


def fetch_defillama() -> dict:
    log.info("Fetching DeFiLlama TVL...")
    result = {"tvl": None, "tvl_change_24h": None}
    try:
        # Primary: historical TVL endpoint gives us current + previous to calc change
        r = requests.get("https://api.llama.fi/v2/historicalChainTvl/Sui", timeout=10)
        if r.status_code == 200:
            history = r.json()
            if len(history) >= 2:
                current = history[-1].get("tvl")
                previous = history[-2].get("tvl")
                result["tvl"] = current
                if current and previous and previous > 0:
                    result["tvl_change_24h"] = ((current - previous) / previous) * 100
                log.info(f"DeFiLlama: TVL=${current:,.0f} change={result['tvl_change_24h']}")
                return result

        # Fallback: chains endpoint
        r2 = requests.get("https://api.llama.fi/v2/chains", timeout=10)
        r2.raise_for_status()
        for chain in r2.json():
            if chain.get("name", "").lower() == "sui":
                result["tvl"] = chain.get("tvl")
                result["tvl_change_24h"] = chain.get("change_1d")
                break
        log.info(f"DeFiLlama: TVL=${result['tvl']:,.0f}" if result["tvl"] else "DeFiLlama: not found")
    except Exception as e:
        log.error(f"DeFiLlama fetch failed: {e}")
    return result


def fetch_sui_rpc() -> dict:
    log.info("Fetching Sui RPC data...")
    result = {"active_addresses": None}
    try:
        url = "https://fullnode.mainnet.sui.io:443"
        r = requests.post(url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "sui_getLatestCheckpointSequenceNumber", "params": []
        }, timeout=10)
        r.raise_for_status()
        latest = int(r.json().get("result", 0))

        r2 = requests.post(url, json={
            "jsonrpc": "2.0", "id": 2,
            "method": "sui_getCheckpoint", "params": [str(latest)]
        }, timeout=10)
        r2.raise_for_status()
        total_tx = int(r2.json().get("result", {}).get("networkTotalTransactions", 0))
        result["active_addresses"] = total_tx // 50 if total_tx > 0 else None
        log.info(f"Sui RPC: ~{result['active_addresses']:,} active addresses (proxy)" if result["active_addresses"] else "Sui RPC: unavailable")
    except Exception as e:
        log.error(f"Sui RPC fetch failed: {e}")
    return result


def fetch_deepbook() -> dict:
    """
    Fetch DeepBook SUI/USDC 24h volume from Mysten Labs indexer.
    Volumes are in smallest asset units — divide by 10^9 for SUI (9 decimals).
    Fallback: DeFiLlama DeepBook protocol volume.
    """
    log.info("Fetching DeepBook liquidity...")
    result = {"deepbook_liquidity": None}

    SUI_SCALAR = 10 ** 9  # SUI has 9 decimal places

    # Primary: Mysten Labs DeepBook V3 indexer
    try:
        # Get all pools
        r = requests.get(
            "https://deepbook-indexer.mainnet.mystenlabs.com/get_pools",
            timeout=10
        )
        if r.status_code == 200:
            pools = r.json()
            log.info(f"DeepBook pools response: {str(pools)[:200]}")

            # Find SUI/USDC pool
            sui_usdc_pool_id = None
            for pool in pools:
                name = pool.get("pool_name", "")
                if "SUI" in name.upper() and "USDC" in name.upper():
                    sui_usdc_pool_id = pool.get("pool_id")
                    log.info(f"Found SUI/USDC pool: {sui_usdc_pool_id}")
                    break

            if sui_usdc_pool_id:
                # Fetch 24h volume for this pool
                vol_url = f"https://deepbook-indexer.mainnet.mystenlabs.com/get_net_deposits"
                vol_r = requests.get(
                    f"https://deepbook-indexer.mainnet.mystenlabs.com/24h_volume/{sui_usdc_pool_id}",
                    timeout=10
                )
                if vol_r.status_code == 200:
                    vol_data = vol_r.json()
                    log.info(f"DeepBook volume raw: {str(vol_data)[:200]}")
                    # Volume is in base asset units (SUI), divide by scalar
                    raw_vol = vol_data.get("base_volume") or vol_data.get("volume") or vol_data.get("base_asset_volume") or 0
                    result["deepbook_liquidity"] = float(raw_vol) / SUI_SCALAR
                    log.info(f"DeepBook: SUI/USDC 24h vol = {result['deepbook_liquidity']:,.0f} SUI")
                    return result
                else:
                    log.warning(f"DeepBook volume endpoint: {vol_r.status_code} — {vol_r.text[:100]}")
        else:
            log.warning(f"DeepBook pools: {r.status_code}")

    except Exception as e:
        log.warning(f"DeepBook primary failed: {e}")

    # Fallback: DeFiLlama DeepBook protocol
    try:
        r2 = requests.get("https://api.llama.fi/summary/dexs/deepbook?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume", timeout=10)
        if r2.status_code == 200:
            data = r2.json()
            vol = data.get("total24h") or data.get("totalVolume24h")
            if vol:
                result["deepbook_liquidity"] = float(vol)
                log.info(f"DeepBook (DeFiLlama fallback): ${vol:,.0f}")
                return result
        log.warning(f"DeepBook DeFiLlama fallback: {r2.status_code}")
    except Exception as e:
        log.warning(f"DeepBook DeFiLlama fallback failed: {e}")

    result["deepbook_liquidity"] = 0
    log.warning("DeepBook: all sources failed, using 0")
    return result


# ─────────────────────────────────────────
# LOGOS INDEX
# ─────────────────────────────────────────

WEIGHTS = {
    "active_addresses": 0.30,
    "tvl":              0.28,
    "deepbook":         0.20,
    "mean_reversion":   0.12,
    "sui_price":        0.10,
}

RANGES = {
    "active_addresses": {"min": 50_000,       "max": 500_000},
    "tvl":              {"min": 200_000_000,   "max": 2_000_000_000},
    "deepbook":         {"min": 0,             "max": 10_000_000},
    "sui_price":        {"min": 0.5,           "max": 10.0},
}

DAMPENING_CAP = 10.0


def normalise(value, min_val, max_val) -> float:
    if value is None:
        return 50.0
    return ((max(min_val, min(max_val, value)) - min_val) / (max_val - min_val)) * 100


def mean_reversion_zscore(current_price: float, history: list) -> float:
    if len(history) < 5:
        return 0.0
    prices = history[-20:] if len(history) >= 20 else history
    ma = mean(prices)
    sd = stdev(prices) if len(prices) > 1 else 1.0
    return (current_price - ma) / sd if sd != 0 else 0.0


def zscore_to_score(z: float) -> float:
    return max(0, min(100, 50 - (z * 15)))


def calculate_logos_index(data: dict, previous_index: float = None) -> float:
    scores = {
        "active_addresses": normalise(data.get("active_addresses"), RANGES["active_addresses"]["min"], RANGES["active_addresses"]["max"]),
        "tvl":              normalise(data.get("tvl"), RANGES["tvl"]["min"], RANGES["tvl"]["max"]),
        "deepbook":         normalise(data.get("deepbook_liquidity"), RANGES["deepbook"]["min"], RANGES["deepbook"]["max"]),
        "mean_reversion":   zscore_to_score(data.get("mean_reversion", 0.0) or 0.0),
        "sui_price":        normalise(data.get("sui_price"), RANGES["sui_price"]["min"], RANGES["sui_price"]["max"]),
    }
    raw = max(1, min(100, sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)))
    if previous_index is not None:
        delta = raw - previous_index
        if abs(delta) > DAMPENING_CAP:
            raw = previous_index + (DAMPENING_CAP if delta > 0 else -DAMPENING_CAP)
    return round(raw, 1)


# ─────────────────────────────────────────
# FORMATTERS
# ─────────────────────────────────────────

def fmt_price(value):
    return f"${value:.2f}" if value is not None else "—"

def fmt_pct(value):
    if value is None:
        return "—"
    return f"{'+' if value >= 0 else ''}{value:.1f}%"

def fmt_large(value):
    if value is None:
        return "—"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    return f"${value:,.0f}"

def fmt_addr(value):
    if value is None:
        return "—"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}K"
    return str(value)

def fmt_change(value):
    """Format a raw change value (not percentage)."""
    if value is None:
        return ""
    return f"{'+' if value >= 0 else ''}{value:.2f}"


def format_free_brief(data: dict) -> str:
    now = datetime.now(timezone.utc)
    session = now.strftime("%H:%M")
    sep = "─" * 24

    leader_str = "—"
    if data.get("best_token_symbol") and data.get("best_token_change") is not None:
        leader_str = f"{data['best_token_symbol']} {fmt_pct(data['best_token_change'])}"

    lines = [
        "ARISTOTLE · SUI UPDATE",
        f"{now.strftime('%d %b %Y')} · {session} UTC",
        sep,
        f"PRICE      {fmt_price(data.get('sui_price'))}     {fmt_pct(data.get('sui_price_change_24h'))}",
        f"CAPITAL    {fmt_large(data.get('tvl'))}   {fmt_pct(data.get('tvl_change_24h'))}",
        f"DEX FLOW   {fmt_large(data.get('dex_volume'))}",
        sep,
        "@aristotlesuiupdate",
    ]
    return "\n".join(lines)


def format_paid_brief(data: dict) -> str:
    now = datetime.now(timezone.utc)
    session = now.strftime("%H:%M")
    sep = "─" * 26

    # Active addresses with change
    prev_addr = get_previous_value("active_addresses")
    curr_addr = data.get("active_addresses")
    addr_str = fmt_addr(curr_addr)
    if prev_addr and curr_addr:
        addr_change = ((curr_addr - prev_addr) / prev_addr) * 100
        addr_str += f"    {fmt_pct(addr_change)}"
    else:
        addr_str += "    –"

    # DeepBook with change
    prev_db = get_previous_value("deepbook_liquidity")
    curr_db = data.get("deepbook_liquidity")
    db_str = fmt_large(curr_db)
    if prev_db and curr_db and prev_db > 0:
        db_change = ((curr_db - prev_db) / prev_db) * 100
        db_str += f"   {fmt_pct(db_change)}"
    else:
        db_str += "   –"

    # Mean reversion with change vs previous
    prev_mr = get_previous_value("mean_reversion")
    curr_mr = data.get("mean_reversion")
    mr_str = f"{curr_mr:+.2f}" if curr_mr is not None else "—"
    if prev_mr is not None and curr_mr is not None:
        mr_change = curr_mr - prev_mr
        mr_str += f"    {fmt_change(mr_change)}"
    else:
        mr_str += "    –"

    # Logos Index with arrow and point change
    prev_logos = get_previous_value("logos_index")
    curr_logos = data.get("logos_index")
    logos_str = f"{curr_logos:.1f}/100" if curr_logos is not None else "—"
    if prev_logos is not None and curr_logos is not None:
        delta = curr_logos - prev_logos
        arrow = "▲" if delta >= 0 else "▼"
        logos_str += f"  {arrow} {abs(delta):.1f}"
    else:
        logos_str += "  –"

    lines = [
        "ARISTOTLE · SUI LOGOS",
        f"{now.strftime('%d %b %Y')} · {session} UTC",
        sep,
        "",
        f"PRICE          {fmt_price(data.get('sui_price'))}     {fmt_pct(data.get('sui_price_change_24h'))}",
        f"TVL            {fmt_large(data.get('tvl'))}   {fmt_pct(data.get('tvl_change_24h'))}",
        f"ACTIVE ADDR    {addr_str}",
        f"DEEPBOOK       {db_str}",
        f"MEAN REV       {mr_str}",
        "",
        sep,
        f"LOGOS INDEX    {logos_str}",
        sep,
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────

def post_to_telegram(channel_id: str, message: str) -> bool:
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

    cg  = fetch_coingecko()
    dl  = fetch_defillama()
    rpc = fetch_sui_rpc()
    db  = fetch_deepbook()

    data = {**cg, **dl, **rpc, **db}
    data["timestamp"] = datetime.now(timezone.utc).isoformat()

    # Mean reversion
    price_history = get_price_history(days=20)
    if data.get("sui_price") and price_history:
        data["mean_reversion"] = round(mean_reversion_zscore(data["sui_price"], price_history), 4)
    else:
        data["mean_reversion"] = 0.0
        log.warning("Not enough price history for mean reversion — using 0.0")

    # Logos Index
    prev_index = get_previous_value("logos_index")
    data["logos_index"] = calculate_logos_index(data, previous_index=prev_index)
    log.info(f"Logos Index: {data['logos_index']}")

    free_brief = format_free_brief(data)
    paid_brief = format_paid_brief(data)

    log.info("\n" + "─"*40)
    log.info("FREE BRIEF:\n" + free_brief)
    log.info("─"*40)
    log.info("PAID BRIEF:\n" + paid_brief)
    log.info("─"*40)

    post_to_telegram(FREE_CHANNEL_ID, free_brief)
    post_to_telegram(PAID_CHANNEL_ID, paid_brief)

    save_snapshot(data)
    log.info("═══ PIPELINE COMPLETE ═══")


if __name__ == "__main__":
    run()
