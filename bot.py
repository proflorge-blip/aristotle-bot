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
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY")

DB_PATH = "aristotle.db"

STABLECOINS = {"USDC", "USDT", "USDE", "DAI", "BUCK", "SUIUSD", "AUSD", "FDUSD"}

# Blockberry API key — add once account approved at blockberry.one
BLOCKBERRY_API_KEY = os.environ.get("BLOCKBERRY_API_KEY")

# X (Twitter) credentials — add once developer account approved
X_API_KEY            = os.environ.get("X_API_KEY")
X_API_SECRET         = os.environ.get("X_API_SECRET")
X_ACCESS_TOKEN       = os.environ.get("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET")

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
            deepbook_ema REAL,
            deepbook_change REAL,
            staking_ratio REAL,
            stablecoin_mcap REAL,
            tx_count_total INTEGER,
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
            deepbook_ema, deepbook_change, staking_ratio, stablecoin_mcap,
            tx_count_total, mean_reversion, mean_reversion_prev, logos_index,
            best_token_symbol, best_token_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("timestamp"),
        data.get("sui_price"),
        data.get("sui_price_change_24h"),
        data.get("dex_volume"),
        data.get("tvl"),
        data.get("tvl_change_24h"),
        data.get("active_addresses"),
        data.get("deepbook_liquidity"),
        data.get("deepbook_ema"),
        data.get("deepbook_change"),
        data.get("staking_ratio"),
        data.get("stablecoin_mcap"),
        data.get("tx_count_total"),
        data.get("mean_reversion"),
        data.get("mean_reversion_prev"),
        data.get("logos_index"),
        data.get("best_token_symbol"),
        data.get("best_token_change"),
    ))
    conn.commit()
    conn.close()

def seed_price_history():
    """Seed DB with 20 days of historical SUI prices from CoinGecko if not enough history."""
    try:
        existing = get_price_history(days=20)
        if len(existing) >= 20:
            log.info("Price history already seeded — skipping.")
            return
        log.info("Seeding price history from CoinGecko (20d)...")
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/sui/market_chart",
            params={"vs_currency": "usd", "days": 20, "interval": "daily"},
            timeout=15
        )
        r.raise_for_status()
        prices = r.json().get("prices", [])
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        for ts_ms, price in prices[:-1]:  # exclude today — bot will insert current
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            c.execute(
                "INSERT INTO snapshots_v3 (timestamp, sui_price) VALUES (?, ?)",
                (ts, round(price, 6))
            )
        conn.commit()
        conn.close()
        log.info(f"Seeded {len(prices)-1} historical price points.")
    except Exception as e:
        log.error(f"Price history seed failed: {e}")


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

def fetch_price_binance() -> dict:
    """
    Fetch SUI price and 24h change from Binance public API.
    No API key required. No rate limits for basic ticker data.
    Primary price source.
    """
    result = {"sui_price": None, "sui_price_change_24h": None}
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": "SUIUSDT"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        result["sui_price"] = float(data["lastPrice"])
        result["sui_price_change_24h"] = float(data["priceChangePercent"])
        log.info(f"Binance: SUI=${result['sui_price']} ({result['sui_price_change_24h']:+.2f}%)")
    except Exception as e:
        log.error(f"Binance price fetch failed: {e}")
    return result


def fetch_coingecko_leader() -> dict:
    """
    Fetch top Sui ecosystem token from CoinGecko.
    Separate from price fetch to isolate rate limit risk.
    """
    result = {"best_token_symbol": None, "best_token_change": None}
    headers = {"accept": "application/json"}
    try:
        time.sleep(2)
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "category": "sui-ecosystem",
                "order": "price_change_percentage_24h_desc",
                "per_page": 20,
                "page": 1,
            },
            headers=headers,
            timeout=15
        )
        if r.status_code == 429:
            log.warning("CoinGecko leader: rate limited, skipping")
            return result
        if r.status_code == 200:
            for token in r.json():
                symbol = token.get("symbol", "").upper()
                change = token.get("price_change_percentage_24h")
                if symbol not in STABLECOINS and symbol != "SUI" and change is not None:
                    result["best_token_symbol"] = symbol
                    result["best_token_change"] = change
                    log.info(f"Leader: {symbol} {change:+.2f}%")
                    break
    except Exception as e:
        log.error(f"CoinGecko leader fetch failed: {e}")
    return result


def fetch_coingecko() -> dict:
    """
    Fallback price fetch from CoinGecko if Binance fails.
    Also fetches leader token.
    """
    log.info("Fetching price data...")

    # Primary: CoinGecko (Binance geo-blocked on Railway)
    price_data = {"sui_price": None, "sui_price_change_24h": None}

    # Fallback to Binance only if CoinGecko fails
    if price_data.get("sui_price") is None:
        log.warning("Trying Binance as fallback...")
        headers = {"accept": "application/json"}
        for attempt in range(3):
            try:
                r = requests.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": "sui", "vs_currencies": "usd",
                            "include_24hr_change": "true", "include_24hr_vol": "true"},
                    headers=headers, timeout=15
                )
                if r.status_code == 429:
                    time.sleep(60 * (attempt + 1))
                    continue
                r.raise_for_status()
                data = r.json().get("sui", {})
                price_data["sui_price"] = data.get("usd")
                price_data["sui_price_change_24h"] = data.get("usd_24h_change")
                log.info(f"CoinGecko fallback: SUI=${price_data['sui_price']}")
                break
            except Exception as e:
                log.error(f"CoinGecko fallback attempt {attempt+1} failed: {e}")
                time.sleep(10)

    # Leader token (CoinGecko only source for this)
    leader_data = fetch_coingecko_leader()

    return {**price_data, **leader_data}


def fetch_defillama() -> dict:
    log.info("Fetching DeFiLlama TVL and DEX volume...")
    result = {"tvl": None, "tvl_change_24h": None, "dex_volume": None}
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

        # Fallback: chains endpoint
        if result["tvl"] is None:
            r2 = requests.get("https://api.llama.fi/v2/chains", timeout=10)
            r2.raise_for_status()
            for chain in r2.json():
                if chain.get("name", "").lower() == "sui":
                    result["tvl"] = chain.get("tvl")
                    result["tvl_change_24h"] = chain.get("change_1d")
                    break
            log.info(f"DeFiLlama: TVL=${result['tvl']:,.0f}" if result["tvl"] else "DeFiLlama: not found")
    except Exception as e:
        log.error(f"DeFiLlama TVL fetch failed: {e}")

    # DEX volume: DeFiLlama Sui DEX aggregator (true on-chain DEX vol)
    try:
        r3 = requests.get(
            "https://api.llama.fi/overview/dexs/sui?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume",
            timeout=10
        )
        if r3.status_code == 200:
            data = r3.json()
            vol = data.get("total24h")
            if vol:
                result["dex_volume"] = float(vol)
                log.info(f"DeFiLlama DEX vol (Sui): ${vol:,.0f}")
            else:
                log.warning("DeFiLlama DEX vol: total24h field not found")
        else:
            log.warning(f"DeFiLlama DEX vol: {r3.status_code}")
    except Exception as e:
        log.error(f"DeFiLlama DEX vol fetch failed: {e}")

    return result


def fetch_stablecoin_mcap() -> dict:
    """
    Fetch Sui stablecoin market cap from DeFiLlama.
    Returns current mcap and 24h change.
    """
    log.info("Fetching Sui stablecoin market cap...")
    result = {"stablecoin_mcap": None}
    try:
        r = requests.get("https://stablecoins.llama.fi/stablecoincharts/Sui", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if len(data) >= 2:
                current = data[-1].get("totalCirculating", {})
                prev = data[-2].get("totalCirculating", {})
                curr_val = sum(v for v in current.values() if isinstance(v, (int, float)))
                prev_val = sum(v for v in prev.values() if isinstance(v, (int, float)))
                result["stablecoin_mcap"] = curr_val
                if prev_val and prev_val > 0:
                    result["stablecoin_mcap_change"] = ((curr_val - prev_val) / prev_val) * 100
                log.info(f"Stablecoin mcap: ${curr_val:,.0f}")
        else:
            log.warning(f"Stablecoin mcap: {r.status_code}")
    except Exception as e:
        log.error(f"Stablecoin mcap fetch failed: {e}")
    return result


def fetch_tx_count() -> dict:
    """
    Fetch cumulative network transaction count from Sui RPC.
    Compares with previous snapshot to derive 12h tx count.
    """
    log.info("Fetching Sui transaction count...")
    result = {"tx_count_total": None}
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
        if total_tx > 0:
            result["tx_count_total"] = total_tx
            log.info(f"Sui RPC: {total_tx:,} total transactions")
    except Exception as e:
        log.error(f"Sui tx count fetch failed: {e}")
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


def fetch_active_addresses_blockberry() -> dict:
    """
    Fetch real 24h Daily Active Users from Blockberry (Suiscan) API.
    Requires BLOCKBERRY_API_KEY environment variable.
    Falls back to RPC proxy if key not available.
    """
    result = {"active_addresses": None}

    if not BLOCKBERRY_API_KEY:
        log.info("Blockberry API key not set — using RPC proxy for active addresses")
        return result

    try:
        url = "https://api.blockberry.one/sui/v1/network/stats"
        headers = {"x-api-key": BLOCKBERRY_API_KEY}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # Try common field names for DAUs
            dau = (
                data.get("dailyActiveAddresses") or
                data.get("active_addresses_24h") or
                data.get("dau") or
                data.get("activeAddresses")
            )
            if dau:
                result["active_addresses"] = int(dau)
                log.info(f"Blockberry: {dau:,} DAUs")
            else:
                log.warning(f"Blockberry: DAU field not found in response: {list(data.keys())}")
        else:
            log.warning(f"Blockberry: {r.status_code}")
    except Exception as e:
        log.error(f"Blockberry fetch failed: {e}")

    return result


def fetch_staking() -> dict:
    """
    Fetch SUI staking ratio from Sui RPC.
    Returns staking_ratio as a float (e.g. 0.65 = 65% staked).
    """
    log.info("Fetching staking ratio...")
    result = {"staking_ratio": None, "total_staked": None}
    try:
        url = "https://fullnode.mainnet.sui.io:443"
        r = requests.post(url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "suix_getLatestSuiSystemState", "params": []
        }, timeout=10)
        r.raise_for_status()
        state = r.json().get("result", {})

        total_staked = int(state.get("totalStake", 0))
        # Total supply is ~10B SUI, in MIST (1 SUI = 1e9 MIST)
        TOTAL_SUPPLY_MIST = 10_000_000_000 * 1_000_000_000
        if total_staked > 0:
            result["total_staked"] = total_staked / 1_000_000_000  # Convert to SUI
            result["staking_ratio"] = total_staked / TOTAL_SUPPLY_MIST
            log.info(f"Staking: {result['staking_ratio']:.1%} staked ({result['total_staked']:,.0f} SUI)")
        else:
            log.warning("Staking: no data returned")
    except Exception as e:
        log.error(f"Staking fetch failed: {e}")
    return result


def calculate_deepbook_ema(current_value: float, days: int = 7) -> float:
    """
    Calculate 7-day EMA of DeepBook liquidity from DB snapshots.
    Smooths intraday spikes.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT deepbook_liquidity FROM snapshots_v3 ORDER BY id DESC LIMIT ?", (days * 2,))
        rows = c.fetchall()
        conn.close()
        values = [r[0] for r in rows if r[0] is not None and r[0] > 0]
    except Exception:
        values = []

    if not values:
        return current_value or 0

    # EMA calculation
    k = 2 / (days + 1)
    ema = values[-1]  # Start from oldest
    for v in reversed(values[:-1]):
        ema = v * k + ema * (1 - k)
    # Include current value in EMA
    ema = current_value * k + ema * (1 - k)
    return round(ema, 2)


# ─────────────────────────────────────────
# LOGOS INDEX
# ─────────────────────────────────────────

WEIGHTS = {
    "tvl":              0.30,  # primary ecosystem health signal
    "staking_ratio":    0.22,  # long-run commitment signal
    "stablecoin_mcap":  0.20,  # real economic demand on-chain
    "deepbook":         0.15,  # SUI/USDC liquidity depth
    "mean_reversion":   0.13,  # contrarian flag — conditionally reduced in trends
}

FACTOR_LABELS = {
    "tvl":              "capital depth",
    "staking_ratio":    "staking commitment",
    "stablecoin_mcap":  "stablecoin demand",
    "deepbook":         "liquidity depth",
    "mean_reversion":   "mean reversion",
}

RANGES = {
    "tvl":              {"min": 200_000_000,   "max": 800_000_000},
    "staking_ratio":    {"min": 0.40,          "max": 0.80},
    "stablecoin_mcap":  {"min": 100_000_000,   "max": 1_100_000_000},
    "deepbook":         {"min": 0,             "max": 25_000_000},
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
    return max(0, min(100, 50 - (z * 10)))


def calculate_logos_index(data: dict, previous_index: float = None) -> dict:
    z = data.get("mean_reversion", 0.0) or 0.0

    # Conditional mean reversion: halve MR weight when |z| > 1 (trend-fading)
    weights = dict(WEIGHTS)
    if abs(z) > 1.0:
        freed = weights["mean_reversion"] * 0.5
        weights["mean_reversion"] -= freed
        other_keys = [k for k in weights if k != "mean_reversion"]
        total_others = sum(weights[k] for k in other_keys)
        for k in other_keys:
            weights[k] += freed * (weights[k] / total_others)

    scores = {
        "tvl":              normalise(data.get("tvl"), RANGES["tvl"]["min"], RANGES["tvl"]["max"]),
        "staking_ratio":    normalise(data.get("staking_ratio"), RANGES["staking_ratio"]["min"], RANGES["staking_ratio"]["max"]),
        "stablecoin_mcap":  normalise(data.get("stablecoin_mcap"), RANGES["stablecoin_mcap"]["min"], RANGES["stablecoin_mcap"]["max"]),
        "deepbook":         normalise(data.get("deepbook_ema"), RANGES["deepbook"]["min"], RANGES["deepbook"]["max"]),
        "mean_reversion":   zscore_to_score(z),
    }

    contributions = {k: scores[k] * weights[k] for k in weights}
    raw = max(1, min(100, sum(contributions.values())))

    if previous_index is not None:
        delta = raw - previous_index
        if abs(delta) > DAMPENING_CAP:
            raw = previous_index + (DAMPENING_CAP if delta > 0 else -DAMPENING_CAP)

    raw = round(raw, 1)
    top_two  = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:2]
    lagging  = min(contributions, key=contributions.get)

    def _fmt_factor(k):
        if k == "tvl":              return f"TVL {fmt_large(data.get('tvl'))}"
        if k == "staking_ratio":    return f"staking {(data.get('staking_ratio') or 0)*100:.1f}%"
        if k == "stablecoin_mcap":  return f"stablecoin {fmt_large(data.get('stablecoin_mcap'))}"
        if k == "deepbook":         return f"DeepBook {fmt_large(data.get('deepbook_ema'))}"
        if k == "mean_reversion":   return f"mean rev {(data.get('mean_reversion') or 0):+.2f}σ"
        return k

    anchors = " and ".join(_fmt_factor(k) for k, _ in top_two)
    price_change = data.get("sui_price_change_24h") or 0
    if abs(price_change) >= 3:
        suffix = f"; SUI price ({fmt_pct(price_change)}) excluded from index"
    else:
        suffix = f"; {FACTOR_LABELS[lagging]} provides least support"

    driver_line = f"Score anchored by {anchors}{suffix}"

    return {"score": raw, "contributions": contributions, "driver_line": driver_line}


# ─────────────────────────────────────────
# COMMENTARY
# ─────────────────────────────────────────

ARISTOTLE_SYSTEM_PROMPT = """
You are Aristotle — a Sui blockchain data service. Mechanical clarity with philosophical tone.

Voice rules:
- Observational, never predictive
- Never use: bullish, bearish, moon, dump, pumping, soaring, plunging
- Never say "I" or refer to yourself
- One sentence only
- No vague macro terms (ecosystem, market conditions, environment) unless grounded in a specific number
- No standalone sentences — always contrast or connect exactly two metrics
- No questions — they introduce prediction pressure
- Zero opinion — the data speaks, you report

Metric anchoring (mandatory):
- Every metric reference must include its value: "staking at 75.6%" not "staking holds"
- Every change reference must include magnitude: "DEX volume down 16%" not "volume fell"

Contrast requirement (mandatory):
- The sentence must hold two signals in tension: one that rose or held, one that fell or diverged

Pre-publish checklist — reject the sentence if:
- It could apply to 10 different days without changing a word
- It references fewer than 2 named metrics with values
- It contains an opinion, forecast, or implied recommendation

Examples:
"Staking steady at 75.6% while DEX volume dropped 16% to $59M — commitment holds as trading activity contracts."
"TVL at $585M unchanged while DeepBook EMA fell to $12M — capital is present but liquidity depth is thinning."
"Price +5.5% to $0.98 against a mean reversion of +0.93σ — price has moved ahead of its 20-day average."
"Logos Index 54.1 with staking at 75.7% and DEX volume at $59M — index is held down by weak volume, not by commitment."
"""


def generate_closing_line(data: dict) -> str:
    """Sends brief data to Claude, returns one closing line.
    Falls back to empty string if API call fails."""
    try:
        import anthropic
        tvl_b   = (data.get("tvl") or 0) / 1_000_000_000
        dex_m   = (data.get("dex_volume") or 0) / 1_000_000
        staking = (data.get("staking_ratio") or 0) * 100
        prompt = (
            f"Given this Sui blockchain data, write one closing sentence for the brief:\n\n"
            f"Price: ${data.get('sui_price', '—')} ({fmt_pct(data.get('sui_price_change_24h'))})\n"
            f"TVL: ${tvl_b:.2f}B\n"
            f"DEX Volume: ${dex_m:.1f}M\n"
            f"Staking Rate: {staking:.1f}%\n"
            f"Mean Reversion z-score: {data.get('mean_reversion', 0):+.2f}σ\n"
            f"Logos Index: {data.get('logos_index', '—')}/100\n\n"
            f"One sentence only. No preamble. No punctuation beyond the sentence itself."
        )
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=100,
            system=ARISTOTLE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.warning(f"Commentary generation failed: {e}")
        return ""


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
        return f"{round(value/1_000_000)}M"
    if value >= 1_000:
        return f"{round(value/1_000)}K"
    return str(value)

def fmt_change(value):
    """Format a raw change value (not percentage)."""
    if value is None:
        return ""
    return f"{'+' if value >= 0 else ''}{value:.2f}"

def get_arrow(change_pct: float, minor_threshold: float = 2.0, major_threshold: float = 5.0) -> str:
    if change_pct >= major_threshold:
        return "▲"
    elif change_pct >= minor_threshold:
        return "△"
    elif change_pct <= -major_threshold:
        return "▼"
    elif change_pct <= -minor_threshold:
        return "▽"
    else:
        return "="


def format_free_brief(data: dict, commentary: str = "") -> str:
    now = datetime.now(timezone.utc)
    session = "7h UTC · MORNING" if now.hour < 14 else "19h UTC · EVENING"
    sep = "─" * 24

    leader_str = "—"
    if data.get("best_token_symbol") and data.get("best_token_change") is not None:
        leader_str = f"{data['best_token_symbol']} {fmt_pct(data['best_token_change'])}"

    # DEX VOL with change vs previous snapshot
    prev_dex = get_previous_value("dex_volume")
    curr_dex = data.get("dex_volume")
    dex_str = fmt_large(curr_dex)
    if prev_dex and curr_dex and prev_dex > 0:
        dex_change = ((curr_dex - prev_dex) / prev_dex) * 100
        dex_str += f"   {fmt_pct(dex_change)}"
    else:
        dex_str += "   —"

    # Show Logos Index teaser on Monday 07:00 and Friday 21:00
    show_logos = (
        (now.weekday() == 0 and now.hour < 14) or   # Monday morning
        (now.weekday() == 4 and now.hour >= 14)       # Friday evening
    )
    logos = data.get("logos_index")
    logos_teaser = f"{logos:.1f}/100" if logos is not None else "—"

    lines = [
        "ARISTOTLE · SUI UPDATE",
        f"{now.strftime('%d %b %Y')} · {session}",
        sep,
        f"SUI        {fmt_price(data.get('sui_price'))}     {fmt_pct(data.get('sui_price_change_24h'))} {get_arrow(data.get('sui_price_change_24h') or 0)}",
        f"TVL        {fmt_large(data.get('tvl'))}   {fmt_pct(data.get('tvl_change_24h'))} {get_arrow(data.get('tvl_change_24h') or 0)}",
        f"DEX VOL    {dex_str}",
    ]
    if show_logos:
        lines.append(sep)
        lines.append(f"LOGOS INDEX  {logos_teaser}")
    lines.append(sep)
    lines.append("@aristotlesuiupdate")
    if commentary:
        lines.append("")
        lines.append(commentary)
    return "\n".join(lines)


def format_paid_brief(data: dict, commentary: str = "") -> str:
    now = datetime.now(timezone.utc)
    session = "7h UTC · MORNING" if now.hour < 14 else "19h UTC · EVENING"
    sep = "─" * 26

    # DeepBook with change
    prev_db = get_previous_value("deepbook_liquidity")
    curr_db = data.get("deepbook_liquidity")
    db_str = fmt_large(curr_db)
    if prev_db and curr_db and prev_db > 0:
        db_change = ((curr_db - prev_db) / prev_db) * 100
        db_str += f"    {fmt_pct(db_change)} {get_arrow(db_change)}"
    else:
        db_str += "    —"

    # Mean reversion with change vs previous
    prev_mr = get_previous_value("mean_reversion")
    curr_mr = data.get("mean_reversion")
    mr_str = f"{curr_mr:+.2f}σ" if curr_mr is not None else "—"
    if prev_mr is not None and curr_mr is not None:
        mr_change = curr_mr - prev_mr
        mr_str += f"    {fmt_change(mr_change)} {get_arrow(mr_change, minor_threshold=0.3, major_threshold=1.0)}"
    else:
        mr_str += "     —"

    # Logos Index with arrow and point change
    prev_logos = get_previous_value("logos_index")
    curr_logos = data.get("logos_index")
    logos_str = f"{curr_logos:.1f}/100" if curr_logos is not None else "—"
    if prev_logos is not None and curr_logos is not None:
        delta = curr_logos - prev_logos
        arrow = get_arrow(delta)
        logos_str += f"  {arrow}"
    else:
        logos_str += "  —"

    # DEX VOL with change vs previous snapshot
    prev_dex = get_previous_value("dex_volume")
    curr_dex = data.get("dex_volume")
    dex_str = fmt_large(curr_dex)
    if prev_dex and curr_dex and prev_dex > 0:
        dex_change = ((curr_dex - prev_dex) / prev_dex) * 100
        dex_str += f"   {fmt_pct(dex_change)} {get_arrow(dex_change)}"
    else:
        dex_str += "   —"

    lines = [
        "ARISTOTLE · SUI LOGOS",
        f"{now.strftime('%d %b %Y')} · {session}",
        sep,
        "",
        f"SUI            {fmt_price(data.get('sui_price'))}     {fmt_pct(data.get('sui_price_change_24h'))} {get_arrow(data.get('sui_price_change_24h') or 0)}",
        f"TVL            {fmt_large(data.get('tvl'))}   {fmt_pct(data.get('tvl_change_24h'))} {get_arrow(data.get('tvl_change_24h') or 0)}",
        f"DEX VOL        {dex_str}",
        f"DEEPBOOK       {db_str}",
        f"MEAN REV       {mr_str}",
        "",
        sep,
        f"LOGOS INDEX    {logos_str}",
    ]
    driver = data.get("logos_driver", "")
    if driver:
        lines.append(driver)
    lines.append(sep)
    if commentary:
        lines.append("")
        lines.append(commentary)
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
# X (TWITTER) POSTING — manual for now
# X API requires paid plan ($100/mo) to post
# Post manually by screenshotting the Telegram card
# ─────────────────────────────────────────


# ─────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────

def run():
    log.info("═══ ARISTOTLE PIPELINE START ═══")
    init_db()
    seed_price_history()

    cg      = fetch_coingecko()
    dl      = fetch_defillama()
    sc      = fetch_stablecoin_mcap()
    db      = fetch_deepbook()
    staking = fetch_staking()
    tx      = fetch_tx_count()

    data = {**cg, **dl, **sc, **db, **staking, **tx}

    # If DeepBook returned 0 or None, use last known good value from DB
    if not data.get("deepbook_liquidity"):
        last_db = get_previous_value("deepbook_liquidity")
        if last_db and last_db > 0:
            data["deepbook_liquidity"] = last_db
            log.warning(f"DeepBook fetch returned 0 — using last known value: ${last_db:,.0f}")

    # If DEX volume missing, use last known good value from DB
    if not data.get("dex_volume"):
        last_dex = get_previous_value("dex_volume")
        if last_dex and last_dex > 0:
            data["dex_volume"] = last_dex
            log.warning(f"DEX volume fetch failed — using last known value: ${last_dex:,.0f}")

    # Calculate 7-day EMA for DeepBook
    data["deepbook_ema"] = calculate_deepbook_ema(data.get("deepbook_liquidity") or 0)
    log.info(f"DeepBook EMA (7d): {data['deepbook_ema']:,.0f}")
    data["timestamp"] = datetime.now(timezone.utc).isoformat()

    # Mean reversion
    price_history = get_price_history(days=20)
    if data.get("sui_price") and price_history:
        data["mean_reversion"] = round(mean_reversion_zscore(data["sui_price"], price_history), 4)
    else:
        data["mean_reversion"] = 0.0
        log.warning("Not enough price history for mean reversion — using 0.0")

    # 12h TX delta for Logos Index (activity in this window, not all-time cumulative)
    prev_tx = get_previous_value("tx_count_total")
    if data.get("tx_count_total") and prev_tx:
        data["tx_12h_delta"] = data["tx_count_total"] - prev_tx
    else:
        data["tx_12h_delta"] = None

    # Logos Index
    prev_index = get_previous_value("logos_index")
    logos_result = calculate_logos_index(data, previous_index=prev_index)
    data["logos_index"]  = logos_result["score"]
    data["logos_driver"] = logos_result["driver_line"]
    log.info(f"Logos Index: {data['logos_index']} | {data['logos_driver']}")
    log.info(f"Contributions: { {k: round(v, 1) for k, v in logos_result['contributions'].items()} }")

    commentary = generate_closing_line(data)
    log.info(f"Commentary: {commentary or '(none)'}")

    free_brief = format_free_brief(data, commentary)
    paid_brief = format_paid_brief(data, commentary)

    log.info("\n" + "─"*40)
    log.info("FREE BRIEF:\n" + free_brief)
    log.info("─"*40)
    log.info("PAID BRIEF:\n" + paid_brief)
    log.info("─"*40)

    post_to_telegram(FREE_CHANNEL_ID, free_brief)
    post_to_telegram(PAID_CHANNEL_ID, paid_brief)

    # X posting: manual for now (API requires paid plan)

    save_snapshot(data)
    log.info("═══ PIPELINE COMPLETE ═══")


if __name__ == "__main__":
    run()
