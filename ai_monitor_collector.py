"""
AI Monitor — Unified Data Collector
=====================================
Single DB for the AI Thematic Monitor dashboard.
Collects 3 data streams on their optimal schedules:

  DAILY (08:00)   → RunPod GraphQL  : GPU utilisation (rentedCount/totalCount)
                  → Vast.ai REST API: H100 spot count + median price
  WEEKLY (Mon 08) → OpenRouter HTML : LLM rankings + token volume + app rankings

All data stored in: ./data/ai_monitor.db (SQLite)
Exports:  ./exports/  (CSV per table)

Usage:
    pip install requests beautifulsoup4

    python ai_monitor_collector.py --collect-gpu        # RunPod + Vast.ai (daily)
    python ai_monitor_collector.py --collect-rankings   # OpenRouter (weekly)
    python ai_monitor_collector.py --collect-all        # everything
    python ai_monitor_collector.py --audit              # audit DB
    python ai_monitor_collector.py --export             # export CSVs
    python ai_monitor_collector.py --setup-cron         # install cron jobs
    python ai_monitor_collector.py --history gpu        # GPU history
    python ai_monitor_collector.py --history rankings   # Rankings history

Env vars:
    RUNPOD_API_KEY   — optional (free key from runpod.io → Settings → API Keys)
    VAST_API_KEY     — required  (free key from vast.ai  → Account → API Keys)

Cron schedule installed by --setup-cron:
    0 8 * * *   python ai_monitor_collector.py --collect-gpu        # every day
    0 8 * * 1   python ai_monitor_collector.py --collect-rankings   # every Monday
"""

import sqlite3, json, re, os, sys, time, argparse, csv, statistics
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install requests beautifulsoup4")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

BASE_DIR    = Path(__file__).parent
DB_PATH     = BASE_DIR / "data" / "ai_monitor.db"
EXPORTS_DIR = BASE_DIR / "exports"
LOGS_DIR    = BASE_DIR / "logs"
EXPORTS_DIR.mkdir(exist_ok=True)
(BASE_DIR / "data").mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# API keys (free)
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY", "")
VAST_API_KEY   = os.environ.get("VAST_API_KEY", "")

# GPU targets for RunPod
RUNPOD_GPU_TARGETS = {
    "NVIDIA H100 80GB HBM3":  "H100_SXM",
    "NVIDIA H100 SXM":        "H100_SXM",
    "NVIDIA H200 SXM":        "H200_SXM",
    "NVIDIA B200":            "B200",
    "NVIDIA A100-SXM4-80GB":  "A100_SXM",
}

# Vast.ai GPU targets
VAST_GPU_TARGETS = {
    "H100_SXM":  "H100 SXM",
    "H100_PCIE": "H100 PCIe",
    "A100_SXM4": "A100 SXM",
}

# Classification helpers (from openrouter scraper)
CHINESE_PROVIDERS = {
    "deepseek","alibaba","qwen","minimax","moonshotai","stepfun",
    "xiaomi","bytedance","baidu","zhipu","01-ai","baichuan",
    "sensetime","thudm","kwaipilot","infini-ai",
}
PROPRIETARY_PROVIDERS = {
    "openai","anthropic","x-ai","perplexity","openrouter","google",
    "mistralai","cohere","amazon",
}

HEADERS_HTML = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ═══════════════════════════════════════════════════════════
# DATABASE — UNIFIED SCHEMA
# ═══════════════════════════════════════════════════════════

SCHEMA = """
-- ── GPU UTILISATION (daily, RunPod GraphQL) ───────────────────────────────
CREATE TABLE IF NOT EXISTS gpu_utilisation (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at     TEXT NOT NULL,        -- ISO datetime
    date             TEXT NOT NULL,        -- YYYY-MM-DD
    source           TEXT NOT NULL,        -- 'runpod'
    gpu_model        TEXT NOT NULL,        -- 'H100_SXM', 'B200', 'A100_SXM'
    -- Utilisation
    rented_count     INTEGER,              -- GPUs currently rented
    total_count      INTEGER,              -- total GPUs in RunPod fleet
    rental_pct       REAL,                 -- rented_count/total_count * 100
    stock_status     TEXT,                 -- 'High'/'Medium'/'Low' (qualitative)
    available_counts TEXT,                 -- JSON array e.g. [1,2,4,8]
    -- Prices ($/hr on-demand)
    secure_price     REAL,                 -- Secure Cloud on-demand
    community_price  REAL,                 -- Community Cloud on-demand
    spot_price       REAL,                 -- community spot minimum bid
    UNIQUE(date, source, gpu_model)
);

-- ── GPU SPOT MARKET (daily, Vast.ai REST) ────────────────────────────────
CREATE TABLE IF NOT EXISTS gpu_spot_market (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at     TEXT NOT NULL,
    date             TEXT NOT NULL,
    source           TEXT NOT NULL,        -- 'vastai'
    gpu_model        TEXT NOT NULL,        -- 'H100_SXM', 'H100_PCIE', 'A100_SXM'
    -- Availability (demand signal — inverse: high count = slack)
    n_listings       INTEGER,              -- number of available spot instances
    -- Price stats ($/GPU/hr, normalized by num_gpus)
    price_p10        REAL,                 -- 10th percentile (cheapest reliable)
    price_p50        REAL,                 -- median spot price
    price_p90        REAL,                 -- 90th percentile
    price_min        REAL,
    price_max        REAL,
    UNIQUE(date, source, gpu_model)
);

-- ── GPU ROLLING 7-DAY METRICS (computed daily) ───────────────────────────
CREATE TABLE IF NOT EXISTS gpu_weekly_metrics (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at      TEXT NOT NULL,
    as_of_date       TEXT NOT NULL,        -- date of computation
    gpu_model        TEXT NOT NULL,
    -- RunPod 7-day averages
    avg_rental_pct_7d    REAL,
    min_rental_pct_7d    REAL,
    max_rental_pct_7d    REAL,
    -- Vast.ai 7-day averages
    avg_spot_count_7d    INTEGER,
    avg_spot_p50_7d      REAL,
    spot_price_wow_pct   REAL,             -- % change vs prior 7-day avg
    count_wow_pct        REAL,             -- % change in listing count vs prior 7d
    -- Signal score (0-10)
    demand_score         REAL,             -- composite bull/bear score
    signal               TEXT,             -- 'STRONG_BULL'/'BULL'/'NEUT'/'BEAR'
    UNIQUE(as_of_date, gpu_model)
);

-- ── LLM MODEL RANKINGS (weekly, OpenRouter) ──────────────────────────────
CREATE TABLE IF NOT EXISTS model_rankings (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    week_date      TEXT NOT NULL,          -- Monday of that week
    scraped_at     TEXT NOT NULL,
    rank           INTEGER,
    model_id       TEXT NOT NULL,          -- 'anthropic/claude-sonnet-4-6'
    model_name     TEXT,
    provider       TEXT,
    token_count    INTEGER,
    is_open_source INTEGER,
    is_chinese     INTEGER,
    UNIQUE(week_date, model_id)
);

-- ── PROVIDER MARKET SHARE (weekly, OpenRouter) ───────────────────────────
CREATE TABLE IF NOT EXISTS provider_share (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    week_date       TEXT NOT NULL,
    scraped_at      TEXT NOT NULL,
    provider        TEXT NOT NULL,
    token_count     INTEGER,
    token_share_pct REAL,
    UNIQUE(week_date, provider)
);

-- ── APP RANKINGS (weekly, OpenRouter) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS app_rankings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    week_date    TEXT NOT NULL,
    scraped_at   TEXT NOT NULL,
    rank         INTEGER,
    app_url      TEXT NOT NULL,
    app_name     TEXT,
    token_count  INTEGER,
    UNIQUE(week_date, app_url)
);

-- ── LLM WEEKLY METRICS (weekly, OpenRouter) ──────────────────────────────
CREATE TABLE IF NOT EXISTS llm_weekly_metrics (
    week_date           TEXT PRIMARY KEY,
    scraped_at          TEXT NOT NULL,
    total_tokens        INTEGER,
    open_source_pct     REAL,
    chinese_pct         REAL,
    proprietary_pct     REAL,
    top1_model_id       TEXT,
    top1_tokens         INTEGER,
    top_provider        TEXT,
    n_models_tracked    INTEGER
);

-- ── INDEXES ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_gpu_util_date  ON gpu_utilisation(date, gpu_model);
CREATE INDEX IF NOT EXISTS idx_gpu_spot_date  ON gpu_spot_market(date, gpu_model);
CREATE INDEX IF NOT EXISTS idx_gpu_wk_date    ON gpu_weekly_metrics(as_of_date);
CREATE INDEX IF NOT EXISTS idx_mr_week        ON model_rankings(week_date);
CREATE INDEX IF NOT EXISTS idx_ps_week        ON provider_share(week_date);
CREATE INDEX IF NOT EXISTS idx_ar_week        ON app_rankings(week_date);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

def today_str():
    return date.today().isoformat()

def monday_str():
    d = date.today()
    return (d - timedelta(days=d.weekday())).isoformat()

# ═══════════════════════════════════════════════════════════
# GPU COLLECTOR — RUNPOD GRAPHQL
# ═══════════════════════════════════════════════════════════

RUNPOD_GRAPHQL_URL = "https://api.runpod.io/graphql"

RUNPOD_QUERY = """
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    securePrice
    communityPrice
    communitySpotPrice
    secureSpotPrice
    lowestPrice(input: { gpuCount: 1 }) {
      minimumBidPrice
      uninterruptablePrice
      rentedCount
      totalCount
      rentalPercentage
      stockStatus
      availableGpuCounts
    }
  }
}
"""

def collect_runpod(conn):
    """
    Query RunPod GraphQL for GPU utilisation.
    Key fields: rentedCount, totalCount → rental_pct = rentedCount/totalCount*100
    
    This gives us the B200 utilisation rate — the frontier training signal.
    RunPod Secure Cloud has significant B200 inventory (Vast.ai doesn't).
    
    Auth: RUNPOD_API_KEY env var (free, from runpod.io → Settings → API Keys)
    No key = still works for public gpuTypes query, but rate-limited.
    """
    headers = {"Content-Type": "application/json"}
    if RUNPOD_API_KEY:
        headers["Authorization"] = f"Bearer {RUNPOD_API_KEY}"

    print("  [RunPod] Querying GraphQL...")
    try:
        r = requests.post(
            RUNPOD_GRAPHQL_URL,
            headers=headers,
            json={"query": RUNPOD_QUERY},
            timeout=20
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [RunPod] ERROR: {e}")
        return 0

    gpu_types = data.get("data", {}).get("gpuTypes", [])
    if not gpu_types:
        print(f"  [RunPod] No data returned")
        return 0

    # Identify target GPUs by display name pattern
    TARGET_PATTERNS = {
        "H100_SXM":  ["H100 80GB HBM3", "H100 SXM", "H100-SXM"],
        "H200_SXM":  ["H200 SXM", "H200"],
        "B200":      ["B200"],
        "A100_SXM":  ["A100-SXM4-80GB", "A100 SXM4", "A100 SXM"],
        "H100_PCIE": ["H100 PCIe", "H100 PCIE"],
    }

    stored = 0
    now = now_iso()
    dt  = today_str()
    found_models = set()

    for gpu in gpu_types:
        display = gpu.get("displayName", "") or ""
        gpu_id  = gpu.get("id", "") or ""

        # Match against target patterns
        matched_key = None
        for key, patterns in TARGET_PATTERNS.items():
            if any(p in display or p in gpu_id for p in patterns):
                matched_key = key
                break
        if not matched_key or matched_key in found_models:
            continue
        found_models.add(matched_key)

        lp = gpu.get("lowestPrice") or {}

        # Compute rental percentage from raw counts (more accurate than rentalPercentage field)
        rented = lp.get("rentedCount")
        total  = lp.get("totalCount")
        if rented is not None and total and total > 0:
            rental_pct = round(rented / total * 100, 2)
        else:
            rental_pct = lp.get("rentalPercentage")  # fallback to API computed value

        avail_counts = lp.get("availableGpuCounts")

        conn.execute("""
            INSERT OR REPLACE INTO gpu_utilisation
            (collected_at, date, source, gpu_model,
             rented_count, total_count, rental_pct, stock_status, available_counts,
             secure_price, community_price, spot_price)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, dt, "runpod", matched_key,
            rented, total, rental_pct,
            lp.get("stockStatus"),
            json.dumps(avail_counts) if avail_counts else None,
            gpu.get("securePrice"),
            gpu.get("communityPrice"),
            gpu.get("communitySpotPrice") or lp.get("minimumBidPrice"),
        ))
        stored += 1

        status = f"rental={rental_pct:.1f}%" if rental_pct is not None else f"stock={lp.get('stockStatus','?')}"
        print(f"  [RunPod] {matched_key:<12} {display[:30]:<30} {status}")

    conn.commit()
    print(f"  [RunPod] {stored} GPUs stored for {dt}")
    return stored


# ═══════════════════════════════════════════════════════════
# GPU COLLECTOR — VAST.AI REST
# ═══════════════════════════════════════════════════════════

VAST_API_URL = "https://console.vast.ai/api/v0/bundles/"

def collect_vastai(conn):
    """
    Query Vast.ai marketplace for H100/A100 spot availability and price.
    
    Key signal: H100 listing count is an INVERSE demand indicator.
      Low count + rising price = demand tight = BULLISH
      High count + falling price = supply loose = NEUTRAL/BEARISH
    
    Auth: VAST_API_KEY env var (free, from vast.ai → Account → API Keys)
    """
    if not VAST_API_KEY:
        print("  [Vast.ai] No VAST_API_KEY — skipping")
        print("  [Vast.ai] Get free key: vast.ai → Account → API Keys")
        return 0

    headers = {
        "Authorization": f"Bearer {VAST_API_KEY}",
        "Content-Type": "application/json",
    }

    stored = 0
    now = now_iso()
    dt  = today_str()

    for gpu_api_name, gpu_display in VAST_GPU_TARGETS.items():
        print(f"  [Vast.ai] Querying {gpu_display}...", end="", flush=True)
        payload = {
            "gpu_name":  {"eq": gpu_api_name},
            "type":      "on-demand",
            "verified":  {"eq": True},
            "rentable":  {"eq": True},
            "rented":    {"eq": False},
            "reliability": {"gte": 0.90},
            "limit": 500,
        }

        try:
            r = requests.post(VAST_API_URL, headers=headers, json=payload, timeout=20)
            r.raise_for_status()
            offers = r.json().get("offers", [])
        except Exception as e:
            print(f" ERROR: {e}")
            continue

        # Compute price per GPU (normalize multi-GPU listings)
        prices = []
        for o in offers:
            dph   = o.get("dph_total")
            n_gpu = o.get("num_gpus", 1) or 1
            if dph and n_gpu > 0:
                price_per_gpu = dph / n_gpu
                if 0.05 < price_per_gpu < 50.0:  # sanity filter
                    prices.append(price_per_gpu)

        n   = len(prices)
        if n == 0:
            print(f" no valid listings")
            continue

        prices.sort()
        conn.execute("""
            INSERT OR REPLACE INTO gpu_spot_market
            (collected_at, date, source, gpu_model,
             n_listings, price_p10, price_p50, price_p90, price_min, price_max)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            now, dt, "vastai", gpu_api_name,
            n,
            round(prices[max(0, int(n*0.10))], 4),
            round(statistics.median(prices), 4),
            round(prices[min(n-1, int(n*0.90))], 4),
            round(prices[0], 4),
            round(prices[-1], 4),
        ))
        stored += 1
        print(f" n={n}  p50=${statistics.median(prices):.2f}/hr  "
              f"p10=${prices[max(0,int(n*0.10))]:.2f}/hr")

        time.sleep(1)  # polite delay

    conn.commit()
    print(f"  [Vast.ai] {stored} GPU models stored for {dt}")
    return stored


# ═══════════════════════════════════════════════════════════
# ROLLING 7-DAY METRICS — GPU SIGNALS
# ═══════════════════════════════════════════════════════════

def compute_gpu_weekly_metrics(conn):
    """
    Compute rolling 7-day averages for GPU utilisation and spot market.
    Also compute WoW delta (current 7d vs prior 7d).
    Derives the demand_score (0-10) and signal label.

    RunPod rental_pct scoring (B200 primary signal):
      >92% = 10 | 85-92% = 8 | 75-85% = 6 | 65-75% = 4 | <65% = 2

    Vast.ai H100 spot composite (count + price direction):
      count falling + price rising = 10
      count stable  + price stable = 7
      count rising  + price flat   = 5
      count high    + price falling = 3
    """
    now   = now_iso()
    today = today_str()
    t7    = (date.today() - timedelta(days=6)).isoformat()  # 7 days window
    t14   = (date.today() - timedelta(days=13)).isoformat() # prior 7 days

    target_gpus = list(set(list(RUNPOD_GPU_TARGETS.values()) + list(VAST_GPU_TARGETS.keys())))
    target_gpus = list(dict.fromkeys(target_gpus))  # deduplicate preserving order

    for gpu_model in target_gpus:

        # ── RunPod 7-day rolling ──────────────────────────────────────
        rp_rows = conn.execute("""
            SELECT rental_pct FROM gpu_utilisation
            WHERE gpu_model=? AND date>=? AND date<=? AND rental_pct IS NOT NULL
            ORDER BY date
        """, (gpu_model, t7, today)).fetchall()
        rp_vals = [r["rental_pct"] for r in rp_rows]

        avg_rp = round(statistics.mean(rp_vals), 2)     if rp_vals else None
        min_rp = round(min(rp_vals), 2)                 if rp_vals else None
        max_rp = round(max(rp_vals), 2)                 if rp_vals else None

        # ── Vast.ai 7-day rolling ────────────────────────────────────
        va_rows_cur = conn.execute("""
            SELECT n_listings, price_p50 FROM gpu_spot_market
            WHERE gpu_model=? AND date>=? AND date<=?
            ORDER BY date
        """, (gpu_model, t7, today)).fetchall()

        va_rows_pri = conn.execute("""
            SELECT n_listings, price_p50 FROM gpu_spot_market
            WHERE gpu_model=? AND date>=? AND date<?
            ORDER BY date
        """, (gpu_model, t14, t7)).fetchall()

        counts_cur = [r["n_listings"] for r in va_rows_cur if r["n_listings"]]
        prices_cur = [r["price_p50"]  for r in va_rows_cur if r["price_p50"]]
        counts_pri = [r["n_listings"] for r in va_rows_pri if r["n_listings"]]
        prices_pri = [r["price_p50"]  for r in va_rows_pri if r["price_p50"]]

        avg_count_cur = round(statistics.mean(counts_cur)) if counts_cur else None
        avg_price_cur = round(statistics.mean(prices_cur), 4) if prices_cur else None
        avg_count_pri = round(statistics.mean(counts_pri)) if counts_pri else None
        avg_price_pri = round(statistics.mean(prices_pri), 4) if prices_pri else None

        # WoW deltas
        price_wow = None
        count_wow = None
        if avg_price_cur and avg_price_pri and avg_price_pri > 0:
            price_wow = round((avg_price_cur - avg_price_pri) / avg_price_pri * 100, 2)
        if avg_count_cur and avg_count_pri and avg_count_pri > 0:
            count_wow = round((avg_count_cur - avg_count_pri) / avg_count_pri * 100, 2)

        # ── Demand Score (0-10) ───────────────────────────────────────
        score = 5.0  # neutral default

        # RunPod utilisation component (weight 60% for B200, 30% for H100)
        if avg_rp is not None:
            if   avg_rp > 92: rp_score = 10.0
            elif avg_rp > 85: rp_score = 8.0
            elif avg_rp > 75: rp_score = 6.0
            elif avg_rp > 65: rp_score = 4.0
            else:             rp_score = 2.0
            score = rp_score * 0.6 + score * 0.4

        # Vast.ai spot component (inverse: lower count = higher demand)
        if avg_count_cur is not None and price_wow is not None:
            # Count falling + price rising = very bullish
            if   count_wow is not None and count_wow < -10 and price_wow > 2:  va_score = 10.0
            elif count_wow is not None and count_wow < 0   and price_wow > 0:  va_score = 8.0
            elif abs(price_wow or 0) < 2 and abs(count_wow or 0) < 10:         va_score = 6.0
            elif count_wow is not None and count_wow > 10  and price_wow < 0:  va_score = 3.0
            else:                                                                va_score = 5.0
            score = score * 0.6 + va_score * 0.4

        score = round(min(10.0, max(0.0, score)), 2)

        # Signal label
        if   score >= 8.5: signal = "STRONG_BULL"
        elif score >= 7.0: signal = "BULL"
        elif score >= 5.0: signal = "NEUT"
        elif score >= 3.0: signal = "BEAR"
        else:              signal = "STRONG_BEAR"

        conn.execute("""
            INSERT OR REPLACE INTO gpu_weekly_metrics
            (computed_at, as_of_date, gpu_model,
             avg_rental_pct_7d, min_rental_pct_7d, max_rental_pct_7d,
             avg_spot_count_7d, avg_spot_p50_7d,
             spot_price_wow_pct, count_wow_pct,
             demand_score, signal)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, today, gpu_model,
            avg_rp, min_rp, max_rp,
            avg_count_cur, avg_price_cur,
            price_wow, count_wow,
            score, signal,
        ))

        print(f"  [Metrics] {gpu_model:<12} score={score:.1f} {signal:<12} "
              f"rp={avg_rp:.1f}% " if avg_rp else f"  [Metrics] {gpu_model:<12} score={score:.1f} {signal}")

    conn.commit()
    print(f"  [Metrics] GPU weekly metrics computed for {today}")


# ═══════════════════════════════════════════════════════════
# OPENROUTER COLLECTOR (same logic as openrouter_scraper.py)
# ═══════════════════════════════════════════════════════════

RANKINGS_URL = "https://openrouter.ai/rankings"
SKIP_PREFIXES = {
    "models","chat","rankings","apps","enterprise","pricing","docs",
    "settings","providers","about","announcements","careers","privacy",
    "terms","support","api","sdk","status","redeem","works-with-openrouter",
}

def classify_model(model_id):
    p = model_id.split("/")[0].lower() if "/" in model_id else model_id.lower()
    return int(p not in PROPRIETARY_PROVIDERS), int(p in CHINESE_PROVIDERS)

def model_display_name(model_id):
    name = model_id.split("/")[-1] if "/" in model_id else model_id
    name = re.sub(r"-\d{8}$", "", name)
    name = re.sub(r":.*$", "", name)
    return name.replace("-", " ").title()

def parse_tokens_str(raw):
    if not raw: return None
    m = re.search(r"([\d]+(?:[,.][\d]+)?)\s*([TBM])\s*tokens?", raw, re.I)
    if not m: return None
    val  = float(m.group(1).replace(",", "."))
    unit = m.group(2).upper()
    return int(val * {"T": 1e12, "B": 1e9, "M": 1e6}.get(unit, 1))

def fetch_rankings_html():
    print(f"  [OpenRouter] GET {RANKINGS_URL}")
    r = requests.get(RANKINGS_URL, headers=HEADERS_HTML, timeout=30)
    r.raise_for_status()
    print(f"  [OpenRouter] HTTP {r.status_code} — {len(r.text):,} chars")
    return r.text

def extract_rsc_chunks(html):
    chunks, seen, pos = [], set(), 0
    while True:
        idx = html.find('"x":"202', pos)
        if idx == -1: break
        start = html.rfind("{", max(0, idx-30), idx+1)
        if start == -1: pos = idx+1; continue
        depth = 0; end = -1
        for i in range(start, min(len(html), start+300_000)):
            if   html[i] == "{": depth += 1
            elif html[i] == "}":
                depth -= 1
                if depth == 0: end = i; break
        if end == -1: pos = idx+1; continue
        raw = html[start:end+1]
        key = raw[:60]
        if key in seen: pos = idx+1; continue
        seen.add(key)
        try:
            obj = json.loads(raw)
            if (isinstance(obj, dict) and "x" in obj and "ys" in obj
                    and isinstance(obj["ys"], dict)
                    and re.match(r"\d{4}-\d{2}-\d{2}", str(obj["x"]))
                    and len(obj["ys"]) > 0):
                chunks.append(obj)
        except: pass
        pos = idx+1
    print(f"  [OpenRouter] RSC: {len(chunks)} chunks")
    return chunks

def classify_chunks(chunks):
    PROV_IDS = {"anthropic","openai","google","deepseek","minimax","meta-llama",
                "mistralai","x-ai","cohere","amazon","nvidia","moonshotai",
                "xiaomi","stepfun","others"}
    LANG_KEYS = {"English","Chinese","French","Spanish","German","Japanese","Korean"}
    PROG_KEYS = {"Python","JavaScript","TypeScript","Go","Rust","Java","C++","C#"}
    out = {"models": {}, "providers": {}, "languages": {}, "programming": {}}
    for c in chunks:
        d, ys = str(c["x"]), c["ys"]
        keys  = set(ys.keys())
        if   any("/" in k for k in keys): section = "models"
        elif keys & LANG_KEYS:            section = "languages"
        elif keys & PROG_KEYS:            section = "programming"
        else:                             section = "providers"
        if d not in out[section]: out[section][d] = {}
        out[section][d].update(ys)
    for s, data in out.items():
        print(f"    {s}: {len(data)} weeks")
    return out

def scrape_dom_models(html):
    soup, results, seen = BeautifulSoup(html, "html.parser"), [], set()
    for a in soup.find_all("a", href=re.compile(r"^/[^/]+/[^/]+$")):
        href  = a.get("href", "")
        parts = [p for p in href.split("/") if p]
        if len(parts) != 2 or parts[0] in SKIP_PREFIXES or href in seen: continue
        name = a.get_text(strip=True)
        if not name or len(name) > 100: continue
        container = a; token_raw = None
        for _ in range(10):
            container = container.parent
            if not container: break
            txt = container.get_text(" ", strip=True)
            m = re.search(r"([\d]+(?:[,.][\d]+)?)\s*([TBM])\s*tokens?", txt, re.I)
            if m: token_raw = m.group(0); break
        if not token_raw: continue
        seen.add(href)
        results.append({"rank": len(results)+1, "model_id": "/".join(parts),
                         "model_name": name, "provider": parts[0],
                         "token_count": parse_tokens_str(token_raw)})
        if len(results) >= 50: break
    print(f"  [OpenRouter] DOM models: {len(results)}")
    return results

def scrape_dom_apps(html):
    soup, apps = BeautifulSoup(html, "html.parser"), []
    for a in soup.find_all("a", href=re.compile(r"/apps\?url=")):
        href  = a.get("href", "")
        param = re.search(r"url=([^&]+)", href)
        if not param: continue
        app_url = unquote(param.group(1))
        name    = a.get_text(separator=" ", strip=True).split("\n")[0][:100]
        container = a; token_raw = None
        for _ in range(8):
            container = container.parent
            if not container: break
            txt = container.get_text(" ", strip=True)
            m = re.search(r"([\d]+(?:[,.][\d]+)?)\s*([TBM])\s*tokens?", txt, re.I)
            if m: token_raw = m.group(0); break
        apps.append({"rank": len(apps)+1, "app_url": app_url,
                     "app_name": name, "token_count": parse_tokens_str(token_raw)})
        if len(apps) >= 30: break
    print(f"  [OpenRouter] DOM apps: {len(apps)}")
    return apps

def store_rankings(conn, sections, dom_models, dom_apps):
    now  = now_iso()
    week = monday_str()

    # Model rankings — RSC time series (all history)
    mr = 0
    for date_str, model_dict in sections["models"].items():
        for rank, (mid, tc) in enumerate(
            sorted(model_dict.items(), key=lambda x: x[1], reverse=True), 1
        ):
            if mid.lower() in {"others","other"}: continue
            is_oss, is_cn = classify_model(mid)
            conn.execute("""
                INSERT OR REPLACE INTO model_rankings
                (week_date, scraped_at, rank, model_id, model_name, provider,
                 token_count, is_open_source, is_chinese)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (date_str, now, rank, mid, model_display_name(mid),
                  mid.split("/")[0] if "/" in mid else "",
                  int(tc) if tc else None, is_oss, is_cn))
            mr += 1

    # Supplement with DOM if current week missing from RSC
    if dom_models and week not in sections["models"]:
        for m in dom_models:
            is_oss, is_cn = classify_model(m["model_id"])
            conn.execute("""
                INSERT OR REPLACE INTO model_rankings
                (week_date, scraped_at, rank, model_id, model_name, provider,
                 token_count, is_open_source, is_chinese)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (week, now, m["rank"], m["model_id"], m["model_name"],
                  m["provider"], m["token_count"], is_oss, is_cn))
            mr += 1
    print(f"  [OpenRouter] {mr:,} model_rankings rows upserted")

    # Provider share
    pr = 0
    for date_str, pd in sections["providers"].items():
        total = sum(v for v in pd.values() if v) or 1
        for provider, tc in pd.items():
            if not tc: continue
            conn.execute("""
                INSERT OR REPLACE INTO provider_share
                (week_date, scraped_at, provider, token_count, token_share_pct)
                VALUES (?,?,?,?,?)
            """, (date_str, now, provider, int(tc), round(tc/total*100, 4)))
            pr += 1
    print(f"  [OpenRouter] {pr:,} provider_share rows upserted")

    # Apps
    for a in dom_apps:
        conn.execute("""
            INSERT OR REPLACE INTO app_rankings
            (week_date, scraped_at, rank, app_url, app_name, token_count)
            VALUES (?,?,?,?,?,?)
        """, (week, now, a["rank"], a["app_url"], a["app_name"], a["token_count"]))
    print(f"  [OpenRouter] {len(dom_apps)} app_rankings rows upserted")

    # LLM weekly metrics
    mdates = sorted(sections["models"].keys())
    pdates = sorted(sections["providers"].keys())
    if mdates:
        ld  = mdates[-1]
        lmd = {k: v for k, v in sections["models"][ld].items()
               if k.lower() not in {"others","other"} and v}
        total    = sum(lmd.values()) or 1
        oss_t    = sum(v for k, v in lmd.items() if classify_model(k)[0])
        cn_t     = sum(v for k, v in lmd.items() if classify_model(k)[1])
        prop_t   = sum(v for k, v in lmd.items() if not classify_model(k)[0])
        top1     = max(lmd.items(), key=lambda x: x[1], default=("?", 0))
        top_prov = ""
        if pdates:
            pd_latest = sections["providers"][pdates[-1]]
            top_prov  = max(pd_latest.items(), key=lambda x: x[1], default=("?",0))[0]
        conn.execute("""
            INSERT OR REPLACE INTO llm_weekly_metrics
            (week_date, scraped_at, total_tokens, open_source_pct, chinese_pct,
             proprietary_pct, top1_model_id, top1_tokens, top_provider, n_models_tracked)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (ld, now, total,
               round(oss_t/total*100, 2), round(cn_t/total*100, 2),
               round(prop_t/total*100, 2), top1[0], int(top1[1]),
               top_prov, len(lmd)))

    conn.commit()
    print(f"  [OpenRouter] LLM weekly metrics updated")


def collect_rankings(conn):
    html     = fetch_rankings_html()
    chunks   = extract_rsc_chunks(html)
    sections = classify_chunks(chunks)
    doms     = scrape_dom_models(html)
    apps     = scrape_dom_apps(html)
    store_rankings(conn, sections, doms, apps)


# ═══════════════════════════════════════════════════════════
# EXPORT CSV
# ═══════════════════════════════════════════════════════════

def export_all(conn):
    today = date.today()
    label = f"{today.year}-W{today.isocalendar()[1]:02d}"

    def write(path, sql, params=()):
        rows = conn.execute(sql, params).fetchall()
        if not rows: return
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(dict(rows[0]).keys()))
            w.writeheader(); w.writerows([dict(r) for r in rows])
        print(f"  {path.name}  ({len(rows)} rows)")

    print("\n[Export]")
    # GPU tables — full history
    write(EXPORTS_DIR / "gpu_utilisation.csv",
          "SELECT * FROM gpu_utilisation ORDER BY date, gpu_model")
    write(EXPORTS_DIR / "gpu_spot_market.csv",
          "SELECT * FROM gpu_spot_market ORDER BY date, gpu_model")
    write(EXPORTS_DIR / "gpu_weekly_metrics.csv",
          "SELECT * FROM gpu_weekly_metrics ORDER BY as_of_date, gpu_model")
    # LLM — latest week models
    lw = conn.execute("SELECT MAX(week_date) FROM model_rankings").fetchone()[0]
    if lw:
        write(EXPORTS_DIR / f"models_{label}.csv",
              "SELECT * FROM model_rankings WHERE week_date=? ORDER BY rank", (lw,))
    # Provider share — full history for time series
    write(EXPORTS_DIR / "provider_share_history.csv",
          "SELECT * FROM provider_share ORDER BY week_date, token_count DESC")
    # LLM metrics history
    write(EXPORTS_DIR / "llm_weekly_metrics.csv",
          "SELECT * FROM llm_weekly_metrics ORDER BY week_date")
    # Latest apps
    la = conn.execute("SELECT MAX(week_date) FROM app_rankings").fetchone()[0]
    if la:
        write(EXPORTS_DIR / f"apps_{label}.csv",
              "SELECT * FROM app_rankings WHERE week_date=? ORDER BY rank", (la,))
    print(f"  → {EXPORTS_DIR}/")


# ═══════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════

def audit(conn):
    print("\n" + "═"*64)
    print("  AI Monitor DB — Audit")
    print("═"*64)

    tables = [
        "gpu_utilisation", "gpu_spot_market", "gpu_weekly_metrics",
        "model_rankings", "provider_share", "app_rankings", "llm_weekly_metrics"
    ]
    DATE_COL = {
        "gpu_utilisation":   "date",
        "gpu_spot_market":   "date",
        "gpu_weekly_metrics":"as_of_date",
        "model_rankings":    "week_date",
        "provider_share":    "week_date",
        "app_rankings":      "week_date",
        "llm_weekly_metrics":"week_date",
    }
    for t in tables:
        try:
            n  = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            dc = DATE_COL.get(t, "date")
            mn = conn.execute(f"SELECT MIN({dc}) FROM {t}").fetchone()[0]
            mx = conn.execute(f"SELECT MAX({dc}) FROM {t}").fetchone()[0]
            print(f"  {t:<28} {n:>6,} rows   {mn or '?'} → {mx or '?'}")
        except Exception as e:
            print(f"  {t:<28} error: {e}")

    # GPU utilisation latest
    print("\n" + "─"*64)
    print("  GPU Utilisation — Latest Readings")
    rows = conn.execute("""
        SELECT date, gpu_model, rental_pct, rented_count, total_count,
               stock_status, secure_price, community_price
        FROM gpu_utilisation
        WHERE date = (SELECT MAX(date) FROM gpu_utilisation)
        ORDER BY gpu_model
    """).fetchall()
    if rows:
        print(f"  Date: {rows[0]['date']}")
        print(f"  {'GPU':<14} {'Rental%':<10} {'Rented':<8} {'Total':<8} "
              f"{'Stock':<8} {'Secure$/hr':<12} {'Comm$/hr'}")
        print(f"  {'-'*72}")
        for r in rows:
            rp = f"{r['rental_pct']:.1f}%" if r['rental_pct'] is not None else "?"
            print(f"  {r['gpu_model']:<14} {rp:<10} "
                  f"{str(r['rented_count'] or '?'):<8} {str(r['total_count'] or '?'):<8} "
                  f"{r['stock_status'] or '?':<8} "
                  f"${r['secure_price'] or '?':<11} ${r['community_price'] or '?'}")

    # Vast.ai spot latest
    print("\n" + "─"*64)
    print("  GPU Spot Market (Vast.ai) — Latest Readings")
    rows = conn.execute("""
        SELECT date, gpu_model, n_listings, price_p10, price_p50, price_p90
        FROM gpu_spot_market
        WHERE date = (SELECT MAX(date) FROM gpu_spot_market)
        ORDER BY gpu_model
    """).fetchall()
    if rows:
        print(f"  Date: {rows[0]['date']}")
        print(f"  {'GPU':<14} {'Listings':<10} {'p10$/hr':<10} {'p50$/hr':<10} {'p90$/hr'}")
        print(f"  {'-'*55}")
        for r in rows:
            print(f"  {r['gpu_model']:<14} {str(r['n_listings'] or '?'):<10} "
                  f"${r['price_p10'] or '?':<9} ${r['price_p50'] or '?':<9} ${r['price_p90'] or '?'}")

    # GPU signals latest
    print("\n" + "─"*64)
    print("  GPU Demand Signals — Rolling 7d")
    rows = conn.execute("""
        SELECT as_of_date, gpu_model, avg_rental_pct_7d, avg_spot_count_7d,
               avg_spot_p50_7d, spot_price_wow_pct, count_wow_pct, demand_score, signal
        FROM gpu_weekly_metrics
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM gpu_weekly_metrics)
        ORDER BY gpu_model
    """).fetchall()
    if rows:
        print(f"  As of: {rows[0]['as_of_date']}")
        print(f"  {'GPU':<14} {'Util7d%':<10} {'Listings':<10} "
              f"{'P50$':<8} {'PriceWoW':<10} {'Score':<7} Signal")
        print(f"  {'-'*72}")
        for r in rows:
            rp    = f"{r['avg_rental_pct_7d']:.1f}%" if r['avg_rental_pct_7d'] is not None else "?"
            count = str(r['avg_spot_count_7d'] or "?")
            p50   = f"${r['avg_spot_p50_7d']:.2f}" if r['avg_spot_p50_7d'] else "?"
            wow   = f"{r['spot_price_wow_pct']:+.1f}%" if r['spot_price_wow_pct'] is not None else "?"
            score = f"{r['demand_score']:.1f}" if r['demand_score'] is not None else "?"
            print(f"  {r['gpu_model']:<14} {rp:<10} {count:<10} "
                  f"{p50:<8} {wow:<10} {score:<7} {r['signal'] or '?'}")

    # LLM metrics
    print("\n" + "─"*64)
    print("  LLM Weekly Metrics — Last 8 weeks")
    rows = conn.execute("""
        SELECT week_date, total_tokens, open_source_pct, chinese_pct,
               proprietary_pct, top1_model_id
        FROM llm_weekly_metrics ORDER BY week_date DESC LIMIT 8
    """).fetchall()
    if rows:
        print(f"  {'Week':<12} {'Total T':<9} {'OSS%':<7} {'CN%':<7} {'Prop%':<7} Top Model")
        print(f"  {'-'*65}")
        for r in rows:
            tt  = f"{r['total_tokens']/1e12:.2f}" if r['total_tokens'] else "?"
            oss = f"{r['open_source_pct']:.1f}"   if r['open_source_pct'] else "?"
            cn  = f"{r['chinese_pct']:.1f}"       if r['chinese_pct'] else "?"
            pr  = f"{r['proprietary_pct']:.1f}"   if r['proprietary_pct'] else "?"
            top = (r['top1_model_id'] or "?")[:28]
            print(f"  {r['week_date']:<12} {tt:<9} {oss:<7} {cn:<7} {pr:<7} {top}")

    # DB size
    sz = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    print(f"\n  DB: {DB_PATH}  ({sz/1024:.0f} KB)")
    print("═"*64 + "\n")


# ═══════════════════════════════════════════════════════════
# HISTORY
# ═══════════════════════════════════════════════════════════

def print_history(section, conn):
    if section == "gpu":
        rows = conn.execute("""
            SELECT date, gpu_model, rental_pct, n_listings, price_p50
            FROM gpu_utilisation u
            LEFT JOIN gpu_spot_market s USING (date, gpu_model)
            WHERE gpu_model IN ('H100_SXM','B200')
            ORDER BY date DESC, gpu_model LIMIT 30
        """).fetchall()
        print(f"\n{'Date':<12} {'GPU':<14} {'Util%':<9} {'Listings':<10} {'P50$/hr'}")
        print("-"*55)
        for r in rows:
            rp = f"{r['rental_pct']:.1f}" if r['rental_pct'] is not None else "?"
            nl = str(r['n_listings'] or "?")
            p50= f"${r['price_p50']:.2f}" if r['price_p50'] else "?"
            print(f"{r['date']:<12} {r['gpu_model']:<14} {rp:<9} {nl:<10} {p50}")
    else:
        rows = conn.execute("""
            SELECT week_date, total_tokens, open_source_pct, chinese_pct, top1_model_id
            FROM llm_weekly_metrics ORDER BY week_date DESC LIMIT 20
        """).fetchall()
        print(f"\n{'Week':<12} {'Total(T)':<10} {'OSS%':<8} {'CN%':<8} Top Model")
        print("-"*65)
        for r in rows:
            tt  = f"{r['total_tokens']/1e12:.2f}" if r['total_tokens'] else "?"
            oss = f"{r['open_source_pct']:.1f}"   if r['open_source_pct'] else "?"
            cn  = f"{r['chinese_pct']:.1f}"       if r['chinese_pct'] else "?"
            print(f"{r['week_date']:<12} {tt:<10} {oss:<8} {cn:<8} {r['top1_model_id'] or '?'}")


# ═══════════════════════════════════════════════════════════
# CRON SETUP
# ═══════════════════════════════════════════════════════════

def setup_cron():
    script = Path(__file__).resolve()
    py     = sys.executable
    log    = LOGS_DIR / "collector.log"

    daily  = f"0 8 * * *   cd {script.parent} && {py} {script} --collect-gpu     >> {log} 2>&1"
    weekly = f"0 8 * * 1   cd {script.parent} && {py} {script} --collect-rankings >> {log} 2>&1"

    print(f"\nCron lines to add:")
    print(f"  {daily}")
    print(f"  {weekly}")
    print(f"\nSchedule:")
    print(f"  Daily  08:00 → GPU utilisation (RunPod + Vast.ai) + 7d rolling metrics")
    print(f"  Weekly 08:00 → OpenRouter LLM rankings + token volumes + app rankings\n")

    if input("Install now? [y/N] ").strip().lower() == "y":
        import subprocess
        existing = subprocess.run("crontab -l 2>/dev/null", shell=True,
                                  capture_output=True, text=True).stdout
        new_cron = existing.rstrip() + f"\n{daily}\n{weekly}\n"
        r = subprocess.run("crontab -", shell=True, input=new_cron,
                           capture_output=True, text=True)
        print("✓ Cron installed" if r.returncode == 0 else f"✗ {r.stderr}")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def run_gpu(conn):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*64}")
    print(f"  GPU Collector  —  {ts}")
    print(f"{'='*64}\n")
    print("[1/3] RunPod GraphQL...")
    collect_runpod(conn)
    print("\n[2/3] Vast.ai REST API...")
    collect_vastai(conn)
    print("\n[3/3] Computing 7-day rolling metrics...")
    compute_gpu_weekly_metrics(conn)
    print(f"\n✓ GPU collection complete  —  {DB_PATH}")

def run_rankings(conn):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*64}")
    print(f"  OpenRouter Rankings Collector  —  {ts}")
    print(f"{'='*64}\n")
    collect_rankings(conn)
    print(f"\n✓ Rankings collection complete  —  {DB_PATH}")

def run_all(conn):
    run_gpu(conn)
    run_rankings(conn)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="AI Monitor — Unified Collector")
    p.add_argument("--collect-gpu",      action="store_true", help="RunPod + Vast.ai (daily)")
    p.add_argument("--collect-rankings", action="store_true", help="OpenRouter (weekly)")
    p.add_argument("--collect-all",      action="store_true", help="Run all collectors")
    p.add_argument("--audit",            action="store_true", help="Audit DB")
    p.add_argument("--export",           action="store_true", help="Export CSVs")
    p.add_argument("--history",          type=str, choices=["gpu","rankings"], help="Print history")
    p.add_argument("--setup-cron",       action="store_true", help="Install cron jobs")
    args = p.parse_args()

    conn = get_conn()

    if   args.collect_gpu:      run_gpu(conn)
    elif args.collect_rankings: run_rankings(conn)
    elif args.collect_all:      run_all(conn)
    elif args.audit:            audit(conn)
    elif args.export:           export_all(conn)
    elif args.history:          print_history(args.history, conn)
    elif args.setup_cron:       setup_cron()
    else:
        # Default: show audit
        print("No flag provided. Options:")
        print("  --collect-gpu       RunPod + Vast.ai (run daily)")
        print("  --collect-rankings  OpenRouter (run weekly Monday)")
        print("  --collect-all       everything")
        print("  --audit             audit DB contents")
        print("  --export            export CSVs")
        print("  --history gpu|rankings")
        print("  --setup-cron        install cron jobs")
        print(f"\nDB: {DB_PATH}")
        audit(conn)
