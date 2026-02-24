# Poly Analysis v1

Modular Polymarket trade tracker — monitors a single market, stores trades in SQLite, and surfaces a real-time web dashboard.

---

## Project Structure

```
poly_analysis_v1/
├── app/
│   ├── app.py                  # Flask routes
│   ├── templates/index.html    # Dashboard HTML
│   └── static/
│       ├── css/style.css
│       └── js/main.js
├── conf/
│   └── config.py               # Centralised config (loads from .env)
├── services/
│   ├── ingestion.py            # REST polling + WebSocket monitor
│   └── analysis.py             # Trade analysis / filtering
├── scripts/
│   └── health_check.py         # Pre-flight validation
├── output/                     # SQLite DB + CSV exports
├── logs/                       # Rotating app logs
├── db.py                       # SQLite storage layer
├── run.py                      # Entry point
├── requirements.txt
└── .env.template
```

---

## Quick Start

### 1. Create conda environment

```bash
conda create -n polymarket_v1 python=3.12 -y
conda activate popolymarket_v1
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.template .env
```

Edit `.env` and set at minimum:

```
MARKET_ID=0xYourConditionIdHere
```

To find a market's condition ID:

```bash
# Search by keyword
curl "https://gamma-api.polymarket.com/markets?q=bitcoin&limit=5" | python -m json.tool

# example: Get market details from slug
curl "https://gamma-api.polymarket.com/events?slug=khamenei-out-as-supreme-leader-of-iran-by-march-31"

# result

#[{"id":"102773","ticker":"khamenei-out-as-supreme-leader-of-iran-by-march-31","slug":"khamenei-out-as-supreme-leader-of-iran-by-march-31","title":"Khamenei out as Supreme Leader of Iran by March 31?","description":"This market will resolve to \"Yes\" if Iran's Supreme Leader, Ali Khamenei, is removed from power for any length of time between this market's creation and the specified date ...,"createdAt":"2025-12-11T22:53:00.158873Z","updatedAt":"2026-02-21T20:07:25.357727Z","competitive":0.9296920395119117,"volume24hr":221678.2695279999,"volume1wk":3005377.3630319997,"volume1mo":8026950.584189965,"volume1yr":14996969.72654293,"enableOrderBook":true,"liquidityClob":489909.3444,"commentCount":0,"markets":[{"id":"916732","question":"Khamenei out as Supreme Leader of Iran by March 31?","conditionId":"0x70909f0ba8256a89c301da58812ae47203df54957a07c7f8b10235e877ad63c2","slug":"khamenei-out-as-supreme-leader-of-iran-by-march-31"...


# The conditionId field is what you need
```

### 3. Run

```bash
python run.py
```

`run.py` executes the health check first; only proceeds if all checks pass.

Open **http://localhost:5000** in your browser.

---

## Configuration Reference

All options can be set in `.env` or as real environment variables.

| Variable | Default | Description |
|---|---|---|
| `MARKET_ID` | *(required)* | Polymarket condition ID to monitor |
| `FETCH_INTERVAL` | `60` | Seconds between REST API polls |
| `WHALE_THRESHOLD` | `1000` | USDC trade size to flag as "whale" |
| `DB_PATH` | `output/trades.db` | SQLite file path |
| `OUTPUT_DIR` | `output` | Directory for CSV exports |
| `LOGS_DIR` | `logs` | Directory for log files |
| `FLASK_HOST` | `0.0.0.0` | Flask bind address |
| `FLASK_PORT` | `5000` | Flask port |
| `FLASK_DEBUG` | `false` | Flask debug mode |
| `CLOB_API_URL` | `https://clob.polymarket.com` | CLOB REST base |
| `DATA_API_URL` | `https://data-api.polymarket.com` | Trade data base |
| `GAMMA_API_URL` | `https://gamma-api.polymarket.com` | Profile data base |
| `WS_URL` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | WebSocket URL |
| `POLY_API_KEY` | *(optional)* | API key for authenticated endpoints |
| `POLY_API_SECRET` | *(optional)* | API secret |
| `POLY_API_PASSPHRASE` | *(optional)* | API passphrase |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Dashboard UI |
| `GET` | `/api/trades` | Recent trades (JSON) |
| `GET` | `/api/stats` | Aggregate stats |
| `GET` | `/api/traders` | Top traders by volume |
| `GET` | `/api/whales` | Trades above whale threshold |
| `GET` | `/api/volume` | Volume by outcome |
| `GET` | `/api/export/csv` | Download trades as CSV |
| `GET` | `/api/status` | Service health info |

Common query parameters for `/api/trades`, `/api/whales`, `/api/export/csv`:

| Param | Description |
|---|---|
| `market_id` | Override configured market |
| `limit` | Max results (default 100) |
| `min_amount` | Minimum USDC trade size |
| `wallet` | Filter by wallet address |

---

## Health Check

Run manually at any time:

```bash
python scripts/health_check.py
```

Checks performed:

| Check | What it validates |
|---|---|
| Dependencies | All pip packages importable |
| Configuration | `.env` loaded, `MARKET_ID` set |
| SQLite Database | File accessible, schema present |
| File System | `output/` and `logs/` writable |
| Polymarket REST API | `GET /ok` returns 200 |
| Polymarket WebSocket | Connect + clean disconnect |

---

## Data Model

### `trades` table

| Column | Type | Description |
|---|---|---|
| `transaction_hash` | TEXT UNIQUE | On-chain tx hash |
| `market_id` | TEXT | Condition ID |
| `token_id` | TEXT | Token (YES/NO) asset ID |
| `proxy_wallet` | TEXT | Trader's proxy wallet |
| `side` | TEXT | BUY or SELL |
| `price` | REAL | Probability price (0–1) |
| `size` | REAL | Number of shares |
| `amount` | REAL | USDC value (price × size) |
| `outcome` | TEXT | Yes / No |
| `outcome_index` | INTEGER | 0 or 1 |
| `market_title` | TEXT | Market question |
| `match_time` | INTEGER | Unix timestamp |

### `traders` table

| Column | Type | Description |
|---|---|---|
| `proxy_wallet` | TEXT PK | Wallet address |
| `name` | TEXT | Display name |
| `pseudonym` | TEXT | Pseudonym / handle |
| `profile_image` | TEXT | Avatar URL |
| `bio` | TEXT | Profile bio |
| `num_trades` | INTEGER | Trade count (updated by analysis) |
| `pnl_cumulative` | REAL | Cumulative PnL (reserved) |

---

## Extending the System

### Add a new analysis method

Edit `services/analysis.py` — add a method to `AnalysisService`.
Expose it via a new route in `app/app.py`.

### Add a new data source

Add a new method to `services/ingestion.py` (e.g. `_fetch_positions()`).
Call it from `_fetch_and_store_trades()` or schedule it independently.

### Add a new DB table

Add the `CREATE TABLE` statement inside `Database._create_schema()` in `db.py`.
Add corresponding CRUD methods.
Re-run `python scripts/health_check.py` to verify the schema.

### Multi-market support

Change `MARKET_ID` to a comma-separated list (requires minor update to
`ingestion.py` to iterate over markets per poll).

---

## Development Notes

- The ingestion service runs two daemon threads: one async (WebSocket) and one sync (REST poll).
- Flask runs on the main thread with `use_reloader=False` to avoid duplicating background threads.
- SQLite uses WAL mode and thread-local connections for safe concurrent reads/writes.
- All timestamps are stored as Unix seconds (integer) for portable sorting/filtering.
- Trades are deduplicated by `transaction_hash`; re-fetching the same trades is safe.
