#!/usr/bin/env python3
"""
scripts/health_check.py
-----------------------
Pre-flight validation for poly-analysis-v1.

Run automatically by run.py before starting services, or manually:

    python scripts/health_check.py

Exit codes
----------
  0  All checks passed
  1  One or more checks failed
"""

import asyncio
import importlib
import os
import sqlite3
import sys

# ----------------------------------------------------------------
# Ensure project root is on sys.path so local imports work
# ----------------------------------------------------------------
_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ----------------------------------------------------------------
# Result tracking
# ----------------------------------------------------------------
_passed: list[str] = []
_failed: list[str] = []


def _report(name: str, ok: bool, detail: str = ""):
    symbol = "✅" if ok else "❌"
    line   = f"{symbol} {name}"
    if detail:
        line += f": {detail}"
    print(line)
    (_passed if ok else _failed).append(name)


# ================================================================
# Check 1 — Python dependencies
# ================================================================
def check_dependencies():
    packages = {
        "flask":      "flask",
        "websockets": "websockets",
        "requests":   "requests",
        "dotenv":     "dotenv",
        "colorama":   "colorama",
    }
    missing = []
    for label, module in packages.items():
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(label)

    if missing:
        _report("Dependencies", False, f"Missing packages: {', '.join(missing)}.  Run: pip install -r requirements.txt")
    else:
        _report("Dependencies", True, f"{len(packages)} packages OK")


# ================================================================
# Helper — resolve market slug from Gamma API
# ================================================================
def _fetch_market_title(condition_id: str, data_api_url: str) -> str:
    """
    Return a human-readable title for the given condition ID.

    Uses data-api.polymarket.com/trades (fetches 1 trade) because that
    endpoint is correctly filtered by market and embeds title + slug in
    every record.  Falls back to slug if title is absent.

    Returns an empty string on any failure (non-fatal).
    """
    if not condition_id:
        return ""
    try:
        import requests
        resp = requests.get(
            f"{data_api_url}/trades",
            params={"market": condition_id, "limit": 1, "takerOnly": "false"},
            timeout=8,
        )
        if resp.ok:
            trades = resp.json()
            if isinstance(trades, list) and trades:
                t = trades[0]
                return t.get("title") or t.get("slug") or ""
    except Exception:
        pass
    return ""


# ================================================================
# Check 2 — Configuration / environment variables
# ================================================================
def check_config():
    """Returns the Config object if valid, else None."""
    try:
        from dotenv import load_dotenv
        load_dotenv()

        from conf.config import Config
        cfg = Config.from_env()
        errors = cfg.validate()

        if errors:
            for e in errors:
                _report("Configuration", False, e)
            return None

        title = _fetch_market_title(cfg.market_id, cfg.data_api_url)
        market_label = f"{cfg.market_id!r}"
        if title:
            market_label += f"  ({title})"

        _report(
            "Configuration",
            True,
            f"MARKET_ID={market_label}  FETCH_INTERVAL={cfg.fetch_interval}s  WHALE=${cfg.whale_threshold:,.0f}",
        )
        return cfg

    except Exception as exc:
        _report("Configuration", False, str(exc))
        return None


# ================================================================
# Check 3 — SQLite database
# ================================================================
def check_database(cfg):
    from dotenv import load_dotenv
    load_dotenv()

    db_path = (cfg.db_path if cfg else None) or os.getenv("DB_PATH", "output/trades.db")
    db_dir  = os.path.dirname(db_path)

    try:
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Basic connectivity
        conn = sqlite3.connect(db_path)
        conn.execute("SELECT 1")
        conn.close()

        # Schema check / creation
        from db import Database
        db = Database(db_path)
        if db.verify_schema():
            _report("SQLite Database", True, f"Schema OK — {db_path}")
        else:
            _report("SQLite Database", False, "Tables missing after schema creation attempt")

    except Exception as exc:
        _report("SQLite Database", False, str(exc))


# ================================================================
# Check 4 — File system write permissions
# ================================================================
def check_filesystem():
    from dotenv import load_dotenv
    load_dotenv()

    dirs = [
        os.getenv("OUTPUT_DIR", "output"),
        os.getenv("LOGS_DIR",   "logs"),
    ]
    failures = []
    for d in dirs:
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".write_probe")
            with open(probe, "w") as fh:
                fh.write("ok")
            os.remove(probe)
        except OSError as exc:
            failures.append(f"{d}: {exc}")

    if failures:
        _report("File System", False, " | ".join(failures))
    else:
        _report("File System", True, f"Write OK — {', '.join(dirs)}")


# ================================================================
# Check 5 — Polymarket REST API
# ================================================================
def check_rest_api():
    try:
        import requests
        from dotenv import load_dotenv
        load_dotenv()

        base = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        resp = requests.get(f"{base}/ok", timeout=10)
        resp.raise_for_status()
        _report("Polymarket REST API", True, f"HTTP {resp.status_code} from {base}/ok")

    except Exception as exc:
        _report("Polymarket REST API", False, str(exc))


# ================================================================
# Check 6 — Polymarket WebSocket
# ================================================================
async def _ws_test(url: str) -> None:
    import websockets
    async with websockets.connect(url, open_timeout=10, close_timeout=5) as ws:
        await asyncio.sleep(0.5)   # stay connected briefly
        # Graceful close handled by context manager


def check_websocket():
    from dotenv import load_dotenv
    load_dotenv()

    ws_url = os.getenv("WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
    try:
        asyncio.run(_ws_test(ws_url))
        _report("Polymarket WebSocket", True, f"Connected to {ws_url}")
    except Exception as exc:
        _report("Polymarket WebSocket", False, str(exc))


# ================================================================
# Entry point
# ================================================================
def main():
    print("─" * 52)
    print("  Pre-flight Health Check — poly-analysis-v1")
    print("─" * 52)

    check_dependencies()
    cfg = check_config()
    check_database(cfg)
    check_filesystem()
    check_rest_api()
    check_websocket()

    print("─" * 52)
    total = len(_passed) + len(_failed)
    print(f"  {len(_passed)}/{total} checks passed", end="")
    if _failed:
        print(f"  |  Failed: {', '.join(_failed)}")
    else:
        print("  — all good!")
    print("─" * 52)

    sys.exit(0 if not _failed else 1)


if __name__ == "__main__":
    main()
