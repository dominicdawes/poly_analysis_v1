"""
app/app.py
----------
Flask web application.

Routes
------
  GET  /                         — Live trades dashboard (HTML)
  GET  /wallet-dashboard         — Wallet analytics page (HTML)
  GET  /watchlist                — Watchlist page (HTML)

  GET  /api/trades               — Recent trades (JSON)
  GET  /api/stats                — Aggregate stats (JSON)
  GET  /api/traders              — Top traders (JSON)
  GET  /api/whales               — Whale trades (JSON)
  GET  /api/volume               — Volume by outcome (JSON)
  GET  /api/export/csv           — CSV download

  GET  /api/wallet/<address>     — Full wallet analytics (JSON)
  GET  /api/wallets              — Top wallets leaderboard (JSON)
  GET  /api/markets              — Tracked markets metadata (JSON)

  GET  /api/status               — Service health (JSON)

All /api/* endpoints accept optional query params:
  market_id   — override the configured market
  limit       — result limit (default 100)
  min_amount  — minimum USDC trade size
  wallet      — filter by wallet address
"""

import concurrent.futures
import io
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, render_template, request, Response

from conf.config import Config
from db import Database
from services.analysis import AnalysisService

logger = logging.getLogger(__name__)

# Service refs — set by create_app(); accessed by module-level helpers and routes
_ingestion_ref       = None
_wallet_analyzer_ref = None
_market_analyzer_ref = None
_db_ref              = None   # Database instance; used by tag persistence helpers

_GAMMA_BASE    = "https://gamma-api.polymarket.com"
_DATA_API_BASE = "https://data-api.polymarket.com"
_REQ_HEADERS   = {"User-Agent": "poly-analysis-v1/1.0", "Accept": "application/json"}

_scanner_cache: dict = {"data": [], "fetched_at": 0.0}
_SCANNER_CACHE_TTL = 300  # 5 minutes

# All-tags cache — loaded from DB at startup, refreshed by background thread
_all_tags: list        = []   # [{id, slug, label}, ...]
_tags_by_slug: dict    = {}   # slug → {id, slug, label}
_tags_fetched_at: float = 0.0  # 0 = not yet API-synced; non-zero = last API sync
_TAGS_CACHE_TTL = 86_400       # 24 hours between API re-syncs


def _sync_load_tags_from_db() -> None:
    """Synchronously populate `_all_tags` / `_tags_by_slug` from the local DB.

    Called once in `create_app()` before the background API sync thread starts.
    Leaves `_tags_fetched_at = 0` so the background thread always does a fresh
    API diff on the first run, regardless of how old the DB data is.
    """
    global _all_tags, _tags_by_slug
    if _db_ref is None:
        return
    tags = _db_ref.get_all_tags()
    if tags:
        _all_tags     = tags
        _tags_by_slug = {t["slug"]: t for t in tags}
        logger.info("Tags loaded from DB: %d tags", len(tags))


def _fetch_all_tags(force: bool = False) -> list:
    """Fetch every tag from Gamma API using offset pagination, persist new
    ones to the DB, and update the in-memory cache.

    Behaviour:
    - If `_tags_fetched_at` is non-zero and within TTL, returns cached list.
    - `_tags_fetched_at = 0` (the default after startup) always triggers an
      API sync so the background thread always runs on first call.
    - New tags are appended to the DB (INSERT OR IGNORE on slug).
    - After a successful API sync the full list is reloaded from DB so the
      in-memory cache is authoritative and sorted.
    """
    global _all_tags, _tags_by_slug, _tags_fetched_at
    now = time.time()
    if not force and _tags_fetched_at > 0 and (now - _tags_fetched_at) < _TAGS_CACHE_TTL:
        return _all_tags

    # ── Paginate /tags until empty page ──────────────────────────────
    fetched: list = []
    limit, offset = 100, 0
    while True:
        try:
            r = requests.get(
                f"{_GAMMA_BASE}/tags",
                params={"limit": limit, "offset": offset},
                headers=_REQ_HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            page = r.json()
        except Exception as exc:
            logger.warning("_fetch_all_tags offset=%d: %s", offset, exc)
            break
        if not page:
            break
        fetched.extend(page)
        if len(page) < limit:
            break
        offset += limit

    if not fetched:
        # API unavailable — keep whatever is in memory / DB
        logger.warning("_fetch_all_tags: no data returned from API")
        return _all_tags

    normalized = [
        {
            "id":    t.get("id"),
            "slug":  (t.get("slug") or "").lower().strip(),
            "label": (t.get("label") or t.get("slug") or "").strip(),
        }
        for t in fetched
        if t.get("id") and (t.get("slug") or t.get("label"))
    ]

    if _db_ref is not None:
        new_count = _db_ref.upsert_tags(normalized)
        if new_count:
            logger.info("Tags: %d new tags persisted (total API: %d)", new_count, len(normalized))
        # Reload from DB — canonical sorted order, includes pre-existing rows
        all_from_db = _db_ref.get_all_tags()
        _all_tags = all_from_db
    else:
        # No DB available — use in-memory only
        _all_tags = normalized

    _tags_by_slug    = {t["slug"]: t for t in _all_tags}
    _tags_fetched_at = time.time()
    logger.info("Tags cache: %d tags total", len(_all_tags))
    return _all_tags


def _fetch_events_by_tag_ids(tag_ids: list, active_only: bool = False) -> list:
    """Fetch events for a list of tag IDs in parallel, union-deduplicated.

    Uses ``related_tags=true`` so Polymarket returns the full cluster of
    events that the website would show for each tag.
    """
    base_params: dict = {"limit": 500, "related_tags": "true"}
    if active_only:
        base_params["active"] = "true"
        base_params["closed"] = "false"

    def _one(tid) -> list:
        r = requests.get(
            f"{_GAMMA_BASE}/events",
            params={**base_params, "tag_id": tid},
            headers=_REQ_HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("data", [])

    workers = min(len(tag_ids), 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_one, tid): tid for tid in tag_ids}
        pages: list[list] = []
        for fut in concurrent.futures.as_completed(futures):
            try:
                pages.append(fut.result())
            except Exception as exc:
                logger.warning("_fetch_events_by_tag_ids tag_id=%s: %s", futures[fut], exc)

    seen: set = set()
    combined: list = []
    for page in pages:
        for e in page:
            eid = e.get("id") or e.get("slug")
            if eid and eid not in seen:
                seen.add(eid)
                combined.append(e)
    return combined


def _parse_polymarket_slug(raw: str):
    """Return the event slug from a Polymarket URL or a bare slug string."""
    s = raw.strip()
    if not s:
        return None
    # If it looks like a URL, parse the path
    if "polymarket.com" in s or s.startswith("http"):
        if "://" not in s:
            s = "https://" + s
        try:
            parts = [p for p in urlparse(s).path.split("/") if p]
            # path: /event/<event-slug>[/<market-slug>]
            if parts and parts[0] == "event" and len(parts) >= 2:
                return parts[1]
        except Exception:
            pass
        return None
    # Treat as bare slug (no slash, no spaces)
    if "/" not in s and " " not in s:
        return s
    return None


def _normalize_trade_record(raw: dict, fallback_market_id: str):
    """Normalise a raw data-api trade dict to the DB schema (mirrors IngestionService)."""
    try:
        wallet = raw.get("proxyWallet") or raw.get("maker_address") or raw.get("owner")
        if not wallet:
            return None
        price  = float(raw.get("price") or 0)
        size   = float(raw.get("size")  or 0)
        amount = round(price * size, 6)

        ts = raw.get("timestamp")
        if ts is None:
            match_time = int(time.time())
        elif isinstance(ts, (int, float)):
            match_time = int(ts)
        elif isinstance(ts, str):
            try:
                match_time = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
            except ValueError:
                match_time = int(time.time())
        else:
            match_time = int(time.time())

        return {
            "transaction_hash": raw.get("transactionHash") or raw.get("transaction_hash"),
            "market_id":        raw.get("conditionId") or raw.get("market") or fallback_market_id,
            "token_id":         raw.get("asset") or raw.get("asset_id"),
            "proxy_wallet":     wallet,
            "side":             str(raw.get("side", "")).upper(),
            "price":            price,
            "size":             size,
            "amount":           amount,
            "outcome":          raw.get("outcome"),
            "outcome_index":    raw.get("outcomeIndex"),
            "market_title":     raw.get("title"),
            "market_slug":      raw.get("slug"),
            "market_icon":      raw.get("icon"),
            "match_time":       match_time,
        }
    except (KeyError, ValueError, TypeError):
        return None


def _normalize_trader_record(raw: dict, now_iso: str):
    """Extract trader profile from a raw trade dict (mirrors IngestionService)."""
    wallet = raw.get("proxyWallet") or raw.get("maker_address") or raw.get("owner")
    if not wallet:
        return None
    return {
        "proxy_wallet":   wallet,
        "name":           raw.get("name"),
        "pseudonym":      raw.get("pseudonym"),
        "profile_image":  raw.get("profileImageOptimized") or raw.get("profileImage"),
        "bio":            raw.get("bio"),
        "num_trades":     0,
        "pnl_cumulative": 0.0,
        "last_updated":   now_iso,
    }


def create_app(
    config: Config,
    db: Database,
    analysis: AnalysisService,
    ingestion=None,
    wallet_analyzer=None,
    market_analyzer=None,
) -> Flask:
    """
    Factory function.  Creates and configures the Flask app.

    Parameters
    ----------
    config           : loaded Config object
    db               : Database instance
    analysis         : AnalysisService instance
    ingestion        : IngestionService instance (optional)
    wallet_analyzer  : WalletAnalyzer instance (optional)
    market_analyzer  : MarketAnalyzer instance (optional)
    """
    global _ingestion_ref, _wallet_analyzer_ref, _market_analyzer_ref, _db_ref
    _ingestion_ref       = ingestion
    _wallet_analyzer_ref = wallet_analyzer
    _market_analyzer_ref = market_analyzer
    _db_ref              = db

    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    static_dir = os.path.join(os.path.dirname(__file__), "static")

    app = Flask(
        __name__,
        template_folder=template_dir,
        static_folder=static_dir,
    )
    app.config["JSON_SORT_KEYS"] = False

    # Ensure default watchlists exist on every startup
    db.ensure_default_watchlists()

    # Immediately load any already-persisted tags from the local DB so the
    # scanner combobox is populated on the very first request (day 2+).
    # _tags_fetched_at stays 0, so the background thread will always do a
    # fresh API diff-and-append on startup regardless of DB state.
    _sync_load_tags_from_db()

    # Background thread: paginate /tags from Gamma API, persist new ones,
    # then update the in-memory cache with the full DB list.
    threading.Thread(target=_fetch_all_tags, daemon=True, name="tag-prefetch").start()

    # ----------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------

    def _market_id() -> str:
        """Use query-param market_id if provided, else fall back to config."""
        return request.args.get("market_id", "") or config.market_id or ""

    def _limit(default: int = 100, cap: int = 1_000) -> int:
        try:
            return min(int(request.args.get("limit", default)), cap)
        except ValueError:
            return default

    def _min_amount() -> float | None:
        val = request.args.get("min_amount")
        try:
            return float(val) if val is not None else None
        except ValueError:
            return None

    def _fetch_scanner_data(force_refresh: bool = False) -> list:
        """Fetch events from Gamma API (with in-memory TTL cache).

        Events are the top-level containers; each event embeds a `markets`
        array of binary YES/NO child markets plus a `tags` array for
        multi-label categorisation.
        """
        now = time.time()
        if (
            not force_refresh
            and _scanner_cache["data"]
            and (now - _scanner_cache["fetched_at"]) < _SCANNER_CACHE_TTL
        ):
            return _scanner_cache["data"]

        def _fetch_page(offset: int) -> list:
            r = requests.get(
                f"{_GAMMA_BASE}/events",
                params={"limit": 500, "offset": offset},
                headers=_REQ_HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            payload = r.json()
            return payload if isinstance(payload, list) else payload.get("data", [])

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(_fetch_page, 0)
            f2 = pool.submit(_fetch_page, 500)
            page1 = f1.result()
            page2 = f2.result()

        combined = page1 + page2
        _scanner_cache["data"] = combined
        _scanner_cache["fetched_at"] = time.time()
        return combined

    def _filter_events(
        events: list,
        *,
        q: str,
        event_status: str,
        tags: list,
        exclude_tags: list,
        market_status: str,
        min_market_vol,
        max_market_vol,
        min_market_liq,
        max_market_liq,
        max_spread,
        sort_by: str,
        sort_dir: str,
        page: int,
        page_size: int,
    ):
        """
        Two-phase filter:
          Phase 1 — event-level: text search, event status, include tags,
                    exclude tags.  Exclude is always evaluated in Python
                    regardless of how events were fetched, since the Gamma
                    API has no exclude_tag_id parameter.
          Phase 2 — market-level: each event's markets are filtered by
                    volume, liquidity, spread, status.
                    Events with zero surviving markets are dropped.
        """
        result = events

        # ── Phase 1: event-level ──────────────────────────────────────
        if q:
            ql = q.lower()
            def _matches_q(e):
                if ql in (e.get("title") or "").lower():
                    return True
                for m in (e.get("markets") or []):
                    if ql in (m.get("question") or "").lower():
                        return True
                return False
            result = [e for e in result if _matches_q(e)]

        if event_status == "active":
            result = [e for e in result if e.get("active") and not e.get("closed")]
        elif event_status == "closed":
            result = [e for e in result if e.get("closed") or not e.get("active")]

        if tags:
            tags_set = {t.lower() for t in tags}
            def _has_tag(e):
                return any(
                    (t.get("slug") or "").lower() in tags_set
                    or (t.get("label") or "").lower() in tags_set
                    for t in (e.get("tags") or [])
                )
            result = [e for e in result if _has_tag(e)]

        # Exclude filter — always Python-side (no API support for this)
        if exclude_tags:
            excl_set = {t.lower() for t in exclude_tags}
            def _lacks_excl_tag(e):
                return not any(
                    (t.get("slug") or "").lower() in excl_set
                    or (t.get("label") or "").lower() in excl_set
                    for t in (e.get("tags") or [])
                )
            result = [e for e in result if _lacks_excl_tag(e)]

        # ── Phase 2: market-level ─────────────────────────────────────
        has_mkt_filter = any(x is not None for x in [
            min_market_vol, max_market_vol, min_market_liq, max_market_liq, max_spread
        ]) or market_status != "all"

        if has_mkt_filter:
            narrowed = []
            for e in result:
                kept = []
                for m in (e.get("markets") or []):
                    if market_status == "active" and not (m.get("active") and not m.get("closed")):
                        continue
                    if market_status == "closed" and not (m.get("closed") or not m.get("active")):
                        continue
                    mvol = float(m.get("volume") or 0)
                    if min_market_vol is not None and mvol < min_market_vol:
                        continue
                    if max_market_vol is not None and mvol > max_market_vol:
                        continue
                    mliq = float(m.get("liquidity") or 0)
                    if min_market_liq is not None and mliq < min_market_liq:
                        continue
                    if max_market_liq is not None and mliq > max_market_liq:
                        continue
                    if max_spread is not None:
                        try:
                            prices = json.loads(m.get("outcomePrices") or "[]")
                            spread_pct = (1.0 - sum(float(p) for p in prices)) * 100
                            if spread_pct > max_spread:
                                continue
                        except Exception:
                            pass
                    kept.append(m)
                if kept:
                    narrowed.append({**e, "markets": kept})
            result = narrowed

        # ── Sort (event-level fields) ──────────────────────────────────
        reverse = sort_dir != "asc"
        key_map = {
            "volume":     lambda e: float(e.get("volume") or 0),
            "volume24hr": lambda e: float(e.get("volume24hr") or 0),
            "liquidity":  lambda e: float(e.get("liquidity") or 0),
            "end_date":   lambda e: e.get("endDate") or "",
        }
        result.sort(key=key_map.get(sort_by, key_map["volume"]), reverse=reverse)

        total = len(result)
        start = (page - 1) * page_size
        return result[start : start + page_size], total

    # ----------------------------------------------------------------
    # Pages
    # ----------------------------------------------------------------

    @app.route("/")
    def index():
        market_id = request.args.get("market_id") or config.market_id
        return render_template(
            "index.html",
            market_id=market_id,
            whale_threshold=config.whale_threshold,
            fetch_interval=config.fetch_interval,
        )

    # ----------------------------------------------------------------
    # API — trades
    # ----------------------------------------------------------------

    @app.route("/api/trades")
    def api_trades():
        trades = db.get_recent_trades(
            limit=_limit(),
            market_id=_market_id() or None,
            min_amount=_min_amount(),
            wallet=request.args.get("wallet") or None,
        )
        # Annotate each trade with derived fields
        trades = [analysis.classify_trade(t) for t in trades]
        return jsonify(trades)

    # ----------------------------------------------------------------
    # API — stats
    # ----------------------------------------------------------------

    @app.route("/api/stats")
    def api_stats():
        summary = analysis.get_summary(_market_id() or None)
        return jsonify(summary)

    # ----------------------------------------------------------------
    # API — traders
    # ----------------------------------------------------------------

    @app.route("/api/traders")
    def api_traders():
        traders = analysis.get_top_traders(
            market_id=_market_id() or None,
            limit=_limit(default=20, cap=100),
        )
        return jsonify(traders)

    # ----------------------------------------------------------------
    # API — whales
    # ----------------------------------------------------------------

    @app.route("/api/whales")
    def api_whales():
        trades = analysis.get_whale_trades(
            market_id=_market_id() or None,
            limit=_limit(default=50, cap=500),
        )
        trades = [analysis.classify_trade(t) for t in trades]
        return jsonify(trades)

    # ----------------------------------------------------------------
    # API — volume breakdown
    # ----------------------------------------------------------------

    @app.route("/api/volume")
    def api_volume():
        summary = analysis.get_summary(_market_id() or None)
        return jsonify(summary.get("volume_by_outcome", []))

    # ----------------------------------------------------------------
    # API — CSV export
    # ----------------------------------------------------------------

    @app.route("/api/export/csv")
    def api_export_csv():
        market_id = _market_id() or None
        min_amount = _min_amount()
        limit = _limit(default=10_000, cap=100_000)

        csv_bytes = db.export_csv_bytes(
            market_id=market_id,
            min_amount=min_amount,
            limit=limit,
        )

        if not csv_bytes:
            return jsonify({"error": "No trades match the filter criteria"}), 404

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        mid = market_id or "all"
        filename = f"trades_{mid[:16]}_{ts}.csv"

        return Response(
            csv_bytes,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(csv_bytes)),
            },
        )

    # ----------------------------------------------------------------
    # Page — wallet dashboard
    # ----------------------------------------------------------------

    @app.route("/wallet-dashboard")
    def wallet_dashboard():
        address = request.args.get("address", "")
        return render_template(
            "wallet_dashboard.html",
            address=address,
            whale_threshold=config.whale_threshold,
        )

    @app.route("/watchlist")
    def watchlist_page():
        return render_template("watchlist.html", whale_threshold=config.whale_threshold)

    @app.route("/scanner")
    def scanner_page():
        return render_template("scanner.html", whale_threshold=config.whale_threshold)

    # ----------------------------------------------------------------
    # API — single wallet analytics
    # ----------------------------------------------------------------

    @app.route("/api/wallet/<address>")
    def api_wallet(address: str):
        wallet = db.get_wallet(address)
        if wallet is None:
            return jsonify({
                "error": "No data found for this wallet. "
                         "It may not have any trades yet or the analyzer hasn't run."
            }), 404

        positions = db.get_positions_for_wallet(address)
        trades    = db.get_recent_trades(
            limit=_limit(default=50, cap=200),
            wallet=address,
        )
        trades = [analysis.classify_trade(t) for t in trades]

        return jsonify({
            "wallet":    wallet,
            "positions": positions,
            "trades":    trades,
        })

    # ----------------------------------------------------------------
    # API — wallet lifetime stats (sourced live from Polymarket APIs)
    # ----------------------------------------------------------------

    @app.route("/api/wallet-lifetime/<address>")
    def api_wallet_lifetime(address: str):
        """
        Fetch lifetime stats directly from Polymarket public APIs:
          - createdAt  : Gamma /public-profile  → account creation timestamp
          - total_traded : data-api /traded     → lifetime trade count
          - realized_pnl : data-api /closed-positions → sum of realizedPnl
        All three calls run in parallel; partial failures return null for
        that field so the frontend can fall back gracefully.
        """
        def _fetch_profile():
            r = requests.get(
                f"{_GAMMA_BASE}/public-profile",
                params={"address": address},
                headers=_REQ_HEADERS,
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()
            if not isinstance(d, dict) or not d:
                return None
            raw = d.get("createdAt")
            if raw:
                try:
                    return int(
                        datetime.fromisoformat(
                            raw.replace("Z", "+00:00")
                        ).timestamp()
                    )
                except ValueError:
                    pass
            return None

        def _fetch_traded():
            r = requests.get(
                f"{_DATA_API_BASE}/traded",
                params={"user": address},
                headers=_REQ_HEADERS,
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()
            return d.get("traded") if isinstance(d, dict) else None

        def _fetch_pnl():
            r = requests.get(
                f"{_DATA_API_BASE}/closed-positions",
                params={"user": address, "limit": 500},
                headers=_REQ_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            positions = r.json()
            if isinstance(positions, list):
                return round(
                    sum(float(p.get("realizedPnl") or 0) for p in positions), 4
                )
            return None

        result = {
            "address":       address,
            "created_at_ts": None,
            "total_traded":  None,
            "realized_pnl":  None,
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                "created_at_ts": pool.submit(_fetch_profile),
                "total_traded":  pool.submit(_fetch_traded),
                "realized_pnl":  pool.submit(_fetch_pnl),
            }
            for key, fut in futures.items():
                try:
                    result[key] = fut.result(timeout=18)
                except Exception as exc:
                    logger.warning("wallet-lifetime %s %s: %s", address, key, exc)

        return jsonify(result)

    # ----------------------------------------------------------------
    # API — wallet PnL history (for chart)
    # Fetches closed positions from Polymarket data-api, returns a
    # time-series of cumulative realized PnL sorted oldest-first.
    # Each point: { ts: <unix seconds>, pnl: <float>, cum_pnl: <float> }
    # ----------------------------------------------------------------

    @app.route("/api/wallet-pnl-history/<address>")
    def api_wallet_pnl_history(address: str):
        if not address:
            return jsonify([])

        try:
            r = requests.get(
                f"{_DATA_API_BASE}/closed-positions",
                params={"user": address, "limit": 500},
                headers=_REQ_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            positions = r.json()
        except Exception as exc:
            logger.warning("wallet-pnl-history fetch failed for %s: %s", address, exc)
            return jsonify([])

        if not isinstance(positions, list) or not positions:
            return jsonify([])

        # Keep only entries with both a timestamp and a realizedPnl value
        points = []
        for p in positions:
            ts = p.get("timestamp")
            pnl = p.get("realizedPnl")
            if ts is None or pnl is None:
                continue
            try:
                points.append({"ts": int(ts), "pnl": float(pnl)})
            except (ValueError, TypeError):
                continue

        # Sort oldest-first then compute cumulative PnL
        points.sort(key=lambda x: x["ts"])
        cum = 0.0
        result = []
        for pt in points:
            cum += pt["pnl"]
            result.append({"ts": pt["ts"], "pnl": round(pt["pnl"], 4), "cum_pnl": round(cum, 4)})

        return jsonify(result)

    # ----------------------------------------------------------------
    # API — wallet leaderboard
    # ----------------------------------------------------------------

    @app.route("/api/wallets")
    def api_wallets():
        _valid_order = {"total_volume", "realized_pnl", "total_trades", "last_seen"}
        order_by  = request.args.get("order_by", "total_volume")
        if order_by not in _valid_order:
            order_by = "total_volume"

        wallets = db.get_wallets(
            limit=_limit(default=50, cap=200),
            order_by=order_by,
            min_volume=_min_amount(),
        )
        return jsonify(wallets)

    # ----------------------------------------------------------------
    # API — watchlists
    # NOTE: /api/watchlists/status must be registered BEFORE /<int:wl_id>
    # so Flask matches it before trying to coerce "status" to an integer.
    # ----------------------------------------------------------------

    @app.route("/api/watchlists/status")
    def api_watchlist_status():
        type_      = request.args.get("type", "")
        identifier = request.args.get("identifier", "")
        if not type_ or not identifier:
            return jsonify({"error": "type and identifier are required"}), 400
        return jsonify(db.get_watchlist_status(type_, identifier))

    @app.route("/api/watchlists", methods=["GET"])
    def api_watchlists_list():
        type_ = request.args.get("type") or None
        return jsonify(db.get_watchlists(type_=type_))

    @app.route("/api/watchlists", methods=["POST"])
    def api_watchlists_create():
        body  = request.get_json(silent=True) or {}
        name  = (body.get("name") or "").strip()
        type_ = (body.get("type") or "").strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        if type_ not in ("wallet", "market"):
            return jsonify({"error": "type must be 'wallet' or 'market'"}), 400
        result = db.create_watchlist(name, type_)
        if result is None:
            return jsonify({"error": "Failed to create watchlist"}), 500
        return jsonify(result), 201

    @app.route("/api/watchlists/<int:wl_id>", methods=["PUT"])
    def api_watchlists_rename(wl_id: int):
        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        if db.rename_watchlist(wl_id, name):
            return jsonify({"ok": True})
        return jsonify({"error": "Watchlist not found"}), 404

    @app.route("/api/watchlists/<int:wl_id>", methods=["DELETE"])
    def api_watchlists_delete(wl_id: int):
        result = db.delete_watchlist(wl_id)
        if result:
            return jsonify({"ok": True})
        # Check if it exists at all or is a default
        wls = db.get_watchlists()
        ids = [w["id"] for w in wls]
        if wl_id not in ids:
            return jsonify({"error": "Watchlist not found"}), 404
        return jsonify({"error": "Cannot delete a default watchlist"}), 409

    @app.route("/api/watchlists/<int:wl_id>/items", methods=["GET"])
    def api_watchlist_items_list(wl_id: int):
        return jsonify(db.get_watchlist_items(wl_id))

    @app.route("/api/watchlists/<int:wl_id>/items", methods=["POST"])
    def api_watchlist_items_add(wl_id: int):
        body       = request.get_json(silent=True) or {}
        identifier = (body.get("identifier") or "").strip()
        label      = (body.get("label") or "").strip() or None
        if not identifier:
            return jsonify({"error": "identifier is required"}), 400
        result = db.add_watchlist_item(wl_id, identifier, label)
        if result is None:
            return jsonify({"error": "Failed to add item"}), 500
        return jsonify(result), 201

    @app.route("/api/watchlists/<int:wl_id>/items/<int:item_id>", methods=["DELETE"])
    def api_watchlist_items_remove(wl_id: int, item_id: int):
        if db.remove_watchlist_item(item_id):
            return jsonify({"ok": True})
        return jsonify({"error": "Item not found"}), 404

    @app.route("/api/watchlists/move-item", methods=["PUT"])
    def api_watchlist_move_item():
        body       = request.get_json(silent=True) or {}
        item_id    = body.get("item_id")
        to_wl_id   = body.get("to_watchlist_id")
        if item_id is None or to_wl_id is None:
            return jsonify({"error": "item_id and to_watchlist_id are required"}), 400
        if db.move_watchlist_item(int(item_id), int(to_wl_id)):
            return jsonify({"ok": True})
        return jsonify({"error": "Move failed — item may already be in target list"}), 409

    @app.route("/api/watchlists/<int:wl_id>/enriched", methods=["GET"])
    def api_watchlist_enriched(wl_id: int):
        return jsonify(db.get_watchlist_enriched(wl_id))

    # ----------------------------------------------------------------
    # API — markets metadata
    # ----------------------------------------------------------------

    @app.route("/api/markets")
    def api_markets():
        markets = db.get_markets(limit=_limit(default=50, cap=200))
        return jsonify(markets)

    # ----------------------------------------------------------------
    # API — market scanner (Gamma API with in-memory cache)
    # NOTE: specific sub-paths (/tags, /categories) must be registered
    # BEFORE the plain /api/scanner route to avoid routing conflicts.
    # ----------------------------------------------------------------

    @app.route("/api/scanner/tags")
    def api_scanner_tags():
        """Return every Polymarket tag (id, slug, label) sorted by label.

        The list is fetched once at startup and cached for 24 h.  If the
        background prefetch hasn't finished yet this call will block briefly
        (first cold hit only).
        """
        try:
            tags = _fetch_all_tags()
            return jsonify(sorted(tags, key=lambda t: t.get("label", "").lower()))
        except Exception as exc:
            logger.error("scanner/tags: %s", exc)
            return jsonify([])

    @app.route("/api/scanner/categories")
    def api_scanner_categories():
        # Legacy endpoint — falls back to extracting tags from event cache
        try:
            events = _fetch_scanner_data()
        except Exception as exc:
            logger.error("scanner/categories: %s", exc)
            return jsonify([])
        cats: dict[str, str] = {}
        for e in events:
            for t in (e.get("tags") or []):
                slug  = (t.get("slug") or "").strip()
                label = (t.get("label") or slug).strip()
                if slug:
                    cats[slug] = label
        return jsonify(
            [{"slug": k, "label": v} for k, v in sorted(cats.items(), key=lambda x: x[1])]
        )

    @app.route("/api/scanner")
    def api_scanner():
        force = request.args.get("refresh", "").lower() == "true"

        q             = (request.args.get("q") or "").strip()
        event_status  = request.args.get("event_status", "all")
        market_status = request.args.get("market_status", "all")
        tag_slugs     = [
            t.strip() for t in (request.args.get("tags") or "").split(",") if t.strip()
        ]
        exclude_slugs = [
            t.strip() for t in (request.args.get("exclude_tags") or "").split(",") if t.strip()
        ]
        sort_by  = request.args.get("sort_by", "volume")
        sort_dir = request.args.get("sort_dir", "desc")
        try:
            page = max(1, int(request.args.get("page", 1) or 1))
        except ValueError:
            page = 1
        try:
            page_size = min(100, max(1, int(request.args.get("page_size", 25) or 25)))
        except ValueError:
            page_size = 25

        def _flt(name):
            v = request.args.get(name)
            try:
                return float(v) if v else None
            except ValueError:
                return None

        # ── Fetch strategy ────────────────────────────────────────────
        # When specific tags are selected we resolve their IDs and query
        # the Gamma /events?tag_id=... endpoint directly — much faster
        # than pulling 1 000 events and filtering.
        # Custom slugs not in our tag cache fall back to the bulk fetch.
        effective_tags: list[str] = []  # passed to _filter_events Phase-1
        events: list = []

        if tag_slugs:
            known_ids     = [_tags_by_slug[s]["id"] for s in tag_slugs if s in _tags_by_slug]
            unknown_slugs = [s for s in tag_slugs if s not in _tags_by_slug]

            try:
                if known_ids:
                    events = _fetch_events_by_tag_ids(
                        known_ids,
                        active_only=(event_status == "active"),
                    )
                    # For unknown custom slugs, union results from bulk fetch
                    if unknown_slugs:
                        bulk = _fetch_scanner_data()
                        seen = {e.get("id") for e in events}
                        for e in bulk:
                            eid = e.get("id")
                            if eid in seen:
                                continue
                            if any(
                                (t.get("slug") or "").lower() in unknown_slugs
                                or (t.get("label") or "").lower() in unknown_slugs
                                for t in (e.get("tags") or [])
                            ):
                                events.append(e)
                                seen.add(eid)
                    # API already filtered by tag; don't re-apply in Phase 1
                    effective_tags = []
                else:
                    # All slugs are unknown → bulk fetch + Python tag filter
                    events = _fetch_scanner_data(force_refresh=force)
                    effective_tags = tag_slugs
            except Exception as exc:
                logger.error("scanner (tag fetch): %s", exc)
                return jsonify({"error": "Could not fetch events from Polymarket"}), 502
        else:
            try:
                events = _fetch_scanner_data(force_refresh=force)
            except Exception as exc:
                logger.error("scanner: %s", exc)
                return jsonify({"error": "Could not fetch events from Polymarket"}), 502

        # ── Filter + paginate ─────────────────────────────────────────
        page_events, total = _filter_events(
            events,
            q=q,
            event_status=event_status,
            tags=effective_tags,
            exclude_tags=exclude_slugs,  # always applied in Python
            market_status=market_status,
            min_market_vol=_flt("min_market_vol"),
            max_market_vol=_flt("max_market_vol"),
            min_market_liq=_flt("min_market_liq"),
            max_market_liq=_flt("max_market_liq"),
            max_spread=_flt("max_spread"),
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        pages     = max(1, (total + page_size - 1) // page_size)
        cache_age = int(time.time() - (_scanner_cache.get("fetched_at") or 0))

        return jsonify({
            "markets":     page_events,   # key kept as "markets" for JS compat
            "total":       total,
            "page":        page,
            "pages":       pages,
            "page_size":   page_size,
            "cached":      not force,
            "cache_age_s": cache_age,
        })

    # ----------------------------------------------------------------
    # API — service status
    # ----------------------------------------------------------------

    @app.route("/api/status")
    def api_status():
        ing  = _ingestion_ref
        wa   = _wallet_analyzer_ref
        ma   = _market_analyzer_ref
        status = {
            "status": "ok",
            "market_id": config.market_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ingestion": {
                "ws_connected":    ing.ws_connected    if ing else None,
                "poll_count":      ing.poll_count      if ing else None,
                "last_poll":       ing.last_poll_ts    if ing else None,
                "trades_ingested": ing.new_trades_total if ing else None,
            },
            "wallet_analyzer": {
                "run_count":  wa.run_count   if wa else None,
                "last_run":   wa.last_run_ts if wa else None,
            },
            "market_analyzer": {
                "run_count":  ma.run_count   if ma else None,
                "last_run":   ma.last_run_ts if ma else None,
            },
        }
        return jsonify(status)

    # ----------------------------------------------------------------
    # API — resolve a Polymarket URL to condition_id(s)
    # ----------------------------------------------------------------

    @app.route("/api/resolve-market")
    def api_resolve_market():
        url = request.args.get("url", "").strip()
        if not url:
            return jsonify({"error": "url parameter required"}), 400

        slug = _parse_polymarket_slug(url)
        if not slug:
            return jsonify({"error": "Could not parse a market slug from this URL"}), 400

        try:
            resp = requests.get(
                f"{_GAMMA_BASE}/events",
                params={"slug": slug},
                headers=_REQ_HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error("resolve-market: gamma-api error: %s", exc)
            return jsonify({"error": "Could not reach Polymarket API"}), 502

        events = resp.json()
        if not events:
            return jsonify({"error": f"No market found for slug '{slug}'"}), 404

        event     = events[0]
        raw_markets = event.get("markets") or []
        parsed = [
            {
                "condition_id": m.get("conditionId"),
                "question":     m.get("question") or event.get("title") or slug,
            }
            for m in raw_markets if m.get("conditionId")
        ]

        if not parsed:
            return jsonify({"error": "Event found but no conditionId available"}), 404

        return jsonify({
            "title":   event.get("title") or slug,
            "slug":    event.get("slug")  or slug,
            "markets": parsed,
        })

    # ----------------------------------------------------------------
    # API — on-demand trade load for a market (POST)
    # ----------------------------------------------------------------

    @app.route("/api/load-market", methods=["POST"])
    def api_load_market():
        body      = request.get_json(silent=True) or {}
        market_id = (body.get("market_id") or "").strip()
        if not market_id:
            return jsonify({"error": "market_id required"}), 400

        # Always flush stale records for this market so corrected API
        # mappings are never blocked by INSERT OR IGNORE on old rows.
        cleared = db.delete_trades_for_market(market_id)
        if cleared:
            logger.info("load-market: cleared %d stale trades for %s", cleared, market_id)

        try:
            resp = requests.get(
                f"{_DATA_API_BASE}/trades",
                params={"market": market_id, "limit": 500, "takerOnly": "true"},
                headers=_REQ_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error("load-market: data-api error: %s", exc)
            return jsonify({"error": "Could not fetch trades from Polymarket"}), 502

        payload = resp.json()
        if isinstance(payload, list):
            raw_trades = payload
        elif isinstance(payload, dict):
            raw_trades = payload.get("data") or payload.get("trades") or []
        else:
            raw_trades = []

        stored   = 0
        now_iso  = datetime.now(timezone.utc).isoformat()
        for raw in raw_trades:
            trade = _normalize_trade_record(raw, market_id)
            if not trade:
                continue
            if db.insert_trade(trade):
                stored += 1
                trader = _normalize_trader_record(raw, now_iso)
                if trader:
                    db.upsert_trader(trader)

        logger.info("load-market: market=%s cleared=%d fetched=%d stored=%d",
                    market_id, cleared, len(raw_trades), stored)
        return jsonify({
            "market_id": market_id,
            "cleared":   cleared,
            "fetched":   len(raw_trades),
            "stored":    stored,
        })

    # ----------------------------------------------------------------
    # API — raw trade inspector (field-mapping debug)
    # ----------------------------------------------------------------

    @app.route("/api/debug/raw-trades")
    def api_debug_raw_trades():
        """
        Return the raw Polymarket API response for the first N records of a
        market so field names / values can be compared against what the tool
        displays.  Use ?market_id=<id>&limit=5&taker_only=true|false.
        """
        market_id  = request.args.get("market_id", "").strip()
        if not market_id:
            return jsonify({"error": "market_id required"}), 400
        limit      = min(int(request.args.get("limit", 5)), 20)
        taker_only = request.args.get("taker_only", "true").lower()

        try:
            resp = requests.get(
                f"{_DATA_API_BASE}/trades",
                params={"market": market_id, "limit": limit, "takerOnly": taker_only},
                headers=_REQ_HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            return jsonify({"error": str(exc)}), 502

        payload    = resp.json()
        raw_trades = payload if isinstance(payload, list) else (
            payload.get("data") or payload.get("trades") or []
        )

        # Also show how the normalizer would map each record
        normalized = [_normalize_trade_record(r, market_id) for r in raw_trades]

        return jsonify({
            "market_id":  market_id,
            "taker_only": taker_only,
            "count":      len(raw_trades),
            "raw":        raw_trades,
            "normalized": normalized,
        })

    # ----------------------------------------------------------------
    # Error handlers
    # ----------------------------------------------------------------

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "not found"}), 404

    @app.errorhandler(500)
    def server_error(e):
        logger.error("Unhandled error: %s", e, exc_info=True)
        return jsonify({"error": "internal server error"}), 500

    return app
