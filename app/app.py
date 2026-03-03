"""
app/app.py
----------
Flask web application.

Routes
------
  GET  /                         — Live trades dashboard (HTML)
  GET  /wallet-dashboard         — Wallet analytics page (HTML)

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

import io
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, render_template, request, Response

from conf.config import Config
from db import Database
from services.analysis import AnalysisService

logger = logging.getLogger(__name__)

# Service refs — set by run.py; accessed by the status endpoint
_ingestion_ref       = None
_wallet_analyzer_ref = None
_market_analyzer_ref = None

_GAMMA_BASE    = "https://gamma-api.polymarket.com"
_DATA_API_BASE = "https://data-api.polymarket.com"
_REQ_HEADERS   = {"User-Agent": "poly-analysis-v1/1.0", "Accept": "application/json"}


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
    global _ingestion_ref, _wallet_analyzer_ref, _market_analyzer_ref
    _ingestion_ref       = ingestion
    _wallet_analyzer_ref = wallet_analyzer
    _market_analyzer_ref = market_analyzer

    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    static_dir = os.path.join(os.path.dirname(__file__), "static")

    app = Flask(
        __name__,
        template_folder=template_dir,
        static_folder=static_dir,
    )
    app.config["JSON_SORT_KEYS"] = False

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

    # ----------------------------------------------------------------
    # Pages
    # ----------------------------------------------------------------

    @app.route("/")
    def index():
        return render_template(
            "index.html",
            market_id=config.market_id,
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
    # API — markets metadata
    # ----------------------------------------------------------------

    @app.route("/api/markets")
    def api_markets():
        markets = db.get_markets(limit=_limit(default=50, cap=200))
        return jsonify(markets)

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
