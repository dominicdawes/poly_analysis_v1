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
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request, Response

from conf.config import Config
from db import Database
from services.analysis import AnalysisService

logger = logging.getLogger(__name__)

# Service refs — set by run.py; accessed by the status endpoint
_ingestion_ref       = None
_wallet_analyzer_ref = None
_market_analyzer_ref = None


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
