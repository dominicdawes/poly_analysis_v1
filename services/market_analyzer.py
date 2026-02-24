"""
services/market_analyzer.py
----------------------------
Fetches and caches market metadata for every condition ID seen in trades.

Runs every `config.market_analyzer_interval` seconds (default 3600s).

Data source
-----------
Uses data-api /trades?market=<id>&limit=1 to get title + slug + icon.
(gamma-api /markets?conditionId= does not filter correctly — do not use.)

Extended fields (category, description, end_date, resolved, winning_outcome)
are left NULL in Phase 2; they are reserved for Phase 3 when a reliable
market-detail endpoint is confirmed.

Re-fetch gate
-------------
A market row is skipped if its `last_fetched` is < 1 hour ago, preventing
unnecessary API calls when the analyzer loop fires on its normal schedule.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from conf.config import Config
from db import Database
from services.polymarket_client import PolymarketClient

logger = logging.getLogger(__name__)

_REFETCH_INTERVAL = 3_600   # seconds: skip market if fetched within last hour


class MarketAnalyzer:
    """Keeps the `markets` table populated with metadata for tracked markets."""

    def __init__(self, config: Config, db: Database, client: PolymarketClient):
        self.config   = config
        self.db       = db
        self.client   = client
        self._running  = False
        self._interval = config.market_analyzer_interval

        self._run_count    = 0
        self._last_run_ts: Optional[float] = None

    # ----------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        threading.Thread(
            target=self._loop,
            name="market-analyzer",
            daemon=True,
        ).start()
        logger.info("MarketAnalyzer started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._running = False

    @property
    def run_count(self) -> int:
        return self._run_count

    @property
    def last_run_ts(self) -> Optional[float]:
        return self._last_run_ts

    # ----------------------------------------------------------------
    # Loop
    # ----------------------------------------------------------------

    def _loop(self) -> None:
        self._safe_run()
        while self._running:
            deadline = time.monotonic() + self._interval
            while self._running and time.monotonic() < deadline:
                time.sleep(1)
            if self._running:
                self._safe_run()

    def _safe_run(self) -> None:
        try:
            self._run_once()
        except Exception as exc:
            logger.error("MarketAnalyzer unhandled error: %s", exc, exc_info=True)

    def _run_once(self) -> None:
        condition_ids = self.db.get_distinct_markets_from_trades()
        if not condition_ids:
            logger.debug("MarketAnalyzer: no markets in trades table yet")
            return

        updated = 0
        skipped = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for cid in condition_ids:
            # Skip if we fetched this market recently
            existing = self.db.get_market(cid)
            if existing and existing.get("last_fetched"):
                try:
                    last = datetime.fromisoformat(existing["last_fetched"])
                    age_s = (datetime.now(timezone.utc) - last).total_seconds()
                    if age_s < _REFETCH_INTERVAL:
                        skipped += 1
                        continue
                except (ValueError, TypeError):
                    pass

            info = self.client.get_market_info(cid)

            # Always upsert even if info is None, so we record last_fetched
            # and don't retry within the next hour.
            self.db.upsert_market({
                "condition_id":   cid,
                "title":          info.get("title")   if info else None,
                "slug":           info.get("slug")    if info else None,
                "icon":           info.get("icon")    if info else None,
                "description":    None,   # Phase 3
                "category":       None,   # Phase 3
                "end_date":       None,   # Phase 3
                "resolved":       0,
                "winning_outcome": None,
                "last_fetched":   now_iso,
            })
            updated += 1

        self._run_count += 1
        self._last_run_ts = time.time()
        logger.info(
            "MarketAnalyzer run #%d: updated %d, skipped %d markets",
            self._run_count, updated, skipped,
        )
