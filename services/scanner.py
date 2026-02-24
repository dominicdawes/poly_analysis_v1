"""
services/scanner.py
-------------------
Stub for Phase 3 — wallet scanning / market discovery pipeline.

Planned functionality
---------------------
- scan_whales(min_volume_usd)      → wallets exceeding volume threshold
- scan_markets(category, min_24h)  → markets matching filters
- alert on new whale activity      → notification hooks

The interface mirrors WalletAnalyzer / MarketAnalyzer so it can be dropped
in to run.py without changes when Phase 3 is implemented.
"""

import logging
from typing import Optional

from conf.config import Config
from db import Database

logger = logging.getLogger(__name__)


class ScannerService:
    """Stub — logs a warning at startup and does nothing."""

    def __init__(self, config: Config, db: Database):
        self.config   = config
        self.db       = db
        self._running = False

    def start(self) -> None:
        logger.info("ScannerService: stub (Phase 3) — not yet active")
        self._running = True

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Phase 3 stubs — implement here
    # ------------------------------------------------------------------

    def scan_whales(self, min_volume_usd: float = 10_000) -> list:
        """Return wallets whose total_volume >= min_volume_usd."""
        return self.db.get_wallets(order_by="total_volume", min_volume=min_volume_usd)

    def scan_markets(
        self,
        category: Optional[str] = None,
        min_volume_24h: Optional[float] = None,
    ) -> list:
        """Return markets matching the given filters (Phase 3 — returns all for now)."""
        return self.db.get_markets(limit=200)
