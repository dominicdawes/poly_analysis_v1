"""
services/analysis.py
--------------------
Trade analysis: filtering, volume metrics, whale detection.

Designed for extension — add new analysis methods without touching
other modules.  All methods read from the DB and return plain dicts /
lists suitable for JSON serialisation.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from db import Database

logger = logging.getLogger(__name__)


class AnalysisService:
    """Stateless analysis layer on top of the DB."""

    def __init__(self, db: Database, whale_threshold: float = 1_000.0):
        self.db = db
        self.whale_threshold = whale_threshold

    # ----------------------------------------------------------------
    # Summary / Dashboard
    # ----------------------------------------------------------------

    def get_summary(self, market_id: Optional[str] = None) -> Dict:
        """Aggregate stats plus volume-by-outcome breakdown."""
        stats = self.db.get_stats(market_id)
        volume_breakdown = self.db.get_volume_by_outcome(market_id)

        # Reshape breakdown into {outcome: {buy, sell, …}} for easy JS access
        by_outcome: Dict[str, Dict] = {}
        for row in volume_breakdown:
            outcome = row.get("outcome") or "Unknown"
            if outcome not in by_outcome:
                by_outcome[outcome] = {
                    "outcome": outcome,
                    "buy_volume": 0.0,
                    "sell_volume": 0.0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "avg_price": 0.0,
                }
            side = row.get("side", "").upper()
            if side == "BUY":
                by_outcome[outcome]["buy_volume"] = row["volume"]
                by_outcome[outcome]["buy_count"] = row["trade_count"]
                by_outcome[outcome]["avg_price"] = row["avg_price"]
            elif side == "SELL":
                by_outcome[outcome]["sell_volume"] = row["volume"]
                by_outcome[outcome]["sell_count"] = row["trade_count"]

        return {
            **stats,
            "whale_threshold": self.whale_threshold,
            "volume_by_outcome": list(by_outcome.values()),
        }

    # ----------------------------------------------------------------
    # Trade filters
    # ----------------------------------------------------------------

    def get_whale_trades(
        self,
        market_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """Trades above the whale threshold, newest first."""
        return self.db.get_recent_trades(
            limit=limit,
            market_id=market_id,
            min_amount=self.whale_threshold,
        )

    def get_recent_trades(
        self,
        market_id: Optional[str] = None,
        limit: int = 100,
        min_amount: Optional[float] = None,
        wallet: Optional[str] = None,
    ) -> List[Dict]:
        return self.db.get_recent_trades(
            limit=limit,
            market_id=market_id,
            min_amount=min_amount,
            wallet=wallet,
        )

    # ----------------------------------------------------------------
    # Trader leaderboard
    # ----------------------------------------------------------------

    def get_top_traders(
        self,
        market_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """Top traders ranked by total USDC volume, with size classification."""
        traders = self.db.get_top_traders(market_id=market_id, limit=limit)
        for t in traders:
            t["size_class"] = self._classify_size(t.get("total_volume") or 0)
        return traders

    # ----------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------

    def classify_trade(self, trade: Dict) -> Dict:
        """
        Annotate a trade dict with derived fields.
        Returns a new dict (does not mutate the input).
        """
        amount = trade.get("amount") or 0
        return {
            **trade,
            "size_class": self._classify_size(amount),
            "is_whale": amount >= self.whale_threshold,
            "display_time": self._fmt_time(trade.get("match_time")),
        }

    def _classify_size(self, amount: float) -> str:
        if amount >= self.whale_threshold:
            return "whale"
        if amount >= self.whale_threshold * 0.1:
            return "medium"
        return "small"

    @staticmethod
    def _fmt_time(ts: Optional[int]) -> str:
        if not ts:
            return "—"
        try:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M UTC")
        except (OSError, OverflowError, ValueError):
            return str(ts)
