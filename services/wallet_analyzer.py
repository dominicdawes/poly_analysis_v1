"""
services/wallet_analyzer.py
----------------------------
Aggregates on-chain trade data into per-wallet analytics.

Runs every `config.wallet_analyzer_interval` seconds (default 300s) in a
background daemon thread.

What it does each cycle
-----------------------
1. Collect all distinct wallet addresses from the trades table.
2. For each wallet:
   a. Pull all trades (oldest-first) from the trades table.
   b. Compute aggregate stats (volume, trade count, first/last seen, etc.).
   c. Compute per-position FIFO PnL → write to positions table.
   d. Sum realized PnL across positions.
   e. Resolve profile: traders table → wallets table → Gamma API (at most
      once per 24h per wallet to stay within rate limits).
   f. Upsert the wallets row with fresh metrics.

FIFO PnL notes
--------------
- Each (market_id, outcome) pair is treated as a separate lot queue.
- Cost basis is per-share `price` (already stored as the 0–1 decimal).
- "Orphaned" sells (buys before our ingestion window) are logged at
  DEBUG level but excluded from realized PnL to avoid inflated numbers.
- `win_rate` is set to None until Phase 3 provides market resolution data.
"""

import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from conf.config import Config
from db import Database
from services.polymarket_client import PolymarketClient

logger = logging.getLogger(__name__)

# Minimum seconds between Gamma API profile calls for the same wallet
_PROFILE_CACHE_TTL = 86_400  # 24 hours


class WalletAnalyzer:
    """Aggregates trades → wallets + positions on a recurring schedule."""

    def __init__(self, config: Config, db: Database, client: PolymarketClient):
        self.config  = config
        self.db      = db
        self.client  = client
        self._running = False
        self._interval = config.wallet_analyzer_interval

        # Track last Gamma API attempt per wallet (in-memory, resets on restart)
        self._profile_attempted: Dict[str, float] = {}

        # Stats for status reporting
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
            name="wallet-analyzer",
            daemon=True,
        ).start()
        logger.info("WalletAnalyzer started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._running = False

    @property
    def run_count(self) -> int:
        return self._run_count

    @property
    def last_run_ts(self) -> Optional[float]:
        return self._last_run_ts

    # ----------------------------------------------------------------
    # Main loop
    # ----------------------------------------------------------------

    def _loop(self) -> None:
        # Immediate first run
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
            logger.error("WalletAnalyzer unhandled error: %s", exc, exc_info=True)

    def _run_once(self) -> None:
        wallets = self.db.get_distinct_wallets_from_trades()
        if not wallets:
            logger.debug("WalletAnalyzer: no wallets in trades table yet")
            return

        processed = 0
        for address in wallets:
            try:
                self._process_wallet(address)
                processed += 1
            except Exception as exc:
                logger.warning("WalletAnalyzer: error processing %s: %s", address, exc)

        self._run_count += 1
        self._last_run_ts = time.time()
        logger.info(
            "WalletAnalyzer run #%d: processed %d/%d wallets",
            self._run_count, processed, len(wallets),
        )

    # ----------------------------------------------------------------
    # Per-wallet processing
    # ----------------------------------------------------------------

    def _process_wallet(self, address: str) -> None:
        trades = self.db.get_trades_for_wallet(address)
        if not trades:
            return

        # 1. Aggregate stats
        stats = _aggregate_stats(trades)

        # 2. Compute positions + FIFO PnL
        positions = _compute_positions(trades, address)

        # 3. Count open positions
        active_count = sum(
            1 for p in positions.values() if p["net_shares"] > 1e-6
        )

        # 4. Total realized PnL
        total_pnl = sum(p["realized_pnl"] for p in positions.values())

        # 5. Profile enrichment
        profile = self._resolve_profile(address)

        # 6. Upsert wallet row
        now_iso = datetime.now(timezone.utc).isoformat()
        self.db.upsert_wallet({
            "address":           address,
            "name":              profile.get("name"),
            "pseudonym":         profile.get("pseudonym"),
            "profile_image":     profile.get("profile_image"),
            "bio":               profile.get("bio"),
            "first_seen":        stats["first_seen"],
            "last_seen":         stats["last_seen"],
            "total_trades":      stats["total_trades"],
            "total_volume":      stats["total_volume"],
            "total_buy_volume":  stats["total_buy_volume"],
            "total_sell_volume": stats["total_sell_volume"],
            "largest_trade":     stats["largest_trade"],
            "avg_trade_size":    stats["avg_trade_size"],
            "num_active_positions": active_count,
            "win_rate":          None,   # Phase 3
            "realized_pnl":      total_pnl,
            "last_updated":      now_iso,
        })

        # 7. Upsert each position
        for (condition_id, outcome), pos in positions.items():
            self.db.upsert_position({
                "wallet_address": address,
                "condition_id":   condition_id,
                "outcome":        outcome,
                "net_shares":     pos["net_shares"],
                "avg_entry_price": pos["avg_entry_price"],
                "total_bought":   pos["total_bought"],
                "total_sold":     pos["total_sold"],
                "realized_pnl":   pos["realized_pnl"],
                "last_updated":   now_iso,
            })

    # ----------------------------------------------------------------
    # Profile enrichment
    # ----------------------------------------------------------------

    def _resolve_profile(self, address: str) -> Dict:
        """
        Return profile dict with keys: name, pseudonym, profile_image, bio.

        Resolution order:
          1. traders table (populated by ingestion from trade metadata)
          2. wallets table (previously fetched and cached)
          3. Gamma API (rate-limited to once per 24h per wallet)
        """
        # 1. traders table (ingestion keeps this fresh)
        trader = self.db.get_trader(address)
        if trader and (trader.get("name") or trader.get("pseudonym")):
            return {
                "name":          trader.get("name"),
                "pseudonym":     trader.get("pseudonym"),
                "profile_image": trader.get("profile_image"),
                "bio":           trader.get("bio"),
            }

        # 2. wallets table (our own previous enrichment)
        existing = self.db.get_wallet(address)
        if existing and (existing.get("name") or existing.get("pseudonym")):
            return {
                "name":          existing.get("name"),
                "pseudonym":     existing.get("pseudonym"),
                "profile_image": existing.get("profile_image"),
                "bio":           existing.get("bio"),
            }

        # 3. Gamma API — throttled per wallet
        now = time.time()
        last_attempt = self._profile_attempted.get(address, 0.0)
        if now - last_attempt < _PROFILE_CACHE_TTL:
            return {}   # already tried recently, wait until TTL expires

        self._profile_attempted[address] = now
        api_data = self.client.get_trader_profile(address)
        if api_data:
            return api_data

        return {}


# ================================================================
# Pure functions (no DB/network access)
# ================================================================

def _aggregate_stats(trades: List[Dict]) -> Dict:
    amounts = [float(t["amount"]) for t in trades]
    total   = sum(amounts)
    return {
        "first_seen":        min(int(t["match_time"]) for t in trades),
        "last_seen":         max(int(t["match_time"]) for t in trades),
        "total_trades":      len(trades),
        "total_volume":      total,
        "total_buy_volume":  sum(float(t["amount"]) for t in trades if t["side"] == "BUY"),
        "total_sell_volume": sum(float(t["amount"]) for t in trades if t["side"] == "SELL"),
        "largest_trade":     max(amounts) if amounts else 0.0,
        "avg_trade_size":    total / len(amounts) if amounts else 0.0,
    }


def _compute_positions(
    trades: List[Dict],
    address: str,
) -> Dict[Tuple[str, str], Dict]:
    """
    Group trades by (market_id, outcome) and compute FIFO PnL per group.
    `trades` must be sorted oldest-first (get_trades_for_wallet guarantees this).
    """
    groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    for t in trades:
        key = (t["market_id"], t.get("outcome") or "Unknown")
        groups[key].append(t)

    return {key: _fifo_pnl(group, address, key[0], key[1])
            for key, group in groups.items()}


def _fifo_pnl(
    trades: List[Dict],
    address: str,
    condition_id: str,
    outcome: str,
) -> Dict:
    """
    FIFO realized PnL for one (wallet, market, outcome) position.

    Cost queue stores (shares_remaining, price_per_share) tuples.
    Realized PnL = sum of matched * (sell_price - buy_price) for each sell.
    """
    buy_queue:         deque  = deque()   # (shares, price_per_share)
    total_buy_shares:  float  = 0.0
    total_buy_usdc:    float  = 0.0
    total_sell_shares: float  = 0.0
    total_sell_usdc:   float  = 0.0
    realized_pnl:      float  = 0.0

    for t in trades:
        size   = float(t["size"])
        price  = float(t["price"])
        amount = float(t["amount"])

        if t["side"] == "BUY":
            buy_queue.append((size, price))
            total_buy_shares += size
            total_buy_usdc   += amount

        elif t["side"] == "SELL":
            total_sell_shares += size
            total_sell_usdc   += amount
            remaining          = size

            while remaining > 1e-9 and buy_queue:
                lot_shares, lot_price = buy_queue[0]
                matched        = min(lot_shares, remaining)
                realized_pnl  += matched * (price - lot_price)
                lot_shares    -= matched
                remaining     -= matched
                if lot_shares < 1e-9:
                    buy_queue.popleft()
                else:
                    buy_queue[0] = (lot_shares, lot_price)

            if remaining > 1e-9:
                # Orphaned sell — bought before our ingestion window
                logger.debug(
                    "Orphaned sell %.4f shares: wallet=%s market=%s outcome=%s",
                    remaining, address, condition_id, outcome,
                )

    net_shares  = max(0.0, total_buy_shares - total_sell_shares)
    avg_entry   = total_buy_usdc / total_buy_shares if total_buy_shares > 1e-9 else 0.0

    return {
        "net_shares":      net_shares,
        "avg_entry_price": avg_entry,
        "total_bought":    total_buy_usdc,
        "total_sold":      total_sell_usdc,
        "realized_pnl":    realized_pnl,
    }
