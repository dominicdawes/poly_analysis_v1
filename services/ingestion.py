"""
services/ingestion.py
---------------------
Data ingestion from Polymarket.

Two parallel channels:
  1. REST polling  — `data-api.polymarket.com/trades` every N seconds.
     This is the primary data source: it returns complete trade records
     including wallet addresses and embedded trader profile info.

  2. WebSocket monitor — `ws-subscriptions-clob.polymarket.com/ws/market`.
     Used for health-checking and real-time signalling.  Does NOT supply
     wallet-level trade data on its own, so the REST poll remains the
     authoritative source of truth.

Both channels run in daemon threads so Flask can own the main thread.
"""

import asyncio
import json
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Optional

import requests
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from conf.config import Config
from db import Database

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Manages REST polling and WebSocket monitoring for a single market.

    Usage::

        svc = IngestionService(config, db)
        svc.start()   # spawns background threads; returns immediately
        ...
        svc.stop()
    """

    # Public API base URLs (overridden via Config in production)
    _DEFAULT_DATA_API = "https://data-api.polymarket.com"
    _DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db
        self._running = False
        self._ws_connected = False
        self._poll_count = 0
        self._last_poll_ts: Optional[float] = None
        self._new_trades_total = 0

        # Shared requests session (thread-safe for reads, not writes)
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "poly-analysis-v1/1.0",
                "Accept": "application/json",
            }
        )

    # ----------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------

    def start(self):
        """Spawn background threads and return immediately."""
        if self._running:
            logger.warning("IngestionService.start() called while already running")
            return

        self._running = True

        threading.Thread(
            target=self._poll_loop,
            name="ingestion-poll",
            daemon=True,
        ).start()

        threading.Thread(
            target=self._ws_thread_main,
            name="ingestion-ws",
            daemon=True,
        ).start()

        logger.info(
            "IngestionService started — market=%s, interval=%ds",
            self.config.market_id,
            self.config.fetch_interval,
        )

    def stop(self):
        self._running = False

    # ----------------------------------------------------------------
    # Properties for health reporting
    # ----------------------------------------------------------------

    @property
    def ws_connected(self) -> bool:
        return self._ws_connected

    @property
    def poll_count(self) -> int:
        return self._poll_count

    @property
    def last_poll_ts(self) -> Optional[float]:
        return self._last_poll_ts

    @property
    def new_trades_total(self) -> int:
        return self._new_trades_total

    # ================================================================
    # REST Polling
    # ================================================================

    def _poll_loop(self):
        """Blocking loop: fetch trades, sleep, repeat."""
        logger.info("Poll loop started")

        # Do an immediate first fetch
        self._safe_fetch()

        while self._running:
            # Sleep in 1-second increments so we can exit cleanly
            deadline = time.monotonic() + self.config.fetch_interval
            while self._running and time.monotonic() < deadline:
                time.sleep(1)

            if self._running:
                self._safe_fetch()

    def _safe_fetch(self):
        """Wrap _fetch_and_store so a single failure doesn't kill the loop."""
        try:
            self._fetch_and_store_trades()
        except Exception as exc:
            logger.error("Unhandled error in fetch cycle: %s", exc, exc_info=True)

    def _fetch_and_store_trades(self):
        """Fetch the latest trades for the configured market and persist new ones."""
        data_api = self.config.data_api_url or self._DEFAULT_DATA_API

        try:
            resp = self._session.get(
                f"{data_api}/trades",
                params={
                    "market": self.config.market_id,
                    "limit": 500,
                    "takerOnly": "false",
                },
                timeout=30,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.warning("Trade fetch timed out")
            return
        except requests.exceptions.RequestException as exc:
            logger.error("Trade fetch failed: %s", exc)
            return

        payload = resp.json()

        # data-api may return a list directly or wrap it
        if isinstance(payload, list):
            raw_trades = payload
        elif isinstance(payload, dict):
            raw_trades = payload.get("data") or payload.get("trades") or []
        else:
            logger.warning("Unexpected trades response type: %s", type(payload))
            return

        if not raw_trades:
            logger.debug("No trades returned for market %s", self.config.market_id)
            self._last_poll_ts = time.time()
            self._poll_count += 1
            return

        new_count = 0
        for raw in raw_trades:
            trade = self._normalize_trade(raw)
            if not trade:
                continue

            inserted = self.db.insert_trade(trade)
            if inserted:
                new_count += 1
                # Persist trader profile embedded in the trade record
                trader = self._normalize_trader(raw)
                if trader:
                    self.db.upsert_trader(trader)

        self._new_trades_total += new_count
        self._last_poll_ts = time.time()
        self._poll_count += 1

        if new_count:
            logger.info(
                "Poll #%d: stored %d new trades (fetched %d)",
                self._poll_count,
                new_count,
                len(raw_trades),
            )
        else:
            logger.debug(
                "Poll #%d: %d trades fetched, 0 new",
                self._poll_count,
                len(raw_trades),
            )

    # ----------------------------------------------------------------
    # Data normalisation
    # ----------------------------------------------------------------

    def _normalize_trade(self, raw: dict) -> Optional[dict]:
        """
        Map a raw Data API trade response to the DB schema.
        Returns None if the record is missing required fields.
        """
        try:
            # Wallet address — Data API returns proxyWallet
            wallet = (
                raw.get("proxyWallet")
                or raw.get("maker_address")
                or raw.get("owner")
            )
            if not wallet:
                return None

            price = float(raw.get("price") or 0)
            size = float(raw.get("size") or 0)
            amount = round(price * size, 6)

            # Timestamp normalisation
            ts = raw.get("timestamp")
            if ts is None:
                match_time = int(time.time())
            elif isinstance(ts, (int, float)):
                match_time = int(ts)
            elif isinstance(ts, str):
                try:
                    # ISO-8601 string
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    match_time = int(dt.timestamp())
                except ValueError:
                    match_time = int(time.time())
            else:
                match_time = int(time.time())

            return {
                "transaction_hash": raw.get("transactionHash") or raw.get("transaction_hash"),
                "market_id": raw.get("conditionId") or raw.get("market") or self.config.market_id,
                "token_id": raw.get("asset") or raw.get("asset_id"),
                "proxy_wallet": wallet,
                "side": str(raw.get("side", "")).upper(),
                "price": price,
                "size": size,
                "amount": amount,
                "outcome": raw.get("outcome"),
                "outcome_index": raw.get("outcomeIndex"),
                "market_title": raw.get("title"),
                "market_slug": raw.get("slug"),
                "market_icon": raw.get("icon"),
                "match_time": match_time,
            }
        except (KeyError, ValueError, TypeError) as exc:
            logger.debug("Could not normalise trade: %s | raw=%s", exc, raw)
            return None

    def _normalize_trader(self, raw: dict) -> Optional[dict]:
        """Extract trader profile data from an embedded trade record."""
        wallet = (
            raw.get("proxyWallet")
            or raw.get("maker_address")
            or raw.get("owner")
        )
        if not wallet:
            return None

        return {
            "proxy_wallet": wallet,
            "name": raw.get("name"),
            "pseudonym": raw.get("pseudonym"),
            "profile_image": (
                raw.get("profileImageOptimized")
                or raw.get("profileImage")
            ),
            "bio": raw.get("bio"),
            "num_trades": 0,
            "pnl_cumulative": 0.0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    # ================================================================
    # WebSocket Monitor
    # ================================================================

    def _ws_thread_main(self):
        """Run an asyncio event loop in this thread for the WebSocket."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._ws_connect_loop())
        except Exception as exc:
            logger.error("WebSocket thread fatal error: %s", exc, exc_info=True)
        finally:
            loop.close()

    async def _ws_connect_loop(self):
        """Reconnect-forever WebSocket loop with exponential back-off."""
        ws_url = self.config.ws_url or self._DEFAULT_WS_URL
        backoff = 5

        while self._running:
            try:
                logger.info("WebSocket connecting to %s", ws_url)
                async with websockets.connect(
                    ws_url,
                    ping_interval=10,
                    ping_timeout=20,
                    open_timeout=15,
                ) as ws:
                    self._ws_connected = True
                    backoff = 5  # reset on successful connect
                    logger.info("WebSocket connected")
                    await self._ws_receive_loop(ws)

            except (ConnectionClosed, WebSocketException, OSError) as exc:
                self._ws_connected = False
                logger.warning(
                    "WebSocket disconnected (%s). Retrying in %ds…", exc, backoff
                )
            except Exception as exc:
                self._ws_connected = False
                logger.error("WebSocket error: %s", exc, exc_info=True)

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

        self._ws_connected = False

    async def _ws_receive_loop(self, ws):
        """
        Process incoming WebSocket messages.

        The CLOB market channel sends orderbook snapshots/updates.
        We log event types for debugging; the REST poll is the
        primary trade data source.
        """
        async for message in ws:
            if not self._running:
                break

            if message in ("PING", "PONG"):
                continue

            try:
                data = json.loads(message)
                event_type = data.get("event_type") or data.get("type") or "unknown"
                logger.debug("WS event_type=%s", event_type)

                # Hook: extend here to trigger immediate poll on trade events
                # if event_type in ("last_trade_price", "trade"):
                #     self._safe_fetch()

            except (json.JSONDecodeError, AttributeError):
                pass  # Non-JSON frames (e.g. plain-text PONG)
