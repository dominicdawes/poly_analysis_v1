"""
services/polymarket_client.py
------------------------------
Thin REST wrapper around the Polymarket public APIs.

Design
------
- Single shared requests.Session with a threading.Lock-based rate limiter.
- 1 request/second global limit (safe margin for public endpoints).
- Up to 3 retries with exponential back-off on transient errors.
- All methods return plain dicts / None — never raise.
- Callers (wallet_analyzer, market_analyzer) must tolerate None returns.

Known API constraints
---------------------
- gamma-api /markets?conditionId=  does NOT filter — returns arbitrary rows.
  Use data-api /trades?market=<id>&limit=1 to get market title/slug instead.
- gamma-api /profiles?address=<addr> returns a list; empty list = no profile.
"""

import logging
import threading
import time
from typing import Dict, Optional

import requests

from conf.config import Config

logger = logging.getLogger(__name__)


class PolymarketClient:
    """Thread-safe Polymarket REST client with rate limiting."""

    _RETRY_STATUSES = {429, 500, 502, 503, 504}
    _MAX_RETRIES    = 3

    def __init__(self, config: Config):
        self._data_api  = config.data_api_url
        self._gamma_api = config.gamma_api_url

        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "poly-analysis-v1/1.0", "Accept": "application/json"}
        )

        # Rate limiting: 1 req/sec across all callers / threads
        self._rate_lock      = threading.Lock()
        self._last_call_time = 0.0
        self._min_interval   = 1.0  # seconds

    # ----------------------------------------------------------------
    # Public interface
    # ----------------------------------------------------------------

    def get_market_info(self, condition_id: str) -> Optional[Dict]:
        """
        Return basic market metadata for a condition ID.

        Fetches one trade from data-api (correctly filtered by market) and
        extracts the embedded title / slug / icon fields.

        Returns dict with keys: condition_id, title, slug, icon
        or None if the market has no trades yet or the call fails.
        """
        if not condition_id:
            return None

        data = self._get(
            f"{self._data_api}/trades",
            params={"market": condition_id, "limit": 1, "takerOnly": "false"},
        )
        if not isinstance(data, list) or not data:
            return None

        t = data[0]
        return {
            "condition_id": condition_id,
            "title": t.get("title"),
            "slug":  t.get("slug"),
            "icon":  t.get("icon"),
        }

    def get_trader_profile(self, wallet_address: str) -> Optional[Dict]:
        """
        Return profile info for a wallet from the Gamma API.

        GET /profiles?address=<wallet>
        Returns a list; takes first element.

        Returns dict with keys: name, pseudonym, profile_image, bio
        or None if no profile found or the call fails.
        """
        if not wallet_address:
            return None

        data = self._get(
            f"{self._gamma_api}/profiles",
            params={"address": wallet_address},
        )

        if isinstance(data, list) and data:
            p = data[0]
        elif isinstance(data, dict) and data:
            p = data
        else:
            return None  # empty list = no profile (not an error)

        return {
            "name":          p.get("name"),
            "pseudonym":     p.get("pseudonym"),
            "profile_image": p.get("profileImageOptimized") or p.get("profileImage"),
            "bio":           p.get("bio"),
        }

    # ----------------------------------------------------------------
    # Internal HTTP
    # ----------------------------------------------------------------

    def _rate_wait(self) -> None:
        """Block until 1 second has passed since the last request."""
        with self._rate_lock:
            now     = time.monotonic()
            elapsed = now - self._last_call_time
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call_time = time.monotonic()

    def _get(self, url: str, params: dict = None) -> Optional[object]:
        """
        Execute a GET with rate limiting and retry logic.
        Returns parsed JSON (dict or list) or None on failure.
        """
        backoff = 2.0
        for attempt in range(self._MAX_RETRIES + 1):
            self._rate_wait()
            try:
                resp = self._session.get(url, params=params, timeout=15)

                if resp.status_code in self._RETRY_STATUSES and attempt < self._MAX_RETRIES:
                    logger.warning(
                        "HTTP %d from %s — retry %d/%d in %.0fs",
                        resp.status_code, url, attempt + 1, self._MAX_RETRIES, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                if not resp.ok:
                    logger.debug("HTTP %d from %s (not retrying)", resp.status_code, url)
                    return None

                return resp.json()

            except requests.exceptions.Timeout:
                logger.warning("Timeout fetching %s (attempt %d)", url, attempt + 1)
            except requests.exceptions.RequestException as exc:
                logger.warning("Request error fetching %s: %s", url, exc)
            except ValueError as exc:
                logger.warning("JSON decode error from %s: %s", url, exc)
                return None  # bad JSON — no point retrying

            if attempt < self._MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2

        logger.error("All retries exhausted for %s", url)
        return None
