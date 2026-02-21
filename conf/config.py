"""
conf/config.py
--------------
Centralised configuration loaded from environment variables / .env file.
All other modules import `Config` and call `Config.from_env()`.
"""

import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    # --- Market ---
    market_id: str

    # --- Ingestion ---
    fetch_interval: int          # seconds between REST API polls
    whale_threshold: float       # USD amount above which a trade is "whale"

    # --- API endpoints ---
    clob_api_url: str
    data_api_url: str
    gamma_api_url: str
    ws_url: str

    # --- Storage ---
    db_path: str
    output_dir: str
    logs_dir: str

    # --- Flask ---
    flask_host: str
    flask_port: int
    flask_debug: bool

    # --- Credentials (optional for public endpoints) ---
    api_key: str
    api_secret: str
    api_passphrase: str

    # ----------------------------------------------------------------
    # Factory
    # ----------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            market_id=os.getenv("MARKET_ID", ""),
            fetch_interval=int(os.getenv("FETCH_INTERVAL", "60")),
            whale_threshold=float(os.getenv("WHALE_THRESHOLD", "1000")),
            clob_api_url=os.getenv("CLOB_API_URL", "https://clob.polymarket.com"),
            data_api_url=os.getenv("DATA_API_URL", "https://data-api.polymarket.com"),
            gamma_api_url=os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
            ws_url=os.getenv(
                "WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
            ),
            db_path=os.getenv("DB_PATH", "output/trades.db"),
            output_dir=os.getenv("OUTPUT_DIR", "output"),
            logs_dir=os.getenv("LOGS_DIR", "logs"),
            flask_host=os.getenv("FLASK_HOST", "0.0.0.0"),
            flask_port=int(os.getenv("FLASK_PORT", "5000")),
            flask_debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
            api_key=os.getenv("POLY_API_KEY", ""),
            api_secret=os.getenv("POLY_API_SECRET", ""),
            api_passphrase=os.getenv("POLY_API_PASSPHRASE", ""),
        )

    # ----------------------------------------------------------------
    # Validation
    # ----------------------------------------------------------------

    def validate(self) -> List[str]:
        """Return a list of human-readable error strings; empty = valid."""
        errors: List[str] = []

        if not self.market_id:
            errors.append("MARKET_ID is required — set it in .env")

        if self.fetch_interval < 10:
            errors.append("FETCH_INTERVAL must be >= 10 seconds")

        if self.whale_threshold <= 0:
            errors.append("WHALE_THRESHOLD must be > 0")

        return errors

    def __repr__(self) -> str:
        return (
            f"Config(market_id={self.market_id!r}, "
            f"fetch_interval={self.fetch_interval}s, "
            f"whale_threshold=${self.whale_threshold:,.0f})"
        )
