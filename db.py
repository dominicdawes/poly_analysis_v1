"""
db.py
-----
SQLite storage layer.

Design notes:
- Each thread gets its own connection via threading.local() to avoid
  sqlite3's "check_same_thread" restriction.
- Public methods accept plain dicts and return plain dicts (no ORM).
- The schema is created automatically on first run.
"""

import csv
import io
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Thread-local storage so each thread owns its connection
_local = threading.local()


class Database:
    """Thin wrapper around SQLite providing CRUD + CSV export."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._create_schema()

    # ----------------------------------------------------------------
    # Connection management
    # ----------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Return a thread-local SQLite connection, creating it if needed."""
        if not hasattr(_local, "conn") or _local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")   # safe for multi-threaded reads
            conn.execute("PRAGMA foreign_keys=ON")
            _local.conn = conn
        return _local.conn

    def close(self):
        """Close the thread-local connection if open (useful in tests / cleanup)."""
        conn = getattr(_local, "conn", None)
        if conn is not None:
            conn.close()
            _local.conn = None

    # ----------------------------------------------------------------
    # Schema
    # ----------------------------------------------------------------

    def _create_schema(self):
        """Create tables and indexes if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_hash TEXT    UNIQUE,
                market_id        TEXT    NOT NULL,
                token_id         TEXT,
                proxy_wallet     TEXT    NOT NULL,
                side             TEXT    NOT NULL,   -- BUY | SELL
                price            REAL    NOT NULL,   -- 0–1 probability price
                size             REAL    NOT NULL,   -- shares
                amount           REAL    NOT NULL,   -- USDC value (price * size)
                outcome          TEXT,               -- Yes | No
                outcome_index    INTEGER,
                market_title     TEXT,
                market_slug      TEXT,
                market_icon      TEXT,
                match_time       INTEGER NOT NULL,   -- Unix timestamp (seconds)
                created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS traders (
                proxy_wallet     TEXT    PRIMARY KEY,
                name             TEXT,
                pseudonym        TEXT,
                profile_image    TEXT,
                bio              TEXT,
                num_trades       INTEGER DEFAULT 0,
                pnl_cumulative   REAL    DEFAULT 0.0,
                last_updated     TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_market  ON trades(market_id);
            CREATE INDEX IF NOT EXISTS idx_trades_wallet  ON trades(proxy_wallet);
            CREATE INDEX IF NOT EXISTS idx_trades_time    ON trades(match_time DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_amount  ON trades(amount DESC);
        """)

        conn.commit()
        conn.close()

    def verify_schema(self) -> bool:
        """Return True if required tables exist."""
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        existing = {r[0] for r in rows}
        return {"trades", "traders"}.issubset(existing)

    # ----------------------------------------------------------------
    # Writes
    # ----------------------------------------------------------------

    def insert_trade(self, trade: Dict) -> bool:
        """
        Insert a trade record.  Silently ignores duplicates (by transaction_hash).
        Returns True if the row was actually inserted.
        """
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO trades (
                    transaction_hash, market_id, token_id, proxy_wallet, side,
                    price, size, amount, outcome, outcome_index,
                    market_title, market_slug, market_icon, match_time
                ) VALUES (
                    :transaction_hash, :market_id, :token_id, :proxy_wallet, :side,
                    :price, :size, :amount, :outcome, :outcome_index,
                    :market_title, :market_slug, :market_icon, :match_time
                )
                """,
                trade,
            )
            conn.commit()
            return conn.execute("SELECT changes()").fetchone()[0] > 0
        except Exception as exc:
            logger.error("insert_trade failed: %s | trade=%s", exc, trade)
            conn.rollback()
            return False

    def upsert_trader(self, trader: Dict):
        """Insert or update a trader record."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO traders (
                    proxy_wallet, name, pseudonym, profile_image, bio,
                    num_trades, pnl_cumulative, last_updated
                ) VALUES (
                    :proxy_wallet, :name, :pseudonym, :profile_image, :bio,
                    :num_trades, :pnl_cumulative, :last_updated
                )
                ON CONFLICT(proxy_wallet) DO UPDATE SET
                    name           = COALESCE(excluded.name, traders.name),
                    pseudonym      = COALESCE(excluded.pseudonym, traders.pseudonym),
                    profile_image  = COALESCE(excluded.profile_image, traders.profile_image),
                    bio            = COALESCE(excluded.bio, traders.bio),
                    num_trades     = excluded.num_trades,
                    pnl_cumulative = excluded.pnl_cumulative,
                    last_updated   = excluded.last_updated
                """,
                trader,
            )
            conn.commit()
        except Exception as exc:
            logger.error("upsert_trader failed: %s", exc)
            conn.rollback()

    # ----------------------------------------------------------------
    # Reads
    # ----------------------------------------------------------------

    def get_recent_trades(
        self,
        limit: int = 100,
        market_id: Optional[str] = None,
        min_amount: Optional[float] = None,
        wallet: Optional[str] = None,
    ) -> List[Dict]:
        """Return recent trades, newest first, with trader profile joined."""
        conn = self._get_conn()

        conditions: List[str] = []
        params: List = []

        if market_id:
            conditions.append("t.market_id = ?")
            params.append(market_id)
        if min_amount is not None:
            conditions.append("t.amount >= ?")
            params.append(min_amount)
        if wallet:
            conditions.append("t.proxy_wallet = ?")
            params.append(wallet)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT
                t.*,
                tr.name        AS trader_name,
                tr.pseudonym   AS trader_pseudonym,
                tr.profile_image AS trader_profile_image
            FROM trades t
            LEFT JOIN traders tr ON t.proxy_wallet = tr.proxy_wallet
            {where}
            ORDER BY t.match_time DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

        return [dict(r) for r in rows]

    def get_stats(self, market_id: Optional[str] = None) -> Dict:
        """Aggregate stats: counts, volume, etc."""
        conn = self._get_conn()
        params = [market_id] if market_id else []
        where = "WHERE market_id = ?" if market_id else ""

        row = conn.execute(
            f"""
            SELECT
                COUNT(*)                    AS total_trades,
                COALESCE(SUM(amount), 0)    AS total_volume,
                COALESCE(AVG(amount), 0)    AS avg_trade_size,
                COALESCE(MAX(amount), 0)    AS largest_trade,
                COUNT(DISTINCT proxy_wallet) AS unique_traders
            FROM trades {where}
            """,
            params,
        ).fetchone()

        return dict(row)

    def get_volume_by_outcome(self, market_id: Optional[str] = None) -> List[Dict]:
        """Volume breakdown by outcome × side."""
        conn = self._get_conn()
        params = [market_id] if market_id else []
        where = "WHERE market_id = ?" if market_id else ""

        rows = conn.execute(
            f"""
            SELECT
                outcome,
                side,
                COUNT(*)        AS trade_count,
                SUM(amount)     AS volume,
                AVG(price)      AS avg_price
            FROM trades {where}
            GROUP BY outcome, side
            ORDER BY outcome, side
            """,
            params,
        ).fetchall()

        return [dict(r) for r in rows]

    def get_top_traders(
        self,
        market_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """Top traders by total USDC volume traded."""
        conn = self._get_conn()
        params: List = []
        where = ""
        if market_id:
            where = "WHERE t.market_id = ?"
            params.append(market_id)
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT
                t.proxy_wallet,
                tr.name,
                tr.pseudonym,
                tr.profile_image,
                COUNT(t.id)           AS trade_count,
                SUM(t.amount)         AS total_volume,
                SUM(CASE WHEN t.side='BUY'  THEN t.amount ELSE 0 END) AS buy_volume,
                SUM(CASE WHEN t.side='SELL' THEN t.amount ELSE 0 END) AS sell_volume,
                MAX(t.amount)         AS largest_trade,
                MAX(t.match_time)     AS last_trade_time
            FROM trades t
            LEFT JOIN traders tr ON t.proxy_wallet = tr.proxy_wallet
            {where}
            GROUP BY t.proxy_wallet
            ORDER BY total_volume DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

        return [dict(r) for r in rows]

    # ----------------------------------------------------------------
    # CSV Export
    # ----------------------------------------------------------------

    def export_csv(
        self,
        filepath: str,
        market_id: Optional[str] = None,
        min_amount: Optional[float] = None,
        limit: int = 100_000,
    ) -> int:
        """Write trades to a CSV file. Returns the number of rows written."""
        trades = self.get_recent_trades(
            limit=limit, market_id=market_id, min_amount=min_amount
        )
        if not trades:
            return 0

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(trades[0].keys()))
            writer.writeheader()
            writer.writerows(trades)

        return len(trades)

    def export_csv_bytes(
        self,
        market_id: Optional[str] = None,
        min_amount: Optional[float] = None,
        limit: int = 100_000,
    ) -> bytes:
        """Return CSV content as bytes (for HTTP response streaming)."""
        trades = self.get_recent_trades(
            limit=limit, market_id=market_id, min_amount=min_amount
        )
        if not trades:
            return b""

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(trades[0].keys()))
        writer.writeheader()
        writer.writerows(trades)
        return buf.getvalue().encode("utf-8")
