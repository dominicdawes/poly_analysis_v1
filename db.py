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
        self._extend_schema()

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

    def _extend_schema(self):
        """Add Phase 2 tables (wallets, markets, positions).  Non-destructive."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wallets (
                address              TEXT    PRIMARY KEY,
                name                 TEXT,
                pseudonym            TEXT,
                profile_image        TEXT,
                bio                  TEXT,
                first_seen           INTEGER,
                last_seen            INTEGER,
                total_trades         INTEGER DEFAULT 0,
                total_volume         REAL    DEFAULT 0.0,
                total_buy_volume     REAL    DEFAULT 0.0,
                total_sell_volume    REAL    DEFAULT 0.0,
                largest_trade        REAL    DEFAULT 0.0,
                avg_trade_size       REAL    DEFAULT 0.0,
                num_active_positions INTEGER DEFAULT 0,
                win_rate             REAL,
                realized_pnl         REAL    DEFAULT 0.0,
                last_updated         TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS markets (
                condition_id     TEXT    PRIMARY KEY,
                title            TEXT,
                slug             TEXT,
                icon             TEXT,
                description      TEXT,
                category         TEXT,
                end_date         TEXT,
                resolved         INTEGER DEFAULT 0,
                winning_outcome  TEXT,
                last_fetched     TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS positions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address   TEXT    NOT NULL REFERENCES wallets(address),
                condition_id     TEXT    NOT NULL REFERENCES markets(condition_id),
                outcome          TEXT    NOT NULL,
                net_shares       REAL    DEFAULT 0.0,
                avg_entry_price  REAL    DEFAULT 0.0,
                total_bought     REAL    DEFAULT 0.0,
                total_sold       REAL    DEFAULT 0.0,
                realized_pnl     REAL    DEFAULT 0.0,
                last_updated     TEXT    NOT NULL,
                UNIQUE(wallet_address, condition_id, outcome)
            );

            CREATE INDEX IF NOT EXISTS idx_wallets_volume   ON wallets(total_volume DESC);
            CREATE INDEX IF NOT EXISTS idx_wallets_pnl      ON wallets(realized_pnl DESC);
            CREATE INDEX IF NOT EXISTS idx_positions_wallet ON positions(wallet_address);
            CREATE INDEX IF NOT EXISTS idx_positions_market ON positions(condition_id);
        """)

        conn.commit()
        conn.close()

    def verify_schema(self) -> bool:
        """Return True if all required tables exist."""
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        existing = {r[0] for r in rows}
        return {"trades", "traders", "wallets", "markets", "positions"}.issubset(existing)

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

    # ================================================================
    # Phase 2 — Wallets
    # ================================================================

    def upsert_wallet(self, wallet: Dict) -> None:
        """Insert or update an aggregated wallet row."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO wallets (
                    address, name, pseudonym, profile_image, bio,
                    first_seen, last_seen, total_trades, total_volume,
                    total_buy_volume, total_sell_volume, largest_trade,
                    avg_trade_size, num_active_positions, win_rate,
                    realized_pnl, last_updated
                ) VALUES (
                    :address, :name, :pseudonym, :profile_image, :bio,
                    :first_seen, :last_seen, :total_trades, :total_volume,
                    :total_buy_volume, :total_sell_volume, :largest_trade,
                    :avg_trade_size, :num_active_positions, :win_rate,
                    :realized_pnl, :last_updated
                )
                ON CONFLICT(address) DO UPDATE SET
                    name             = COALESCE(excluded.name,          wallets.name),
                    pseudonym        = COALESCE(excluded.pseudonym,     wallets.pseudonym),
                    profile_image    = COALESCE(excluded.profile_image, wallets.profile_image),
                    bio              = COALESCE(excluded.bio,           wallets.bio),
                    first_seen       = excluded.first_seen,
                    last_seen        = excluded.last_seen,
                    total_trades     = excluded.total_trades,
                    total_volume     = excluded.total_volume,
                    total_buy_volume = excluded.total_buy_volume,
                    total_sell_volume= excluded.total_sell_volume,
                    largest_trade    = excluded.largest_trade,
                    avg_trade_size   = excluded.avg_trade_size,
                    num_active_positions = excluded.num_active_positions,
                    win_rate         = excluded.win_rate,
                    realized_pnl     = excluded.realized_pnl,
                    last_updated     = excluded.last_updated
                """,
                wallet,
            )
            conn.commit()
        except Exception as exc:
            logger.error("upsert_wallet failed: %s", exc)
            conn.rollback()

    def get_wallet(self, address: str) -> Optional[Dict]:
        """Return the wallets row for an address, or None."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM wallets WHERE address = ?", (address,)
        ).fetchone()
        return dict(row) if row else None

    def get_wallets(
        self,
        limit: int = 50,
        order_by: str = "total_volume",
        min_volume: Optional[float] = None,
    ) -> List[Dict]:
        """Top wallets sorted by the given column (SQL-injection safe)."""
        _allowed = {"total_volume", "realized_pnl", "total_trades", "last_seen"}
        if order_by not in _allowed:
            order_by = "total_volume"
        conn = self._get_conn()
        params: List = []
        where = ""
        if min_volume is not None:
            where = "WHERE total_volume >= ?"
            params.append(min_volume)
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM wallets {where} ORDER BY {order_by} DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def get_trader(self, proxy_wallet: str) -> Optional[Dict]:
        """Return the traders row for this wallet (profile snapshot from ingestion)."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM traders WHERE proxy_wallet = ?", (proxy_wallet,)
        ).fetchone()
        return dict(row) if row else None

    def get_distinct_wallets_from_trades(self) -> List[str]:
        """All distinct proxy_wallet values seen in the trades table."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT DISTINCT proxy_wallet FROM trades"
        ).fetchall()
        return [r[0] for r in rows]

    def get_trades_for_wallet(self, address: str) -> List[Dict]:
        """
        All trades for a wallet, sorted oldest-first.
        Used by wallet_analyzer for FIFO PnL computation.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT market_id, outcome, side, price, size, amount, match_time
            FROM trades
            WHERE proxy_wallet = ?
            ORDER BY match_time ASC
            """,
            (address,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ================================================================
    # Phase 2 — Positions
    # ================================================================

    def upsert_position(self, position: Dict) -> None:
        """Insert or replace a wallet position row."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO positions (
                    wallet_address, condition_id, outcome,
                    net_shares, avg_entry_price, total_bought, total_sold,
                    realized_pnl, last_updated
                ) VALUES (
                    :wallet_address, :condition_id, :outcome,
                    :net_shares, :avg_entry_price, :total_bought, :total_sold,
                    :realized_pnl, :last_updated
                )
                ON CONFLICT(wallet_address, condition_id, outcome) DO UPDATE SET
                    net_shares      = excluded.net_shares,
                    avg_entry_price = excluded.avg_entry_price,
                    total_bought    = excluded.total_bought,
                    total_sold      = excluded.total_sold,
                    realized_pnl    = excluded.realized_pnl,
                    last_updated    = excluded.last_updated
                """,
                position,
            )
            conn.commit()
        except Exception as exc:
            logger.error("upsert_position failed: %s", exc)
            conn.rollback()

    def get_positions_for_wallet(self, address: str) -> List[Dict]:
        """All positions for a wallet, joined with market title."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT p.*, m.title AS market_title, m.slug AS market_slug
            FROM positions p
            LEFT JOIN markets m ON p.condition_id = m.condition_id
            WHERE p.wallet_address = ?
            ORDER BY p.net_shares DESC, p.realized_pnl DESC
            """,
            (address,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ================================================================
    # Phase 2 — Markets
    # ================================================================

    def upsert_market(self, market: Dict) -> None:
        """Insert or update a market metadata row."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO markets (
                    condition_id, title, slug, icon, description, category,
                    end_date, resolved, winning_outcome, last_fetched
                ) VALUES (
                    :condition_id, :title, :slug, :icon, :description, :category,
                    :end_date, :resolved, :winning_outcome, :last_fetched
                )
                ON CONFLICT(condition_id) DO UPDATE SET
                    title           = COALESCE(excluded.title,           markets.title),
                    slug            = COALESCE(excluded.slug,            markets.slug),
                    icon            = COALESCE(excluded.icon,            markets.icon),
                    description     = COALESCE(excluded.description,     markets.description),
                    category        = COALESCE(excluded.category,        markets.category),
                    end_date        = COALESCE(excluded.end_date,        markets.end_date),
                    resolved        = excluded.resolved,
                    winning_outcome = COALESCE(excluded.winning_outcome, markets.winning_outcome),
                    last_fetched    = excluded.last_fetched
                """,
                market,
            )
            conn.commit()
        except Exception as exc:
            logger.error("upsert_market failed: %s", exc)
            conn.rollback()

    def get_market(self, condition_id: str) -> Optional[Dict]:
        """Return the markets row for a condition ID, or None."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM markets WHERE condition_id = ?", (condition_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_markets(self, limit: int = 50) -> List[Dict]:
        """Return market rows ordered alphabetically by title."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM markets ORDER BY title ASC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_distinct_markets_from_trades(self) -> List[str]:
        """All distinct market_id values seen in the trades table."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT DISTINCT market_id FROM trades"
        ).fetchall()
        return [r[0] for r in rows]
