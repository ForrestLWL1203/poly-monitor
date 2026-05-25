from __future__ import annotations

import datetime as dt
import gzip
import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

ACTIVITY_CASHFLOW_TYPES = ("SPLIT", "MERGE", "REDEEM")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_iso(value: Any | None = None) -> str:
    if value is None:
        stamp = utc_now()
    elif isinstance(value, dt.datetime):
        stamp = value
    else:
        raw = str(value)
        try:
            stamp = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    return stamp.astimezone(dt.timezone.utc).isoformat()


def json_dumps(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


WATCHLIST_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS watchlist_wallets (
    wallet TEXT PRIMARY KEY,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watchlist_updated ON watchlist_wallets(updated_at);
"""
WATCHLIST_UPSERT_SQL = """
INSERT INTO watchlist_wallets(wallet,note,created_at,updated_at)
VALUES(?,?,?,?)
ON CONFLICT(wallet) DO UPDATE SET
    note=CASE WHEN excluded.note='' THEN watchlist_wallets.note ELSE excluded.note END,
    updated_at=excluded.updated_at
"""
WATCHLIST_DELETE_SQL = "DELETE FROM watchlist_wallets WHERE wallet=?"
WATCHLIST_WALLETS_SQL = """
SELECT wallet
FROM watchlist_wallets
ORDER BY updated_at DESC, wallet ASC
"""
WATCHLIST_ROWS_SQL = """
SELECT wallet, note, created_at, updated_at
FROM watchlist_wallets
ORDER BY updated_at DESC, wallet ASC
"""


class WatchlistStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(WATCHLIST_SCHEMA_SQL)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def add_wallet(self, wallet: str, *, note: str = "") -> None:
        normalized = wallet.lower()
        now = utc_now().isoformat()
        self.conn.execute(WATCHLIST_UPSERT_SQL, (normalized, note, now, now))
        self.conn.commit()

    def remove_wallet(self, wallet: str) -> int:
        cursor = self.conn.execute(WATCHLIST_DELETE_SQL, (wallet.lower(),))
        self.conn.commit()
        return int(cursor.rowcount or 0)

    def wallets(self) -> list[str]:
        rows = self.conn.execute(WATCHLIST_WALLETS_SQL).fetchall()
        return [str(row["wallet"]).lower() for row in rows]

    def rows(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(WATCHLIST_ROWS_SQL).fetchall()
        return [dict(row) for row in rows]


class JsonlEventWriter:
    def __init__(self, data_dir: Path, *, flush_interval_sec: float = 2.0, buffer_size: int = 65536) -> None:
        self.data_dir = data_dir
        self.flush_interval_sec = flush_interval_sec
        self.buffer_size = buffer_size
        self._current_date: str | None = None
        self._handle = None
        self._last_flush = time.monotonic()

    def write(self, row: dict[str, Any], *, now: dt.datetime | None = None) -> None:
        stamp = now or utc_now()
        date_key = stamp.date().isoformat()
        if date_key != self._current_date:
            self.close()
            path = self.data_dir / "raw" / date_key / "events.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = path.open("a", encoding="utf-8", buffering=self.buffer_size)
            self._current_date = date_key
        assert self._handle is not None
        self._handle.write(json_dumps(row) + "\n")
        if time.monotonic() - self._last_flush >= self.flush_interval_sec:
            self.flush()

    def flush(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            self._last_flush = time.monotonic()

    def close(self) -> None:
        if self._handle is not None:
            self.flush()
            self._handle.close()
            self._handle = None


def cleanup_raw_retention(raw_dir: Path, *, now: dt.date | None = None, retention_days: int = 7) -> None:
    if not raw_dir.exists():
        return
    today = now or dt.datetime.now(dt.timezone.utc).date()
    cutoff = today - dt.timedelta(days=retention_days)
    for child in raw_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            day = dt.date.fromisoformat(child.name)
        except ValueError:
            continue
        if day < cutoff:
            shutil.rmtree(child)
            continue
        if day < today:
            _gzip_raw_events(child / "events.jsonl")


def _gzip_raw_events(path: Path) -> None:
    if not path.exists():
        return
    gz_path = path.with_suffix(path.suffix + ".gz")
    if gz_path.exists():
        path.unlink()
        return
    tmp_path = gz_path.with_suffix(gz_path.suffix + ".tmp")
    try:
        with path.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        tmp_path.replace(gz_path)
        path.unlink()
    except OSError:
        try:
            tmp_path.unlink()
        except OSError:
            pass


class ObserverStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trades (
                tx_hash TEXT NOT NULL,
                fill_id TEXT NOT NULL DEFAULT '',
                wallet TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange_ts INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                side TEXT NOT NULL DEFAULT '',
                price REAL NOT NULL,
                size REAL NOT NULL,
                usdc REAL NOT NULL,
                name TEXT,
                pseudonym TEXT,
                PRIMARY KEY (tx_hash, fill_id, wallet, market_slug, outcome, price, size)
            );
            CREATE TABLE IF NOT EXISTS candidate_scores (
                wallet TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                rank_score REAL NOT NULL,
                metrics_json TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS market_windows (
                symbol TEXT PRIMARY KEY,
                market_slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS market_settlements (
                market_slug TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                winning_side TEXT NOT NULL,
                settlement_open_price REAL,
                settlement_close_price REAL,
                settled_at TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wallet_market_pnl (
                wallet TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                realized_pnl REAL NOT NULL,
                buy_usdc REAL NOT NULL,
                sell_usdc REAL NOT NULL,
                settled_value REAL NOT NULL,
                net_shares_up REAL NOT NULL,
                net_shares_down REAL NOT NULL,
                trades INTEGER NOT NULL,
                winning_side TEXT NOT NULL,
                settled_at TEXT NOT NULL,
                incomplete INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(wallet, market_slug)
            );
            CREATE TABLE IF NOT EXISTS wallet_activity_events (
                tx_hash TEXT NOT NULL,
                wallet TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange_ts INTEGER NOT NULL,
                activity_type TEXT NOT NULL,
                side TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                outcome_index INTEGER NOT NULL DEFAULT -1,
                price REAL NOT NULL DEFAULT 0,
                size REAL NOT NULL DEFAULT 0,
                usdc REAL NOT NULL DEFAULT 0,
                asset TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                pseudonym TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                observed_at TEXT NOT NULL,
                PRIMARY KEY(tx_hash, wallet, condition_id, activity_type, outcome_index, asset, price, size)
            );
            CREATE TABLE IF NOT EXISTS watchlist_market_pnl (
                wallet TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                realized_pnl REAL NOT NULL,
                cash_flow REAL NOT NULL,
                buy_usdc REAL NOT NULL,
                sell_usdc REAL NOT NULL,
                merge_usdc REAL NOT NULL,
                redeem_usdc REAL NOT NULL,
                split_usdc REAL NOT NULL,
                settled_value REAL NOT NULL,
                net_shares_up REAL NOT NULL,
                net_shares_down REAL NOT NULL,
                activity_events INTEGER NOT NULL,
                has_merge INTEGER NOT NULL DEFAULT 0,
                has_redeem INTEGER NOT NULL DEFAULT 0,
                winning_side TEXT NOT NULL DEFAULT '',
                settled_at TEXT NOT NULL DEFAULT '',
                incomplete INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(wallet, market_slug)
            );
            """
        )
        self.conn.executescript(WATCHLIST_SCHEMA_SQL)
        table_info = self.conn.execute("PRAGMA table_info(trades)").fetchall()
        columns = {str(row["name"]) for row in table_info}
        if "fill_id" not in columns:
            self.conn.execute("ALTER TABLE trades ADD COLUMN fill_id TEXT NOT NULL DEFAULT ''")
            table_info = self.conn.execute("PRAGMA table_info(trades)").fetchall()
            columns = {str(row["name"]) for row in table_info}
        if "side" not in columns:
            self.conn.execute("ALTER TABLE trades ADD COLUMN side TEXT NOT NULL DEFAULT ''")
            table_info = self.conn.execute("PRAGMA table_info(trades)").fetchall()
        pnl_table_info = self.conn.execute("PRAGMA table_info(wallet_market_pnl)").fetchall()
        pnl_columns = {str(row["name"]) for row in pnl_table_info}
        if "incomplete" not in pnl_columns:
            self.conn.execute("ALTER TABLE wallet_market_pnl ADD COLUMN incomplete INTEGER NOT NULL DEFAULT 0")
        fill_id_pk = next((int(row["pk"]) for row in table_info if str(row["name"]) == "fill_id"), 0)
        if fill_id_pk == 0:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS trades_new (
                    tx_hash TEXT NOT NULL,
                    fill_id TEXT NOT NULL DEFAULT '',
                    wallet TEXT NOT NULL,
                    market_slug TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    exchange_ts INTEGER NOT NULL,
                    outcome TEXT NOT NULL,
                    side TEXT NOT NULL DEFAULT '',
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    usdc REAL NOT NULL,
                    name TEXT,
                    pseudonym TEXT,
                    PRIMARY KEY (tx_hash, fill_id, wallet, market_slug, outcome, price, size)
                );
                INSERT OR IGNORE INTO trades_new(
                    tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,side,price,size,usdc,name,pseudonym
                )
                SELECT tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,side,price,size,usdc,name,pseudonym
                FROM trades;
                DROP TABLE trades;
                ALTER TABLE trades_new RENAME TO trades;
                """
            )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA mmap_size=268435456")
        self.conn.execute("PRAGMA cache_size=-32000")
        self.conn.execute("PRAGMA wal_autocheckpoint=2000")
        self.conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_trades_wallet_ts ON trades(wallet, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_trades_market_ts ON trades(market_slug, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_trades_condition_ts ON trades(condition_id, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_scores_status_rank ON candidate_scores(status, rank_score DESC);
            CREATE INDEX IF NOT EXISTS idx_scores_status_updated ON candidate_scores(status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_market_windows_slug ON market_windows(market_slug);
            CREATE INDEX IF NOT EXISTS idx_settlements_completed ON market_settlements(completed, settled_at);
            CREATE INDEX IF NOT EXISTS idx_wallet_market_pnl_wallet_settled ON wallet_market_pnl(wallet, settled_at);
            CREATE INDEX IF NOT EXISTS idx_wallet_market_pnl_market ON wallet_market_pnl(market_slug);
            CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_ts ON wallet_activity_events(wallet, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_wallet_activity_market_ts ON wallet_activity_events(market_slug, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_wallet_activity_type ON wallet_activity_events(activity_type, exchange_ts);
            CREATE INDEX IF NOT EXISTS idx_watchlist_market_pnl_wallet_settled ON watchlist_market_pnl(wallet, settled_at);
            CREATE INDEX IF NOT EXISTS idx_watchlist_market_pnl_market ON watchlist_market_pnl(market_slug);
            """
        )
        self._purge_traditional_pnl_shadowed_by_activity_cashflows()
        self.conn.commit()

    def add_watchlist_wallet(self, wallet: str, *, note: str = "") -> None:
        WatchlistStore.add_wallet(self, wallet, note=note)

    def remove_watchlist_wallet(self, wallet: str) -> int:
        return WatchlistStore.remove_wallet(self, wallet)

    def watchlist_wallets(self) -> list[str]:
        return WatchlistStore.wallets(self)

    def watchlist_rows(self) -> list[dict[str, Any]]:
        return WatchlistStore.rows(self)

    def upsert_market_window(
        self,
        *,
        symbol: str,
        market_slug: str,
        condition_id: str,
        window_start: str,
        window_end: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO market_windows(
                symbol, market_slug, condition_id, window_start, window_end, updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                symbol.upper(),
                market_slug,
                condition_id,
                window_start,
                window_end,
                utc_now().isoformat(),
            ),
        )
        self.conn.commit()

    def candidate_status(self, wallet: str) -> str | None:
        row = self.conn.execute("SELECT status FROM candidate_scores WHERE wallet=?", (wallet.lower(),)).fetchone()
        return str(row["status"]) if row else None

    def insert_trade(self, row: dict[str, Any]) -> bool:
        return bool(self.insert_trades([row]))

    def insert_trades(self, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        inserted: list[dict[str, Any]] = []
        for row in rows:
            cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO trades(
                tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,side,price,size,usdc,name,pseudonym
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row["tx_hash"],
                row.get("fill_id", ""),
                row["wallet"],
                row["market_slug"],
                row["condition_id"],
                row["symbol"],
                row["exchange_ts"],
                row["outcome"],
                str(row.get("side") or "").upper(),
                row["price"],
                row["size"],
                row["usdc"],
                row.get("name", ""),
                row.get("pseudonym", ""),
            ),
            )
            if cursor.rowcount:
                inserted.append(row)
        self.conn.commit()
        return inserted

    def insert_wallet_activity_events(self, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        inserted: list[dict[str, Any]] = []
        changed_keys: set[tuple[str, str]] = set()
        for row in rows:
            wallet = str(row.get("wallet") or "").lower()
            market_slug = str(row.get("market_slug") or "")
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO wallet_activity_events(
                    tx_hash,wallet,market_slug,condition_id,symbol,exchange_ts,activity_type,side,outcome,
                    outcome_index,price,size,usdc,asset,name,pseudonym,raw_json,observed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row.get("tx_hash") or ""),
                    wallet,
                    market_slug,
                    str(row.get("condition_id") or ""),
                    str(row.get("symbol") or "").upper(),
                    int(row.get("exchange_ts") or 0),
                    str(row.get("activity_type") or "").upper(),
                    str(row.get("side") or "").upper(),
                    str(row.get("outcome") or ""),
                    int(row.get("outcome_index") if row.get("outcome_index") is not None else -1),
                    float(row.get("price") or 0.0),
                    float(row.get("size") or 0.0),
                    float(row.get("usdc") or 0.0),
                    str(row.get("asset") or ""),
                    str(row.get("name") or ""),
                    str(row.get("pseudonym") or ""),
                    str(row.get("raw_json") or "{}"),
                    utc_iso(row.get("observed_at")),
                ),
            )
            if cursor.rowcount:
                inserted.append(row)
                if wallet and market_slug:
                    changed_keys.add((wallet, market_slug))
        for wallet, market_slug in changed_keys:
            self._recompute_watchlist_activity_pnl(wallet, market_slug)
        self.conn.commit()
        return inserted

    def wallet_activity_events(self, wallet: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM wallet_activity_events WHERE wallet=? ORDER BY exchange_ts ASC, tx_hash ASC"
        params: tuple[Any, ...] = (wallet.lower(),)
        if limit is not None:
            sql += " LIMIT ?"
            params = (wallet.lower(), int(limit))
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def last_wallet_activity_ts(self, wallet: str) -> int:
        row = self.conn.execute(
            "SELECT MAX(exchange_ts) AS last_ts FROM wallet_activity_events WHERE wallet=?",
            (wallet.lower(),),
        ).fetchone()
        return int(row["last_ts"] or 0) if row else 0

    def _recompute_watchlist_activity_pnl(self, wallet: str, market_slug: str) -> None:
        wallet = wallet.lower()
        rows = self.conn.execute(
            """
            SELECT *
            FROM wallet_activity_events
            WHERE wallet=? AND market_slug=?
            ORDER BY exchange_ts ASC, tx_hash ASC
            """,
            (wallet, market_slug),
        ).fetchall()
        if not rows:
            self.conn.execute("DELETE FROM watchlist_market_pnl WHERE wallet=? AND market_slug=?", (wallet, market_slug))
            return
        if self._has_activity_cashflow_rows(wallet, market_slug):
            self.conn.execute("DELETE FROM wallet_market_pnl WHERE wallet=? AND market_slug=?", (wallet, market_slug))
        settlement = self.conn.execute(
            """
            SELECT *
            FROM market_settlements
            WHERE market_slug=? AND completed=1 AND winning_side != ''
            """,
            (market_slug,),
        ).fetchone()
        winning_side = str(settlement["winning_side"] or "") if settlement else ""
        settled_at = str(settlement["settled_at"] or "") if settlement else ""
        condition_id = str(rows[-1]["condition_id"] or "")
        symbol = str(rows[-1]["symbol"] or "").upper()
        cash = buy_usdc = sell_usdc = merge_usdc = redeem_usdc = split_usdc = 0.0
        up = down = 0.0
        has_merge = False
        has_redeem = False
        for row in rows:
            activity_type = str(row["activity_type"] or "").upper()
            side = str(row["side"] or "").upper()
            outcome = str(row["outcome"] or "")
            size = float(row["size"] or 0.0)
            usdc = float(row["usdc"] or 0.0)
            share_key = outcome.lower()
            if activity_type == "TRADE":
                if side == "SELL":
                    cash += usdc
                    sell_usdc += usdc
                    if share_key == "up":
                        up -= size
                    elif share_key == "down":
                        down -= size
                else:
                    cash -= usdc
                    buy_usdc += usdc
                    if share_key == "up":
                        up += size
                    elif share_key == "down":
                        down += size
            elif activity_type == "SPLIT":
                cash -= usdc
                split_usdc += usdc
                up += size
                down += size
            elif activity_type == "MERGE":
                cash += usdc
                merge_usdc += usdc
                up -= size
                down -= size
                has_merge = True
            elif activity_type == "REDEEM":
                cash += usdc
                redeem_usdc += usdc
                has_redeem = True
                if winning_side.lower() == "up":
                    up -= size
                elif winning_side.lower() == "down":
                    down -= size
        settled_value = 0.0
        if winning_side.lower() == "up" and up > 0:
            settled_value = up
            up = 0.0
        elif winning_side.lower() == "down" and down > 0:
            settled_value = down
            down = 0.0
        incomplete = int(up < -1e-6 or down < -1e-6 or (not winning_side and (abs(up) > 1e-6 or abs(down) > 1e-6)))
        realized = cash + settled_value
        self.conn.execute(
            """
            INSERT OR REPLACE INTO watchlist_market_pnl(
                wallet, market_slug, condition_id, symbol, realized_pnl, cash_flow, buy_usdc,
                sell_usdc, merge_usdc, redeem_usdc, split_usdc, settled_value, net_shares_up,
                net_shares_down, activity_events, has_merge, has_redeem, winning_side, settled_at,
                incomplete, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                wallet,
                market_slug,
                condition_id,
                symbol,
                round(realized, 6),
                round(cash, 6),
                round(buy_usdc, 6),
                round(sell_usdc, 6),
                round(merge_usdc, 6),
                round(redeem_usdc, 6),
                round(split_usdc, 6),
                round(settled_value, 6),
                round(up, 6),
                round(down, 6),
                len(rows),
                int(has_merge),
                int(has_redeem),
                winning_side,
                settled_at,
                incomplete,
                utc_now().isoformat(),
            ),
        )

    def _recompute_watchlist_activity_pnl_for_market(self, market_slug: str) -> None:
        rows = self.conn.execute(
            "SELECT DISTINCT wallet FROM wallet_activity_events WHERE market_slug=?",
            (market_slug,),
        ).fetchall()
        for row in rows:
            self._recompute_watchlist_activity_pnl(str(row["wallet"]), market_slug)

    def watchlist_market_pnl_rows(self, wallet: str | None = None) -> list[dict[str, Any]]:
        if wallet is None:
            rows = self.conn.execute("SELECT * FROM watchlist_market_pnl ORDER BY wallet, market_slug").fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM watchlist_market_pnl WHERE wallet=? ORDER BY settled_at DESC, market_slug", (wallet.lower(),)).fetchall()
        return [dict(row) for row in rows]

    def cleanup_wallet_activity_events(
        self,
        *,
        watchlist_cutoff_ts: int,
        non_watchlist_cutoff_ts: int,
    ) -> dict[str, int]:
        stale_pairs = {
            (str(row["wallet"]), str(row["market_slug"]))
            for row in self.conn.execute(
                """
                SELECT wallet, market_slug
                FROM wallet_activity_events
                WHERE (wallet IN (SELECT wallet FROM watchlist_wallets) AND exchange_ts < ?)
                   OR (wallet NOT IN (SELECT wallet FROM watchlist_wallets) AND exchange_ts < ?)
                """,
                (watchlist_cutoff_ts, non_watchlist_cutoff_ts),
            ).fetchall()
        }
        cursor = self.conn.execute(
            """
            DELETE FROM wallet_activity_events
            WHERE (wallet IN (SELECT wallet FROM watchlist_wallets) AND exchange_ts < ?)
               OR (wallet NOT IN (SELECT wallet FROM watchlist_wallets) AND exchange_ts < ?)
            """,
            (watchlist_cutoff_ts, non_watchlist_cutoff_ts),
        )
        removed_activity = int(cursor.rowcount or 0)
        removed_pnl = 0
        for wallet, market_slug in stale_pairs:
            remaining = self.conn.execute(
                "SELECT 1 FROM wallet_activity_events WHERE wallet=? AND market_slug=? LIMIT 1",
                (wallet, market_slug),
            ).fetchone()
            if remaining is None:
                pnl_cursor = self.conn.execute(
                    "DELETE FROM watchlist_market_pnl WHERE wallet=? AND market_slug=?",
                    (wallet, market_slug),
                )
                removed_pnl += int(pnl_cursor.rowcount or 0)
            else:
                self._recompute_watchlist_activity_pnl(wallet, market_slug)
        if removed_activity or removed_pnl:
            self.conn.commit()
        return {
            "removed_activity_events": removed_activity,
            "removed_watchlist_pnl_rows": removed_pnl,
        }

    def recent_wallets(self, *, limit: int = 200) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT wallet, MAX(exchange_ts) AS last_ts
            FROM trades
            GROUP BY wallet
            ORDER BY last_ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [str(row["wallet"]) for row in rows]

    def market_last_exchange_ts(self, condition_id: str) -> int:
        row = self.conn.execute(
            "SELECT MAX(exchange_ts) AS last_ts FROM trades WHERE condition_id=?",
            (condition_id,),
        ).fetchone()
        return int(row["last_ts"] or 0) if row else 0

    def wallet_trade_metrics(self, wallet: str, *, now_ts: int | None = None) -> dict[str, Any]:
        now_value = now_ts if now_ts is not None else int(dt.datetime.now(dt.timezone.utc).timestamp())
        cutoff_7d = now_value - 7 * 86400
        cutoff_30d = now_value - 30 * 86400
        cutoff_24h = now_value - 86400
        row = self.conn.execute(
            """
            SELECT
                SUM(CASE WHEN exchange_ts >= ? THEN 1 ELSE 0 END) AS trades_24h,
                COUNT(DISTINCT CASE WHEN exchange_ts >= ? THEN market_slug END) AS markets_24h,
                SUM(CASE WHEN exchange_ts >= ? THEN 1 ELSE 0 END) AS trades_7d,
                COUNT(DISTINCT CASE WHEN exchange_ts >= ? THEN market_slug END) AS markets_7d,
                SUM(CASE WHEN exchange_ts >= ? THEN 1 ELSE 0 END) AS trades_30d,
                COUNT(DISTINCT CASE WHEN exchange_ts >= ? THEN market_slug END) AS markets_30d,
                COUNT(*) AS historical_trades,
                COUNT(DISTINCT market_slug) AS historical_markets,
                MAX(exchange_ts) AS last_ts
            FROM trades
            WHERE wallet=?
            """,
            (
                cutoff_24h,
                cutoff_24h,
                cutoff_7d,
                cutoff_7d,
                cutoff_30d,
                cutoff_30d,
                wallet.lower(),
            ),
        ).fetchone()
        metrics: dict[str, Any] = {
            "wallet": wallet.lower(),
            "trades_24h": 0,
            "markets_24h": 0,
            "markets_24h_source": "local_observed",
            "trades_7d": 0,
            "markets_7d": 0,
            "trades_30d": 0,
            "markets_30d": 0,
            "pnl_7d": 0.0,
            "pnl_30d": 0.0,
            "wins_7d": 0,
            "losses_7d": 0,
            "top1_concentration": 1.0,
            "top3_concentration": 1.0,
            "longshot_profit_share": 0.0,
            "last_active_age_hours": 999999.0,
            "historical_trades": 0,
            "historical_markets": 0,
            "historical_pnl": 0.0,
        }
        if not row or not int(row["historical_trades"] or 0):
            metrics.update(self._preferred_observed_pnl_metrics(wallet, now_ts=now_value))
            return metrics
        metrics["trades_24h"] = int(row["trades_24h"] or 0)
        metrics["markets_24h"] = int(row["markets_24h"] or 0)
        metrics["trades_7d"] = int(row["trades_7d"] or 0)
        metrics["markets_7d"] = int(row["markets_7d"] or 0)
        metrics["trades_30d"] = int(row["trades_30d"] or 0)
        metrics["markets_30d"] = int(row["markets_30d"] or 0)
        metrics["historical_trades"] = int(row["historical_trades"] or 0)
        metrics["historical_markets"] = int(row["historical_markets"] or 0)
        last_ts = int(row["last_ts"] or 0)
        metrics["last_active_age_hours"] = round((now_value - last_ts) / 3600.0, 3)
        metrics.update(self._preferred_observed_pnl_metrics(wallet, now_ts=now_value))
        return metrics

    def upsert_market_settlement(self, row: dict[str, Any]) -> bool:
        market_slug = str(row["market_slug"])
        completed = 1 if row.get("completed") else 0
        winning_side = str(row.get("winning_side") or "")
        existing = self.conn.execute(
            """
            SELECT winning_side, settlement_open_price, settlement_close_price, completed
            FROM market_settlements
            WHERE market_slug=?
            """,
            (market_slug,),
        ).fetchone()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO market_settlements(
                market_slug, condition_id, symbol, winning_side, settlement_open_price,
                settlement_close_price, settled_at, completed, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                market_slug,
                str(row.get("condition_id") or ""),
                str(row.get("symbol") or "").upper(),
                winning_side,
                row.get("settlement_open_price"),
                row.get("settlement_close_price"),
                utc_iso(row.get("settled_at")),
                completed,
                utc_now().isoformat(),
            ),
        )
        changed = (
            existing is None
            or str(existing["winning_side"] or "") != winning_side
            or float(existing["settlement_open_price"] or 0.0) != float(row.get("settlement_open_price") or 0.0)
            or float(existing["settlement_close_price"] or 0.0) != float(row.get("settlement_close_price") or 0.0)
            or int(existing["completed"] or 0) != completed
        )
        if completed and winning_side:
            self._recompute_market_pnl(market_slug)
            self._recompute_watchlist_activity_pnl_for_market(market_slug)
        self.conn.commit()
        return changed

    def _recompute_market_pnl(self, market_slug: str) -> None:
        settlement = self.conn.execute(
            """
            SELECT * FROM market_settlements
            WHERE market_slug=? AND completed=1 AND winning_side != ''
            """,
            (market_slug,),
        ).fetchone()
        if settlement is None:
            return
        rows = self.conn.execute(
            "SELECT * FROM trades WHERE market_slug=? ORDER BY exchange_ts ASC",
            (market_slug,),
        ).fetchall()
        by_wallet: dict[str, dict[str, Any]] = {}
        for row in rows:
            wallet = str(row["wallet"]).lower()
            item = by_wallet.setdefault(
                wallet,
                {
                    "cash": 0.0,
                    "buy_usdc": 0.0,
                    "sell_usdc": 0.0,
                    "up": 0.0,
                    "down": 0.0,
                    "trades": 0,
                    "condition_id": str(row["condition_id"]),
                    "symbol": str(row["symbol"]).upper(),
                },
            )
            side = str(row["side"] or "").upper()
            outcome = str(row["outcome"] or "")
            size = float(row["size"] or 0.0)
            usdc = float(row["usdc"] or 0.0)
            share_key = "up" if outcome.lower() == "up" else "down"
            if side == "SELL":
                item["cash"] += usdc
                item["sell_usdc"] += usdc
                item[share_key] -= size
            else:
                item["cash"] -= usdc
                item["buy_usdc"] += usdc
                item[share_key] += size
            item["trades"] += 1
        winning_side = str(settlement["winning_side"])
        for wallet, item in by_wallet.items():
            if self._has_activity_cashflow_rows(wallet, market_slug):
                self.conn.execute("DELETE FROM wallet_market_pnl WHERE wallet=? AND market_slug=?", (wallet, market_slug))
                continue
            settled_value = float(item["up"] if winning_side.lower() == "up" else item["down"])
            realized = float(item["cash"]) + settled_value
            incomplete = int(float(item["up"]) < -1e-6 or float(item["down"]) < -1e-6)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO wallet_market_pnl(
                    wallet, market_slug, condition_id, symbol, realized_pnl, buy_usdc, sell_usdc,
                    settled_value, net_shares_up, net_shares_down, trades, winning_side, settled_at,
                    incomplete
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    wallet,
                    market_slug,
                    item["condition_id"],
                    item["symbol"],
                    round(realized, 6),
                    round(float(item["buy_usdc"]), 6),
                    round(float(item["sell_usdc"]), 6),
                    round(settled_value, 6),
                    round(float(item["up"]), 6),
                    round(float(item["down"]), 6),
                    int(item["trades"]),
                    winning_side,
                    str(settlement["settled_at"]),
                    incomplete,
                ),
            )

    def _has_activity_cashflow_rows(self, wallet: str, market_slug: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM wallet_activity_events
            WHERE wallet=? AND market_slug=? AND activity_type IN (?, ?, ?)
            LIMIT 1
            """,
            (wallet.lower(), market_slug, *ACTIVITY_CASHFLOW_TYPES),
        ).fetchone()
        return row is not None

    def _purge_traditional_pnl_shadowed_by_activity_cashflows(self) -> int:
        cursor = self.conn.execute(
            """
            DELETE FROM wallet_market_pnl
            WHERE EXISTS (
                SELECT 1
                FROM wallet_activity_events AS events
                WHERE events.wallet = wallet_market_pnl.wallet
                  AND events.market_slug = wallet_market_pnl.market_slug
                  AND events.activity_type IN (?, ?, ?)
            )
            """,
            ACTIVITY_CASHFLOW_TYPES,
        )
        return int(cursor.rowcount or 0)

    def wallet_market_pnl_rows(self, wallet: str | None = None) -> list[dict[str, Any]]:
        if wallet is None:
            rows = self.conn.execute("SELECT * FROM wallet_market_pnl ORDER BY wallet, market_slug").fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM wallet_market_pnl WHERE wallet=? ORDER BY market_slug", (wallet.lower(),)).fetchall()
        return [dict(row) for row in rows]

    def _preferred_observed_pnl_metrics(self, wallet: str, *, now_ts: int | None = None) -> dict[str, Any]:
        watchlist_metrics = self.watchlist_observed_pnl_metrics(wallet, now_ts=now_ts)
        if watchlist_metrics:
            return watchlist_metrics
        return self.wallet_observed_pnl_metrics(wallet, now_ts=now_ts)

    def watchlist_observed_pnl_metrics(self, wallet: str, *, now_ts: int | None = None) -> dict[str, Any]:
        now_value = now_ts if now_ts is not None else int(dt.datetime.now(dt.timezone.utc).timestamp())
        cutoff_7d_iso = dt.datetime.fromtimestamp(now_value - 7 * 86400, dt.timezone.utc).isoformat()
        cutoff_30d_iso = dt.datetime.fromtimestamp(now_value - 30 * 86400, dt.timezone.utc).isoformat()
        rows_7d = self.conn.execute(
            "SELECT * FROM watchlist_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=0",
            (wallet.lower(), cutoff_7d_iso),
        ).fetchall()
        rows_30d = self.conn.execute(
            "SELECT * FROM watchlist_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=0",
            (wallet.lower(), cutoff_30d_iso),
        ).fetchall()
        rows_total = self.conn.execute(
            "SELECT * FROM watchlist_market_pnl WHERE wallet=? AND incomplete=0",
            (wallet.lower(),),
        ).fetchall()
        all_rows = self.conn.execute(
            "SELECT * FROM watchlist_market_pnl WHERE wallet=?",
            (wallet.lower(),),
        ).fetchall()
        if not all_rows:
            return {}
        incomplete_7d = self.conn.execute(
            "SELECT COUNT(*) AS n FROM watchlist_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=1",
            (wallet.lower(), cutoff_7d_iso),
        ).fetchone()
        incomplete_30d = self.conn.execute(
            "SELECT COUNT(*) AS n FROM watchlist_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=1",
            (wallet.lower(), cutoff_30d_iso),
        ).fetchone()
        incomplete_total = self.conn.execute(
            "SELECT COUNT(*) AS n FROM watchlist_market_pnl WHERE wallet=? AND incomplete=1",
            (wallet.lower(),),
        ).fetchone()

        def pnl(rows: list[sqlite3.Row]) -> float:
            return round(sum(float(row["realized_pnl"] or 0.0) for row in rows), 6)

        pnl_7d = pnl(rows_7d)
        pnl_30d = pnl(rows_30d)
        pnl_total = pnl(rows_total)
        settled_times: list[dt.datetime] = []
        for row in rows_total:
            raw = str(row["settled_at"] or "")
            try:
                settled_times.append(dt.datetime.fromisoformat(raw.replace("Z", "+00:00")))
            except ValueError:
                continue
        first_settled = min(settled_times) if settled_times else None
        observed_span_hours = (
            round((dt.datetime.fromtimestamp(now_value, dt.timezone.utc) - first_settled).total_seconds() / 3600.0, 3)
            if first_settled is not None
            else 0.0
        )
        positive_30d = sorted([float(row["realized_pnl"] or 0.0) for row in rows_30d if float(row["realized_pnl"] or 0.0) > 0], reverse=True)
        positive_total = sum(positive_30d)
        return {
            "pnl_7d": pnl_7d,
            "pnl_30d": pnl_30d,
            "pnl_total": pnl_total,
            "pnl_source": "watchlist_activity_ledger",
            "observed_span_hours": observed_span_hours,
            "wins_7d": sum(1 for row in rows_7d if float(row["realized_pnl"] or 0.0) > 0),
            "losses_7d": sum(1 for row in rows_7d if float(row["realized_pnl"] or 0.0) < 0),
            "settled_markets_7d": len(rows_7d),
            "settled_markets_30d": len(rows_30d),
            "settled_markets_total": len(rows_total),
            "incomplete_settled_markets_7d": int(incomplete_7d["n"] or 0),
            "incomplete_settled_markets_30d": int(incomplete_30d["n"] or 0),
            "incomplete_settled_markets_total": int(incomplete_total["n"] or 0),
            "top1_concentration": round((sum(positive_30d[:1]) / positive_total), 6) if positive_total > 0 else 1.0,
            "top3_concentration": round((sum(positive_30d[:3]) / positive_total), 6) if positive_total > 0 else 1.0,
            "historical_pnl": pnl_total,
        }

    def wallet_observed_pnl_metrics(self, wallet: str, *, now_ts: int | None = None) -> dict[str, Any]:
        now_value = now_ts if now_ts is not None else int(dt.datetime.now(dt.timezone.utc).timestamp())
        cutoff_7d_iso = dt.datetime.fromtimestamp(now_value - 7 * 86400, dt.timezone.utc).isoformat()
        cutoff_30d_iso = dt.datetime.fromtimestamp(now_value - 30 * 86400, dt.timezone.utc).isoformat()
        rows_7d = self.conn.execute(
            "SELECT * FROM wallet_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=0",
            (wallet.lower(), cutoff_7d_iso),
        ).fetchall()
        rows_30d = self.conn.execute(
            "SELECT * FROM wallet_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=0",
            (wallet.lower(), cutoff_30d_iso),
        ).fetchall()
        rows_total = self.conn.execute(
            "SELECT * FROM wallet_market_pnl WHERE wallet=? AND incomplete=0",
            (wallet.lower(),),
        ).fetchall()
        incomplete_7d = self.conn.execute(
            "SELECT COUNT(*) AS n FROM wallet_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=1",
            (wallet.lower(), cutoff_7d_iso),
        ).fetchone()
        incomplete_30d = self.conn.execute(
            "SELECT COUNT(*) AS n FROM wallet_market_pnl WHERE wallet=? AND settled_at >= ? AND incomplete=1",
            (wallet.lower(), cutoff_30d_iso),
        ).fetchone()
        incomplete_total = self.conn.execute(
            "SELECT COUNT(*) AS n FROM wallet_market_pnl WHERE wallet=? AND incomplete=1",
            (wallet.lower(),),
        ).fetchone()

        def pnl(rows: list[sqlite3.Row]) -> float:
            return round(sum(float(row["realized_pnl"] or 0.0) for row in rows), 6)

        pnl_7d = pnl(rows_7d)
        pnl_30d = pnl(rows_30d)
        pnl_total = pnl(rows_total)
        positive_30d = sorted([float(row["realized_pnl"] or 0.0) for row in rows_30d if float(row["realized_pnl"] or 0.0) > 0], reverse=True)
        positive_total = sum(positive_30d)
        settled_times: list[dt.datetime] = []
        for row in rows_total:
            raw = str(row["settled_at"] or "")
            try:
                settled_times.append(dt.datetime.fromisoformat(raw.replace("Z", "+00:00")))
            except ValueError:
                continue
        first_settled = min(settled_times) if settled_times else None
        observed_span_hours = (
            round((dt.datetime.fromtimestamp(now_value, dt.timezone.utc) - first_settled).total_seconds() / 3600.0, 3)
            if first_settled is not None
            else 0.0
        )
        return {
            "pnl_7d": pnl_7d,
            "pnl_30d": pnl_30d,
            "pnl_total": pnl_total,
            "pnl_source": "local_observed_ledger",
            "observed_span_hours": observed_span_hours,
            "wins_7d": sum(1 for row in rows_7d if float(row["realized_pnl"] or 0.0) > 0),
            "losses_7d": sum(1 for row in rows_7d if float(row["realized_pnl"] or 0.0) < 0),
            "settled_markets_7d": len(rows_7d),
            "settled_markets_30d": len(rows_30d),
            "settled_markets_total": len(rows_total),
            "incomplete_settled_markets_7d": int(incomplete_7d["n"] or 0),
            "incomplete_settled_markets_30d": int(incomplete_30d["n"] or 0),
            "incomplete_settled_markets_total": int(incomplete_total["n"] or 0),
            "top1_concentration": round((sum(positive_30d[:1]) / positive_total), 6) if positive_total > 0 else 1.0,
            "top3_concentration": round((sum(positive_30d[:3]) / positive_total), 6) if positive_total > 0 else 1.0,
            "historical_pnl": pnl_total,
        }

    def wallet_24h_counts(self, wallet: str, *, now_ts: int | None = None) -> dict[str, int]:
        now_value = now_ts if now_ts is not None else int(dt.datetime.now(dt.timezone.utc).timestamp())
        cutoff_24h = now_value - 86400
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS trades_24h,
                COUNT(DISTINCT market_slug) AS markets_24h
            FROM trades
            WHERE wallet=? AND exchange_ts >= ?
            """,
            (wallet.lower(), cutoff_24h),
        ).fetchone()
        return {
            "trades_24h": int(row["trades_24h"] or 0) if row else 0,
            "markets_24h": int(row["markets_24h"] or 0) if row else 0,
        }

    def upsert_score(self, score) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO candidate_scores(wallet,status,rank_score,metrics_json,reasons_json,updated_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                score.wallet,
                score.status,
                score.rank_score,
                json_dumps(score.metrics),
                json.dumps(score.reasons, separators=(",", ":")),
                utc_now().isoformat(),
            ),
        )
        self.conn.commit()

    def delete_candidate_score(self, wallet: str) -> int:
        cursor = self.conn.execute("DELETE FROM candidate_scores WHERE wallet=?", (wallet.lower(),))
        self.conn.commit()
        return int(cursor.rowcount or 0)

    def candidate_rows(self, *, limit: int = 30) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {
            "active_candidate": [],
            "dormant_candidate": [],
            "archive_candidate": [],
        }
        rows = self.conn.execute(
            """
            SELECT * FROM candidate_scores
            ORDER BY
                CASE status
                    WHEN 'active_candidate' THEN 0
                    WHEN 'dormant_candidate' THEN 1
                    ELSE 2
                END,
                rank_score DESC,
                updated_at DESC
            """
        ).fetchall()
        for row in rows:
            status = str(row["status"])
            item = {
                "wallet": row["wallet"],
                "status": status,
                "rank_score": row["rank_score"],
                "metrics": json.loads(row["metrics_json"]),
                "reasons": json.loads(row["reasons_json"]),
                "updated_at": row["updated_at"],
            }
            bucket = out.setdefault(status, [])
            if status == "active_candidate" and len(bucket) >= limit:
                continue
            bucket.append(item)
        return out

    def candidate_wallets(self, status: str, *, limit: int) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT wallet
            FROM candidate_scores
            WHERE status=?
            ORDER BY rank_score DESC, updated_at DESC, wallet ASC
            LIMIT ?
            """,
            (status, limit),
        ).fetchall()
        return [str(row["wallet"]).lower() for row in rows]

    def candidate_wallets_due(self, status: str, *, limit: int, min_age_seconds: float, now: dt.datetime | None = None) -> list[str]:
        cutoff = (now or utc_now()) - dt.timedelta(seconds=max(0.0, min_age_seconds))
        rows = self.conn.execute(
            """
            SELECT wallet
            FROM candidate_scores
            WHERE status=? AND updated_at<=?
            ORDER BY rank_score DESC, updated_at ASC, wallet ASC
            LIMIT ?
            """,
            (status, cutoff.isoformat(), limit),
        ).fetchall()
        return [str(row["wallet"]).lower() for row in rows]

    def candidate_statuses(self, wallets: Iterable[str]) -> dict[str, str]:
        normalized = [wallet.lower() for wallet in wallets]
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        rows = self.conn.execute(
            f"SELECT wallet, status FROM candidate_scores WHERE wallet IN ({placeholders})",
            normalized,
        ).fetchall()
        return {str(row["wallet"]).lower(): str(row["status"]) for row in rows}

    def reactivatable_archive_wallets(
        self,
        *,
        limit: int,
        now: dt.datetime | None = None,
        min_trades_24h: int,
        min_markets_24h: int,
        min_age_seconds: float,
    ) -> list[str]:
        now_value = now or utc_now()
        now_ts = int(now_value.timestamp())
        cutoff_24h = now_ts - 86400
        updated_at_cutoff = now_value - dt.timedelta(seconds=max(0.0, min_age_seconds))
        rows = self.conn.execute(
            """
            SELECT
                trades.wallet AS wallet,
                COUNT(*) AS trades_24h,
                COUNT(DISTINCT trades.market_slug) AS markets_24h,
                MAX(trades.exchange_ts) AS last_ts
            FROM trades
            JOIN candidate_scores AS scores ON scores.wallet = trades.wallet
            WHERE scores.status='archive_candidate'
              AND scores.updated_at <= ?
              AND trades.exchange_ts >= ?
            GROUP BY trades.wallet
            HAVING trades_24h >= ? OR markets_24h >= ?
            ORDER BY last_ts DESC, trades.wallet ASC
            LIMIT ?
            """,
            (updated_at_cutoff.isoformat(), cutoff_24h, min_trades_24h, min_markets_24h, limit),
        ).fetchall()
        return [str(row["wallet"]).lower() for row in rows]

    def prune_candidate_scores(
        self,
        status: str,
        *,
        max_rows: int,
        min_age_seconds: float = 0.0,
        now: dt.datetime | None = None,
    ) -> int:
        cutoff = (now or utc_now()) - dt.timedelta(seconds=max(0.0, min_age_seconds))
        rows = self.conn.execute(
            """
            SELECT wallet, updated_at
            FROM candidate_scores
            WHERE status=?
            ORDER BY rank_score DESC, updated_at DESC, wallet ASC
            """,
            (status,),
        ).fetchall()
        watchlist = set(self.watchlist_wallets())
        protected = [row for row in rows if str(row["wallet"]).lower() in watchlist or str(row["updated_at"]) > cutoff.isoformat()]
        removable = [row for row in rows if str(row["wallet"]).lower() not in watchlist and str(row["updated_at"]) <= cutoff.isoformat()]
        # Cooldown rows are protected even if that temporarily lets archive rows exceed max_rows.
        keep_removable = max(0, max_rows - len(protected))
        doomed = [str(row["wallet"]) for row in removable][keep_removable:]
        if not doomed:
            return 0
        self.conn.executemany("DELETE FROM candidate_scores WHERE wallet=?", [(wallet,) for wallet in doomed])
        self.conn.commit()
        return len(doomed)

    def prune_low_sample_archives(
        self,
        *,
        min_age_seconds: float = 0.0,
        now: dt.datetime | None = None,
    ) -> int:
        cutoff = (now or utc_now()) - dt.timedelta(seconds=max(0.0, min_age_seconds))
        watchlist = set(self.watchlist_wallets())
        rows = self.conn.execute(
            "SELECT wallet, metrics_json, updated_at FROM candidate_scores WHERE status='archive_candidate'"
        ).fetchall()
        doomed: list[str] = []
        for row in rows:
            wallet = str(row["wallet"]).lower()
            if wallet in watchlist:
                continue
            if str(row["updated_at"]) > cutoff.isoformat():
                continue
            try:
                metrics = json.loads(row["metrics_json"])
            except (TypeError, json.JSONDecodeError):
                metrics = {}
            markets_24h = float(metrics.get("markets_24h") or metrics.get("markets_7d") or 0)
            enough_sample = (
                float(metrics.get("trades_7d") or 0) >= 100
                or markets_24h >= 3
                or float(metrics.get("historical_trades") or 0) >= 300
            )
            if not enough_sample:
                doomed.append(wallet)
        if not doomed:
            return 0
        self.conn.executemany("DELETE FROM candidate_scores WHERE wallet=?", [(wallet,) for wallet in doomed])
        self.conn.commit()
        return len(doomed)

    def prune_archive_scores(self, *, max_archive: int = 50, min_age_seconds: float = 0.0) -> int:
        return self.prune_candidate_scores(
            "archive_candidate",
            max_rows=max_archive,
            min_age_seconds=min_age_seconds,
        )

    def cleanup_inactive_wallet_data(
        self,
        *,
        inactive_cutoff_ts: int,
        max_non_candidate_wallets: int | None = None,
    ) -> dict[str, int]:
        self.conn.execute("CREATE TEMP TABLE IF NOT EXISTS cleanup_keep(wallet TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TEMP TABLE IF NOT EXISTS cleanup_delete_wallets(wallet TEXT PRIMARY KEY)")
        self.conn.execute("DELETE FROM cleanup_keep")
        self.conn.execute("DELETE FROM cleanup_delete_wallets")
        self.conn.executescript(
            """
            INSERT OR IGNORE INTO cleanup_keep(wallet)
            SELECT wallet FROM candidate_scores WHERE status IN ('active_candidate','dormant_candidate');
            INSERT OR IGNORE INTO cleanup_keep(wallet)
            SELECT wallet FROM watchlist_wallets;
            """
        )
        self.conn.execute(
            """
            INSERT OR IGNORE INTO cleanup_delete_wallets(wallet)
            SELECT recent.wallet
            FROM (
                SELECT wallet, MAX(exchange_ts) AS last_ts
                FROM trades
                GROUP BY wallet
                HAVING last_ts < ?
            ) AS recent
            LEFT JOIN cleanup_keep AS keep ON keep.wallet = recent.wallet
            WHERE keep.wallet IS NULL
            """,
            (inactive_cutoff_ts,),
        )
        if max_non_candidate_wallets is not None and max_non_candidate_wallets >= 0:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO cleanup_delete_wallets(wallet)
                SELECT wallet
                FROM (
                    SELECT
                        trades.wallet AS wallet,
                        ROW_NUMBER() OVER (ORDER BY MAX(exchange_ts) DESC, trades.wallet ASC) AS rn
                    FROM trades
                    LEFT JOIN cleanup_keep AS keep ON keep.wallet = trades.wallet
                    WHERE keep.wallet IS NULL
                    GROUP BY trades.wallet
                )
                WHERE rn > ?
                """,
                (max_non_candidate_wallets,),
            )

        removed_wallets = int(self.conn.execute("SELECT COUNT(*) AS n FROM cleanup_delete_wallets").fetchone()["n"] or 0)
        removed_trades = int(
            self.conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM trades
                WHERE wallet IN (SELECT wallet FROM cleanup_delete_wallets)
                """
            ).fetchone()["n"]
            or 0
        )
        removed_score_rows = int(
            self.conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM candidate_scores
                WHERE status NOT IN ('active_candidate','dormant_candidate')
                  AND wallet NOT IN (SELECT wallet FROM cleanup_keep)
                """
            ).fetchone()["n"]
            or 0
        )
        if removed_trades:
            self.conn.execute("DELETE FROM trades WHERE wallet IN (SELECT wallet FROM cleanup_delete_wallets)")
        if removed_score_rows:
            self.conn.execute(
                """
                DELETE FROM candidate_scores
                WHERE status NOT IN ('active_candidate','dormant_candidate')
                  AND wallet NOT IN (SELECT wallet FROM cleanup_keep)
                """
            )
        if removed_wallets or removed_score_rows:
            self.conn.commit()
        return {
            "removed_wallets": removed_wallets,
            "removed_trades": removed_trades,
            "removed_score_rows": removed_score_rows,
        }


def write_latest_candidates(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
