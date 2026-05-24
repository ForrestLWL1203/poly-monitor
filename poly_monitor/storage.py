from __future__ import annotations

import datetime as dt
import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def json_dumps(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


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
            """
        )
        table_info = self.conn.execute("PRAGMA table_info(trades)").fetchall()
        columns = {str(row["name"]) for row in table_info}
        if "fill_id" not in columns:
            self.conn.execute("ALTER TABLE trades ADD COLUMN fill_id TEXT NOT NULL DEFAULT ''")
            table_info = self.conn.execute("PRAGMA table_info(trades)").fetchall()
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
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    usdc REAL NOT NULL,
                    name TEXT,
                    pseudonym TEXT,
                    PRIMARY KEY (tx_hash, fill_id, wallet, market_slug, outcome, price, size)
                );
                INSERT OR IGNORE INTO trades_new(
                    tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,price,size,usdc,name,pseudonym
                )
                SELECT tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,price,size,usdc,name,pseudonym
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
            """
        )
        self.conn.commit()

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
                tx_hash,fill_id,wallet,market_slug,condition_id,symbol,exchange_ts,outcome,price,size,usdc,name,pseudonym
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        # Realized PnL requires settlement. Until closed-position refresh is added,
        # use zero so wallets discovered from live-only flow do not get promoted prematurely.
        return metrics

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
        protected = [row for row in rows if str(row["updated_at"]) > cutoff.isoformat()]
        removable = [row for row in rows if str(row["updated_at"]) <= cutoff.isoformat()]
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
        rows = self.conn.execute(
            "SELECT wallet, metrics_json, updated_at FROM candidate_scores WHERE status='archive_candidate'"
        ).fetchall()
        doomed: list[str] = []
        for row in rows:
            wallet = str(row["wallet"]).lower()
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
