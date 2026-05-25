from __future__ import annotations

import datetime as dt
import json
import sqlite3
import zipfile
from pathlib import Path
from typing import Any, Iterable

from .storage import json_dumps, utc_iso


def _connect(data_dir: Path) -> sqlite3.Connection:
    path = data_dir / "state" / "observer.sqlite"
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json_dumps(row) + "\n")
            count += 1
    return count


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_iso(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _slug_window_start(slug: str) -> dt.datetime | None:
    try:
        epoch = int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None
    if epoch <= 0:
        return None
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc)


def _window_metadata(
    conn: sqlite3.Connection,
    *,
    slug: str,
    watched: dict[str, Any] | None,
    activity_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    settlement = conn.execute("SELECT * FROM market_settlements WHERE market_slug=?", (slug,)).fetchone()
    if watched:
        start = _parse_iso(watched.get("window_start"))
        end = _parse_iso(watched.get("window_end"))
        condition_id = str(watched.get("condition_id") or "")
        symbol = str(watched.get("symbol") or "")
    else:
        start = _slug_window_start(slug)
        end = start + dt.timedelta(seconds=300) if start else None
        condition_id = str((activity_rows or trade_rows or [{}])[0].get("condition_id") or "")
        symbol = str((activity_rows or trade_rows or [{}])[0].get("symbol") or "")
    return {
        "market_slug": slug,
        "condition_id": condition_id,
        "symbol": symbol,
        "window_start": start.isoformat() if start else None,
        "window_end": end.isoformat() if end else None,
        "watched_market": watched or None,
        "settlement": dict(settlement) if settlement else None,
    }


def _sample_gap_seconds(rows: list[dict[str, Any]]) -> float | None:
    values = sorted(int(row.get("sampled_ts") or 0) for row in rows if int(row.get("sampled_ts") or 0) > 0)
    if len(values) < 2:
        return None
    return max(float(values[idx] - values[idx - 1]) for idx in range(1, len(values)))


def _coverage_summary(
    *,
    metadata: dict[str, Any],
    wallet_activity: list[dict[str, Any]],
    wallet_trades: list[dict[str, Any]],
    market_trades: list[dict[str, Any]],
    contexts: list[dict[str, Any]],
    samples: list[dict[str, Any]],
    wallet_pnl: list[dict[str, Any]],
) -> dict[str, Any]:
    watched = metadata.get("watched_market") if isinstance(metadata.get("watched_market"), dict) else {}
    first_seen = _parse_iso(watched.get("first_seen_at"))
    window_start = _parse_iso(metadata.get("window_start"))
    window_end = _parse_iso(metadata.get("window_end"))
    first_seen_delay = None
    if first_seen is not None and window_start is not None:
        first_seen_delay = round((first_seen - window_start).total_seconds(), 3)
    settlement = metadata.get("settlement") if isinstance(metadata.get("settlement"), dict) else None
    settlement_complete = bool(settlement and settlement.get("completed") and settlement.get("winning_side"))
    has_redeem = any(str(row.get("activity_type") or "").upper() == "REDEEM" for row in wallet_activity)
    insufficient = (
        not watched
        or first_seen_delay is None
        or first_seen_delay > 30.0
        or not market_trades
        or not samples
        or not settlement_complete
    )
    if window_end is not None and dt.datetime.now(dt.timezone.utc) < window_end:
        insufficient = True
    return {
        "market_slug": metadata["market_slug"],
        "condition_id": metadata.get("condition_id") or "",
        "symbol": metadata.get("symbol") or "",
        "window_start": metadata.get("window_start"),
        "window_end": metadata.get("window_end"),
        "first_seen_at": watched.get("first_seen_at") if watched else None,
        "first_seen_delay_sec": first_seen_delay,
        "captured_from_window_early": first_seen_delay is not None and first_seen_delay <= 30.0,
        "market_trade_rows": len(market_trades),
        "wallet_trade_rows": len(wallet_trades),
        "wallet_activity_rows": len(wallet_activity),
        "wallet_trade_context_rows": len(contexts),
        "market_state_sample_rows": len(samples),
        "max_market_state_sample_gap_sec": _sample_gap_seconds(samples),
        "wallet_pnl_rows": len(wallet_pnl),
        "settlement_complete": settlement_complete,
        "has_redeem": has_redeem,
        "insufficient_market_capture": insufficient,
    }


def _zip_dir(source_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(source_dir.rglob("*")):
            if path == zip_path or not path.is_file():
                continue
            bundle.write(path, path.relative_to(source_dir))


def export_watchlist_wallet(
    wallet: str,
    *,
    data_dir: Path,
    out_dir: Path | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    wallet = wallet.lower()
    now_dt = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    stamp = now_dt.strftime("%Y%m%d-%H%M%S")
    target = (out_dir or data_dir / "exports" / wallet / stamp).resolve()
    target.mkdir(parents=True, exist_ok=True)
    conn = _connect(data_dir)
    try:
        watch = conn.execute("SELECT 1 FROM watchlist_wallets WHERE wallet=?", (wallet,)).fetchone()
        if watch is None:
            raise ValueError("wallet_not_watchlisted")
        activity = _rows(
            conn,
            "SELECT * FROM wallet_activity_events WHERE wallet=? ORDER BY exchange_ts ASC, tx_hash ASC",
            (wallet,),
        )
        wallet_trades = _rows(
            conn,
            "SELECT * FROM trades WHERE wallet=? ORDER BY exchange_ts ASC, tx_hash ASC",
            (wallet,),
        )
        wallet_pnl = _rows(
            conn,
            "SELECT * FROM wallet_market_pnl WHERE wallet=? ORDER BY settled_at ASC, market_slug ASC",
            (wallet,),
        )
        watchlist_pnl = (
            _rows(
                conn,
                "SELECT * FROM watchlist_market_pnl WHERE wallet=? ORDER BY settled_at ASC, market_slug ASC",
                (wallet,),
            )
            if _table_exists(conn, "watchlist_market_pnl")
            else []
        )
        slugs = {
            str(row.get("market_slug") or "")
            for row in [*activity, *wallet_trades, *wallet_pnl, *watchlist_pnl]
            if row.get("market_slug")
        }
        if _table_exists(conn, "watched_market_windows"):
            if slugs:
                placeholders = ",".join("?" for _ in slugs)
                watched_query = f"SELECT * FROM watched_market_windows WHERE source_wallet=? OR market_slug IN ({placeholders})"
                watched_params: tuple[Any, ...] = (wallet, *sorted(slugs))
            else:
                watched_query = "SELECT * FROM watched_market_windows WHERE source_wallet=?"
                watched_params = (wallet,)
            watched_rows = {
                str(row["market_slug"]): dict(row)
                for row in conn.execute(watched_query, watched_params).fetchall()
            }
        else:
            watched_rows = {}
        slugs.update(watched_rows)

        root_counts = {
            "wallet_activity_rows": _write_jsonl(target / "wallet_activity.jsonl", activity),
            "wallet_trades_rows": _write_jsonl(target / "wallet_trades.jsonl", wallet_trades),
            "wallet_market_pnl_rows": _write_jsonl(target / "wallet_market_pnl.jsonl", wallet_pnl),
            "watchlist_market_pnl_rows": _write_jsonl(target / "watchlist_market_pnl.jsonl", watchlist_pnl),
        }
        windows: list[dict[str, Any]] = []
        for slug in sorted(slugs):
            market_dir = target / "markets" / slug
            market_activity = [row for row in activity if row.get("market_slug") == slug]
            market_wallet_trades = [row for row in wallet_trades if row.get("market_slug") == slug]
            market_wallet_pnl = [row for row in [*wallet_pnl, *watchlist_pnl] if row.get("market_slug") == slug]
            market_trades = _rows(
                conn,
                "SELECT * FROM trades WHERE market_slug=? ORDER BY exchange_ts ASC, tx_hash ASC",
                (slug,),
            )
            contexts = _rows(
                conn,
                "SELECT * FROM wallet_trade_contexts WHERE wallet=? AND market_slug=? ORDER BY exchange_ts ASC, tx_hash ASC",
                (wallet, slug),
            )
            samples = _rows(
                conn,
                "SELECT * FROM market_state_samples WHERE market_slug=? ORDER BY sampled_ts ASC",
                (slug,),
            )
            metadata = _window_metadata(
                conn,
                slug=slug,
                watched=watched_rows.get(slug),
                activity_rows=market_activity,
                trade_rows=market_trades,
            )
            _write_jsonl(market_dir / "wallet_activity.jsonl", market_activity)
            _write_jsonl(market_dir / "wallet_trades.jsonl", market_wallet_trades)
            _write_jsonl(market_dir / "wallet_market_pnl.jsonl", market_wallet_pnl)
            _write_jsonl(market_dir / "market_trades.jsonl", market_trades)
            _write_jsonl(market_dir / "wallet_trade_contexts.jsonl", contexts)
            _write_jsonl(market_dir / "market_state_samples.jsonl", samples)
            _write_json(market_dir / "metadata.json", metadata)
            _write_json(market_dir / "settlement.json", metadata.get("settlement") or {})
            windows.append(
                _coverage_summary(
                    metadata=metadata,
                    wallet_activity=market_activity,
                    wallet_trades=market_wallet_trades,
                    market_trades=market_trades,
                    contexts=contexts,
                    samples=samples,
                    wallet_pnl=market_wallet_pnl,
                )
            )
        manifest = {
            "wallet": wallet,
            "generated_at": utc_iso(now_dt),
            "data_dir": str(data_dir),
            "export_dir": str(target),
            "policy": "local_capture_only_no_historical_market_backfill",
            "window_count": len(windows),
            "root_counts": root_counts,
            "windows": windows,
            "insufficient_market_capture": any(row["insufficient_market_capture"] for row in windows),
        }
        manifest_path = target / "manifest.json"
        _write_json(manifest_path, manifest)
        zip_path = target / "bundle.zip"
        _zip_dir(target, zip_path)
        return {
            "wallet": wallet,
            "export_dir": str(target),
            "manifest_path": str(manifest_path),
            "zip_path": str(zip_path),
            "window_count": len(windows),
            "zip_bytes": zip_path.stat().st_size,
            "insufficient_market_capture": manifest["insufficient_market_capture"],
        }
    finally:
        conn.close()
