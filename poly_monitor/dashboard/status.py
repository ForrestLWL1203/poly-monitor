from __future__ import annotations

import datetime as dt
import gzip
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


CANDIDATE_GROUPS = ("active_candidate", "dormant_candidate", "archive_candidate")
SCORED_GROUPS = ("active_candidate", "dormant_candidate", "archive_candidate")
MAX_ARCHIVE_DISPLAY = 0
MAX_ACTIVE_DISPLAY = 15
MAX_DORMANT_DISPLAY = 10
_SCORE_REQUIRED = frozenset(("pnl_7d", "pnl_30d", "wins_7d", "losses_7d"))


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_dt(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def compact_wallet(value: str | None) -> str:
    if not value:
        return ""
    text = str(value)
    if len(text) <= 14:
        return text
    return f"{text[:6]}...{text[-4:]}"


def _safe_json_loads(raw: str | bytes | None, default: Any) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


def _sqlite_connect(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _raw_files(raw_dir: Path) -> list[Path]:
    if not raw_dir.exists():
        return []
    files = [
        path
        for pattern in ("*/events.jsonl", "*/events.jsonl.gz")
        for path in raw_dir.glob(pattern)
        if path.is_file()
    ]
    return sorted(files, key=lambda path: (path.stat().st_mtime, str(path)))


def _tail_lines(path: Path, *, max_lines: int, block_size: int = 65536) -> list[str]:
    if max_lines <= 0:
        return []
    if path.suffix == ".gz":
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
                return [line.rstrip("\n") for line in handle.readlines()[-max_lines:] if line.strip()]
        except OSError:
            return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            chunks: list[bytes] = []
            line_count = 0
            while position > 0 and line_count <= max_lines:
                read_size = min(block_size, position)
                position -= read_size
                handle.seek(position)
                chunk = handle.read(read_size)
                chunks.append(chunk)
                line_count += chunk.count(b"\n")
    except OSError:
        return []
    data = b"".join(reversed(chunks))
    return [line.decode("utf-8", errors="replace") for line in data.splitlines()[-max_lines:] if line.strip()]


def _tail_raw_events(raw_dir: Path, *, max_lines: int = 2500) -> list[dict[str, Any]]:
    lines: list[str] = []
    remaining = max_lines
    for path in reversed(_raw_files(raw_dir)):
        if remaining <= 0:
            break
        file_lines = _tail_lines(path, max_lines=remaining)
        if not file_lines:
            continue
        lines = file_lines + lines
        remaining = max_lines - len(lines)
    events: list[dict[str, Any]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            events.append(row)
    return events


def _event_sort_key(row: dict[str, Any]) -> str:
    return str(row.get("observed_at") or row.get("exchange_ts") or "")


def _report_candidates(path: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    report = _safe_json_loads(path.read_text(encoding="utf-8") if path.exists() else None, {})
    if not isinstance(report, dict):
        report = {}
    raw_candidates = report.get("candidates") if isinstance(report.get("candidates"), dict) else {}
    candidates: dict[str, list[dict[str, Any]]] = {group: [] for group in CANDIDATE_GROUPS}
    for group in SCORED_GROUPS:
        rows = raw_candidates.get(group, []) if isinstance(raw_candidates, dict) else []
        if isinstance(rows, list):
            candidates[group] = [_normalize_candidate(row) for row in rows if isinstance(row, dict)]
    return report, candidates


def _wallet_names(conn: sqlite3.Connection) -> dict[str, str]:
    names: dict[str, str] = {}
    rows = conn.execute(
        """
        SELECT wallet, name, MAX(exchange_ts) AS last_ts
        FROM trades
        WHERE name IS NOT NULL AND name != ''
        GROUP BY wallet, name
        ORDER BY last_ts DESC
        """
    ).fetchall()
    for row in rows:
        wallet = str(row["wallet"]).lower()
        if wallet not in names:
            names[wallet] = str(row["name"])
    return names


def _sqlite_candidates(data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    candidates: dict[str, list[dict[str, Any]]] = {group: [] for group in CANDIDATE_GROUPS}
    conn = _sqlite_connect(data_dir / "state" / "observer.sqlite")
    if conn is None:
        return candidates
    try:
        trade_names = _wallet_names(conn)
        rows = conn.execute("SELECT * FROM candidate_scores ORDER BY status ASC, rank_score DESC").fetchall()
        for row in rows:
            metrics = _safe_json_loads(row["metrics_json"], {})
            wallet = str(row["wallet"]).lower()
            display_name = trade_names.get(wallet, "") or str(metrics.get("profile_name") or "")
            if display_name and isinstance(metrics, dict):
                metrics.setdefault("name", display_name)
            item = _normalize_candidate(
                {
                    "wallet": wallet,
                    "status": row["status"],
                    "rank_score": row["rank_score"],
                    "metrics": metrics if isinstance(metrics, dict) else {},
                    "reasons": _safe_json_loads(row["reasons_json"], []),
                    "updated_at": row["updated_at"],
                    "name": display_name,
                }
            )
            candidates.setdefault(str(row["status"]), []).append(item)
        candidates["active_candidate"] = candidates.get("active_candidate", [])[:MAX_ACTIVE_DISPLAY]
        candidates["dormant_candidate"] = candidates.get("dormant_candidate", [])[:MAX_DORMANT_DISPLAY]
        candidates["archive_candidate"] = candidates.get("archive_candidate", [])[:MAX_ARCHIVE_DISPLAY]
        return candidates
    except sqlite3.Error:
        return candidates
    finally:
        conn.close()


def _normalize_candidate(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    wallet = str(row.get("wallet") or metrics.get("wallet") or "").lower()
    return {
        "wallet": wallet,
        "wallet_short": compact_wallet(wallet),
        "name": row.get("name") or metrics.get("name") or metrics.get("profile_name") or metrics.get("pseudonym") or "",
        "status": row.get("status") or "unknown",
        "rank_score": row.get("rank_score"),
        "metrics": metrics,
        "reasons": row.get("reasons") if isinstance(row.get("reasons"), list) else [],
        "updated_at": row.get("updated_at") or "",
        "score_state": _score_state(metrics, row),
    }


def _score_state(metrics: dict[str, Any], row: dict[str, Any]) -> str:
    if "score_error" in metrics or "score_error" in row:
        return "score_error"
    if not _SCORE_REQUIRED.issubset(metrics):
        return "pending"
    return "ready"


def _sqlite_summary(data_dir: Path) -> dict[str, Any]:
    db_path = data_dir / "state" / "observer.sqlite"
    summary: dict[str, Any] = {
        "path": str(db_path),
        "exists": db_path.exists(),
        "trade_count": 0,
        "candidate_count": 0,
        "latest_trade_ts": None,
        "latest_trade_at": None,
        "latest_score_at": None,
    }
    conn = _sqlite_connect(db_path)
    if conn is None:
        return summary
    try:
        trade = conn.execute("SELECT COUNT(*) AS n, MAX(exchange_ts) AS latest FROM trades").fetchone()
        score = conn.execute("SELECT COUNT(*) AS n, MAX(updated_at) AS latest FROM candidate_scores").fetchone()
        summary["trade_count"] = int(trade["n"] or 0)
        summary["candidate_count"] = int(score["n"] or 0)
        if trade["latest"] is not None:
            latest_ts = int(trade["latest"])
            summary["latest_trade_ts"] = latest_ts
            summary["latest_trade_at"] = dt.datetime.fromtimestamp(latest_ts, tz=dt.timezone.utc).isoformat()
        summary["latest_score_at"] = score["latest"]
    except sqlite3.Error:
        summary["error"] = "sqlite_read_error"
    finally:
        conn.close()
    return summary


def _market_trade_counts(data_dir: Path, windows: dict[str, tuple[int | None, int | None]] | None = None) -> dict[str, int]:
    conn = _sqlite_connect(data_dir / "state" / "observer.sqlite")
    if conn is None:
        return {}
    try:
        counts: Counter[str] = Counter()
        windows = windows or {}
        for slug, (start_ts, end_ts) in windows.items():
            clauses = ["market_slug=?"]
            params: list[Any] = [slug]
            if start_ts is not None:
                clauses.append("exchange_ts>=?")
                params.append(start_ts)
            if end_ts is not None:
                clauses.append("exchange_ts<?")
                params.append(end_ts)
            row = conn.execute(f"SELECT COUNT(*) AS n FROM trades WHERE {' AND '.join(clauses)}", params).fetchone()
            counts[slug] = int(row["n"] or 0) if row else 0
        if windows:
            return dict(counts)
        rows = conn.execute("SELECT market_slug, COUNT(*) AS n FROM trades GROUP BY market_slug").fetchall()
        for row in rows:
            counts[str(row["market_slug"])] = int(row["n"] or 0)
        return dict(counts)
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def _recent_trades_from_sqlite(data_dir: Path, *, limit: int) -> list[dict[str, Any]]:
    conn = _sqlite_connect(data_dir / "state" / "observer.sqlite")
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT * FROM trades
            ORDER BY exchange_ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_trade_row(dict(row)) for row in rows]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _trade_row(row: dict[str, Any]) -> dict[str, Any]:
    wallet = str(row.get("wallet") or "").lower()
    exchange_ts = row.get("exchange_ts")
    exchange_at = None
    if exchange_ts is not None:
        try:
            exchange_at = dt.datetime.fromtimestamp(int(exchange_ts), tz=dt.timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            exchange_at = None
    return {
        "event": row.get("event") or "trade_observed",
        "observed_at": row.get("observed_at"),
        "exchange_ts": exchange_ts,
        "exchange_at": exchange_at,
        "symbol": row.get("symbol"),
        "market_slug": row.get("market_slug"),
        "condition_id": row.get("condition_id"),
        "wallet": wallet,
        "wallet_short": compact_wallet(wallet),
        "name": row.get("name") or row.get("pseudonym") or "",
        "outcome": row.get("outcome") or row.get("side"),
        "price": row.get("price"),
        "size": row.get("size"),
        "usdc": row.get("usdc"),
        "tx_hash": row.get("tx_hash"),
    }


def _wallet_ledger_rows(conn: sqlite3.Connection, wallet: str, *, limit: int = 50) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM wallet_market_pnl
            WHERE wallet=?
            ORDER BY settled_at DESC
            LIMIT ?
            """,
            (wallet.lower(), limit),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        {
            "wallet": row["wallet"],
            "market_slug": row["market_slug"],
            "condition_id": row["condition_id"],
            "symbol": row["symbol"],
            "realized_pnl": row["realized_pnl"],
            "buy_usdc": row["buy_usdc"],
            "sell_usdc": row["sell_usdc"],
            "settled_value": row["settled_value"],
            "net_shares_up": row["net_shares_up"],
            "net_shares_down": row["net_shares_down"],
            "trades": row["trades"],
            "winning_side": row["winning_side"],
            "settled_at": row["settled_at"],
            "incomplete": bool(row["incomplete"]) if "incomplete" in row.keys() else False,
        }
        for row in rows
    ]


def _ledger_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    complete_rows = [row for row in rows if not row.get("incomplete")]
    pnl = [float(row.get("realized_pnl") or 0.0) for row in complete_rows]
    return {
        "settled_markets": len(complete_rows),
        "realized_pnl": round(sum(pnl), 6),
        "wins": sum(1 for value in pnl if value > 0),
        "losses": sum(1 for value in pnl if value < 0),
        "buy_usdc": round(sum(float(row.get("buy_usdc") or 0.0) for row in complete_rows), 6),
        "sell_usdc": round(sum(float(row.get("sell_usdc") or 0.0) for row in complete_rows), 6),
        "incomplete_markets": sum(1 for row in rows if row.get("incomplete")),
    }


def recent_trades(data_dir: Path, *, limit: int = 100) -> list[dict[str, Any]]:
    events = _tail_raw_events(data_dir / "raw", max_lines=max(limit * 20, 500))
    trades = [_trade_row(row) for row in events if row.get("event") == "trade_observed"]
    trades.sort(key=_event_sort_key, reverse=True)
    if trades:
        return trades[:limit]
    return _recent_trades_from_sqlite(data_dir, limit=limit)


def _event_summary_from_events(
    events: list[dict[str, Any]],
    *,
    now: dt.datetime,
    wallet_names: dict[str, str] | None = None,
) -> dict[str, Any]:
    counts = Counter(str(row.get("event") or "unknown") for row in events)
    last_event = max((parse_dt(row.get("observed_at")) for row in events), default=None)
    age = None
    if last_event is not None:
        age = max(0, int((now - last_event).total_seconds()))
    visible_events = [row for row in events if _is_dashboard_event(row)]
    recent = sorted(visible_events, key=_event_sort_key, reverse=True)[:50]
    return {
        "counts": dict(counts),
        "last_event_at": last_event.isoformat() if last_event else None,
        "last_event_age_seconds": age,
        "recent": [_compact_event(row, wallet_names=wallet_names or {}) for row in recent],
    }


def _event_summary(data_dir: Path, *, now: dt.datetime) -> dict[str, Any]:
    return _event_summary_from_events(_tail_raw_events(data_dir / "raw"), now=now)


def _compact_event(row: dict[str, Any], *, wallet_names: dict[str, str] | None = None) -> dict[str, Any]:
    wallet_names = wallet_names or {}
    event = str(row.get("event") or "unknown")
    base = {
        "event": event,
        "event_label": _event_label(event),
        "observed_at": row.get("observed_at"),
        "symbol": row.get("symbol"),
        "market_slug": row.get("market_slug"),
    }
    if event == "trade_observed":
        trade = _trade_row(row)
        name = wallet_names.get(str(trade.get("wallet") or "").lower())
        if name and not trade.get("name"):
            trade["name"] = name
        base.update(trade)
    elif event == "context_snapshot":
        wallet = str(row.get("wallet") or "").lower()
        base.update(
            {
                "wallet": wallet,
                "wallet_short": compact_wallet(wallet),
                "name": wallet_names.get(wallet, ""),
                "outcome": row.get("outcome"),
                "price": row.get("price"),
                "btc_price": row.get("btc_price"),
                "eth_price": row.get("eth_price"),
                "book_age_ms": row.get("book_age_ms"),
            }
        )
    elif event in {"api_error", "observer_error", "score_error"}:
        base.update({"message": row.get("message") or row.get("error") or row.get("reason")})
    elif event == "sqlite_cleanup":
        base.update(
            {
                "removed_wallets": row.get("removed_wallets"),
                "removed_trades": row.get("removed_trades"),
                "removed_score_rows": row.get("removed_score_rows"),
            }
        )
    return base


def _candidate_wallet_names(candidates: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    names: dict[str, str] = {}
    for rows in candidates.values():
        for row in rows:
            wallet = str(row.get("wallet") or "").lower()
            name = str(row.get("name") or (row.get("metrics") or {}).get("name") or "")
            if wallet and name and wallet not in names:
                names[wallet] = name
    return names


def _is_dashboard_event(row: dict[str, Any]) -> bool:
    event = str(row.get("event") or "")
    if event == "candidate_score":
        return False
    return event in {"trade_observed", "api_error", "observer_error", "score_error", "archive_pruned", "sqlite_cleanup"}


def _event_label(event: str) -> str:
    labels = {
        "trade_observed": "成交",
        "context_snapshot": "盘口快照",
        "api_error": "API 异常",
        "observer_error": "采集异常",
        "score_error": "评分异常",
        "archive_pruned": "归档清理",
        "sqlite_cleanup": "数据清理",
    }
    return labels.get(event, event)


def _current_markets_from_sqlite(data_dir: Path) -> dict[str, dict[str, Any]]:
    conn = _sqlite_connect(data_dir / "state" / "observer.sqlite")
    if conn is None:
        return {}
    try:
        rows = conn.execute(
            """
            SELECT symbol, market_slug, condition_id, window_start, window_end, updated_at
            FROM market_windows
            ORDER BY symbol ASC
            """
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        conn.close()
    current: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").upper()
        if not symbol:
            continue
        current[symbol] = {
            "symbol": symbol,
            "market_slug": row["market_slug"],
            "condition_id": row["condition_id"],
            "window_start": row["window_start"],
            "window_end": row["window_end"],
            "observed_at": row["updated_at"],
            "trade_count": 0,
        }
    return current


def _current_markets(events: list[dict[str, Any]], data_dir: Path) -> dict[str, Any]:
    current = _current_markets_from_sqlite(data_dir)
    if current:
        windows: dict[str, tuple[int | None, int | None]] = {}
        for row in current.values():
            start = parse_dt(row.get("window_start"))
            end = parse_dt(row.get("window_end"))
            windows[str(row.get("market_slug") or "")] = (
                int(start.timestamp()) if start else None,
                int(end.timestamp()) if end else None,
            )
        counts = _market_trade_counts(data_dir, windows)
        for row in current.values():
            row["trade_count"] = counts.get(str(row.get("market_slug") or ""), 0)
        return {"current": current}

    selected = [row for row in events if row.get("event") == "market_selected"]
    selected.sort(key=_event_sort_key)
    current = {}
    for row in selected:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        slug = str(row.get("market_slug") or "")
        current[symbol] = {
            "symbol": symbol,
            "market_slug": slug,
            "condition_id": row.get("condition_id"),
            "window_start": row.get("window_start"),
            "window_end": row.get("window_end"),
            "observed_at": row.get("observed_at"),
            "trade_count": 0,
        }
    windows: dict[str, tuple[int | None, int | None]] = {}
    for row in current.values():
        start = parse_dt(row.get("window_start"))
        end = parse_dt(row.get("window_end"))
        windows[str(row.get("market_slug") or "")] = (
            int(start.timestamp()) if start else None,
            int(end.timestamp()) if end else None,
        )
    counts = _market_trade_counts(data_dir, windows)
    for row in current.values():
        row["trade_count"] = counts.get(str(row.get("market_slug") or ""), 0)
    return {"current": current}


def _raw_size_summary(data_dir: Path, *, now: dt.datetime) -> dict[str, Any]:
    raw_dir = data_dir / "raw"
    files = _raw_files(raw_dir)
    total = sum(path.stat().st_size for path in files)
    today_dir = raw_dir / now.date().isoformat()
    today_file = today_dir / "events.jsonl"
    today = today_file.stat().st_size if today_file.exists() else 0
    return {
        "raw_dir": str(raw_dir),
        "raw_today_path": str(today_file),
        "raw_today_bytes": today,
        "raw_total_bytes": total,
        "raw_file_count": len(files),
    }


def _wallet_local_metrics(conn: sqlite3.Connection, wallet: str, *, now: dt.datetime | None = None) -> dict[str, Any]:
    now_ts = int((now or utc_now()).timestamp())
    rows = conn.execute("SELECT * FROM trades WHERE wallet=?", (wallet,)).fetchall()
    cutoff_7d = now_ts - 7 * 86400
    cutoff_30d = now_ts - 30 * 86400
    rows_7d = [row for row in rows if int(row["exchange_ts"]) >= cutoff_7d]
    rows_30d = [row for row in rows if int(row["exchange_ts"]) >= cutoff_30d]
    metrics: dict[str, Any] = {
        "wallet": wallet,
        "trades_7d": len(rows_7d),
        "markets_7d": len({row["market_slug"] for row in rows_7d}),
        "trades_30d": len(rows_30d),
        "markets_30d": len({row["market_slug"] for row in rows_30d}),
        "historical_trades": len(rows),
        "historical_markets": len({row["market_slug"] for row in rows}),
        "last_active_age_hours": None,
    }
    if rows:
        last_ts = max(int(row["exchange_ts"]) for row in rows)
        metrics["last_active_age_hours"] = round(max(0, now_ts - last_ts) / 3600.0, 3)
    return metrics


def build_dashboard_status(data_dir: Path, *, now: dt.datetime | None = None, recent_limit: int = 100) -> dict[str, Any]:
    now = now or utc_now()
    data_dir = data_dir.expanduser().resolve()
    report, report_candidates = _report_candidates(data_dir / "reports" / "latest_candidates.json")
    sqlite_candidates = _sqlite_candidates(data_dir)
    has_sqlite_scores = any(sqlite_candidates.get(group) for group in SCORED_GROUPS)
    candidates = sqlite_candidates if has_sqlite_scores else report_candidates
    sqlite = _sqlite_summary(data_dir)
    events = _tail_raw_events(data_dir / "raw")
    event_summary = _event_summary_from_events(events, now=now, wallet_names=_candidate_wallet_names(candidates))
    raw_summary = _raw_size_summary(data_dir, now=now)
    health = {
        "ok": True,
        "data_dir": str(data_dir),
        "generated_at": now.isoformat(),
        "last_event_at": event_summary["last_event_at"],
        "last_event_age_seconds": event_summary["last_event_age_seconds"],
        "raw_today_bytes": raw_summary["raw_today_bytes"],
        "latest_score_at": sqlite.get("latest_score_at") or report.get("generated_at"),
    }
    return {
        "health": health,
        "sqlite": sqlite,
        "raw": raw_summary,
        "report": {
            "exists": (data_dir / "reports" / "latest_candidates.json").exists(),
            "generated_at": report.get("generated_at"),
            "max_candidates": report.get("max_candidates"),
            "symbols": report.get("symbols") or [],
        },
        "markets": _current_markets(events, data_dir),
        "events": event_summary,
        "candidates": candidates,
        "candidate_counts": {group: len(rows) for group, rows in candidates.items()},
        "recent_trades": recent_trades(data_dir, limit=recent_limit),
    }


def wallet_detail(data_dir: Path, address: str, *, trade_limit: int = 100) -> dict[str, Any] | None:
    wallet = address.lower()
    conn = _sqlite_connect(data_dir / "state" / "observer.sqlite")
    if conn is None:
        return None
    try:
        score_row = conn.execute("SELECT * FROM candidate_scores WHERE wallet=?", (wallet,)).fetchone()
        name_row = conn.execute(
            """
            SELECT name FROM trades
            WHERE wallet=? AND name IS NOT NULL AND name != ''
            ORDER BY exchange_ts DESC
            LIMIT 1
            """,
            (wallet,),
        ).fetchone()
        trade_rows = conn.execute(
            """
            SELECT * FROM trades
            WHERE wallet=?
            ORDER BY exchange_ts DESC
            LIMIT ?
            """,
            (wallet, trade_limit),
        ).fetchall()
        if score_row is None and not trade_rows:
            return None
        metrics = _safe_json_loads(score_row["metrics_json"], {}) if score_row else _wallet_local_metrics(conn, wallet)
        display_name = name_row["name"] if name_row else ""
        if display_name and "name" not in metrics:
            metrics["name"] = display_name
        reasons = _safe_json_loads(score_row["reasons_json"], []) if score_row else []
        trades = [_trade_row(dict(row)) for row in trade_rows]
        distribution: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "usdc": 0.0, "last_exchange_ts": 0})
        for row in trade_rows:
            key = str(row["market_slug"])
            item = distribution[key]
            item["market_slug"] = key
            item["symbol"] = row["symbol"]
            item["trades"] += 1
            item["usdc"] += float(row["usdc"])
            item["last_exchange_ts"] = max(int(item["last_exchange_ts"]), int(row["exchange_ts"]))
        markets = sorted(distribution.values(), key=lambda item: item["last_exchange_ts"], reverse=True)
        behavior = {
            key: metrics.get(key)
            for key in ("dual_side_rate", "late_bias_shift", "winner_add_rate", "longshot_profit_share", "top1_concentration", "top3_concentration")
            if key in metrics
        }
        ledger_rows = _wallet_ledger_rows(conn, wallet)
        return {
            "wallet": wallet,
            "wallet_short": compact_wallet(wallet),
            "status": score_row["status"] if score_row else "unscored",
            "rank_score": score_row["rank_score"] if score_row else None,
            "updated_at": score_row["updated_at"] if score_row else None,
            "score_state": _score_state(metrics, dict(score_row) if score_row else {}),
            "metrics": metrics,
            "reasons": reasons if isinstance(reasons, list) else [],
            "behavior": behavior,
            "market_distribution": markets,
            "recent_trades": trades,
            "ledger_summary": _ledger_summary(ledger_rows),
            "settled_markets": ledger_rows,
        }
    except sqlite3.Error:
        return None
    finally:
        conn.close()
