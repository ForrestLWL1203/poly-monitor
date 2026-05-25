from __future__ import annotations

import datetime as dt
import gzip
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from .data_api import fetch_user_activity
from .wallet_metrics import is_crypto_5m_row, row_slug

PRICE_BUCKETS = (
    ("<0.15", 0.0, 0.15),
    ("0.15-0.35", 0.15, 0.35),
    ("0.35-0.55", 0.35, 0.55),
    ("0.55-0.75", 0.55, 0.75),
    (">0.75", 0.75, float("inf")),
)
TIME_BUCKETS = (
    ("0-30s", 0.0, 30.0),
    ("30-60s", 30.0, 60.0),
    ("60-120s", 60.0, 120.0),
    ("120-240s", 120.0, 240.0),
    ("240s+", 240.0, float("inf")),
)
TARGETS = ("5", "25", "100")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _bucket(value: float, buckets: Iterable[tuple[str, float, float]]) -> str:
    for name, low, high in buckets:
        if low <= value < high:
            return name
    return "unknown"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _dedupe_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        str(row.get("tx_hash") or row.get("transactionHash") or ""),
        str(row.get("fill_id") or row.get("id") or row.get("logIndex") or ""),
        str(row.get("market_slug") or row_slug(row)),
        str(row.get("outcome") or ""),
        str(row.get("price") or ""),
        str(row.get("size") or row.get("usdcSize") or ""),
    )


def _normalize_api_trade(row: dict[str, Any], wallet: str) -> dict[str, Any] | None:
    if row.get("type") not in (None, "TRADE") or not is_crypto_5m_row(row):
        return None
    slug = row_slug(row)
    price = _safe_float(row.get("price"))
    size = _safe_float(row.get("size"))
    usdc = _safe_float(row.get("usdcSize"), price * size)
    symbol = "ETH" if slug.startswith("eth-") else "BTC" if slug.startswith("btc-") else str(row.get("symbol") or "")
    return {
        "source": "api",
        "tx_hash": str(row.get("transactionHash") or ""),
        "fill_id": str(row.get("id") or row.get("fillId") or row.get("logIndex") or row.get("transactionIndex") or ""),
        "wallet": wallet.lower(),
        "market_slug": slug,
        "condition_id": str(row.get("conditionId") or ""),
        "symbol": symbol.upper(),
        "exchange_ts": _safe_int(row.get("timestamp")),
        "outcome": str(row.get("outcome") or ""),
        "side": str(row.get("side") or "").upper(),
        "price": price,
        "size": size,
        "usdc": round(usdc, 6),
        "name": str(row.get("name") or ""),
    }


def _load_local_trades(data_dir: Path, wallet: str, *, cutoff_ts: int) -> list[dict[str, Any]]:
    db = data_dir / "state" / "observer.sqlite"
    if not db.exists():
        return []
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT * FROM trades
            WHERE wallet=? AND exchange_ts >= ?
            ORDER BY exchange_ts ASC
            """,
            (wallet.lower(), cutoff_ts),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    trades = [dict(row) for row in rows]
    for row in trades:
        row["source"] = "local"
    return _dedupe_trades(trades)


def _dedupe_trades(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = _dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _load_api_trades(wallet: str, *, start_ts: int, end_ts: int, max_pages: int = 12) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    truncated = False
    for page in range(max_pages):
        batch = fetch_user_activity(wallet, limit=500, offset=page * 500, start=start_ts, end=end_ts)
        if not batch:
            break
        for raw in batch:
            if isinstance(raw, dict):
                normalized = _normalize_api_trade(raw, wallet)
                if normalized is not None:
                    rows.append(normalized)
        if len(batch) >= 500 and page + 1 == max_pages:
            truncated = True
        if len(batch) < 500:
            break
    return _dedupe_trades(rows), truncated


def _iter_raw_events(data_dir: Path, *, start_date: dt.date, end_date: dt.date) -> Iterable[dict[str, Any]]:
    raw_dir = data_dir / "raw"
    if not raw_dir.exists():
        yield from ()
        return
    day = start_date
    while day <= end_date:
        day_dir = raw_dir / day.isoformat()
        for path in (day_dir / "events.jsonl", day_dir / "events.jsonl.gz"):
            if not path.exists():
                continue
            try:
                if path.suffix == ".gz":
                    handle_cm = gzip.open(path, "rt", encoding="utf-8")
                else:
                    handle_cm = path.open("r", encoding="utf-8")
                with handle_cm as handle:
                    for line in handle:
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(row, dict):
                            yield row
            except OSError:
                pass
        day += dt.timedelta(days=1)


def _load_context_snapshots(data_dir: Path, wallet: str, *, start: dt.datetime, end: dt.datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _iter_raw_events(data_dir, start_date=start.date(), end_date=end.date()):
        if row.get("event") != "context_snapshot":
            continue
        if str(row.get("wallet") or "").lower() != wallet.lower():
            continue
        observed = _parse_iso(row.get("observed_at"))
        if observed is not None and not (start <= observed <= end):
            continue
        rows.append(row)
    return rows


def _sqlite_scalar(data_dir: Path, sql: str, params: tuple[Any, ...]) -> int:
    path = data_dir / "state" / "observer.sqlite"
    if not path.exists():
        return 0
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = conn.execute(sql, params).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return 0
    return int(row[0] or 0) if row else 0


def _archive_manifest_count(data_dir: Path, *, start_ts: int, end_ts: int) -> int:
    return _sqlite_scalar(
        data_dir,
        """
        SELECT COUNT(*)
        FROM archive_manifest
        WHERE (max_ts IS NULL OR max_ts >= ?)
          AND (min_ts IS NULL OR min_ts <= ?)
        """,
        (start_ts, end_ts),
    )


def _match_contexts(trades: list[dict[str, Any]], contexts: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    by_tx: dict[str, dict[str, Any]] = {}
    fallback: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for ctx in contexts:
        tx = str(ctx.get("trade_tx_hash") or "")
        if tx:
            by_tx[tx] = ctx
        fallback[(str(ctx.get("market_slug") or ""), str(ctx.get("trade_outcome") or ""))].append(ctx)
    matched: dict[int, dict[str, Any]] = {}
    for idx, trade in enumerate(trades):
        tx = str(trade.get("tx_hash") or "")
        if tx and tx in by_tx:
            matched[idx] = by_tx[tx]
            continue
        candidates = fallback.get((str(trade.get("market_slug") or ""), str(trade.get("outcome") or "")), [])
        if candidates:
            trade_ts = _safe_int(trade.get("exchange_ts"))
            candidates.sort(key=lambda ctx: abs(_context_ts(ctx) - trade_ts))
            matched[idx] = candidates.pop(0)
    return matched


def _context_ts(ctx: dict[str, Any]) -> int:
    observed = _parse_iso(ctx.get("observed_at"))
    return int(observed.timestamp()) if observed is not None else 0


def _counts_by_window(trades: list[dict[str, Any]], *, now_ts: int) -> dict[str, int]:
    cut24 = now_ts - 86400
    cut7 = now_ts - 7 * 86400
    cut30 = now_ts - 30 * 86400
    return {
        "trades_24h": sum(1 for row in trades if _safe_int(row.get("exchange_ts")) >= cut24),
        "trades_7d": sum(1 for row in trades if _safe_int(row.get("exchange_ts")) >= cut7),
        "trades_30d": sum(1 for row in trades if _safe_int(row.get("exchange_ts")) >= cut30),
    }


def _price_behavior(trades: list[dict[str, Any]]) -> dict[str, Any]:
    buckets = {name: {"trades": 0, "usdc": 0.0} for name, _low, _high in PRICE_BUCKETS}
    outcomes: Counter[str] = Counter()
    for row in trades:
        name = _bucket(_safe_float(row.get("price")), PRICE_BUCKETS)
        buckets[name]["trades"] += 1
        buckets[name]["usdc"] = round(buckets[name]["usdc"] + _safe_float(row.get("usdc")), 6)
        outcome = str(row.get("outcome") or "")
        if outcome:
            outcomes[outcome] += 1
    return {
        "buckets": buckets,
        "outcomes": dict(outcomes),
    }


def _timing_behavior(trades: list[dict[str, Any]], matched: dict[int, dict[str, Any]]) -> dict[str, Any]:
    buckets = {name: {"trades": 0, "with_context": 0} for name, _low, _high in TIME_BUCKETS}
    unknown = 0
    for idx, _row in enumerate(trades):
        ctx = matched.get(idx)
        remaining = _safe_float(ctx.get("window_remaining_sec")) if ctx else None
        if remaining is None:
            unknown += 1
            continue
        name = _bucket(max(0.0, remaining), TIME_BUCKETS)
        buckets[name]["trades"] += 1
        buckets[name]["with_context"] += 1
    dominant = max(buckets.items(), key=lambda item: item[1]["trades"])[0] if any(item["trades"] for item in buckets.values()) else None
    return {
        "buckets": buckets,
        "unknown_trades": unknown,
        "dominant_bucket": dominant,
    }


def _book_copyability(trades: list[dict[str, Any]], matched: dict[int, dict[str, Any]]) -> dict[str, Any]:
    target_counts = {target: {"seen": 0, "ok": 0, "slippage_sum": 0.0, "slippage_count": 0} for target in TARGETS}
    spreads: list[float] = []
    ages: list[float] = []
    for idx, trade in enumerate(trades):
        ctx = matched.get(idx)
        if not ctx:
            continue
        token_side = "up" if str(trade.get("outcome") or ctx.get("trade_outcome") or "").lower() == "up" else "down"
        trade_side = str(trade.get("side") or ctx.get("trade_side") or "BUY").upper()
        book = ctx.get(token_side)
        if not isinstance(book, dict):
            continue
        spread = book.get("spread")
        if spread is not None:
            spreads.append(_safe_float(spread))
        age = book.get("book_age_ms")
        if age is not None:
            ages.append(_safe_float(age))
        target_key = "bid_targets" if trade_side == "SELL" else "ask_targets"
        fill_targets = book.get(target_key) if isinstance(book.get(target_key), dict) else {}
        trade_price = _safe_float(trade.get("price") or ctx.get("trade_price"))
        for target in TARGETS:
            fill = fill_targets.get(target)
            if not isinstance(fill, dict):
                continue
            target_counts[target]["seen"] += 1
            if fill.get("ok"):
                target_counts[target]["ok"] += 1
                avg = fill.get("avg")
                if avg is not None and trade_price > 0:
                    if trade_side == "SELL":
                        target_counts[target]["slippage_sum"] += (trade_price - _safe_float(avg)) * 100.0
                    else:
                        target_counts[target]["slippage_sum"] += (_safe_float(avg) - trade_price) * 100.0
                    target_counts[target]["slippage_count"] += 1
    targets: dict[str, dict[str, Any]] = {}
    for target, values in target_counts.items():
        seen = values["seen"]
        count = values["slippage_count"]
        targets[target] = {
            "seen": seen,
            "ok": values["ok"],
            "ok_rate": round((values["ok"] / seen) * 100.0, 3) if seen else None,
            "avg_slippage_cents": round(values["slippage_sum"] / count, 4) if count else None,
        }
    return {
        "context_trades": len(matched),
        "avg_spread": round(sum(spreads) / len(spreads), 6) if spreads else None,
        "avg_book_age_ms": round(sum(ages) / len(ages), 3) if ages else None,
        "targets": targets,
    }


def _success_label(trade: dict[str, Any], ctx: dict[str, Any] | None) -> str | None:
    if not ctx:
        return None
    open_price = ctx.get("window_open_reference_price")
    close_price = ctx.get("window_close_reference_price")
    if open_price is None or close_price is None:
        return None
    winning = "Up" if _safe_float(close_price) >= _safe_float(open_price) else "Down"
    token_won = str(trade.get("outcome") or "") == winning
    trade_side = str(trade.get("side") or ctx.get("trade_side") or "BUY").upper()
    profitable = token_won if trade_side != "SELL" else not token_won
    return "success" if profitable else "failure"


def _success_vs_failure(trades: list[dict[str, Any]], matched: dict[int, dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {"success": [], "failure": []}
    for idx, trade in enumerate(trades):
        ctx = matched.get(idx)
        label = _success_label(trade, ctx)
        if label and ctx is not None:
            groups[label].append((trade, ctx))
    total = len(groups["success"]) + len(groups["failure"])
    if total < 10:
        return {
            "state": "insufficient_sample",
            "labeled_trades": total,
            "success_trades": len(groups["success"]),
            "failure_trades": len(groups["failure"]),
        }
    out: dict[str, Any] = {"state": "ok", "labeled_trades": total}
    for label, rows in groups.items():
        prices = [_safe_float(trade.get("price")) for trade, _ctx in rows]
        remaining = [_safe_float(ctx.get("window_remaining_sec")) for _trade, ctx in rows if ctx.get("window_remaining_sec") is not None]
        out[label] = {
            "trades": len(rows),
            "avg_price": round(sum(prices) / len(prices), 6) if prices else None,
            "avg_window_remaining_sec": round(sum(remaining) / len(remaining), 3) if remaining else None,
            "price_buckets": _price_behavior([trade for trade, _ctx in rows])["buckets"],
        }
    return out


def _confidence(local_trades: int, contexts: int, context_pct: float) -> str:
    if local_trades >= 300 and contexts >= 50 and context_pct >= 30.0:
        return "high"
    if local_trades >= 100 or contexts >= 20:
        return "medium"
    return "low"


def _frequency_profile(trades: list[dict[str, Any]], *, now_ts: int) -> dict[str, Any]:
    counts = _counts_by_window(trades, now_ts=now_ts)
    markets_24h = {row.get("market_slug") for row in trades if _safe_int(row.get("exchange_ts")) >= now_ts - 86400 and row.get("market_slug")}
    markets_30d = {row.get("market_slug") for row in trades if row.get("market_slug")}
    trades_30d = counts["trades_30d"]
    distinct_30d = len(markets_30d)
    avg_per_market = round(trades_30d / distinct_30d, 3) if distinct_30d else 0.0
    # Local observed trades are already de-duplicated and cheaper to count than
    # API activity pages, so this saturation threshold is intentionally higher
    # than wallet_metrics.SATURATED_MARKETS_MIN_TRADES_24H.
    saturated = len(markets_24h) >= 288 or counts["trades_24h"] >= 1000
    if saturated:
        bucket = "high"
    elif counts["trades_24h"] >= 100 or avg_per_market >= 20:
        bucket = "medium"
    else:
        bucket = "low"
    return {
        **counts,
        "distinct_markets_24h": len(markets_24h),
        "distinct_markets_30d": distinct_30d,
        "avg_trades_per_market": avg_per_market,
        "frequency_class": bucket,
        "markets_24h_saturated": saturated,
    }


def _volume_quality(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_market: dict[str, float] = defaultdict(float)
    longshot = 0.0
    for row in trades:
        usdc = _safe_float(row.get("usdc"))
        by_market[str(row.get("market_slug") or "")] += usdc
        if _safe_float(row.get("price")) < 0.15:
            longshot += usdc
    values = sorted(by_market.values(), reverse=True)
    total = sum(values)
    return {
        "basis": "trade_usdc_notional",
        "top1_volume_concentration": round(values[0] / total, 6) if total > 0 and values else 1.0,
        "top3_volume_concentration": round(sum(values[:3]) / total, 6) if total > 0 else 1.0,
        "longshot_volume_share": round(longshot / total, 6) if total > 0 else 0.0,
        "single_market_concentration_flag": bool(total > 0 and values and values[0] / total >= 0.5),
    }


def _market_behavior(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_market: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "usdc": 0.0, "outcomes": Counter()})
    for row in trades:
        item = by_market[str(row.get("market_slug") or "")]
        item["trades"] += 1
        item["usdc"] += _safe_float(row.get("usdc"))
        outcome = str(row.get("outcome") or "")
        if outcome:
            item["outcomes"][outcome] += 1
    dual = sum(1 for item in by_market.values() if len(item["outcomes"]) > 1)
    top = sorted(
        (
            {"market_slug": market, "trades": item["trades"], "usdc": round(item["usdc"], 6), "outcomes": dict(item["outcomes"])}
            for market, item in by_market.items()
        ),
        key=lambda item: (item["trades"], item["usdc"]),
        reverse=True,
    )[:10]
    return {
        "markets": len(by_market),
        "dual_side_markets": dual,
        "dual_side_rate": round(dual / len(by_market), 6) if by_market else 0.0,
        "top_markets": top,
    }


def _distillation(freq: dict[str, Any], coverage: dict[str, Any], book: dict[str, Any], success: dict[str, Any]) -> dict[str, Any]:
    copy_ok_25 = book["targets"].get("25", {}).get("ok_rate")
    copyability = copy_ok_25 if copy_ok_25 is not None else 0.0
    context_pct = _safe_float(coverage.get("context_coverage_pct"))
    overtrade = 35.0 if freq.get("markets_24h_saturated") else min(30.0, max(0.0, (_safe_float(freq.get("trades_24h")) - 100.0) / 30.0))
    context_factor = min(20.0, max(-30.0, (context_pct * 0.5) - 30.0))
    # v1 heuristic placeholder; replace after shadow-copy simulator can label
    # success/failure with realistic delay and slippage outcomes.
    pattern = 25.0 if success.get("state") == "ok" else 10.0
    score = max(0.0, min(100.0, copyability * 0.45 + pattern + context_factor - overtrade))
    rules: list[str] = []
    if freq.get("markets_24h_saturated"):
        rules.append("24h 窗口已饱和，优先作为研究样本，不建议直接跟单。")
    if copy_ok_25 is not None:
        rules.append(f"25 USDC 盘口可成交率约 {copy_ok_25:g}%。")
    if context_pct < 20.0:
        rules.append("本地盘口上下文覆盖不足，继续观察后再蒸馏。")
    return {
        "distillability_score": round(score, 3),
        "copyability_score": round(copyability, 3),
        "pattern_consistency_score": pattern,
        "overtrading_penalty": round(overtrade, 3),
        "context_factor": round(context_factor, 3),
        "rule_candidates": rules,
    }


def _recommendation(freq: dict[str, Any], coverage: dict[str, Any], distill: dict[str, Any]) -> dict[str, Any]:
    if freq.get("markets_24h_saturated"):
        return {"action": "too_high_frequency", "reason": "24h market/trade activity is saturated."}
    if _safe_int(coverage.get("local_trades")) < 100 and _safe_int(coverage.get("api_backfill_trades")) == 0:
        return {"action": "insufficient_local_data", "reason": "Local observed sample is below the v1 threshold."}
    if _safe_float(distill.get("distillability_score")) >= 55:
        return {"action": "distillation_candidate", "reason": "Pattern and copyability are strong enough for rule extraction."}
    if _safe_float(distill.get("copyability_score")) >= 60:
        return {"action": "shadow_copy_candidate", "reason": "Book copyability is promising, but behavior pattern needs more evidence."}
    return {"action": "monitor_more", "reason": "Keep collecting local context before deciding."}


def build_wallet_research_report(
    wallet: str,
    *,
    data_dir: Path,
    days: int = 30,
    now: dt.datetime | None = None,
    api_backfill: str = "auto",
    min_local_trades: int = 100,
    min_local_markets: int = 20,
) -> dict[str, Any]:
    if api_backfill not in {"auto", "never", "always"}:
        raise ValueError("api_backfill must be one of: auto, never, always")
    now_dt = (now or utc_now()).astimezone(dt.timezone.utc)
    start = now_dt - dt.timedelta(days=days)
    wallet = wallet.lower()
    start_ts = int(start.timestamp())
    end_ts = int(now_dt.timestamp())
    local_trades = _load_local_trades(data_dir, wallet, cutoff_ts=start_ts)
    local_markets = {row.get("market_slug") for row in local_trades if row.get("market_slug")}
    should_backfill = api_backfill == "always" or (
        api_backfill == "auto" and (len(local_trades) < min_local_trades or len(local_markets) < min_local_markets)
    )
    api_trades, api_truncated = _load_api_trades(wallet, start_ts=start_ts, end_ts=end_ts) if should_backfill else ([], False)
    local_keys = {_dedupe_key(row) for row in local_trades}
    api_only = [row for row in api_trades if _dedupe_key(row) not in local_keys]
    all_trades = sorted(_dedupe_trades([*local_trades, *api_only]), key=lambda row: _safe_int(row.get("exchange_ts")))
    contexts = _load_context_snapshots(data_dir, wallet, start=start, end=now_dt)
    matched = _match_contexts(local_trades, contexts)
    context_pct = round((len(matched) / len(local_trades)) * 100.0, 3) if local_trades else 0.0
    watchlist_activity_events = _sqlite_scalar(
        data_dir,
        "SELECT COUNT(*) FROM wallet_activity_events WHERE wallet=? AND exchange_ts >= ? AND exchange_ts <= ?",
        (wallet, start_ts, end_ts),
    )
    wallet_trade_context_rows = _sqlite_scalar(
        data_dir,
        "SELECT COUNT(*) FROM wallet_trade_contexts WHERE wallet=? AND exchange_ts >= ? AND exchange_ts <= ?",
        (wallet, start_ts, end_ts),
    )
    market_state_samples = _sqlite_scalar(
        data_dir,
        """
        SELECT COUNT(*)
        FROM market_state_samples
        WHERE sampled_ts >= ? AND sampled_ts <= ?
          AND market_slug IN (SELECT DISTINCT market_slug FROM trades WHERE wallet=?)
        """,
        (start_ts, end_ts, wallet),
    )
    valid_local_ts = [_safe_int(row.get("exchange_ts")) for row in local_trades if _safe_int(row.get("exchange_ts")) > 0]
    first_ts = min(valid_local_ts, default=0)
    last_ts = max(valid_local_ts, default=0)
    coverage = {
        "local_trades": len(local_trades),
        "api_backfill_trades": len(api_only),
        "api_backfill_used": should_backfill,
        "api_backfill_truncated": api_truncated,
        "context_snapshots": len(contexts),
        "watchlist_activity_events": watchlist_activity_events,
        "wallet_trade_context_rows": wallet_trade_context_rows,
        "market_state_samples": market_state_samples,
        "archive_manifest_rows": _archive_manifest_count(data_dir, start_ts=start_ts, end_ts=end_ts),
        "context_matched_trades": len(matched),
        "context_coverage_pct": context_pct,
        "local_observed_start": dt.datetime.fromtimestamp(first_ts, dt.timezone.utc).isoformat() if first_ts else None,
        "local_observed_end": dt.datetime.fromtimestamp(last_ts, dt.timezone.utc).isoformat() if last_ts else None,
        "confidence": _confidence(len(local_trades), len(matched), context_pct),
    }
    freq = _frequency_profile(all_trades, now_ts=end_ts)
    price = _price_behavior(all_trades)
    timing = _timing_behavior(local_trades, matched)
    book = _book_copyability(local_trades, matched)
    success = _success_vs_failure(local_trades, matched)
    distill = _distillation(freq, coverage, book, success)
    return {
        "wallet": wallet,
        "generated_at": now_dt.isoformat(),
        "window_days": days,
        "data_coverage": coverage,
        "summary": {
            "total_trades": len(all_trades),
            "local_primary": True,
            "api_backfill_policy": api_backfill,
        },
        "frequency_profile": freq,
        "volume_quality": _volume_quality(all_trades),
        "market_behavior": _market_behavior(all_trades),
        "price_behavior": price,
        "timing_behavior": timing,
        "book_copyability": book,
        "success_vs_failure": success,
        "distillation": distill,
        "recommendation": _recommendation(freq, coverage, distill),
    }


def render_markdown_report(report: dict[str, Any]) -> str:
    coverage = report["data_coverage"]
    freq = report["frequency_profile"]
    distill = report["distillation"]
    recommendation = report["recommendation"]
    lines = [
        f"# Wallet Research: {report['wallet']}",
        "",
        f"- Generated: {report['generated_at']}",
        f"- Recommendation: {recommendation['action']} ({recommendation['reason']})",
        f"- Local trades: {coverage['local_trades']}",
        f"- API backfill trades: {coverage['api_backfill_trades']}",
        f"- Context coverage: {coverage['context_coverage_pct']}%",
        f"- Frequency: {freq['frequency_class']} / trades 24h={freq['trades_24h']} / markets 24h={freq['distinct_markets_24h']}{'+' if freq['markets_24h_saturated'] else ''}",
        f"- Distillability score: {distill['distillability_score']}",
        f"- Copyability score: {distill['copyability_score']}",
        "",
        "## Rule Candidates",
        "",
    ]
    rules = distill.get("rule_candidates") or ["No stable rule candidates yet."]
    lines.extend(f"- {rule}" for rule in rules)
    lines.append("")
    return "\n".join(lines)
