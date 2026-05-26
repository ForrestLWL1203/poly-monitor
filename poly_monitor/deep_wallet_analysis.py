from __future__ import annotations

import json
import math
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


TIME_BUCKETS = (
    ("0-30s", 0.0, 30.0),
    ("30-60s", 30.0, 60.0),
    ("60-120s", 60.0, 120.0),
    ("120-240s", 120.0, 240.0),
    ("240s+", 240.0, math.inf),
)

PRICE_BUCKETS = (
    ("<0.15", 0.0, 0.15),
    ("0.15-0.35", 0.15, 0.35),
    ("0.35-0.55", 0.35, 0.55),
    ("0.55-0.75", 0.55, 0.75),
    ("0.75-0.95", 0.75, 0.95),
    (">=0.95", 0.95, math.inf),
)

TARGETS = ("5", "25", "100")
PATH_BUCKETS = (
    ("0-30s", 0.0, 30.0),
    ("30-60s", 30.0, 60.0),
    ("60-120s", 60.0, 120.0),
    ("120-180s", 120.0, 180.0),
    ("180-240s", 180.0, 240.0),
    ("240-300s", 240.0, 300.0),
)
PATH_CHECKPOINTS = (30, 60, 120, 180, 240, 300)
FIRST_BIAS_MIN_USDC = 25.0
LARGE_WIN_PNL = 100.0
LARGE_LOSS_PNL = -100.0


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


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _bucket(value: float, buckets: Iterable[tuple[str, float, float]]) -> str:
    for name, low, high in buckets:
        if low <= value < high:
            return name
    return "unknown"


def _load_jsonl(bundle: zipfile.ZipFile, name: str) -> list[dict[str, Any]]:
    try:
        raw = bundle.read(name).decode("utf-8")
    except KeyError:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_json(bundle: zipfile.ZipFile, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(bundle.read(name).decode("utf-8"))
    except (KeyError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _decode_context(row: dict[str, Any]) -> dict[str, Any]:
    context = row.get("context_json")
    if isinstance(context, dict):
        out = dict(context)
    elif isinstance(context, str):
        try:
            payload = json.loads(context)
        except json.JSONDecodeError:
            payload = {}
        out = payload if isinstance(payload, dict) else {}
    else:
        out = {}
    out.setdefault("wallet", row.get("wallet"))
    out.setdefault("market_slug", row.get("market_slug"))
    out.setdefault("tx_hash", row.get("tx_hash"))
    out.setdefault("exchange_ts", row.get("exchange_ts"))
    return out


def _context_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("market_slug") or ""), str(row.get("tx_hash") or row.get("trade_tx_hash") or ""), str(row.get("fill_id") or ""))


def _activity_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("market_slug") or ""), str(row.get("tx_hash") or ""), str(row.get("fill_id") or ""))


def _match_contexts(activity: list[dict[str, Any]], contexts: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    by_key = {_context_key(ctx): ctx for ctx in contexts}
    fallback: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for ctx in contexts:
        fallback[str(ctx.get("market_slug") or "")].append(ctx)
    matched: dict[int, dict[str, Any]] = {}
    for idx, row in enumerate(activity):
        key = _activity_key(row)
        if key in by_key:
            matched[idx] = by_key[key]
            continue
        slug = str(row.get("market_slug") or "")
        candidates = fallback.get(slug, [])
        if candidates:
            exchange_ts = _safe_int(row.get("exchange_ts"))
            candidates.sort(key=lambda ctx: abs(_safe_int(ctx.get("exchange_ts")) - exchange_ts))
            matched[idx] = candidates.pop(0)
    return matched


def _coverage(manifest: dict[str, Any], complete_rows: list[dict[str, Any]], excluded_rows: list[dict[str, Any]], deep_samples: list[dict[str, Any]]) -> dict[str, Any]:
    coverages = [_safe_float(row.get("coverage")) for row in complete_rows]
    return {
        "wallet": manifest.get("wallet") or "",
        "policy": manifest.get("policy") or "",
        "complete_windows": len(complete_rows) or int(manifest.get("window_count") or 0),
        "excluded_windows": len(excluded_rows) or int(manifest.get("excluded_window_count") or 0),
        "deep_sample_rows": len(deep_samples),
        "avg_coverage": _round(sum(coverages) / len(coverages), 6) if coverages else None,
        "min_coverage": _round(min(coverages), 6) if coverages else None,
        "max_coverage": _round(max(coverages), 6) if coverages else None,
    }


def _pnl(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, Any]] = defaultdict(lambda: {"markets": 0, "realized_pnl": 0.0, "wins": 0, "losses": 0})
    total = 0.0
    wins = 0
    losses = 0
    for row in rows:
        value = _safe_float(row.get("realized_pnl"))
        if int(row.get("incomplete") or 0):
            continue
        symbol = str(row.get("symbol") or _symbol_from_slug(str(row.get("market_slug") or "")) or "UNKNOWN").upper()
        item = by_symbol[symbol]
        item["markets"] += 1
        item["realized_pnl"] += value
        item["wins"] += 1 if value > 0 else 0
        item["losses"] += 1 if value < 0 else 0
        total += value
        wins += 1 if value > 0 else 0
        losses += 1 if value < 0 else 0
    return {
        "markets": sum(item["markets"] for item in by_symbol.values()),
        "total_realized_pnl": round(total, 6),
        "wins": wins,
        "losses": losses,
        "win_rate": _round(wins / (wins + losses), 6) if wins + losses else None,
        "by_symbol": {symbol: {**item, "realized_pnl": round(item["realized_pnl"], 6)} for symbol, item in sorted(by_symbol.items())},
        "top_markets": sorted(
            (
                {
                    "market_slug": str(row.get("market_slug") or ""),
                    "symbol": str(row.get("symbol") or _symbol_from_slug(str(row.get("market_slug") or "")) or "UNKNOWN").upper(),
                    "realized_pnl": _safe_float(row.get("realized_pnl")),
                }
                for row in rows
                if not int(row.get("incomplete") or 0)
            ),
            key=lambda row: abs(float(row["realized_pnl"])),
            reverse=True,
        )[:15],
    }


def _symbol_from_slug(slug: str) -> str:
    if slug.startswith("btc-"):
        return "BTC"
    if slug.startswith("eth-"):
        return "ETH"
    if slug.startswith("sol-"):
        return "SOL"
    if slug.startswith("xrp-"):
        return "XRP"
    return ""


def _window_start_from_slug(slug: str) -> int | None:
    try:
        return int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _side_from_net(net_usdc: float) -> str:
    if net_usdc > 0:
        return "Up"
    if net_usdc < 0:
        return "Down"
    return "Flat"


def _signed_flow(row: dict[str, Any]) -> float:
    outcome = str(row.get("outcome") or "").lower()
    side = str(row.get("side") or "BUY").upper()
    usdc = _safe_float(row.get("usdc"))
    if outcome not in {"up", "down"}:
        return 0.0
    sign = 1.0 if outcome == "up" else -1.0
    if side == "SELL":
        sign *= -1.0
    return sign * usdc


def _flow_fields(row: dict[str, Any]) -> tuple[str, float]:
    outcome = str(row.get("outcome") or "").lower()
    side = str(row.get("side") or "BUY").upper()
    usdc = _safe_float(row.get("usdc"))
    if outcome not in {"up", "down"}:
        return ("", 0.0)
    if side == "SELL":
        outcome = "down" if outcome == "up" else "up"
    return (outcome, usdc)


def _pnl_by_slug(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if int(row.get("incomplete") or 0):
            continue
        slug = str(row.get("market_slug") or "")
        if slug:
            out[slug] = row
    return out


def _activity(activity_rows: list[dict[str, Any]]) -> dict[str, Any]:
    trade_rows = [row for row in activity_rows if str(row.get("activity_type") or "").upper() == "TRADE"]
    by_type = Counter(str(row.get("activity_type") or "").upper() for row in activity_rows)
    by_side = Counter(str(row.get("side") or "").upper() for row in trade_rows)
    by_outcome = Counter(str(row.get("outcome") or "") for row in trade_rows)
    prices = [_safe_float(row.get("price")) for row in trade_rows if _safe_float(row.get("price")) > 0]
    sizes = [_safe_float(row.get("size")) for row in trade_rows if _safe_float(row.get("size")) > 0]
    usdc = [_safe_float(row.get("usdc")) for row in trade_rows if _safe_float(row.get("usdc")) > 0]
    buckets = {name: {"trades": 0, "usdc": 0.0} for name, _low, _high in PRICE_BUCKETS}
    for row in trade_rows:
        name = _bucket(_safe_float(row.get("price")), PRICE_BUCKETS)
        buckets[name]["trades"] += 1
        buckets[name]["usdc"] = round(buckets[name]["usdc"] + _safe_float(row.get("usdc")), 6)
    return {
        "activity_rows": len(activity_rows),
        "trade_rows": len(trade_rows),
        "by_type": dict(by_type),
        "by_side": dict(by_side),
        "by_outcome": dict(by_outcome),
        "total_usdc": round(sum(usdc), 6),
        "avg_trade_usdc": _round(sum(usdc) / len(usdc), 6) if usdc else None,
        "median_trade_usdc": _round(_median(usdc), 6) if usdc else None,
        "avg_size": _round(sum(sizes) / len(sizes), 6) if sizes else None,
        "avg_price": _round(sum(prices) / len(prices), 6) if prices else None,
        "price_buckets": buckets,
    }


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _market_behavior(activity_rows: list[dict[str, Any]], pnl_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_market: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "usdc": 0.0, "outcomes": Counter(), "first_ts": 0, "last_ts": 0})
    for row in activity_rows:
        if str(row.get("activity_type") or "").upper() != "TRADE":
            continue
        slug = str(row.get("market_slug") or "")
        item = by_market[slug]
        item["trades"] += 1
        item["usdc"] += _safe_float(row.get("usdc"))
        outcome = str(row.get("outcome") or "")
        if outcome:
            item["outcomes"][outcome] += 1
        ts = _safe_int(row.get("exchange_ts"))
        item["first_ts"] = ts if not item["first_ts"] else min(int(item["first_ts"]), ts)
        item["last_ts"] = max(int(item["last_ts"]), ts)
    pnl_by_slug = {str(row.get("market_slug") or ""): _safe_float(row.get("realized_pnl")) for row in pnl_rows}
    dual = sum(1 for item in by_market.values() if len(item["outcomes"]) > 1)
    top = sorted(
        (
            {
                "market_slug": slug,
                "symbol": _symbol_from_slug(slug),
                "trades": item["trades"],
                "usdc": round(item["usdc"], 6),
                "outcomes": dict(item["outcomes"]),
                "dual_side": len(item["outcomes"]) > 1,
                "realized_pnl": pnl_by_slug.get(slug),
            }
            for slug, item in by_market.items()
        ),
        key=lambda row: (row["trades"], row["usdc"]),
        reverse=True,
    )
    return {
        "markets": len(by_market),
        "dual_side_markets": dual,
        "dual_side_rate": _round(dual / len(by_market), 6) if by_market else 0.0,
        "avg_trades_per_market": _round(sum(item["trades"] for item in by_market.values()) / len(by_market), 6) if by_market else 0.0,
        "top_markets": top[:20],
    }


def _timing(activity_rows: list[dict[str, Any]], matched: dict[int, dict[str, Any]]) -> dict[str, Any]:
    buckets = {name: {"trades": 0, "usdc": 0.0} for name, _low, _high in TIME_BUCKETS}
    unknown = 0
    for idx, row in enumerate(activity_rows):
        if str(row.get("activity_type") or "").upper() != "TRADE":
            continue
        ctx = matched.get(idx)
        remaining = ctx.get("window_remaining_sec") if ctx else None
        if remaining is None:
            unknown += 1
            continue
        name = _bucket(max(0.0, _safe_float(remaining)), TIME_BUCKETS)
        buckets[name]["trades"] += 1
        buckets[name]["usdc"] = round(buckets[name]["usdc"] + _safe_float(row.get("usdc")), 6)
    dominant = max(buckets.items(), key=lambda item: item[1]["trades"])[0] if any(v["trades"] for v in buckets.values()) else None
    return {"buckets": buckets, "unknown_trades": unknown, "dominant_bucket": dominant}


def _copyability(activity_rows: list[dict[str, Any]], matched: dict[int, dict[str, Any]]) -> dict[str, Any]:
    targets = {target: {"seen": 0, "ok": 0, "slippage_sum": 0.0, "slippage_count": 0} for target in TARGETS}
    spreads: list[float] = []
    ages: list[float] = []
    for idx, row in enumerate(activity_rows):
        if str(row.get("activity_type") or "").upper() != "TRADE":
            continue
        ctx = matched.get(idx)
        if not ctx:
            continue
        outcome = str(row.get("outcome") or ctx.get("trade_outcome") or "").lower()
        token_side = "up" if outcome == "up" else "down"
        book = ctx.get(token_side)
        if not isinstance(book, dict):
            continue
        if book.get("spread") is not None:
            spreads.append(_safe_float(book.get("spread")))
        if book.get("book_age_ms") is not None:
            ages.append(_safe_float(book.get("book_age_ms")))
        side = str(row.get("side") or "BUY").upper()
        fill_targets = book.get("bid_targets" if side == "SELL" else "ask_targets")
        if not isinstance(fill_targets, dict):
            continue
        trade_price = _safe_float(row.get("price"))
        for target in TARGETS:
            fill = fill_targets.get(target)
            if not isinstance(fill, dict):
                continue
            targets[target]["seen"] += 1
            if fill.get("ok"):
                targets[target]["ok"] += 1
                if fill.get("avg") is not None and trade_price > 0:
                    avg = _safe_float(fill.get("avg"))
                    targets[target]["slippage_sum"] += (trade_price - avg) * 100.0 if side == "SELL" else (avg - trade_price) * 100.0
                    targets[target]["slippage_count"] += 1
    out_targets = {}
    for target, values in targets.items():
        seen = int(values["seen"])
        count = int(values["slippage_count"])
        out_targets[target] = {
            "seen": seen,
            "ok": int(values["ok"]),
            "ok_rate": _round((values["ok"] / seen) * 100.0, 3) if seen else None,
            "avg_slippage_cents": _round(values["slippage_sum"] / count, 4) if count else None,
        }
    return {
        "matched_contexts": len(matched),
        "avg_spread": _round(sum(spreads) / len(spreads), 6) if spreads else None,
        "avg_book_age_ms": _round(sum(ages) / len(ages), 3) if ages else None,
        "targets": out_targets,
    }


def _window_group(pnl: float) -> str:
    if pnl >= LARGE_WIN_PNL:
        return "large_win"
    if pnl <= LARGE_LOSS_PNL:
        return "large_loss"
    return "middle"


def _summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "windows": 0,
            "avg_pnl": None,
            "avg_usdc": None,
            "avg_final_net_usdc": None,
            "avg_max_abs_net_usdc": None,
            "avg_late_usdc": None,
            "final_bias_accuracy": None,
        }
    accuracy_rows = [row for row in rows if row.get("final_bias_correct") is not None]
    correct = sum(1 for row in accuracy_rows if row.get("final_bias_correct"))
    return {
        "windows": len(rows),
        "avg_pnl": _round(sum(_safe_float(row.get("realized_pnl")) for row in rows) / len(rows), 6),
        "avg_usdc": _round(sum(_safe_float(row.get("total_usdc")) for row in rows) / len(rows), 6),
        "avg_final_net_usdc": _round(sum(_safe_float(row.get("final_net_usdc")) for row in rows) / len(rows), 6),
        "avg_max_abs_net_usdc": _round(sum(_safe_float(row.get("max_abs_net_usdc")) for row in rows) / len(rows), 6),
        "avg_late_usdc": _round(sum(_safe_float(row.get("late_usdc")) for row in rows) / len(rows), 6),
        "final_bias_accuracy": _round(correct / len(accuracy_rows), 6) if accuracy_rows else None,
    }


def _path_analysis(activity_rows: list[dict[str, Any]], pnl_rows: list[dict[str, Any]]) -> dict[str, Any]:
    trade_rows = [row for row in activity_rows if str(row.get("activity_type") or "").upper() == "TRADE"]
    by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        slug = str(row.get("market_slug") or "")
        if slug:
            by_market[slug].append(row)
    pnl_lookup = _pnl_by_slug(pnl_rows)
    windows: list[dict[str, Any]] = []
    checkpoint_accuracy = {str(checkpoint): {"seen": 0, "correct": 0, "accuracy": None} for checkpoint in PATH_CHECKPOINTS}

    for slug, rows in sorted(by_market.items()):
        start_ts = _window_start_from_slug(slug)
        bucket_flow = {
            name: {"trades": 0, "usdc": 0.0, "up_usdc": 0.0, "down_usdc": 0.0, "net_up_down_usdc": 0.0}
            for name, _low, _high in PATH_BUCKETS
        }
        total_usdc = 0.0
        ordered: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            ts = _safe_float(row.get("exchange_ts"))
            elapsed = max(0.0, ts - start_ts) if start_ts is not None and ts > 0 else 0.0
            ordered.append((elapsed, row))
            bucket_name = _bucket(min(elapsed, 299.999), PATH_BUCKETS)
            if bucket_name not in bucket_flow:
                continue
            outcome, directional_usdc = _flow_fields(row)
            signed = _signed_flow(row)
            usdc = _safe_float(row.get("usdc"))
            item = bucket_flow[bucket_name]
            item["trades"] += 1
            item["usdc"] += usdc
            if outcome == "up":
                item["up_usdc"] += directional_usdc
            elif outcome == "down":
                item["down_usdc"] += directional_usdc
            item["net_up_down_usdc"] += signed
            total_usdc += usdc

        ordered.sort(key=lambda item: item[0])
        cumulative = 0.0
        first_bias_bucket: str | None = None
        first_bias_side: str | None = None
        max_abs_net = 0.0
        checkpoint_sides: dict[str, str] = {}
        row_idx = 0
        for checkpoint in PATH_CHECKPOINTS:
            while row_idx < len(ordered) and ordered[row_idx][0] <= checkpoint:
                cumulative += _signed_flow(ordered[row_idx][1])
                row_idx += 1
            side = _side_from_net(cumulative)
            checkpoint_sides[str(checkpoint)] = side
            max_abs_net = max(max_abs_net, abs(cumulative))
            if first_bias_bucket is None and abs(cumulative) >= FIRST_BIAS_MIN_USDC:
                first_bias_bucket = _bucket(max(0.0, checkpoint - 0.001), PATH_BUCKETS)
                first_bias_side = side

        final_net = sum(item["net_up_down_usdc"] for item in bucket_flow.values())
        final_side = _side_from_net(final_net)
        for item in bucket_flow.values():
            for key in ("usdc", "up_usdc", "down_usdc", "net_up_down_usdc"):
                item[key] = round(item[key], 6)
        pnl_row = pnl_lookup.get(slug, {})
        winning_side = str(pnl_row.get("winning_side") or pnl_row.get("winner") or pnl_row.get("resolved_outcome") or "")
        winning_side = winning_side.capitalize() if winning_side.lower() in {"up", "down"} else ""
        final_bias_correct = final_side == winning_side if final_side != "Flat" and winning_side else None
        first_bias_correct = first_bias_side == winning_side if first_bias_side and winning_side else None
        checkpoint_correct: dict[str, bool | None] = {}
        for checkpoint, side in checkpoint_sides.items():
            correct = side == winning_side if side != "Flat" and winning_side else None
            checkpoint_correct[checkpoint] = correct
            if correct is not None:
                checkpoint_accuracy[checkpoint]["seen"] += 1
                checkpoint_accuracy[checkpoint]["correct"] += 1 if correct else 0
        late = bucket_flow["240-300s"]
        window = {
            "market_slug": slug,
            "symbol": _symbol_from_slug(slug),
            "winning_side": winning_side or None,
            "realized_pnl": _round(_safe_float(pnl_row.get("realized_pnl")), 6) if pnl_row else None,
            "trades": len(rows),
            "total_usdc": round(total_usdc, 6),
            "bucket_flow": bucket_flow,
            "final_net_usdc": _round(final_net, 6),
            "final_net_side": final_side,
            "max_abs_net_usdc": _round(max_abs_net, 6),
            "first_bias_bucket": first_bias_bucket,
            "first_bias_side": first_bias_side,
            "first_bias_min_usdc": FIRST_BIAS_MIN_USDC,
            "first_bias_correct": first_bias_correct,
            "final_bias_correct": final_bias_correct,
            "checkpoint_sides": checkpoint_sides,
            "checkpoint_correct": checkpoint_correct,
            "late_usdc": late["usdc"],
            "late_net_usdc": late["net_up_down_usdc"],
            "group": _window_group(_safe_float(pnl_row.get("realized_pnl"))) if pnl_row else "middle",
        }
        windows.append(window)

    for checkpoint, values in checkpoint_accuracy.items():
        seen = int(values["seen"])
        values["accuracy"] = _round(values["correct"] / seen, 6) if seen else None
    final_seen = [row for row in windows if row.get("final_bias_correct") is not None]
    first_seen = [row for row in windows if row.get("first_bias_correct") is not None]
    grouped = {
        name: _summarize_group([row for row in windows if row.get("group") == name])
        for name in ("large_win", "large_loss", "middle")
    }
    return {
        "summary": {
            "windows": len(windows),
            "first_bias_min_usdc": FIRST_BIAS_MIN_USDC,
            "large_win_threshold": LARGE_WIN_PNL,
            "large_loss_threshold": LARGE_LOSS_PNL,
            "winner_known": len([row for row in windows if row.get("winning_side")]),
            "final_bias_seen": len(final_seen),
            "final_bias_correct": sum(1 for row in final_seen if row.get("final_bias_correct")),
            "final_bias_accuracy": _round(sum(1 for row in final_seen if row.get("final_bias_correct")) / len(final_seen), 6) if final_seen else None,
            "first_bias_seen": len(first_seen),
            "first_bias_correct": sum(1 for row in first_seen if row.get("first_bias_correct")),
            "first_bias_accuracy": _round(sum(1 for row in first_seen if row.get("first_bias_correct")) / len(first_seen), 6) if first_seen else None,
            "large_win_count": grouped["large_win"]["windows"],
            "large_loss_count": grouped["large_loss"]["windows"],
            "checkpoint_accuracy": checkpoint_accuracy,
        },
        "group_comparison": grouped,
        "windows": sorted(windows, key=lambda row: abs(_safe_float(row.get("realized_pnl"))), reverse=True),
    }


def _hypotheses(activity: dict[str, Any], market: dict[str, Any], timing: dict[str, Any], copyability: dict[str, Any]) -> list[str]:
    out: list[str] = []
    if market.get("dual_side_rate", 0) >= 0.5 and activity.get("median_trade_usdc", 0) and activity["median_trade_usdc"] <= 25:
        out.append("maker_like_fragmented_dual_side_flow")
    if timing.get("dominant_bucket") in {"0-30s", "30-60s"}:
        out.append("late_window_execution_or_confirmation")
    if activity.get("price_buckets", {}).get(">=0.95", {}).get("trades", 0) > 0:
        out.append("terminal_near_certain_component_present")
    target25 = copyability.get("targets", {}).get("25", {})
    if target25.get("ok_rate") is not None and target25["ok_rate"] >= 50:
        out.append("taker_copyability_possible_at_25_usdc")
    if not out:
        out.append("insufficient_pattern_confidence")
    return out


def analyze_deep_wallet_export(zip_path: Path) -> dict[str, Any]:
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path) as bundle:
        manifest = _load_json(bundle, "manifest.json")
        complete_rows = _load_jsonl(bundle, "coverage_windows_complete.jsonl")
        excluded_rows = _load_jsonl(bundle, "coverage_windows_excluded.jsonl")
        wallet_activity = _load_jsonl(bundle, "wallet_activity.jsonl")
        wallet_context_rows = _load_jsonl(bundle, "wallet_trade_contexts.jsonl")
        wallet_pnl = _load_jsonl(bundle, "wallet_market_pnl.jsonl")
        deep_samples = _load_jsonl(bundle, "deep_collection/market_state_samples.jsonl")
    contexts = [_decode_context(row) for row in wallet_context_rows]
    matched = _match_contexts(wallet_activity, contexts)
    coverage = _coverage(manifest, complete_rows, excluded_rows, deep_samples)
    activity = _activity(wallet_activity)
    pnl = _pnl(wallet_pnl)
    market = _market_behavior(wallet_activity, wallet_pnl)
    timing = _timing(wallet_activity, matched)
    copyability = _copyability(wallet_activity, matched)
    path_analysis = _path_analysis(wallet_activity, wallet_pnl)
    return {
        "wallet": str(manifest.get("wallet") or ""),
        "source_zip": str(zip_path),
        "coverage": coverage,
        "pnl": pnl,
        "activity": activity,
        "market_behavior": market,
        "timing": timing,
        "copyability": copyability,
        "path_analysis": path_analysis,
        "possible_strategy_hypotheses": _hypotheses(activity, market, timing, copyability),
    }


def render_markdown_report(report: dict[str, Any]) -> str:
    coverage = report["coverage"]
    pnl = report["pnl"]
    activity = report["activity"]
    market = report["market_behavior"]
    timing = report["timing"]
    copyability = report["copyability"]
    path = report.get("path_analysis", {})
    path_summary = path.get("summary", {})
    groups = path.get("group_comparison", {})
    target25 = copyability["targets"].get("25", {})
    lines = [
        f"# Wallet Deep Analysis: {report['wallet']}",
        "",
        f"- Source: `{report['source_zip']}`",
        f"- Complete windows: {coverage['complete_windows']} (excluded {coverage['excluded_windows']})",
        f"- Deep samples: {coverage['deep_sample_rows']}",
        f"- Realized PnL in bundle: {pnl['total_realized_pnl']}",
        f"- Trade rows: {activity['trade_rows']} / total USDC: {activity['total_usdc']}",
        f"- Markets: {market['markets']} / dual-side rate: {market['dual_side_rate']}",
        f"- Dominant timing bucket: {timing['dominant_bucket']}",
        f"- 25 USDC copyability: ok_rate={target25.get('ok_rate')} avg_slip_cents={target25.get('avg_slippage_cents')}",
        f"- Final path bias accuracy: {path_summary.get('final_bias_accuracy')} ({path_summary.get('final_bias_correct')}/{path_summary.get('final_bias_seen')})",
        "",
        "## Possible Strategy Hypotheses",
        "",
    ]
    lines.extend(f"- {item}" for item in report.get("possible_strategy_hypotheses", []))
    lines.extend(["", "## Window Path Analysis", ""])
    lines.append(f"- First bias threshold: {path_summary.get('first_bias_min_usdc')} USDC net Up-Down")
    lines.append(f"- First bias accuracy: {path_summary.get('first_bias_accuracy')} ({path_summary.get('first_bias_correct')}/{path_summary.get('first_bias_seen')})")
    lines.append(f"- Final bias accuracy: {path_summary.get('final_bias_accuracy')} ({path_summary.get('final_bias_correct')}/{path_summary.get('final_bias_seen')})")
    for name in ("large_win", "large_loss", "middle"):
        group = groups.get(name, {})
        lines.append(
            f"- {name}: windows={group.get('windows')} avg_pnl={group.get('avg_pnl')} "
            f"avg_usdc={group.get('avg_usdc')} avg_final_net={group.get('avg_final_net_usdc')} "
            f"avg_late_usdc={group.get('avg_late_usdc')}"
        )
    lines.extend(["", "## Top PnL Markets", ""])
    for row in pnl.get("top_markets", [])[:10]:
        lines.append(f"- `{row['market_slug']}` {row['symbol']} pnl={row['realized_pnl']}")
    lines.append("")
    return "\n".join(lines)
