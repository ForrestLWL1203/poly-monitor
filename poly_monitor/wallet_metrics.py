from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any

from .data_api import fetch_closed_positions, fetch_user_activity, fetch_user_pnl_history, fetch_user_positions, fetch_user_profit

DEFAULT_ACTIVITY_PAGES = 3
SATURATED_MARKETS_24H = 288
SATURATED_MARKETS_MIN_TRADES_24H = 500


def is_crypto_5m_row(row: dict[str, Any]) -> bool:
    slug = str(row.get("slug") or row.get("eventSlug") or "")
    return slug.startswith(("btc-updown-5m-", "eth-updown-5m-"))


def row_slug(row: dict[str, Any]) -> str:
    return str(row.get("slug") or row.get("eventSlug") or "")


def _timestamp_from_end_date(row: dict[str, Any]) -> int | None:
    raw = row.get("endDate")
    if not raw:
        return None
    try:
        value = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(value.timestamp())


def _timestamp_from_slug(row: dict[str, Any]) -> int | None:
    slug = row_slug(row)
    try:
        return int(slug.rsplit("-", 1)[1])
    except (IndexError, TypeError, ValueError):
        return None


def _position_is_settled(row: dict[str, Any]) -> bool:
    try:
        cur_price = float(row.get("curPrice"))
    except (TypeError, ValueError):
        return False
    return cur_price in (0.0, 1.0)


def _pnl_value(row: dict[str, Any]) -> float:
    try:
        return float(row.get("cashPnl") if row.get("cashPnl") is not None else row.get("realizedPnl") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _concentration(values: list[float], top_n: int) -> float:
    positives = sorted([value for value in values if value > 0], reverse=True)
    total = sum(positives)
    if total <= 0:
        return 1.0
    return sum(positives[:top_n]) / total


def behavior_metrics(trades: list[dict[str, Any]], closed: list[dict[str, Any]]) -> dict[str, Any]:
    by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trades:
        by_market[row_slug(row)].append(row)
    if not by_market:
        return {"dual_side_rate": 0.0, "late_bias_shift": 0.0, "winner_add_rate": 0.0}

    dual_count = 0
    late_bias_count = 0
    winner_match_count = 0
    winner_known_count = 0
    winners = {
        row_slug(row) or str(row.get("title") or ""): str(row.get("outcome") or "")
        for row in closed
        if float(row.get("realizedPnl") or row.get("cashPnl") or 0.0) > 0
    }
    for slug, rows in by_market.items():
        outcomes = {str(row.get("outcome") or "") for row in rows if row.get("outcome")}
        if len(outcomes) > 1:
            dual_count += 1
        ordered = sorted(rows, key=lambda row: int(row.get("timestamp") or 0))
        cutoff_idx = max(0, len(ordered) // 2)
        late_rows = ordered[cutoff_idx:]
        late_by_outcome: dict[str, float] = defaultdict(float)
        for row in late_rows:
            late_by_outcome[str(row.get("outcome") or "")] += float(row.get("usdcSize") or 0.0)
        late_total = sum(late_by_outcome.values())
        dominant_outcome = None
        if late_total > 0 and late_by_outcome:
            dominant_outcome, dominant_usdc = max(late_by_outcome.items(), key=lambda item: item[1])
            if dominant_usdc / late_total >= 0.60:
                late_bias_count += 1
        winner = winners.get(slug)
        if winner:
            winner_known_count += 1
            if dominant_outcome == winner:
                winner_match_count += 1
    market_count = len(by_market)
    return {
        "dual_side_rate": round(dual_count / market_count, 6),
        "late_bias_shift": round(late_bias_count / market_count, 6),
        "winner_add_rate": round(winner_match_count / winner_known_count, 6) if winner_known_count else 0.0,
    }


def _profile_profit(wallet: str, window: str) -> tuple[float | None, str | None, str | None]:
    try:
        row = fetch_user_profit(wallet, window=window)
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"
    if not row:
        return None, None, "empty_profit_response"
    try:
        amount = float(row.get("amount") or 0.0)
    except (TypeError, ValueError):
        return None, None, "invalid_profit_amount"
    name = str(row.get("name") or row.get("pseudonym") or "")
    return amount, name, None


def _portfolio_pnl_delta(wallet: str, interval: str, fidelity: str, *, now_ts: int) -> tuple[float | None, str | None]:
    try:
        rows = fetch_user_pnl_history(wallet, interval=interval, fidelity=fidelity)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not rows:
        return None, "empty_portfolio_pnl_response"
    cutoff = {
        "1d": now_ts - 86400,
        "1w": now_ts - 7 * 86400,
        "1m": now_ts - 30 * 86400,
    }.get(interval.lower())
    if cutoff is not None:
        rows = [row for row in rows if int(row.get("t") or 0) >= cutoff]
    if len(rows) < 2:
        return None, "insufficient_portfolio_pnl_points"
    try:
        start = float(rows[0].get("p") or 0.0)
        end = float(rows[-1].get("p") or 0.0)
    except (TypeError, ValueError):
        return None, "invalid_portfolio_pnl_points"
    return end - start, None


def build_metrics_from_api(
    wallet: str,
    *,
    now_ts: int | None = None,
    activity_pages: int = DEFAULT_ACTIVITY_PAGES,
    closed_pages: int = 4,
) -> dict[str, Any]:
    now = now_ts or int(dt.datetime.now(dt.timezone.utc).timestamp())
    activity: list[dict[str, Any]] = []
    activity_page_cap_hit = False
    for page in range(activity_pages):
        try:
            batch = fetch_user_activity(wallet, limit=500, offset=page * 500, start=now - 30 * 86400, end=now)
        except Exception:
            if activity:
                break
            raise
        if not batch:
            break
        activity.extend(batch)
        if len(batch) < 500:
            break
        if page + 1 == activity_pages:
            activity_page_cap_hit = True
    trades = [row for row in activity if row.get("type") == "TRADE" and is_crypto_5m_row(row)]
    closed_raw: list[dict[str, Any]] = []
    for page in range(closed_pages):
        try:
            batch = fetch_closed_positions(wallet, limit=50, offset=page * 50)
        except Exception:
            if closed_raw:
                break
            raise
        if not batch:
            break
        closed_raw.extend(batch)
        if len(batch) < 50:
            break
    positions_raw: list[dict[str, Any]] = []
    positions_error = ""
    for page in range(closed_pages):
        try:
            batch = fetch_user_positions(wallet, limit=100, offset=page * 100)
        except Exception as exc:
            positions_error = f"{type(exc).__name__}: {exc}"
            if positions_raw:
                break
            batch = []
        if not batch:
            break
        positions_raw.extend(batch)
        if len(batch) < 100:
            break
    closed = [row for row in closed_raw if is_crypto_5m_row(row)]
    settled_positions = [row for row in positions_raw if is_crypto_5m_row(row) and _position_is_settled(row)]
    cutoff_7d = now - 7 * 86400
    cutoff_30d = now - 30 * 86400
    cutoff_24h = now - 86400
    trades_24h = [row for row in trades if int(row.get("timestamp") or 0) >= cutoff_24h]
    trades_7d = [row for row in trades if int(row.get("timestamp") or 0) >= cutoff_7d]
    trades_30d = [row for row in trades if int(row.get("timestamp") or 0) >= cutoff_30d]
    pnl_by_market_7d: dict[str, float] = defaultdict(float)
    pnl_by_market_30d: dict[str, float] = defaultdict(float)
    settled_pnl_by_market_7d: dict[str, float] = defaultdict(float)
    settled_pnl_by_market_30d: dict[str, float] = defaultdict(float)
    longshot_profit_30d = 0.0
    longshot_profit_markets: set[str] = set()
    settled_longshot_profit_30d = 0.0
    settled_longshot_profit_markets: set[str] = set()
    for row in closed:
        end_ts = _timestamp_from_slug(row) or _timestamp_from_end_date(row) or now
        pnl = float(row.get("realizedPnl") or row.get("cashPnl") or 0.0)
        slug = row_slug(row)
        if end_ts >= cutoff_7d:
            pnl_by_market_7d[slug] += pnl
        if end_ts >= cutoff_30d:
            pnl_by_market_30d[slug] += pnl
            if pnl > 0 and float(row.get("avgPrice") or 1.0) < 0.15:
                longshot_profit_30d += pnl
                longshot_profit_markets.add(slug)
    for row in settled_positions:
        end_ts = _timestamp_from_slug(row) or _timestamp_from_end_date(row) or now
        pnl = _pnl_value(row)
        slug = row_slug(row)
        if end_ts >= cutoff_7d:
            settled_pnl_by_market_7d[slug] += pnl
        if end_ts >= cutoff_30d:
            settled_pnl_by_market_30d[slug] += pnl
            if pnl > 0 and float(row.get("avgPrice") or 1.0) < 0.15:
                settled_longshot_profit_30d += pnl
                settled_longshot_profit_markets.add(slug)
    market_pnls_7d = list(pnl_by_market_7d.values())
    market_pnls_30d = list(pnl_by_market_30d.values())
    settled_market_pnls_7d = list(settled_pnl_by_market_7d.values())
    settled_market_pnls_30d = list(settled_pnl_by_market_30d.values())
    closed_position_wins_7d = sum(1 for value in market_pnls_7d if value > 0)
    closed_position_losses_7d = sum(1 for value in market_pnls_7d if value < 0)
    crypto_closed_pnl_estimate_7d = round(sum(market_pnls_7d), 6)
    crypto_closed_pnl_estimate_30d = round(sum(market_pnls_30d), 6)
    crypto_settled_positions_pnl_7d = round(sum(settled_market_pnls_7d), 6)
    crypto_settled_positions_pnl_30d = round(sum(settled_market_pnls_30d), 6)
    settled_positions_available = bool(settled_market_pnls_7d or settled_market_pnls_30d)
    portfolio_pnl_1d, portfolio_error_1d = _portfolio_pnl_delta(wallet, "1d", "1h", now_ts=now)
    portfolio_pnl_7d, portfolio_error_7d = _portfolio_pnl_delta(wallet, "1w", "3h", now_ts=now)
    portfolio_pnl_30d, portfolio_error_30d = _portfolio_pnl_delta(wallet, "1m", "18h", now_ts=now)
    lb_profit_7d, profile_name_7d, lb_error_7d = _profile_profit(wallet, "7d")
    lb_profit_30d, profile_name_30d, lb_error_30d = _profile_profit(wallet, "30d")
    if portfolio_pnl_7d is not None or portfolio_pnl_30d is not None:
        pnl_7d = portfolio_pnl_7d if portfolio_pnl_7d is not None else 0.0
        pnl_30d = portfolio_pnl_30d if portfolio_pnl_30d is not None else 0.0
        pnl_source = "profile_portfolio_pnl"
    elif lb_profit_7d is not None or lb_profit_30d is not None:
        pnl_7d = lb_profit_7d if lb_profit_7d is not None else 0.0
        pnl_30d = lb_profit_30d if lb_profit_30d is not None else 0.0
        pnl_source = "leaderboard_profit"
    elif settled_positions_available:
        pnl_7d = crypto_settled_positions_pnl_7d
        pnl_30d = crypto_settled_positions_pnl_30d
        pnl_source = "crypto_settled_positions"
    else:
        pnl_7d = crypto_closed_pnl_estimate_7d
        pnl_30d = crypto_closed_pnl_estimate_30d
        pnl_source = "crypto_closed_positions"
    if market_pnls_30d:
        concentration_pnls_30d = market_pnls_30d
        source_longshot_profit_30d = longshot_profit_30d
        source_longshot_profit_markets = longshot_profit_markets
    else:
        concentration_pnls_30d = settled_market_pnls_30d
        source_longshot_profit_30d = settled_longshot_profit_30d
        source_longshot_profit_markets = settled_longshot_profit_markets
    total_profit_30d = sum(value for value in concentration_pnls_30d if value > 0)
    last_ts = max([int(row.get("timestamp") or 0) for row in trades] or [0])
    trade_markets_24h = {row_slug(row) for row in trades_24h if row_slug(row)}
    btc_trade_markets_24h = {slug for slug in trade_markets_24h if slug.startswith("btc-updown-5m-")}
    eth_trade_markets_24h = {slug for slug in trade_markets_24h if slug.startswith("eth-updown-5m-")}
    trade_markets_7d = {row_slug(row) for row in trades_7d if row_slug(row)}
    trade_markets_30d = {row_slug(row) for row in trades_30d if row_slug(row)}
    all_trade_markets = {row_slug(row) for row in trades if row_slug(row)}
    markets_24h_lower_bound = activity_page_cap_hit and bool(trades_24h)
    markets_24h = len(trade_markets_24h)
    if activity_page_cap_hit and len(trades_24h) >= SATURATED_MARKETS_MIN_TRADES_24H:
        markets_24h = SATURATED_MARKETS_24H
        markets_24h_lower_bound = True
    metrics = {
        "wallet": wallet.lower(),
        "trades_24h": len(trades_24h),
        "markets_24h": markets_24h,
        "btc_markets_24h": len(btc_trade_markets_24h),
        "eth_markets_24h": len(eth_trade_markets_24h),
        "markets_24h_lower_bound": markets_24h_lower_bound,
        "activity_page_cap_hit": activity_page_cap_hit,
        "activity_rows_sampled": len(activity),
        "trades_7d": len(trades_7d),
        "markets_7d": len(trade_markets_7d),
        "trades_30d": len(trades_30d),
        "markets_30d": len(trade_markets_30d),
        "pnl_7d": round(pnl_7d, 6),
        "pnl_30d": round(pnl_30d, 6),
        "pnl_source": pnl_source,
        "profile_pnl_1d": round(portfolio_pnl_1d, 6) if portfolio_pnl_1d is not None else None,
        "profile_pnl_7d": round(portfolio_pnl_7d, 6) if portfolio_pnl_7d is not None else None,
        "profile_pnl_30d": round(portfolio_pnl_30d, 6) if portfolio_pnl_30d is not None else None,
        "leaderboard_profit_pnl_7d": round(lb_profit_7d, 6) if lb_profit_7d is not None else None,
        "leaderboard_profit_pnl_30d": round(lb_profit_30d, 6) if lb_profit_30d is not None else None,
        "profile_name": profile_name_30d or profile_name_7d,
        "profile_pnl_error": "; ".join(error for error in [portfolio_error_1d, portfolio_error_7d, portfolio_error_30d] if error),
        "leaderboard_profit_error": "; ".join(error for error in [lb_error_7d, lb_error_30d] if error),
        "crypto_settled_positions_pnl_7d": crypto_settled_positions_pnl_7d,
        "crypto_settled_positions_pnl_30d": crypto_settled_positions_pnl_30d,
        "crypto_settled_positions_markets_7d": len(settled_market_pnls_7d),
        "crypto_settled_positions_markets_30d": len(settled_market_pnls_30d),
        "crypto_settled_positions_error": positions_error,
        "crypto_closed_pnl_estimate_7d": crypto_closed_pnl_estimate_7d,
        "crypto_closed_pnl_estimate_30d": crypto_closed_pnl_estimate_30d,
        "wins_7d": 0,
        "losses_7d": 0,
        "closed_position_wins_7d": closed_position_wins_7d,
        "closed_position_losses_7d": closed_position_losses_7d,
        "top1_concentration": round(_concentration(concentration_pnls_30d, 1), 6),
        "top3_concentration": round(_concentration(concentration_pnls_30d, 3), 6),
        "longshot_profit_share": round(source_longshot_profit_30d / total_profit_30d, 6) if total_profit_30d > 0 else 0.0,
        "longshot_profit_markets": len(source_longshot_profit_markets),
        "last_active_age_hours": round((now - last_ts) / 3600.0, 3) if last_ts else 999999.0,
        "historical_trades": len(trades),
        "historical_markets": len(all_trade_markets),
        "historical_pnl": round(pnl_30d, 6),
    }
    metrics.update(behavior_metrics(trades, settled_positions or closed))
    return metrics
