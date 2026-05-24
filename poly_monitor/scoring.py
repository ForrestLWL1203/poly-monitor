from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(frozen=True)
class CandidateThresholds:
    min_trades_7d: int = 500
    min_markets_24h: int = 5
    min_trades_30d: int = 800
    max_top1_concentration: float = 0.25
    max_top3_concentration: float = 0.50
    max_longshot_profit_share: float = 0.35
    min_repeat_longshot_profit_markets: int = 3
    min_resolved_markets_for_win_loss_check: int = 20
    min_settled_markets_for_local_active: int = 5
    active_max_age_hours: float = 48.0
    archive_age_hours: float = 14 * 24.0
    dormant_min_historical_trades: int = 500
    dormant_min_historical_markets: int = 5


@dataclass(frozen=True)
class CandidateScore:
    wallet: str
    status: str
    rank_score: float
    reasons: list[str]
    metrics: dict[str, Any]


def _num(metrics: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = metrics.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _active_failures(metrics: dict[str, Any], thresholds: CandidateThresholds) -> list[str]:
    longshot_high = _num(metrics, "longshot_profit_share") > thresholds.max_longshot_profit_share
    repeat_longshot = _num(metrics, "longshot_profit_markets") >= thresholds.min_repeat_longshot_profit_markets
    markets_24h = _num(metrics, "markets_24h", _num(metrics, "markets_7d"))
    if metrics.get("markets_24h_lower_bound") and markets_24h > 0:
        markets_24h = max(markets_24h, float(thresholds.min_markets_24h))
    wins = _num(metrics, "wins_7d")
    losses = _num(metrics, "losses_7d")
    resolved_markets = wins + losses
    win_loss_failed = resolved_markets >= thresholds.min_resolved_markets_for_win_loss_check and wins < losses
    checks = [
        ("trades_7d_below_threshold", _num(metrics, "trades_7d") < thresholds.min_trades_7d),
        ("markets_24h_below_threshold", markets_24h < thresholds.min_markets_24h),
        ("trades_30d_below_threshold", _num(metrics, "trades_30d") < thresholds.min_trades_30d),
        (
            "settled_markets_7d_below_threshold",
            metrics.get("pnl_source") == "local_observed_ledger"
            and _num(metrics, "settled_markets_7d") < thresholds.min_settled_markets_for_local_active,
        ),
        ("pnl_7d_not_positive", _num(metrics, "pnl_7d") <= 0),
        ("pnl_30d_not_positive", _num(metrics, "pnl_30d") <= 0),
        ("wins_7d_below_losses", win_loss_failed),
        ("top1_concentration_high", _num(metrics, "top1_concentration") > thresholds.max_top1_concentration),
        ("top3_concentration_high", _num(metrics, "top3_concentration") > thresholds.max_top3_concentration),
        ("longshot_profit_share_high_without_repetition", longshot_high and not repeat_longshot),
        ("inactive_for_active", _num(metrics, "last_active_age_hours", 999999) > thresholds.active_max_age_hours),
    ]
    return [reason for reason, failed in checks if failed]


def _dormant_ok(metrics: dict[str, Any], thresholds: CandidateThresholds) -> bool:
    if (
        metrics.get("pnl_source") == "local_observed_ledger"
        and _num(metrics, "historical_trades") >= thresholds.dormant_min_historical_trades
        and _num(metrics, "historical_markets") >= thresholds.dormant_min_historical_markets
        and _num(metrics, "settled_markets_30d") < thresholds.min_settled_markets_for_local_active
    ):
        return True
    return (
        _num(metrics, "historical_trades") >= thresholds.dormant_min_historical_trades
        and _num(metrics, "historical_markets") >= thresholds.dormant_min_historical_markets
        and _num(metrics, "historical_pnl") > 0
        and _num(metrics, "top1_concentration") <= thresholds.max_top1_concentration
        and _num(metrics, "top3_concentration") <= thresholds.max_top3_concentration
        and _num(metrics, "longshot_profit_share") <= thresholds.max_longshot_profit_share
    )


def _capped(value: float, cap: float) -> float:
    if cap <= 0:
        return 0.0
    return min(max(value, 0.0), cap)


def _log_bonus(value: float, *, scale: float, cap: float) -> float:
    if value <= 0:
        return 0.0
    return min(math.log10(value + 1.0) * scale, cap)


def _active_rank_score(metrics: dict[str, Any]) -> float:
    wins = _num(metrics, "wins_7d")
    losses = _num(metrics, "losses_7d")
    resolved_markets = wins + losses
    win_rate = wins / resolved_markets if resolved_markets > 0 else 0.0

    trades_7d = _num(metrics, "trades_7d")
    trades_30d = _num(metrics, "trades_30d")
    markets_24h = _num(metrics, "markets_24h", _num(metrics, "markets_7d"))
    if metrics.get("markets_24h_lower_bound") and markets_24h > 0:
        markets_24h = max(markets_24h, min(120.0, _num(metrics, "trades_24h") / 10.0))
    last_active_age = _num(metrics, "last_active_age_hours", 999999)

    quality = win_rate * 360.0
    sample_confidence = _capped(resolved_markets, 100.0) * 1.5
    activity = (
        _capped(markets_24h, 120.0) * 2.2
        + _capped(trades_7d, 2500.0) * 0.08
        + _capped(trades_30d, 6000.0) * 0.025
    )
    pnl_bonus = (
        _log_bonus(_num(metrics, "pnl_7d"), scale=18.0, cap=75.0)
        + _log_bonus(_num(metrics, "pnl_30d"), scale=12.0, cap=60.0)
    )
    stability_penalty = (
        _num(metrics, "top1_concentration") * 130.0
        + _num(metrics, "top3_concentration") * 90.0
    )
    recency_penalty = _capped(last_active_age, 48.0) * 1.5

    return quality + sample_confidence + activity + pnl_bonus - stability_penalty - recency_penalty


def _dormant_rank_score(metrics: dict[str, Any]) -> float:
    historical_trades = _num(metrics, "historical_trades")
    historical_markets = _num(metrics, "historical_markets")
    last_active_age = _num(metrics, "last_active_age_hours", 999999)

    activity = _capped(historical_markets, 300.0) * 1.4 + _capped(historical_trades, 8000.0) * 0.025
    pnl_bonus = _log_bonus(_num(metrics, "historical_pnl"), scale=18.0, cap=90.0)
    stability_penalty = (
        _num(metrics, "top1_concentration") * 100.0
        + _num(metrics, "top3_concentration") * 70.0
    )
    recency_penalty = _capped(last_active_age, 14 * 24.0) * 0.08
    return activity + pnl_bonus - stability_penalty - recency_penalty


def score_wallet(metrics: dict[str, Any], thresholds: CandidateThresholds | None = None) -> CandidateScore:
    thresholds = thresholds or CandidateThresholds()
    wallet = str(metrics.get("wallet") or "").lower()
    last_active_age = _num(metrics, "last_active_age_hours", 999999)
    if last_active_age > thresholds.archive_age_hours:
        return CandidateScore(wallet, "archive_candidate", 0.0, ["inactive_for_archive"], dict(metrics))

    failures = _active_failures(metrics, thresholds)
    if not failures:
        rank_score = _active_rank_score(metrics)
        return CandidateScore(wallet, "active_candidate", round(rank_score, 6), [], dict(metrics))

    if _dormant_ok(metrics, thresholds):
        rank_score = _dormant_rank_score(metrics)
        return CandidateScore(wallet, "dormant_candidate", round(rank_score, 6), failures, dict(metrics))

    return CandidateScore(wallet, "archive_candidate", 0.0, failures, dict(metrics))
