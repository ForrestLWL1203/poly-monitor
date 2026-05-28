from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .strategy_runtime import (
    ExecutionAdapter,
    ExecutionResult,
    PaperExecutionAdapter,
    RecordingExecutionAdapter,
    StrategyHistory,
    StrategySnapshot,
    TradeIntent,
    _load_jsonl_from_zip,
    winning_side_from_row,
)


@dataclass(frozen=True)
class PathStrategyConfig:
    wallet: str
    checkpoints: tuple[int, ...] = (120, 180, 240)
    notional_usdc: float = 25.0
    first_bias_min_usdc: float = 25.0
    max_price: float = 0.95
    wallet_exposure_scale: float = 0.01
    target_pair_notional_usdc: float = 25.0
    target_pair_shares_per_side: float | None = None
    max_pair_cost: float = 0.99
    max_unpaired_price: float = 0.6
    max_inventory_imbalance_ratio: float = 0.05
    early_inventory_imbalance_ratio: float = 0.30
    mid_inventory_imbalance_ratio: float = 0.15
    late_inventory_imbalance_ratio: float = 0.08
    final_inventory_imbalance_ratio: float = 0.03
    rebalance_start_sec: int = 240
    maker_rebalance_ticks: int = 1
    tick_size: float = 0.01
    min_order_usdc: float = 1.0
    max_quote_spread: float | None = None
    max_quote_book_age_ms: float | None = None
    min_quote_bid_depth_usdc: float | None = None
    execution_style: str = "maker"
    one_trade_per_market: bool = True
    terminal_bias_start_sec: int = 180
    terminal_strong_start_sec: int = 240
    terminal_max_price: float = 0.95
    bias_score_threshold: int = 3
    min_reference_move_bps: float = 1.0
    min_recent_move_bps: float = 0.5
    terminal_favorite_bid: float = 0.85
    terminal_favorite_mid: float = 0.80


SettlementPaperExecutionAdapter = PaperExecutionAdapter


@dataclass(frozen=True)
class ReplayResult:
    intents: list[TradeIntent]
    executions: list[ExecutionResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "intents": [intent.to_dict() for intent in self.intents],
            "executions": [result.to_dict() for result in self.executions],
        }


@dataclass(frozen=True)
class DeepExportReplayInput:
    activity_rows: list[dict[str, Any]]
    market_state_samples: list[dict[str, Any]]
    winning_sides: dict[str, str] = field(default_factory=dict)


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


def _decode_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


def load_deep_export_for_path_strategy(zip_path: Path) -> DeepExportReplayInput:
    with zipfile.ZipFile(Path(zip_path)) as bundle:
        pnl_rows = _load_jsonl_from_zip(bundle, "wallet_market_pnl.jsonl")
        return DeepExportReplayInput(
            activity_rows=_load_jsonl_from_zip(bundle, "wallet_activity.jsonl"),
            market_state_samples=_load_jsonl_from_zip(bundle, "deep_collection/market_state_samples.jsonl"),
            winning_sides={
                str(row.get("market_slug") or ""): winning_side_from_row(row)
                for row in pnl_rows
                if row.get("market_slug") and winning_side_from_row(row)
            },
        )


def _window_start_from_slug(slug: str) -> int | None:
    try:
        return int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _elapsed_sec(slug: str, ts: int) -> int | None:
    start = _window_start_from_slug(slug)
    if start is None:
        return None
    return max(0, ts - start)


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


def _net_side(net_usdc: float) -> str | None:
    if net_usdc > 0:
        return "Up"
    if net_usdc < 0:
        return "Down"
    return None


def _checkpoint_for_elapsed(elapsed: int, checkpoints: tuple[int, ...]) -> int | None:
    ready = [checkpoint for checkpoint in checkpoints if elapsed >= checkpoint]
    return max(ready) if ready else None


def _book_for_outcome(sample: dict[str, Any], outcome: str) -> dict[str, Any]:
    key = "up_json" if outcome == "Up" else "down_json"
    return _decode_json(sample.get(key))


def _signed_shares(row: dict[str, Any]) -> float:
    side = str(row.get("side") or "BUY").upper()
    shares = _safe_float(row.get("size"))
    return -shares if side == "SELL" else shares


def _paper_inventory(history: StrategyHistory, market_slug: str, outcome: str) -> tuple[float, float]:
    shares = 0.0
    cost = 0.0
    for intent in [*history.emitted_intents, *history.pending_intents]:
        if intent.market_slug != market_slug or intent.outcome != outcome or intent.intent != "BUY":
            continue
        if intent.expected_price > 0:
            shares += intent.notional_usdc / intent.expected_price
            cost += intent.notional_usdc
    return shares, cost


def _avg_price(cost: float, shares: float) -> float | None:
    return cost / shares if shares > 0 else None


def _imbalance_ratio(up_shares: float, down_shares: float) -> float:
    total = up_shares + down_shares
    return abs(up_shares - down_shares) / total if total > 0 else 0.0


def _fill_for_order(book: Any, order_notional: float) -> tuple[dict[str, Any] | None, float]:
    target_key = f"{order_notional:g}"
    fill = book.ask_targets.get(target_key)
    if isinstance(fill, dict) and fill.get("ok"):
        return fill, _safe_float(fill.get("avg"))
    rounded_key = f"{round(order_notional, 6):g}"
    fill = book.ask_targets.get(rounded_key)
    if isinstance(fill, dict) and fill.get("ok"):
        return fill, _safe_float(fill.get("avg"))
    ask = _safe_float(book.ask)
    depth = _safe_float(book.ask_depth_usdc, default=float("inf"))
    if ask > 0 and depth + 1e-9 >= order_notional:
        return {"ok": True, "avg": ask, "filled_usdc": round(order_notional, 6), "source": "best_ask_depth_estimate"}, ask
    return None, 0.0


def _reference_move_bps(start_price: float, end_price: float) -> float:
    if start_price <= 0 or end_price <= 0:
        return 0.0
    return round(((end_price - start_price) / start_price) * 10000.0, 6)


def _last_reference_before(snapshots: list[StrategySnapshot], sampled_ts: int, lookback_sec: int) -> StrategySnapshot | None:
    cutoff = int(sampled_ts) - int(lookback_sec)
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.sampled_ts <= cutoff and _safe_float(snapshot.reference_price) > 0
    ]
    return candidates[-1] if candidates else None


def _maker_quote_at_price(order_notional: float, quote_price: float, *, source: str) -> tuple[dict[str, Any] | None, float]:
    if quote_price <= 0:
        return None, 0.0
    return {
        "ok": True,
        "avg": round(quote_price, 6),
        "filled_usdc": round(order_notional, 6),
        "source": source,
    }, quote_price


def _dynamic_imbalance_limit(config: PathStrategyConfig, elapsed_sec: int) -> float:
    if elapsed_sec >= int(config.rebalance_start_sec):
        return float(config.final_inventory_imbalance_ratio)
    if elapsed_sec >= 180:
        return float(config.late_inventory_imbalance_ratio)
    if elapsed_sec >= 60:
        return float(config.mid_inventory_imbalance_ratio)
    return float(config.early_inventory_imbalance_ratio)


class WalletPathStrategy:
    strategy_name = "wallet_path_v0"
    one_trade_per_market = False

    def __init__(self, config: PathStrategyConfig) -> None:
        self.config = config

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        if snapshot.book_stale:
            return None
        checkpoint = _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints)
        if checkpoint is None:
            return None
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        if up_ask <= 0 or down_ask <= 0 or up_ask > self.config.max_price or down_ask > self.config.max_price:
            return None
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        top_pair_cost = round(up_ask + down_ask, 6)
        maker_pair_cost = round(up_bid + down_bid, 6) if up_bid > 0 and down_bid > 0 else None
        progress = min(1.0, max(0.0, snapshot.elapsed_sec / 300.0))
        if self.config.target_pair_shares_per_side is not None and self.config.target_pair_shares_per_side > 0:
            target_pair_shares = float(self.config.target_pair_shares_per_side) * progress
            sizing_mode = "shares_per_side"
        else:
            target_pair_shares = (float(self.config.target_pair_notional_usdc) * progress) / top_pair_cost
            sizing_mode = "notional_pair"
        target_shares = {
            "Up": target_pair_shares,
            "Down": target_pair_shares,
        }
        current_inventory = {
            outcome: _paper_inventory(history, snapshot.market_slug, outcome)
            for outcome in ("Up", "Down")
        }
        current_shares = {outcome: current_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_cost = {outcome: current_inventory[outcome][1] for outcome in ("Up", "Down")}
        deficits = {
            outcome: target_shares[outcome] - current_shares[outcome]
            for outcome in ("Up", "Down")
        }
        current_pair_avg = None
        current_up_avg = _avg_price(current_cost["Up"], current_shares["Up"])
        current_down_avg = _avg_price(current_cost["Down"], current_shares["Down"])
        if current_up_avg is not None and current_down_avg is not None:
            current_pair_avg = current_up_avg + current_down_avg
        current_imbalance = _imbalance_ratio(current_shares["Up"], current_shares["Down"])
        imbalance_limit = _dynamic_imbalance_limit(self.config, snapshot.elapsed_sec)
        deficit_side = "Up" if current_shares["Up"] < current_shares["Down"] else "Down" if current_shares["Down"] < current_shares["Up"] else None
        candidates: list[tuple[float, float, float, str, dict[str, Any], float, float, float | None, float]] = []
        for outcome in ("Up", "Down"):
            book = snapshot.book_for_outcome(outcome)
            ask = _safe_float(book.ask)
            if ask <= 0 or ask > self.config.max_price:
                continue
            quote_source = "maker_quote_at_best_bid"
            quote_price = _safe_float(book.bid) if self.config.execution_style == "maker" else ask
            if self.config.execution_style == "maker" and outcome == deficit_side and snapshot.elapsed_sec >= int(self.config.rebalance_start_sec):
                quote_price = min(
                    ask,
                    float(self.config.max_price),
                    quote_price + float(self.config.tick_size) * max(0, int(self.config.maker_rebalance_ticks)),
                )
                quote_source = "maker_rebalance_quote"
            if quote_price <= 0 or quote_price > self.config.max_price:
                continue
            desired_notional = deficits[outcome] * quote_price
            order_notional = min(float(self.config.notional_usdc), desired_notional)
            if order_notional + 1e-9 < float(self.config.min_order_usdc):
                continue
            if self.config.execution_style == "maker":
                fill, expected_price = _maker_quote_at_price(round(order_notional, 6), quote_price, source=quote_source)
            else:
                fill, expected_price = _fill_for_order(book, round(order_notional, 6))
            if fill is None or expected_price <= 0 or expected_price > self.config.max_price:
                continue
            fill_shares = order_notional / expected_price
            projected_shares = dict(current_shares)
            projected_cost = dict(current_cost)
            projected_shares[outcome] += fill_shares
            projected_cost[outcome] += order_notional
            projected_up_avg = _avg_price(projected_cost["Up"], projected_shares["Up"])
            projected_down_avg = _avg_price(projected_cost["Down"], projected_shares["Down"])
            projected_pair_avg = None
            if projected_up_avg is not None and projected_down_avg is not None:
                projected_pair_avg = projected_up_avg + projected_down_avg
                if projected_pair_avg > float(self.config.max_pair_cost):
                    continue
            elif expected_price > float(self.config.max_unpaired_price):
                continue
            projected_imbalance = _imbalance_ratio(projected_shares["Up"], projected_shares["Down"])
            if projected_pair_avg is not None and projected_imbalance > imbalance_limit:
                if projected_imbalance >= current_imbalance:
                    continue
            imbalance_improvement = current_imbalance - projected_imbalance
            candidates.append((imbalance_improvement, order_notional, deficits[outcome], outcome, fill, expected_price, desired_notional, projected_pair_avg, projected_imbalance))
        if not candidates:
            return None
        _imbalance_improvement, order_notional, _deficit_shares, outcome, fill, expected_price, desired_notional, projected_pair_avg, projected_imbalance = max(
            candidates,
            key=lambda item: (item[0], -item[5], item[1], item[2], item[3]),
        )
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=self.config.wallet.lower(),
            market_slug=snapshot.market_slug,
            sampled_ts=snapshot.sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=round(float(order_notional), 6),
            max_price=float(self.config.max_price),
            expected_price=round(expected_price, 6),
            symbol=snapshot.symbol,
            reason=f"checkpoint_{checkpoint}_pair_cost_inventory",
            features={
                "elapsed_sec": snapshot.elapsed_sec,
                "inventory_progress": round(progress, 6),
                "top_pair_cost": top_pair_cost,
                "pair_cost": top_pair_cost,
                "maker_pair_cost": maker_pair_cost,
                "execution_style": self.config.execution_style,
                "max_pair_cost": float(self.config.max_pair_cost),
                "max_unpaired_price": float(self.config.max_unpaired_price),
                "max_inventory_imbalance_ratio": float(self.config.max_inventory_imbalance_ratio),
                "dynamic_inventory_imbalance_limit": round(imbalance_limit, 6),
                "deficit_side": deficit_side,
                "rebalance_start_sec": int(self.config.rebalance_start_sec),
                "current_pair_avg": round(current_pair_avg, 6) if current_pair_avg is not None else None,
                "projected_pair_avg": round(projected_pair_avg, 6) if projected_pair_avg is not None else None,
                "current_imbalance_ratio": round(current_imbalance, 6),
                "projected_imbalance_ratio": round(projected_imbalance, 6),
                "sizing_mode": sizing_mode,
                "target_pair_notional_usdc": float(self.config.target_pair_notional_usdc),
                "target_pair_shares_per_side": self.config.target_pair_shares_per_side,
                "target_pair_shares": round(target_pair_shares, 6),
                "target_up_shares": round(target_shares["Up"], 6),
                "target_down_shares": round(target_shares["Down"], 6),
                "current_up_shares": round(current_shares["Up"], 6),
                "current_down_shares": round(current_shares["Down"], 6),
                "deficit_up_shares": round(deficits["Up"], 6),
                "deficit_down_shares": round(deficits["Down"], 6),
                "desired_notional_usdc": round(desired_notional, 6),
                "book_fill": fill,
            },
        )

    def evaluate_snapshot(self, sample: dict[str, Any], activity_rows: list[dict[str, Any]]) -> TradeIntent | None:
        snapshot = StrategySnapshot.from_market_state_sample(sample)
        return self.evaluate(snapshot, StrategyHistory(activity_rows=activity_rows, snapshots_by_market={snapshot.market_slug: [snapshot]}))


class ParityTerminalBiasStrategy(WalletPathStrategy):
    strategy_name = "parity_terminal_bias_v0"
    one_trade_per_market = False

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        if snapshot.book_stale:
            return None
        checkpoint = _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints)
        if checkpoint is None:
            return None
        if snapshot.elapsed_sec >= int(self.config.terminal_bias_start_sec):
            terminal = self._terminal_bias_intent(snapshot, history, checkpoint)
            if terminal is not None:
                return terminal
        pair = super().evaluate(snapshot, history)
        if pair is None:
            return None
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=pair.wallet,
            market_slug=pair.market_slug,
            sampled_ts=pair.sampled_ts,
            checkpoint_sec=pair.checkpoint_sec,
            intent=pair.intent,
            outcome=pair.outcome,
            notional_usdc=pair.notional_usdc,
            max_price=pair.max_price,
            expected_price=pair.expected_price,
            symbol=pair.symbol,
            reason="parity_terminal_bias_pair_inventory",
            features={**pair.features, "phase": "pair_inventory", "symbol": snapshot.symbol},
        )

    def _terminal_bias_intent(
        self,
        snapshot: StrategySnapshot,
        history: StrategyHistory,
        checkpoint: int,
    ) -> TradeIntent | None:
        snapshots = [
            item
            for item in history.snapshots_for_market(snapshot.market_slug)
            if item.sampled_ts <= snapshot.sampled_ts and _safe_float(item.reference_price) > 0
        ]
        current_ref = _safe_float(snapshot.reference_price)
        first_ref = _safe_float(snapshots[0].reference_price) if snapshots else 0.0
        window_move_bps = _reference_move_bps(first_ref, current_ref)
        recent_ref = _last_reference_before(snapshots, snapshot.sampled_ts, 30) if snapshots else None
        recent_move_bps = _reference_move_bps(_safe_float(recent_ref.reference_price), current_ref) if recent_ref and current_ref > 0 else 0.0
        up_score = 0
        down_score = 0
        reference_threshold = float(self.config.min_reference_move_bps)
        recent_threshold = float(self.config.min_recent_move_bps)
        epsilon = 1e-9
        if window_move_bps > reference_threshold + epsilon:
            up_score += 1
        elif window_move_bps < -reference_threshold - epsilon:
            down_score += 1
        if recent_move_bps > recent_threshold + epsilon:
            up_score += 1
        elif recent_move_bps < -recent_threshold - epsilon:
            down_score += 1
        reference_signal_seen = up_score > 0 or down_score > 0
        up_mid = (_safe_float(snapshot.up.bid) + _safe_float(snapshot.up.ask)) / 2.0
        down_mid = (_safe_float(snapshot.down.bid) + _safe_float(snapshot.down.ask)) / 2.0
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        book_favorite_side = ""
        if up_mid > 0 and down_mid > 0:
            if up_mid > down_mid:
                up_score += 1
                book_favorite_side = "Up"
            elif down_mid > up_mid:
                down_score += 1
                book_favorite_side = "Down"
        if up_bid >= float(self.config.terminal_favorite_bid) or up_mid >= float(self.config.terminal_favorite_mid):
            up_score += 2
            book_favorite_side = "Up"
        if down_bid >= float(self.config.terminal_favorite_bid) or down_mid >= float(self.config.terminal_favorite_mid):
            down_score += 2
            book_favorite_side = "Down"
        if not reference_signal_seen and max(up_score, down_score) < int(self.config.bias_score_threshold):
            return None
        if snapshot.elapsed_sec >= int(self.config.terminal_strong_start_sec):
            if up_score > down_score:
                up_score += 1
            elif down_score > up_score:
                down_score += 1
        outcome = "Up" if up_score > down_score else "Down" if down_score > up_score else ""
        bias_score = max(up_score, down_score)
        if not outcome or bias_score < int(self.config.bias_score_threshold):
            return None
        book = snapshot.book_for_outcome(outcome)
        fill, expected_price = _fill_for_order(book, round(float(self.config.notional_usdc), 6))
        if fill is None or expected_price <= 0 or expected_price > float(self.config.terminal_max_price):
            return None
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=self.config.wallet.lower(),
            market_slug=snapshot.market_slug,
            sampled_ts=snapshot.sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=round(float(self.config.notional_usdc), 6),
            max_price=float(self.config.terminal_max_price),
            expected_price=round(expected_price, 6),
            symbol=snapshot.symbol,
            reason="parity_terminal_bias_overlay",
            features={
                "phase": "terminal_bias",
                "symbol": snapshot.symbol,
                "elapsed_sec": snapshot.elapsed_sec,
                "bias_score": bias_score,
                "up_score": up_score,
                "down_score": down_score,
                "window_reference_move_bps": round(window_move_bps, 6),
                "recent_reference_move_bps": round(recent_move_bps, 6),
                "up_mid": round(up_mid, 6),
                "down_mid": round(down_mid, 6),
                "up_bid": round(up_bid, 6),
                "down_bid": round(down_bid, 6),
                "book_favorite_side": book_favorite_side or None,
                "reference_signal_seen": reference_signal_seen,
                "terminal_bias_start_sec": int(self.config.terminal_bias_start_sec),
                "terminal_strong_start_sec": int(self.config.terminal_strong_start_sec),
                "bias_score_threshold": int(self.config.bias_score_threshold),
                "terminal_favorite_bid": float(self.config.terminal_favorite_bid),
                "terminal_favorite_mid": float(self.config.terminal_favorite_mid),
                "book_fill": fill,
            },
        )


class D950MarketPathStrategy:
    strategy_name = "d950_path_v0"

    def __init__(self, config: PathStrategyConfig, *, min_reference_delta: float = 0.0) -> None:
        self.config = config
        self.min_reference_delta = float(min_reference_delta)

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        if snapshot.book_stale:
            return None
        checkpoint = _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints)
        if checkpoint is None:
            return None
        current_ref = _safe_float(snapshot.reference_price)
        if current_ref <= 0:
            return None
        refs = [
            item
            for item in history.snapshots_for_market(snapshot.market_slug)
            if item.sampled_ts <= snapshot.sampled_ts and _safe_float(item.reference_price) > 0
        ]
        first_ref = _safe_float(refs[0].reference_price) if refs else current_ref
        if first_ref <= 0:
            return None
        reference_delta = round(current_ref - first_ref, 6)
        if abs(reference_delta) <= self.min_reference_delta:
            return None
        outcome = "Up" if reference_delta > 0 else "Down"
        book = snapshot.book_for_outcome(outcome)
        target_key = f"{self.config.notional_usdc:g}"
        fill = book.ask_targets.get(target_key)
        if not isinstance(fill, dict) or not fill.get("ok"):
            return None
        expected_price = _safe_float(fill.get("avg"))
        if expected_price <= 0 or expected_price > self.config.max_price:
            return None
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=self.config.wallet.lower(),
            market_slug=snapshot.market_slug,
            sampled_ts=snapshot.sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=float(self.config.notional_usdc),
            max_price=float(self.config.max_price),
            expected_price=round(expected_price, 6),
            symbol=snapshot.symbol,
            reason="d950_path_v0_reference_momentum",
            features={
                "elapsed_sec": snapshot.elapsed_sec,
                "reference_delta": reference_delta,
                "reference_price": current_ref,
                "reference_start_price": first_ref,
                "book_fill": fill,
            },
        )

    def evaluate_snapshot(self, sample: dict[str, Any], activity_rows: list[dict[str, Any]]) -> TradeIntent | None:
        snapshot = StrategySnapshot.from_market_state_sample(sample)
        history_rows = sample.get("_market_state_history")
        history_snapshots = [
            StrategySnapshot.from_market_state_sample(row)
            for row in history_rows
            if isinstance(row, dict)
        ] if isinstance(history_rows, list) else [snapshot]
        return self.evaluate(snapshot, StrategyHistory(activity_rows=activity_rows, snapshots_by_market={snapshot.market_slug: history_snapshots}))


def replay_path_strategy(
    activity_rows: list[dict[str, Any]],
    market_state_samples: list[dict[str, Any]],
    config: PathStrategyConfig,
    *,
    adapter: ExecutionAdapter | None = None,
) -> ReplayResult:
    strategy = WalletPathStrategy(config)
    execution_adapter = adapter or RecordingExecutionAdapter()
    emitted_markets: set[str] = set()
    intents: list[TradeIntent] = []
    executions: list[ExecutionResult] = []
    history = StrategyHistory(activity_rows=activity_rows)
    for sample in sorted(market_state_samples, key=lambda row: (_safe_int(row.get("sampled_ts")), str(row.get("market_slug") or ""))):
        slug = str(sample.get("market_slug") or "")
        if config.one_trade_per_market and slug in emitted_markets:
            continue
        snapshot = StrategySnapshot.from_market_state_sample(sample)
        history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
        intent = strategy.evaluate(snapshot, history)
        if not intent:
            continue
        emitted_markets.add(slug)
        intents.append(intent)
        history.emitted_intents.append(intent)
        executions.append(execution_adapter.submit(intent))
    return ReplayResult(intents=intents, executions=executions)
