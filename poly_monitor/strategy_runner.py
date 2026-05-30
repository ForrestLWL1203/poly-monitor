from __future__ import annotations

import json
import asyncio
import time
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from pathlib import Path
from typing import Any, Iterable

from .maker_paper import MakerCancel, MakerFill, PendingMakerReplay, PendingMakerReplayConfig
from .path_strategy import _dynamic_imbalance_limit
from .strategy_runtime import EvaluationTrace, ExecutionAdapter, ExecutionResult, StrategyHistory, StrategyPlugin, StrategySnapshot, TradeIntent, evaluate_strategy_intents, utc_iso


def _json_dumps(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class StrategyRunnerConfig:
    output_path: Path
    mode: str = "paper"
    one_trade_per_market: bool | None = None
    start_sampled_ts: int = 0
    activity_rows: list[dict] = field(default_factory=list)
    winning_sides: dict[str, str] = field(default_factory=dict)


class StrategyRunner:
    def __init__(
        self,
        config: StrategyRunnerConfig,
        *,
        strategy: StrategyPlugin,
        execution_adapter: ExecutionAdapter,
    ) -> None:
        self.config = config
        self.strategy = strategy
        self.execution_adapter = execution_adapter
        self.output_path = Path(config.output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.history = StrategyHistory(activity_rows=list(config.activity_rows), winning_sides=dict(config.winning_sides))
        self._emitted_keys: set[tuple[str, ...]] = set()
        self._load_existing_keys()

    @property
    def one_trade_per_market(self) -> bool:
        if self.config.one_trade_per_market is not None:
            return self.config.one_trade_per_market
        return bool(getattr(self.strategy, "one_trade_per_market", True))

    def _load_existing_keys(self) -> None:
        if not self.output_path.exists():
            return
        for line in self.output_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            intent = row.get("intent") if isinstance(row, dict) else None
            if isinstance(intent, dict):
                slug = str(intent.get("market_slug") or "")
                if self.one_trade_per_market:
                    self._emitted_keys.add((slug, ""))
                else:
                    self._emitted_keys.add(
                        (
                            slug,
                            str(intent.get("intent") or ""),
                            str(intent.get("outcome") or ""),
                            str(intent.get("sampled_ts") or ""),
                        )
                    )

    def _key(self, intent: TradeIntent) -> tuple[str, ...]:
        if self.one_trade_per_market:
            return (intent.market_slug, "")
        return (intent.market_slug, intent.intent, intent.outcome, str(intent.sampled_ts))

    def tick(self, snapshots: Iterable[StrategySnapshot]) -> dict[str, int | str]:
        written = 0
        with self.output_path.open("a", encoding="utf-8") as handle:
            for snapshot in sorted(snapshots, key=lambda item: (item.sampled_ts, item.market_slug)):
                if snapshot.sampled_ts < self.config.start_sampled_ts:
                    continue
                market_key = (snapshot.market_slug, "")
                if self.one_trade_per_market and market_key in self._emitted_keys:
                    continue
                self.history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
                intents = evaluate_strategy_intents(self.strategy, snapshot, self.history)
                if not intents:
                    continue
                snapshot_written = 0
                for intent in intents:
                    key = self._key(intent) if not self.one_trade_per_market else (intent.market_slug, intent.intent, intent.outcome, str(intent.sampled_ts))
                    if not self.one_trade_per_market and key in self._emitted_keys:
                        continue
                    if not self.one_trade_per_market:
                        self._emitted_keys.add(key)
                    self.history.emitted_intents.append(intent)
                    execution = self.execution_adapter.submit(intent)
                    handle.write(
                        _json_dumps(
                            {
                                "recorded_at": utc_iso(),
                                "record_type": "execution",
                                "mode": self.config.mode,
                                "strategy_name": getattr(self.strategy, "strategy_name", self.strategy.__class__.__name__),
                                "intent": intent.to_dict(),
                                "execution": execution.to_dict(),
                                "snapshot": snapshot.to_dict(),
                            }
                        )
                        + "\n"
                    )
                    written += 1
                    snapshot_written += 1
                if self.one_trade_per_market and snapshot_written:
                    self._emitted_keys.add(market_key)
        return {"intents": written, "output_path": str(self.output_path)}

    async def run_live(self, environment, *, seconds: float | None = None, poll_sec: float = 1.0) -> int:
        deadline = time.monotonic() + seconds if seconds is not None else None
        await environment.start()
        try:
            while deadline is None or time.monotonic() < deadline:
                await environment.roll_window_if_needed()
                self.tick(environment.snapshot())
                await asyncio.sleep(max(0.1, poll_sec))
        finally:
            await environment.close()
        return 0


@dataclass(frozen=True)
class LivePaperRunConfig:
    run_dir: Path
    run_id: str
    mode: str = "paper"
    start_sampled_ts: int = 0
    expiry_grace_sec: float = 0.0
    snapshot_retention_sec: int = 600
    maker: PendingMakerReplayConfig = field(default_factory=PendingMakerReplayConfig)


def _compact_book(book) -> dict[str, Any]:
    return {
        "bid": book.bid,
        "ask": book.ask,
        "spread": book.spread,
        "book_age_ms": book.book_age_ms,
        "bid_depth_usdc": book.bid_depth_usdc,
        "ask_depth_usdc": book.ask_depth_usdc,
    }


def _trade_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, str, str, str]:
    return (
        str(row.get("source") or row.get("event") or ""),
        str(row.get("market_slug") or ""),
        str(row.get("tx_hash") or ""),
        str(row.get("fill_id") or ""),
        str(row.get("exchange_ts") or ""),
        str(row.get("outcome") or ""),
        str(row.get("price") or ""),
        str(row.get("size") or ""),
    )


def _feature_subset(features: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "elapsed_sec",
        "top_pair_cost",
        "maker_pair_cost",
        "execution_style",
        "target_pair_notional_usdc",
        "max_pair_cost",
        "max_unpaired_price",
        "dual_build_abs_bid_diff",
        "dual_build_max_abs_bid_diff",
        "dynamic_inventory_imbalance_limit",
        "deficit_side",
        "effective_deficit_side",
        "working_deficit_side",
        "missing_filled_side",
        "current_pair_avg",
        "projected_pair_avg",
        "current_pair_avg_basis",
        "projected_pair_avg_basis",
        "current_imbalance_ratio",
        "projected_imbalance_ratio",
        "order_shares",
        "clip_shares",
        "target_pair_shares_per_side",
        "current_up_shares",
        "current_down_shares",
        "working_up_shares",
        "working_down_shares",
        "deficit_shares",
        "book_fill",
        "quote_source",
        "quote_forced_to_ask",
        "original_quote_price",
        "forced_ask_price",
        "maker_parent_sampled_ts",
        "maker_order_id",
        "maker_touch_trade_price",
        "maker_touch_trade_usdc",
        "maker_fill_rate",
    )
    return {key: features[key] for key in keys if key in features}


def _intent_summary(intent: TradeIntent | None, *, include_features: bool = False) -> dict[str, Any] | None:
    if intent is None:
        return None
    # Keep this list aligned with TradeIntent while intentionally omitting the
    # large features payload unless a caller opts in.
    row = {
        "strategy_name": intent.strategy_name,
        "wallet": intent.wallet,
        "market_slug": intent.market_slug,
        "sampled_ts": intent.sampled_ts,
        "checkpoint_sec": intent.checkpoint_sec,
        "intent": intent.intent,
        "outcome": intent.outcome,
        "notional_usdc": intent.notional_usdc,
        "max_price": intent.max_price,
        "expected_price": intent.expected_price,
        "symbol": intent.symbol,
        "reason": intent.reason,
    }
    if include_features:
        row["features"] = _feature_subset(dict(intent.features))
    return row


def _touch_trade_summary(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_slug": trade.get("market_slug"),
        "condition_id": trade.get("condition_id"),
        "exchange_ts": trade.get("exchange_ts"),
        "observed_at": trade.get("observed_at"),
        "source": trade.get("source"),
        "symbol": trade.get("symbol"),
        "outcome": trade.get("outcome"),
        "side": trade.get("side"),
        "price": trade.get("price"),
        "size": trade.get("size"),
        "usdc": trade.get("usdc"),
        "tx_hash": trade.get("tx_hash"),
        "fill_id": trade.get("fill_id"),
    }


def _current_maker_quote(snapshot: StrategySnapshot, outcome: str) -> float | None:
    book = snapshot.book_for_outcome(outcome)
    try:
        bid = float(book.bid)
    except (TypeError, ValueError):
        return None
    return bid if bid > 0 else None


def _maker_pair_cost(snapshot: StrategySnapshot) -> float | None:
    try:
        up_bid = float(snapshot.up.bid)
        down_bid = float(snapshot.down.bid)
    except (TypeError, ValueError):
        return None
    if up_bid <= 0 or down_bid <= 0:
        return None
    return up_bid + down_bid


class LivePaperStrategyRunner:
    def __init__(self, config: LivePaperRunConfig, *, strategy: StrategyPlugin) -> None:
        self.config = config
        self.strategy = strategy
        self.run_dir = Path(config.run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.decisions_path = self.run_dir / "decisions.jsonl"
        self.executions_path = self.run_dir / "executions.jsonl"
        self.market_trades_path = self.run_dir / "market_trades.jsonl"
        self.ws_trades_path = self.run_dir / "ws_trades.jsonl"
        self.state_path = self.run_dir / "state.json"
        self.summary_path = self.run_dir / "summary.json"
        self.history = StrategyHistory()
        self.maker = PendingMakerReplay(config=config.maker)
        self.started_at = utc_iso()
        self.trade_keys: set[tuple[str, str, str, str, str, str, str, str]] = set()
        self.decision_counts: Counter[str] = Counter()
        self.cancel_counts: Counter[str] = Counter()
        self.filled_markets: set[str] = set()
        self.settled_markets: set[str] = set()
        self.paper_total_pnl = 0.0
        self.paper_wins = 0
        self.paper_losses = 0
        self.ws_trades_seen = 0
        self.ws_trades_written = 0
        self.ws_trades_suppressed = 0

    def tick(self, snapshots: Iterable[StrategySnapshot]) -> dict[str, int]:
        decisions = 0
        orders = 0
        expired: list[Any] = []
        with self.decisions_path.open("a", encoding="utf-8") as decision_handle, self.executions_path.open("a", encoding="utf-8") as execution_handle:
            for snapshot in sorted(snapshots, key=lambda item: (item.sampled_ts, item.market_slug)):
                if snapshot.sampled_ts < self.config.start_sampled_ts:
                    continue
                for order in self._expire(int(snapshot.sampled_ts - self.config.expiry_grace_sec)):
                    execution_handle.write(_json_dumps(self._expired_row(order)) + "\n")
                    expired.append(order)
                self.history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
                self._prune_snapshots(snapshot)
                for cancel in self._reconcile_pending(snapshot):
                    self.cancel_counts[cancel.reason] += 1
                    execution_handle.write(_json_dumps(self._cancelled_row(cancel)) + "\n")
                self.history.pending_intents = self.maker.pending_intents()
                trace = self._evaluate_with_trace(snapshot)
                decision_handle.write(_json_dumps(self._decision_row(snapshot, trace)) + "\n")
                decisions += 1
                self.decision_counts[trace.skip_reason or trace.decision] += 1
                for event in self._submit_intents(snapshot, trace.all_intents()):
                    execution_handle.write(_json_dumps(self._event_row(event)) + "\n")
                    orders += 1 if isinstance(event, ExecutionResult) and event.status == "maker_pending" else 0
        self.write_state(active_windows=[])
        return {"decisions": decisions, "orders": orders}

    def process_market_trades(self, trades: Iterable[dict[str, Any]]) -> dict[str, int]:
        written = 0
        with self.market_trades_path.open("a", encoding="utf-8") as trade_handle:
            for trade in sorted(trades, key=lambda row: int(row.get("exchange_ts") or 0)):
                key = _trade_key(trade)
                if key in self.trade_keys:
                    continue
                self.trade_keys.add(key)
                trade_handle.write(_json_dumps(trade) + "\n")
                written += 1
        self.write_state(active_windows=[])
        return {"market_trades": written}

    def process_ws_trades(self, trades: Iterable[dict[str, Any]]) -> dict[str, int]:
        written = 0
        fills = 0
        recovery_orders = 0
        with self.ws_trades_path.open("a", encoding="utf-8") as trade_handle, self.executions_path.open("a", encoding="utf-8") as execution_handle, self.decisions_path.open("a", encoding="utf-8") as decision_handle:
            recovery_trades_by_market: dict[str, dict[str, Any]] = {}
            for trade in sorted(trades, key=lambda row: int(row.get("exchange_ts") or 0)):
                key = _trade_key(trade)
                if key in self.trade_keys:
                    continue
                self.trade_keys.add(key)
                self.ws_trades_seen += 1
                expired = self.maker.expire_before(int(trade.get("exchange_ts") or 0))
                for order in expired:
                    execution_handle.write(_json_dumps(self._expired_row(order)) + "\n")
                trade_fills = 0
                for fill in self.maker.process_trade(trade, expire_first=False):
                    self.history.emitted_intents.append(fill.intent)
                    self.filled_markets.add(fill.intent.market_slug)
                    execution_handle.write(_json_dumps(self._fill_row(fill)) + "\n")
                    fills += 1
                    trade_fills += 1
                if trade_fills:
                    recovery_trades_by_market[str(trade.get("market_slug") or "")] = trade
                if trade_fills or expired:
                    trade_handle.write(_json_dumps(trade) + "\n")
                    written += 1
                    self.ws_trades_written += 1
                else:
                    self.ws_trades_suppressed += 1
            for trade in recovery_trades_by_market.values():
                recovery_orders += self._recover_after_ws_fill(trade, decision_handle=decision_handle, execution_handle=execution_handle)
        self.write_state(active_windows=[])
        return {"ws_trades": written, "fills": fills, "recovery_orders": recovery_orders}

    def settle_market(self, market_slug: str, winning_side: str) -> dict[str, Any] | None:
        intents = [intent for intent in self.history.emitted_intents if intent.market_slug == market_slug]
        if not intents or market_slug in self.settled_markets:
            return None
        up_shares, up_cost = self._inventory_for_market(market_slug, "Up")
        down_shares, down_cost = self._inventory_for_market(market_slug, "Down")
        up_avg = up_cost / up_shares if up_shares > 0 else None
        down_avg = down_cost / down_shares if down_shares > 0 else None
        settled_value = up_shares if winning_side == "Up" else down_shares if winning_side == "Down" else 0.0
        realized_pnl = settled_value - up_cost - down_cost
        self.paper_total_pnl += realized_pnl
        self.paper_wins += 1 if realized_pnl > 0 else 0
        self.paper_losses += 1 if realized_pnl < 0 else 0
        self.settled_markets.add(market_slug)
        row = {
            "record_type": "settled",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "market_slug": market_slug,
            "winning_side": winning_side,
            "up_shares": round(up_shares, 6),
            "down_shares": round(down_shares, 6),
            "up_avg_price": round(up_avg, 6) if up_avg is not None else None,
            "down_avg_price": round(down_avg, 6) if down_avg is not None else None,
            "final_pair_cost": round(up_avg + down_avg, 6) if up_avg is not None and down_avg is not None else None,
            "realized_pnl": round(realized_pnl, 6),
        }
        self._append_execution_rows([row])
        self.write_state(active_windows=[])
        return row

    def write_state(self, *, active_windows: list[dict[str, Any]] | None = None, stream_diagnostics: dict[str, Any] | None = None, poll_state: dict[str, Any] | None = None) -> None:
        state = {
            "run": {
                "run_id": self.config.run_id,
                "started_at": self.started_at,
                "updated_at": utc_iso(),
                "strategy_name": getattr(self.strategy, "strategy_name", self.strategy.__class__.__name__),
                "mode": self.config.mode,
                "config": self._strategy_config(),
            },
            "active_windows": active_windows or [],
            "pending_orders": [order.to_dict() for order in self.maker.pending],
            "inventory_by_market": self._inventory_by_market(),
            "poll_state": poll_state or {},
            "stream_diagnostics": stream_diagnostics or {},
        }
        self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.summary_path.write_text(json.dumps(self._summary(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _evaluate_with_trace(self, snapshot: StrategySnapshot) -> EvaluationTrace:
        many_method = getattr(self.strategy, "evaluate_many_with_trace", None)
        if callable(many_method):
            return many_method(snapshot, self.history)
        method = getattr(self.strategy, "evaluate_with_trace", None)
        if callable(method):
            return method(snapshot, self.history)
        intents = evaluate_strategy_intents(self.strategy, snapshot, self.history)
        return EvaluationTrace(
            decision="intent" if intents else "skip",
            skip_reason=None if intents else "no_intent",
            intent=intents[0] if intents else None,
            intents=intents,
            features={},
        )

    def _can_submit_intent_batch(self, market_slug: str, intents: tuple[TradeIntent, ...]) -> bool:
        open_orders = len([order for order in self.maker.pending if order.intent.market_slug == market_slug])
        return open_orders + len(intents) <= int(self.maker.config.max_open_orders_per_market)

    def _submit_intents(self, snapshot: StrategySnapshot, intents: tuple[TradeIntent, ...]) -> list[ExecutionResult | MakerFill]:
        if not intents:
            return []
        if len(intents) > 1 and not self._can_submit_intent_batch(snapshot.market_slug, intents):
            return self._reject_intent_batch(snapshot.market_slug, intents)
        results: list[ExecutionResult | MakerFill] = []
        pending_outcomes = {
            (order.intent.market_slug, order.intent.outcome)
            for order in self.maker.pending
            if order.remaining_usdc > 1e-9
        }
        requested_outcomes: set[tuple[str, str]] = set()
        for intent in intents:
            key = (intent.market_slug, intent.outcome)
            if key in pending_outcomes or key in requested_outcomes:
                results.append(self._reject_duplicate_pending_outcome(intent, key))
                continue
            execution = self.maker.submit(intent)
            results.append(execution)
            if execution.status == "maker_pending":
                pending_outcomes.add(key)
                requested_outcomes.add(key)
                fill = self._immediate_fill_if_crosses_ask(snapshot, execution)
                if fill is not None:
                    results.append(fill)
        return results

    def _reject_duplicate_pending_outcome(self, intent: TradeIntent, key: tuple[str, str]) -> ExecutionResult:
        self.maker.rejected += 1
        return ExecutionResult(
            status="maker_rejected_duplicate_pending_outcome",
            intent=intent,
            detail={
                "error": "pending order already exists for market/outcome",
                "market_slug": key[0],
                "outcome": key[1],
            },
        )

    def _reject_intent_batch(self, market_slug: str, intents: tuple[TradeIntent, ...]) -> list[ExecutionResult]:
        open_orders = len([order for order in self.maker.pending if order.intent.market_slug == market_slug])
        detail = {
            "error": "batch would exceed max_open_orders_per_market",
            "open_orders": open_orders,
            "requested_orders": len(intents),
            "limit": int(self.maker.config.max_open_orders_per_market),
            "batch_rejected": True,
        }
        results: list[ExecutionResult] = []
        for intent in intents:
            self.maker.rejected += 1
            results.append(ExecutionResult(status="maker_rejected_open_order_limit", intent=intent, detail=dict(detail)))
        return results

    def _decision_row(self, snapshot: StrategySnapshot, trace: EvaluationTrace) -> dict[str, Any]:
        features = dict(trace.features)
        intents = trace.all_intents()
        return {
            "record_type": "decision",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "strategy_name": getattr(self.strategy, "strategy_name", self.strategy.__class__.__name__),
            "wallet": str(getattr(getattr(self.strategy, "config", None), "wallet", "")),
            "mode": self.config.mode,
            "symbol": snapshot.symbol,
            "market_slug": snapshot.market_slug,
            "condition_id": snapshot.condition_id,
            "sampled_ts": snapshot.sampled_ts,
            "elapsed_sec": snapshot.elapsed_sec,
            "remaining_sec": snapshot.remaining_sec,
            "window_start_ts": snapshot.window_start_ts,
            "window_end_ts": snapshot.window_end_ts,
            "sample_reason": snapshot.sample_reason,
            "reference_price": snapshot.reference_price,
            "reference_price_age_sec": snapshot.reference_price_age_sec,
            "up": _compact_book(snapshot.up),
            "down": _compact_book(snapshot.down),
            "book_stale": snapshot.book_stale,
            "top_pair_cost": features.get("top_pair_cost"),
            "maker_pair_cost": features.get("maker_pair_cost"),
            "max_pair_cost": features.get("max_pair_cost"),
            "quote_quality": features.get("quote_quality"),
            "decision": trace.decision,
            "skip_reason": trace.skip_reason,
            "intent": _intent_summary(trace.intent, include_features=True),
            "intent_count": len(intents),
            "intents": [_intent_summary(intent, include_features=True) for intent in intents],
            "inventory": features.get("inventory") or self._inventory_for_decision(snapshot.market_slug),
            "pending": self._pending_summary(snapshot.market_slug),
            "open_order_limit": self._open_order_limit(snapshot.market_slug),
        }

    def _order_row(self, execution: ExecutionResult) -> dict[str, Any]:
        detail = dict(execution.detail)
        intent = execution.intent
        return {
            "record_type": "maker_order_submitted" if execution.status == "maker_pending" else "maker_rejected",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "status": execution.status,
            "order_id": detail.get("order_id"),
            "intent": _intent_summary(intent),
            "market_slug": intent.market_slug,
            "symbol": intent.symbol,
            "sampled_ts": intent.sampled_ts,
            "outcome": intent.outcome,
            "notional_usdc": intent.notional_usdc,
            "ttl_sec": detail.get("ttl_sec"),
            "expires_ts": detail.get("expires_ts"),
            "quote_price": detail.get("quote_price"),
            "remaining_usdc": detail.get("remaining_usdc"),
            "queue_ahead_shares": detail.get("queue_ahead_shares"),
            "reject_detail": detail if execution.status != "maker_pending" else None,
        }

    def _event_row(self, event: ExecutionResult | MakerFill) -> dict[str, Any]:
        if isinstance(event, MakerFill):
            return self._fill_row(event)
        return self._order_row(event)

    def _expired_row(self, order) -> dict[str, Any]:
        intent = order.intent
        return {
            "record_type": "maker_expired",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "order_id": order.order_id,
            "market_slug": intent.market_slug,
            "symbol": intent.symbol,
            "outcome": intent.outcome,
            "submitted_ts": order.submitted_ts,
            "quote_price": intent.expected_price,
            "original_usdc": round(intent.notional_usdc, 6),
            "unfilled_usdc": round(order.remaining_usdc, 6),
            "lifetime_sec": max(0, int(order.expires_ts) - int(order.submitted_ts)),
            "wallclock_at_log_sec": max(0, int(time.time()) - int(order.submitted_ts)),
            "expires_ts": order.expires_ts,
        }

    def _cancelled_row(self, cancel: MakerCancel) -> dict[str, Any]:
        order = cancel.order
        intent = order.intent
        return {
            "record_type": "maker_cancelled",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "order_id": order.order_id,
            "market_slug": intent.market_slug,
            "symbol": intent.symbol,
            "outcome": intent.outcome,
            "submitted_ts": order.submitted_ts,
            "cancelled_ts": cancel.cancelled_ts,
            "cancel_reason": cancel.reason,
            "quote_price": intent.expected_price,
            "unfilled_usdc": round(order.remaining_usdc, 6),
            "detail": cancel.detail,
        }

    def _fill_row(self, fill) -> dict[str, Any]:
        intent = fill.intent
        fill_shares = intent.notional_usdc / intent.expected_price if intent.expected_price > 0 else 0.0
        return {
            "record_type": "maker_fill",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "order_id": fill.order.order_id,
            "market_slug": intent.market_slug,
            "symbol": intent.symbol,
            "sampled_ts": intent.sampled_ts,
            "parent_sampled_ts": fill.order.submitted_ts,
            "outcome": intent.outcome,
            "quote_price": intent.expected_price,
            "fill_usdc": round(intent.notional_usdc, 6),
            "fill_shares": round(fill_shares, 6),
            "remaining_usdc": round(fill.remaining_usdc, 6),
            "touch_trade": _touch_trade_summary(fill.touch_trade),
            "configured_fill_rate": intent.features.get("maker_fill_rate"),
            "queue_position_ratio": intent.features.get("maker_queue_position_ratio"),
            "original_queue_ahead_shares": intent.features.get("maker_original_queue_ahead_shares"),
            "remaining_queue_ahead_shares": intent.features.get("maker_remaining_queue_ahead_shares"),
            "realized_touch_fill_rate": round(intent.notional_usdc / float(fill.touch_trade.get("usdc") or 0.0), 6) if float(fill.touch_trade.get("usdc") or 0.0) > 0 else None,
        }

    def _expire(self, ts: int) -> list[Any]:
        return self.maker.expire_before(ts)

    def _immediate_fill_if_crosses_ask(self, snapshot: StrategySnapshot, execution: ExecutionResult) -> MakerFill | None:
        if execution.status != "maker_pending":
            return None
        intent = execution.intent
        if intent.features.get("quote_source") != "maker_rebalance_quote":
            return None
        ask = snapshot.book_for_outcome(intent.outcome).ask
        try:
            ask_price = float(ask)
        except (TypeError, ValueError):
            return None
        if ask_price <= 0 or float(intent.expected_price) + 1e-9 < ask_price:
            return None
        if ask_price > float(intent.max_price) + 1e-9:
            return None
        order_id = str(execution.detail.get("order_id") or "")
        fill = self.maker.force_fill_order(
            order_id,
            fill_ts=int(snapshot.sampled_ts),
            fill_price=ask_price,
            source="paper_ioc_at_ask",
            observed_at=snapshot.observed_at,
        )
        if fill is None:
            return None
        self.history.emitted_intents.append(fill.intent)
        self.filled_markets.add(fill.intent.market_slug)
        return fill

    def _recover_after_ws_fill(self, trade: dict[str, Any], *, decision_handle, execution_handle) -> int:
        market_slug = str(trade.get("market_slug") or "")
        snapshot = self._snapshot_for_trade(trade)
        if snapshot is None:
            return 0
        missing_side = self._missing_filled_side_for_market(market_slug, snapshot)
        if missing_side is None:
            return 0
        for cancel in self._cancel_missing_side_for_reprice(snapshot, missing_side):
            self.cancel_counts[cancel.reason] += 1
            execution_handle.write(_json_dumps(self._cancelled_row(cancel)) + "\n")
        self.history.pending_intents = self.maker.pending_intents()
        trace = self._evaluate_with_trace(snapshot)
        recovery_intents = self._force_recovery_quotes_to_ask(snapshot, trace.all_intents(), missing_side)
        if recovery_intents != trace.all_intents():
            trace = replace(
                trace,
                intent=recovery_intents[0] if recovery_intents else None,
                intents=recovery_intents,
            )
        decision_handle.write(_json_dumps(self._decision_row(snapshot, trace)) + "\n")
        self.decision_counts[trace.skip_reason or trace.decision] += 1
        submitted = 0
        for event in self._submit_intents(snapshot, recovery_intents):
            execution_handle.write(_json_dumps(self._event_row(event)) + "\n")
            submitted += 1 if isinstance(event, ExecutionResult) and event.status == "maker_pending" else 0
        return submitted

    def _force_recovery_quotes_to_ask(self, snapshot: StrategySnapshot, intents: tuple[TradeIntent, ...], recovery_side: str) -> tuple[TradeIntent, ...]:
        adjusted: list[TradeIntent] = []
        for intent in intents:
            if intent.outcome != recovery_side or intent.features.get("quote_source") != "maker_rebalance_quote":
                adjusted.append(intent)
                continue
            try:
                ask = float(snapshot.book_for_outcome(intent.outcome).ask)
            except (TypeError, ValueError):
                adjusted.append(intent)
                continue
            if ask <= 0 or ask <= float(intent.expected_price) + 1e-9:
                adjusted.append(intent)
                continue
            if ask > float(intent.max_price) + 1e-9:
                adjusted.append(intent)
                continue
            if not self._forced_ask_pair_cost_ok(intent, ask):
                adjusted.append(intent)
                continue
            shares = float(intent.features.get("order_shares") or 0.0)
            notional = round(shares * ask, 6) if shares > 0 else intent.notional_usdc
            features = dict(intent.features)
            fill = dict(features.get("book_fill") or {})
            fill.update({"avg": ask, "filled_usdc": notional, "source": "maker_rebalance_quote"})
            features.update(
                {
                    "book_fill": fill,
                    "quote_price": ask,
                    "quote_forced_to_ask": True,
                    "original_quote_price": intent.expected_price,
                    "forced_ask_price": ask,
                    "clip_shares": shares or intent.features.get("clip_shares"),
                }
            )
            adjusted.append(replace(intent, expected_price=round(ask, 6), notional_usdc=notional, features=features))
        return tuple(adjusted)

    def _forced_ask_pair_cost_ok(self, intent: TradeIntent, ask: float) -> bool:
        try:
            max_pair_cost = float(intent.features.get("pair_cost_cap", intent.features.get("max_pair_cost")))
        except (TypeError, ValueError):
            return True
        outcome = intent.outcome
        other = "Down" if outcome == "Up" else "Up"
        current_shares, current_cost = self._inventory_for_market(intent.market_slug, outcome)
        other_shares, other_cost = self._inventory_for_market(intent.market_slug, other)
        try:
            order_shares = float(intent.features.get("order_shares") or 0.0)
        except (TypeError, ValueError):
            order_shares = 0.0
        if order_shares <= 0:
            order_shares = float(intent.notional_usdc) / ask if ask > 0 else 0.0
        projected_shares = current_shares + order_shares
        projected_cost = current_cost + order_shares * ask
        projected_avg = projected_cost / projected_shares if projected_shares > 0 else None
        other_avg = other_cost / other_shares if other_shares > 0 else None
        if projected_avg is None or other_avg is None:
            return ask <= float(intent.features.get("max_unpaired_price", intent.max_price)) + 1e-9
        return projected_avg + other_avg <= max_pair_cost + 1e-9

    def _snapshot_for_trade(self, trade: dict[str, Any]) -> StrategySnapshot | None:
        market_slug = str(trade.get("market_slug") or "")
        snapshots = self.history.snapshots_by_market.get(market_slug) or []
        if not snapshots:
            return None
        latest = snapshots[-1]
        trade_ts = int(trade.get("exchange_ts") or latest.sampled_ts)
        elapsed = latest.elapsed_sec
        remaining = latest.remaining_sec
        if latest.window_start_ts is not None:
            elapsed = max(0, trade_ts - int(latest.window_start_ts))
        if latest.window_end_ts is not None:
            remaining = max(0, int(latest.window_end_ts) - trade_ts)
        age_delta_ms = max(0, trade_ts - int(latest.sampled_ts)) * 1000
        up = self._age_book(latest.up, age_delta_ms)
        down = self._age_book(latest.down, age_delta_ms)
        max_age = getattr(getattr(self.strategy, "config", None), "max_quote_book_age_ms", None)
        if max_age is not None:
            for book in (up, down):
                try:
                    age = float(book.book_age_ms)
                except (TypeError, ValueError):
                    return None
                if age > float(max_age):
                    return None
        return replace(
            latest,
            sampled_ts=trade_ts,
            elapsed_sec=elapsed,
            remaining_sec=remaining,
            observed_at=str(trade.get("observed_at") or latest.observed_at),
            up=up,
            down=down,
            sample_reason="ws_fill_recovery",
        )

    def _age_book(self, book, age_delta_ms: int):
        if book.book_age_ms is None:
            return book
        try:
            age = float(book.book_age_ms) + age_delta_ms
        except (TypeError, ValueError):
            return book
        return replace(book, book_age_ms=age)

    def _missing_filled_side_for_market(self, market_slug: str, snapshot: StrategySnapshot) -> str | None:
        up_shares, _up_cost = self._inventory_for_market(market_slug, "Up")
        down_shares, _down_cost = self._inventory_for_market(market_slug, "Down")
        if up_shares > 1e-9 and down_shares <= 1e-9:
            return "Down"
        if down_shares > 1e-9 and up_shares <= 1e-9:
            return "Up"
        total = up_shares + down_shares
        if total <= 1e-9:
            return None
        strategy_config = getattr(self.strategy, "config", None)
        limit = _dynamic_imbalance_limit(strategy_config, snapshot.elapsed_sec) if strategy_config is not None else 0.05
        if abs(up_shares - down_shares) / total > limit:
            return "Up" if up_shares < down_shares else "Down"
        return None

    def _cancel_missing_side_for_reprice(self, snapshot: StrategySnapshot, missing_side: str) -> list[MakerCancel]:
        ids = {
            order.order_id
            for order in self.maker.pending
            if order.intent.market_slug == snapshot.market_slug
            and order.intent.outcome == missing_side
            and order.remaining_usdc > 1e-9
        }
        if not ids:
            return []
        return self.maker.cancel_orders(
            ids,
            reason="missing_leg_reprice",
            cancelled_ts=snapshot.sampled_ts,
            detail={"source": "ws_fill_recovery", "missing_side": missing_side},
        )

    def _reconcile_pending(self, snapshot: StrategySnapshot) -> list[MakerCancel]:
        pending = [order for order in self.maker.pending if order.intent.market_slug == snapshot.market_slug and order.remaining_usdc > 1e-9]
        if not pending:
            return []
        cancelled: list[MakerCancel] = []
        cancelled_ids: set[str] = set()

        quote_improved: set[str] = set()
        quote_unavailable: set[str] = set()
        for order in pending:
            current_quote = _current_maker_quote(snapshot, order.intent.outcome)
            if current_quote is None:
                quote_unavailable.add(order.order_id)
                continue
            expected_price = float(order.intent.expected_price)
            if current_quote > expected_price + 1e-9:
                quote_improved.add(order.order_id)
        if quote_improved:
            cancelled.extend(
                self.maker.cancel_orders(
                    quote_improved,
                    reason="quote_improved_replace",
                    cancelled_ts=snapshot.sampled_ts,
                    detail={"source": "tick_reconcile"},
                )
            )
            cancelled_ids.update(quote_improved)
        if quote_unavailable:
            cancelled.extend(
                self.maker.cancel_orders(
                    quote_unavailable,
                    reason="quote_unavailable",
                    cancelled_ts=snapshot.sampled_ts,
                    detail={"source": "tick_reconcile"},
                )
            )
            cancelled_ids.update(quote_unavailable)

        pending = [order for order in self.maker.pending if order.intent.market_slug == snapshot.market_slug and order.remaining_usdc > 1e-9]
        if not pending:
            return cancelled

        up_shares, _up_cost = self._inventory_for_market(snapshot.market_slug, "Up")
        down_shares, _down_cost = self._inventory_for_market(snapshot.market_slug, "Down")
        total = up_shares + down_shares
        if total > 0:
            imbalance = abs(up_shares - down_shares) / total
            strategy_config = getattr(self.strategy, "config", None)
            limit = _dynamic_imbalance_limit(strategy_config, snapshot.elapsed_sec) if strategy_config is not None else 0.05
            if up_shares > 0 and down_shares > 0 and imbalance <= limit and self._filled_inventory_near_target(snapshot, up_shares, down_shares):
                ids = {order.order_id for order in pending if order.order_id not in cancelled_ids}
                cancelled.extend(
                    self.maker.cancel_orders(
                        ids,
                        reason="balance_reconciled",
                        cancelled_ts=snapshot.sampled_ts,
                        detail={"filled_imbalance_ratio": round(imbalance, 6), "limit": round(limit, 6)},
                    )
                )
                return cancelled

        surplus_side = "Up" if up_shares > down_shares + 1e-9 else "Down" if down_shares > up_shares + 1e-9 else None
        if surplus_side is not None:
            ids = {order.order_id for order in pending if order.intent.outcome == surplus_side and order.order_id not in cancelled_ids}
            cancelled.extend(
                self.maker.cancel_orders(
                    ids,
                    reason="side_no_longer_needed",
                    cancelled_ts=snapshot.sampled_ts,
                    detail={"up_shares": round(up_shares, 6), "down_shares": round(down_shares, 6)},
                )
            )
        return cancelled

    def _filled_inventory_near_target(self, snapshot: StrategySnapshot, up_shares: float, down_shares: float) -> bool:
        target = self._target_pair_shares(snapshot)
        if target is None or target <= 0:
            return snapshot.elapsed_sec >= int(getattr(getattr(self.strategy, "config", None), "rebalance_start_sec", 240))
        threshold = target * 0.9
        return up_shares >= threshold and down_shares >= threshold

    def _target_pair_shares(self, snapshot: StrategySnapshot) -> float | None:
        maker_pair_cost = _maker_pair_cost(snapshot)
        if maker_pair_cost is None:
            return None
        strategy_target = getattr(self.strategy, "_target_pair_shares", None)
        if callable(strategy_target):
            try:
                return float(strategy_target(maker_pair_cost))
            except (TypeError, ValueError):
                return None
        config = getattr(self.strategy, "config", None)
        explicit_shares = getattr(config, "target_pair_shares_per_side", None)
        if explicit_shares is not None:
            try:
                return float(explicit_shares)
            except (TypeError, ValueError):
                return None
        notional = getattr(config, "target_pair_notional_usdc", None)
        if notional is None:
            return None
        try:
            return float(notional) / maker_pair_cost
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    def _prune_snapshots(self, snapshot: StrategySnapshot) -> None:
        cutoff = int(snapshot.sampled_ts) - max(0, int(self.config.snapshot_retention_sec))
        for market_slug, rows in list(self.history.snapshots_by_market.items()):
            kept = [row for row in rows if int(row.sampled_ts) >= cutoff]
            if kept:
                self.history.snapshots_by_market[market_slug] = kept
            else:
                self.history.snapshots_by_market.pop(market_slug, None)

    def _append_execution_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.executions_path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(_json_dumps(row) + "\n")

    def _pending_summary(self, market_slug: str) -> dict[str, Any]:
        orders = [order for order in self.maker.pending if order.intent.market_slug == market_slug]
        by_outcome: dict[str, float] = defaultdict(float)
        for order in orders:
            by_outcome[order.intent.outcome] += order.remaining_usdc
        expiries = [order.expires_ts for order in orders]
        return {
            "count": len(orders),
            "up_notional": round(by_outcome.get("Up", 0.0), 6),
            "down_notional": round(by_outcome.get("Down", 0.0), 6),
            "earliest_expiry_ts": min(expiries) if expiries else None,
            "latest_expiry_ts": max(expiries) if expiries else None,
        }

    def _open_order_limit(self, market_slug: str) -> dict[str, Any]:
        count = len([order for order in self.maker.pending if order.intent.market_slug == market_slug])
        limit = self.maker.config.max_open_orders_per_market
        return {"count": count, "limit": limit, "near_limit": count >= max(1, limit - 1)}

    def _inventory_for_market(self, market_slug: str, outcome: str) -> tuple[float, float]:
        shares = 0.0
        cost = 0.0
        for intent in self.history.emitted_intents:
            if intent.market_slug != market_slug or intent.outcome != outcome or intent.intent != "BUY":
                continue
            if intent.expected_price > 0:
                shares += intent.notional_usdc / intent.expected_price
                cost += intent.notional_usdc
        return shares, cost

    def _inventory_for_decision(self, market_slug: str) -> dict[str, Any]:
        up_shares, up_cost = self._inventory_for_market(market_slug, "Up")
        down_shares, down_cost = self._inventory_for_market(market_slug, "Down")
        up_avg = up_cost / up_shares if up_shares > 0 else None
        down_avg = down_cost / down_shares if down_shares > 0 else None
        return {
            "up_shares": round(up_shares, 6),
            "up_cost": round(up_cost, 6),
            "up_avg": round(up_avg, 6) if up_avg is not None else None,
            "down_shares": round(down_shares, 6),
            "down_cost": round(down_cost, 6),
            "down_avg": round(down_avg, 6) if down_avg is not None else None,
            "pair_avg": round(up_avg + down_avg, 6) if up_avg is not None and down_avg is not None else None,
        }

    def _inventory_by_market(self) -> dict[str, dict[str, Any]]:
        markets = {intent.market_slug for intent in self.history.emitted_intents}
        return {market: self._inventory_for_decision(market) for market in sorted(markets)}

    def _strategy_config(self) -> dict[str, Any]:
        config = getattr(self.strategy, "config", None)
        if config is None:
            return {}
        if is_dataclass(config):
            return asdict(config)
        return dict(getattr(config, "__dict__", {}))

    def _summary(self) -> dict[str, Any]:
        pair_costs = [
            item["pair_avg"]
            for item in self._inventory_by_market().values()
            if item.get("pair_avg") is not None
        ]
        pair_costs = sorted(float(value) for value in pair_costs)
        def quantile(pct: float):
            if not pair_costs:
                return None
            return round(pair_costs[math.floor((len(pair_costs) - 1) * pct)], 6)
        denom = self.paper_wins + self.paper_losses
        return {
            "run_id": self.config.run_id,
            "updated_at": utc_iso(),
            "decision_counts_by_reason": dict(self.decision_counts),
            "cancel_counts_by_reason": dict(self.cancel_counts),
            "orders_submitted": self.maker.submitted,
            "fills": len(self.maker.filled_intents),
            "partial_fill_events": self.maker.partial_fills,
            "expired": self.maker.expired,
            "cancelled": self.maker.cancelled,
            "rejected": self.maker.rejected,
            "ws_trades_seen": self.ws_trades_seen,
            "ws_trades_written": self.ws_trades_written,
            "ws_trades_suppressed": self.ws_trades_suppressed,
            "filled_markets": len(self.filled_markets),
            "settled_markets": len(self.settled_markets),
            "paper_total_pnl": round(self.paper_total_pnl, 6),
            "win_rate": round(self.paper_wins / denom, 6) if denom else None,
            "final_pair_cost_p50": quantile(0.5),
            "final_pair_cost_p75": quantile(0.75),
            "final_pair_cost_p90": quantile(0.9),
            "pair_cost_lt_1_count": sum(1 for value in pair_costs if value < 1),
            "pair_cost_gte_1_count": sum(1 for value in pair_costs if value >= 1),
        }
