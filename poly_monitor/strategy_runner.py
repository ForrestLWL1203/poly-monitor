from __future__ import annotations

import json
import asyncio
import time
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Iterable

from .maker_paper import PendingMakerReplay, PendingMakerReplayConfig
from .strategy_runtime import EvaluationTrace, ExecutionAdapter, ExecutionResult, StrategyHistory, StrategyPlugin, StrategySnapshot, TradeIntent, utc_iso


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
                self.history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
                intent = self.strategy.evaluate(snapshot, self.history)
                if intent is None:
                    continue
                key = self._key(intent)
                if key in self._emitted_keys:
                    continue
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


def _trade_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    return (
        str(row.get("market_slug") or ""),
        str(row.get("tx_hash") or ""),
        str(row.get("fill_id") or ""),
        str(row.get("exchange_ts") or ""),
        str(row.get("outcome") or ""),
        str(row.get("price") or ""),
        str(row.get("size") or ""),
    )


class LivePaperStrategyRunner:
    def __init__(self, config: LivePaperRunConfig, *, strategy: StrategyPlugin) -> None:
        self.config = config
        self.strategy = strategy
        self.run_dir = Path(config.run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.decisions_path = self.run_dir / "decisions.jsonl"
        self.executions_path = self.run_dir / "executions.jsonl"
        self.market_trades_path = self.run_dir / "market_trades.jsonl"
        self.state_path = self.run_dir / "state.json"
        self.summary_path = self.run_dir / "summary.json"
        self.history = StrategyHistory()
        self.maker = PendingMakerReplay(config=config.maker)
        self.started_at = utc_iso()
        self.trade_keys: set[tuple[str, str, str, str, str, str, str]] = set()
        self.decision_counts: Counter[str] = Counter()
        self.filled_markets: set[str] = set()
        self.settled_markets: set[str] = set()
        self.paper_total_pnl = 0.0
        self.paper_wins = 0
        self.paper_losses = 0

    def tick(self, snapshots: Iterable[StrategySnapshot]) -> dict[str, int]:
        decisions = 0
        orders = 0
        expired = self._expire(int(time.time() - self.config.expiry_grace_sec))
        with self.decisions_path.open("a", encoding="utf-8") as decision_handle, self.executions_path.open("a", encoding="utf-8") as execution_handle:
            for snapshot in sorted(snapshots, key=lambda item: (item.sampled_ts, item.market_slug)):
                if snapshot.sampled_ts < self.config.start_sampled_ts:
                    continue
                self.history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
                self._prune_snapshots(snapshot)
                self.history.pending_intents = self.maker.pending_intents()
                trace = self._evaluate_with_trace(snapshot)
                decision_handle.write(_json_dumps(self._decision_row(snapshot, trace)) + "\n")
                decisions += 1
                self.decision_counts[trace.skip_reason or trace.decision] += 1
                if trace.intent is None:
                    continue
                execution = self.maker.submit(trace.intent)
                execution_handle.write(_json_dumps(self._order_row(execution)) + "\n")
                orders += 1 if execution.status == "maker_pending" else 0
        if expired:
            self._append_execution_rows([self._expired_row(order) for order in expired])
        self.write_state(active_windows=[])
        return {"decisions": decisions, "orders": orders}

    def process_market_trades(self, trades: Iterable[dict[str, Any]]) -> dict[str, int]:
        written = 0
        fills = 0
        with self.market_trades_path.open("a", encoding="utf-8") as trade_handle, self.executions_path.open("a", encoding="utf-8") as execution_handle:
            for trade in sorted(trades, key=lambda row: int(row.get("exchange_ts") or 0)):
                key = _trade_key(trade)
                if key in self.trade_keys:
                    continue
                self.trade_keys.add(key)
                trade_handle.write(_json_dumps(trade) + "\n")
                written += 1
                expired = self.maker.expire_before(int(trade.get("exchange_ts") or 0))
                for order in expired:
                    execution_handle.write(_json_dumps(self._expired_row(order)) + "\n")
                for fill in self.maker.process_trade(trade, expire_first=False):
                    self.history.emitted_intents.append(fill.intent)
                    self.filled_markets.add(fill.intent.market_slug)
                    execution_handle.write(_json_dumps(fill.to_execution_row()) + "\n")
                    fills += 1
        self.write_state(active_windows=[])
        return {"market_trades": written, "fills": fills}

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
        method = getattr(self.strategy, "evaluate_with_trace", None)
        if callable(method):
            return method(snapshot, self.history)
        intent = self.strategy.evaluate(snapshot, self.history)
        return EvaluationTrace(decision="intent" if intent else "skip", skip_reason=None if intent else "no_intent", intent=intent, features={})

    def _decision_row(self, snapshot: StrategySnapshot, trace: EvaluationTrace) -> dict[str, Any]:
        features = dict(trace.features)
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
            "intent": trace.intent.to_dict() if trace.intent is not None else None,
            "inventory": features.get("inventory") or self._inventory_for_decision(snapshot.market_slug),
            "pending": self._pending_summary(snapshot.market_slug),
            "open_order_limit": self._open_order_limit(snapshot.market_slug),
        }

    def _order_row(self, execution: ExecutionResult) -> dict[str, Any]:
        detail = dict(execution.detail)
        return {
            "record_type": "maker_order_submitted" if execution.status == "maker_pending" else "maker_rejected",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "execution": execution.to_dict(),
            "order_id": detail.get("order_id"),
            "ttl_sec": detail.get("ttl_sec"),
            "expires_ts": detail.get("expires_ts"),
            "quote_price": detail.get("quote_price"),
            "remaining_usdc": detail.get("remaining_usdc"),
        }

    def _expired_row(self, order) -> dict[str, Any]:
        return {
            "record_type": "maker_expired",
            "recorded_at": utc_iso(),
            "run_id": self.config.run_id,
            "order_id": order.order_id,
            "parent_intent": order.intent.to_dict(),
            "unfilled_usdc": round(order.remaining_usdc, 6),
            "age_sec": max(0, int(time.time()) - int(order.submitted_ts)),
            "expires_ts": order.expires_ts,
        }

    def _expire(self, ts: int) -> list[Any]:
        return self.maker.expire_before(ts)

    def _prune_snapshots(self, snapshot: StrategySnapshot) -> None:
        cutoff = int(snapshot.sampled_ts) - 600
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
            "orders_submitted": self.maker.submitted,
            "fills": len(self.maker.filled_intents),
            "partial_fills": self.maker.partial_fills,
            "expired": self.maker.expired,
            "rejected": self.maker.rejected,
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
