from __future__ import annotations

import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .strategy_runtime import (
    ExecutionAdapter,
    ExecutionResult,
    PaperExecutionAdapter,
    StrategyHistory,
    StrategyPlugin,
    StrategySnapshot,
    TradeIntent,
    _load_jsonl_from_zip,
    book_fill_source,
    winning_side_from_row,
)


@dataclass(frozen=True)
class BacktestResult:
    summary: dict[str, Any]
    trades: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"summary": self.summary, "trades": self.trades}


class DeepExportBacktestEnvironment:
    def __init__(self, zip_path: Path) -> None:
        self.zip_path = Path(zip_path)
        with zipfile.ZipFile(self.zip_path) as bundle:
            self.activity_rows = _load_jsonl_from_zip(bundle, "wallet_activity.jsonl")
            self.market_trade_rows = _load_jsonl_from_zip(bundle, "market_trades.jsonl")
            self.market_state_rows = _load_jsonl_from_zip(bundle, "deep_collection/market_state_samples.jsonl")
            self.pnl_rows = _load_jsonl_from_zip(bundle, "wallet_market_pnl.jsonl")
        self.snapshots = [StrategySnapshot.from_market_state_sample(row) for row in self.market_state_rows]
        self.winning_sides = {
            str(row.get("market_slug") or ""): winning_side_from_row(row)
            for row in self.pnl_rows
            if row.get("market_slug") and winning_side_from_row(row)
        }

    def iter_snapshots(self):
        yield from sorted(self.snapshots, key=lambda row: (row.sampled_ts, row.market_slug))

    def iter_market_trades(self):
        yield from sorted(self.market_trade_rows, key=lambda row: (int(row.get("exchange_ts") or 0), str(row.get("market_slug") or "")))

    def initial_history(self) -> StrategyHistory:
        return StrategyHistory(activity_rows=list(self.activity_rows), winning_sides=dict(self.winning_sides))


def run_strategy_backtest(
    strategy: StrategyPlugin,
    env: DeepExportBacktestEnvironment,
    adapter: ExecutionAdapter | None = None,
) -> BacktestResult:
    execution_adapter = adapter or PaperExecutionAdapter(env.winning_sides)
    history = env.initial_history()
    trades: list[dict[str, Any]] = []
    emitted_markets: set[str] = set()
    settled = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    one_trade_per_market = bool(getattr(strategy, "one_trade_per_market", True))
    for snapshot in env.iter_snapshots():
        history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
        if one_trade_per_market and snapshot.market_slug in emitted_markets:
            continue
        intent = strategy.evaluate(snapshot, history)
        if intent is None:
            continue
        emitted_markets.add(snapshot.market_slug)
        history.emitted_intents.append(intent)
        execution = execution_adapter.submit(intent)
        row = {"intent": intent.to_dict(), "execution": execution.to_dict(), "snapshot": snapshot.to_dict()}
        trades.append(row)
        if execution.status == "paper_settled":
            settled += 1
            pnl = float(execution.detail.get("realized_pnl") or 0.0)
            total_pnl += pnl
            wins += 1 if pnl > 0 else 0
            losses += 1 if pnl < 0 else 0
    return BacktestResult(
        summary={
            "source_zip": str(env.zip_path),
            "strategy_name": getattr(strategy, "strategy_name", strategy.__class__.__name__),
            "snapshots": len(env.snapshots),
            "wallet_activity_rows": len(env.activity_rows),
            "wallet_markets": len({str(row.get("market_slug") or "") for row in env.activity_rows if row.get("market_slug")}),
            "intents": len(trades),
            "paper_settled": settled,
            "paper_total_pnl": round(total_pnl, 6),
            "paper_wins": wins,
            "paper_losses": losses,
            "paper_win_rate": round(wins / (wins + losses), 6) if wins + losses else None,
        },
        trades=trades,
    )


@dataclass
class PendingMakerOrder:
    intent: TradeIntent
    remaining_usdc: float
    expires_ts: int
    filled_usdc: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent.to_dict(),
            "remaining_usdc": round(self.remaining_usdc, 6),
            "filled_usdc": round(self.filled_usdc, 6),
            "expires_ts": self.expires_ts,
        }


@dataclass
class PendingMakerReplayConfig:
    fill_rate: float = 0.1
    order_ttl_sec: int = 30
    max_open_orders_per_market: int = 20
    rebalance_fill_multiplier: float = 2.0
    rebalance_ttl_multiplier: float = 1.5
    excess_ttl_multiplier: float = 0.5


@dataclass
class PendingMakerReplay:
    winning_sides: dict[str, str]
    config: PendingMakerReplayConfig = field(default_factory=PendingMakerReplayConfig)
    pending: list[PendingMakerOrder] = field(default_factory=list)
    filled_intents: list[TradeIntent] = field(default_factory=list)
    fill_rows: list[dict[str, Any]] = field(default_factory=list)
    submitted: int = 0
    expired: int = 0

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        if intent.intent.upper() != "BUY":
            return ExecutionResult(
                status="maker_rejected_unsupported_intent",
                intent=intent,
                detail={"error": "PendingMakerReplay only supports BUY intents"},
            )
        same_market = [order for order in self.pending if order.intent.market_slug == intent.market_slug]
        if len(same_market) >= self.config.max_open_orders_per_market:
            return ExecutionResult(status="maker_rejected_open_order_limit", intent=intent, detail={"open_orders": len(same_market)})
        expires_ts = int(intent.sampled_ts) + self._ttl_for_intent(intent)
        self.pending.append(
            PendingMakerOrder(
                intent=intent,
                remaining_usdc=float(intent.notional_usdc),
                expires_ts=expires_ts,
            )
        )
        self.submitted += 1
        return ExecutionResult(
            status="maker_pending",
            intent=intent,
            detail={"quote_price": intent.expected_price, "remaining_usdc": intent.notional_usdc, "expires_ts": expires_ts},
        )

    def _ttl_for_intent(self, intent: TradeIntent) -> int:
        source = book_fill_source(intent.features)
        if source == "maker_rebalance_quote":
            return max(1, int(round(self.config.order_ttl_sec * self.config.rebalance_ttl_multiplier)))
        return max(1, int(round(self.config.order_ttl_sec * self.config.excess_ttl_multiplier))) if intent.features.get("deficit_side") not in {None, intent.outcome} else int(self.config.order_ttl_sec)

    def expire_before(self, ts: int) -> None:
        kept: list[PendingMakerOrder] = []
        for order in self.pending:
            if order.expires_ts < ts and order.remaining_usdc > 1e-9:
                self.expired += 1
            else:
                kept.append(order)
        self.pending = kept

    def pending_intents(self) -> list[TradeIntent]:
        intents: list[TradeIntent] = []
        for order in self.pending:
            if order.remaining_usdc <= 1e-9:
                continue
            intent = order.intent
            intents.append(
                TradeIntent(
                    strategy_name=intent.strategy_name,
                    wallet=intent.wallet,
                    market_slug=intent.market_slug,
                    sampled_ts=intent.sampled_ts,
                    checkpoint_sec=intent.checkpoint_sec,
                    intent=intent.intent,
                    outcome=intent.outcome,
                    notional_usdc=round(order.remaining_usdc, 6),
                    max_price=intent.max_price,
                    expected_price=intent.expected_price,
                    symbol=intent.symbol,
                    reason=intent.reason,
                    features=dict(intent.features),
                )
            )
        return intents

    def process_trade(self, trade: dict[str, Any]) -> list[TradeIntent]:
        ts = int(trade.get("exchange_ts") or 0)
        self.expire_before(ts)
        market_slug = str(trade.get("market_slug") or "")
        outcome = str(trade.get("outcome") or "").capitalize()
        price = float(trade.get("price") or 0.0)
        trade_usdc = float(trade.get("usdc") or 0.0)
        if not market_slug or outcome not in {"Up", "Down"} or price <= 0 or trade_usdc <= 0:
            return []
        filled: list[TradeIntent] = []
        for order in list(self.pending):
            intent = order.intent
            if intent.market_slug != market_slug or intent.outcome != outcome:
                continue
            if price > intent.expected_price + 1e-9:
                continue
            source = book_fill_source(intent.features)
            fill_rate = max(0.0, self.config.fill_rate)
            if source == "maker_rebalance_quote":
                fill_rate *= max(0.0, self.config.rebalance_fill_multiplier)
            fill_usdc = min(order.remaining_usdc, trade_usdc * fill_rate)
            if fill_usdc <= 1e-9:
                continue
            order.remaining_usdc -= fill_usdc
            order.filled_usdc += fill_usdc
            fill_intent = TradeIntent(
                strategy_name=intent.strategy_name,
                wallet=intent.wallet,
                market_slug=intent.market_slug,
                sampled_ts=ts,
                checkpoint_sec=intent.checkpoint_sec,
                intent=intent.intent,
                outcome=intent.outcome,
                notional_usdc=round(fill_usdc, 6),
                max_price=intent.max_price,
                expected_price=intent.expected_price,
                symbol=intent.symbol,
                reason="maker_replay_fill",
                features={
                    **intent.features,
                    "maker_parent_sampled_ts": intent.sampled_ts,
                    "maker_touch_trade_price": price,
                    "maker_touch_trade_usdc": trade_usdc,
                    "maker_fill_rate": self.config.fill_rate,
                },
            )
            filled.append(fill_intent)
            self.filled_intents.append(fill_intent)
            self.fill_rows.append({"intent": fill_intent.to_dict(), "touch_trade": dict(trade), "parent_intent": intent.to_dict()})
            if order.remaining_usdc <= 1e-9:
                self.pending.remove(order)
        return filled

    def settle(self, intent: TradeIntent) -> ExecutionResult:
        return PaperExecutionAdapter(self.winning_sides).submit(intent)


def run_strategy_maker_replay_backtest(
    strategy: StrategyPlugin,
    env: DeepExportBacktestEnvironment,
    *,
    config: PendingMakerReplayConfig | None = None,
) -> BacktestResult:
    replay = PendingMakerReplay(env.winning_sides, config or PendingMakerReplayConfig())
    history = env.initial_history()
    rows: list[dict[str, Any]] = []
    emitted_markets: set[str] = set()
    market_trades = list(env.iter_market_trades())
    trade_idx = 0
    settled = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    one_trade_per_market = bool(getattr(strategy, "one_trade_per_market", True))

    def process_trades_until(ts: int) -> None:
        nonlocal trade_idx, settled, wins, losses, total_pnl
        while trade_idx < len(market_trades) and int(market_trades[trade_idx].get("exchange_ts") or 0) <= ts:
            trade = market_trades[trade_idx]
            trade_idx += 1
            for fill_intent in replay.process_trade(trade):
                history.emitted_intents.append(fill_intent)
                execution = replay.settle(fill_intent)
                row = {"record_type": "maker_fill", "intent": fill_intent.to_dict(), "execution": execution.to_dict(), "touch_trade": dict(trade)}
                rows.append(row)
                if execution.status == "paper_settled":
                    settled += 1
                    pnl = float(execution.detail.get("realized_pnl") or 0.0)
                    total_pnl += pnl
                    wins += 1 if pnl > 0 else 0
                    losses += 1 if pnl < 0 else 0

    for snapshot in env.iter_snapshots():
        process_trades_until(snapshot.sampled_ts)
        history.snapshots_by_market.setdefault(snapshot.market_slug, []).append(snapshot)
        history.pending_intents = replay.pending_intents()
        if one_trade_per_market and snapshot.market_slug in emitted_markets:
            continue
        intent = strategy.evaluate(snapshot, history)
        if intent is None:
            continue
        emitted_markets.add(snapshot.market_slug)
        pending_execution = replay.submit(intent)
        rows.append({"record_type": "maker_order", "intent": intent.to_dict(), "execution": pending_execution.to_dict(), "snapshot": snapshot.to_dict()})
    process_trades_until(10**18)
    replay.expire_before(10**18)
    return BacktestResult(
        summary={
            "source_zip": str(env.zip_path),
            "strategy_name": getattr(strategy, "strategy_name", strategy.__class__.__name__),
            "snapshots": len(env.snapshots),
            "market_trade_rows": len(env.market_trade_rows),
            "wallet_activity_rows": len(env.activity_rows),
            "wallet_markets": len({str(row.get("market_slug") or "") for row in env.activity_rows if row.get("market_slug")}),
            "maker_orders": replay.submitted,
            "maker_fills": len(replay.filled_intents),
            "maker_expired": replay.expired,
            "open_orders": len(replay.pending),
            "intents": len(replay.filled_intents),
            "paper_settled": settled,
            "paper_total_pnl": round(total_pnl, 6),
            "paper_wins": wins,
            "paper_losses": losses,
            "paper_win_rate": round(wins / (wins + losses), 6) if wins + losses else None,
        },
        trades=rows,
    )
