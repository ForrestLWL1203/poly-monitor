from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .strategy_runtime import (
    ExecutionAdapter,
    PaperExecutionAdapter,
    StrategyHistory,
    StrategyPlugin,
    StrategySnapshot,
    _load_jsonl_from_zip,
    winning_side_from_row,
)
from .maker_paper import PendingMakerReplay, PendingMakerReplayConfig


@dataclass(frozen=True)
class BacktestResult:
    summary: dict[str, Any]
    trades: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"summary": self.summary, "summary_by_symbol": _summary_by_symbol(self.trades), "trades": self.trades}


def _empty_symbol_summary() -> dict[str, Any]:
    return {
        "intents": 0,
        "paper_settled": 0,
        "paper_total_pnl": 0.0,
        "paper_wins": 0,
        "paper_losses": 0,
        "paper_win_rate": None,
    }


def _summary_by_symbol(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {"all_symbols": _empty_symbol_summary()}
    for row in trades:
        intent = row.get("intent") if isinstance(row, dict) else None
        execution = row.get("execution") if isinstance(row, dict) else None
        if not isinstance(intent, dict) or not isinstance(execution, dict):
            continue
        symbol = str(intent.get("symbol") or "UNKNOWN").upper()
        for key in ("all_symbols", symbol):
            grouped.setdefault(key, _empty_symbol_summary())
            grouped[key]["intents"] += 1
        if execution.get("status") != "paper_settled":
            continue
        pnl = float((execution.get("detail") or {}).get("realized_pnl") or 0.0)
        for key in ("all_symbols", symbol):
            item = grouped[key]
            item["paper_settled"] += 1
            item["paper_total_pnl"] = round(float(item["paper_total_pnl"]) + pnl, 6)
            item["paper_wins"] += 1 if pnl > 0 else 0
            item["paper_losses"] += 1 if pnl < 0 else 0
    for item in grouped.values():
        denom = int(item["paper_wins"]) + int(item["paper_losses"])
        item["paper_win_rate"] = round(int(item["paper_wins"]) / denom, 6) if denom else None
    return grouped


class DeepExportBacktestEnvironment:
    def __init__(self, zip_path: Path) -> None:
        self.zip_path = Path(zip_path)
        with zipfile.ZipFile(self.zip_path) as bundle:
            self.activity_rows = _load_jsonl_from_zip(bundle, "wallet_activity.jsonl")
            self.market_trade_rows = _load_market_trade_rows(bundle)
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


def _load_market_trade_rows(bundle: zipfile.ZipFile) -> list[dict[str, Any]]:
    rows = _load_jsonl_from_zip(bundle, "market_trades.jsonl")
    seen = {(str(row.get("market_slug") or ""), str(row.get("tx_hash") or ""), str(row.get("fill_id") or "")) for row in rows}
    for name in bundle.namelist():
        if not name.startswith("markets/") or not name.endswith("/market_trades.jsonl"):
            continue
        for row in _load_jsonl_from_zip(bundle, name):
            key = (str(row.get("market_slug") or ""), str(row.get("tx_hash") or ""), str(row.get("fill_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


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
            for fill in replay.process_trade(trade):
                fill_intent = fill.intent
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
