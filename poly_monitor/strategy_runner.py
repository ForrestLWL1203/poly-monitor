from __future__ import annotations

import json
import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .strategy_runtime import ExecutionAdapter, StrategyHistory, StrategyPlugin, StrategySnapshot, TradeIntent, utc_iso


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
