from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .path_strategy import ExecutionAdapter, PathStrategyConfig, SettlementPaperExecutionAdapter, WalletPathStrategy
from .storage import ObserverStore, utc_iso


@dataclass(frozen=True)
class PathPaperRunnerConfig:
    wallet: str
    data_dir: Path
    strategy_name: str = "path_strategy"
    poll_sec: float = 1.0
    checkpoints: tuple[int, ...] = (120, 180, 240)
    notional_usdc: float = 25.0
    first_bias_min_usdc: float = 25.0
    max_price: float = 0.95
    winning_sides: dict[str, str] = field(default_factory=dict)


class StrategyDataSource(Protocol):
    def load_strategy_rows(self, wallet: str) -> dict[str, list[dict[str, Any]]]:
        ...

    def close(self) -> None:
        ...


class SnapshotStrategy(Protocol):
    def evaluate_snapshot(self, sample: dict[str, Any], activity_rows: list[dict[str, Any]]) -> Any:
        ...


class SqliteStrategyDataSource:
    def __init__(self, data_dir: Path) -> None:
        self.store = ObserverStore(data_dir / "observer.db")

    def load_strategy_rows(self, wallet: str) -> dict[str, list[dict[str, Any]]]:
        return {
            "activity_rows": self.store.wallet_activity_events(wallet),
            "market_state_samples": self.store.market_state_samples(),
        }

    def close(self) -> None:
        self.store.close()


def _json_dumps(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class PathPaperRunner:
    def __init__(
        self,
        config: PathPaperRunnerConfig,
        *,
        data_source: StrategyDataSource | None = None,
        execution_adapter: ExecutionAdapter | None = None,
        strategy: SnapshotStrategy | None = None,
    ) -> None:
        self.config = config
        self.wallet = config.wallet.lower()
        self.data_source = data_source or SqliteStrategyDataSource(config.data_dir)
        self.adapter = execution_adapter or SettlementPaperExecutionAdapter(config.winning_sides)
        self.strategy = strategy
        self._emitted_keys: set[tuple[str, int, str]] = set()
        self.output_path = config.data_dir / "paper" / config.strategy_name / self.wallet / "executions.jsonl"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_existing_keys()

    def close(self) -> None:
        self.data_source.close()

    def _load_existing_keys(self) -> None:
        if not self.output_path.exists():
            return
        for line in self.output_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            intent = payload.get("intent") if isinstance(payload, dict) else None
            if isinstance(intent, dict):
                self._emitted_keys.add(self._intent_key(intent))

    def _intent_key(self, intent: Any) -> tuple[str, int, str]:
        if hasattr(intent, "market_slug"):
            return (str(intent.market_slug), int(intent.checkpoint_sec), str(intent.outcome))
        return (str(intent.get("market_slug") or ""), int(intent.get("checkpoint_sec") or 0), str(intent.get("outcome") or ""))

    def tick(self) -> dict[str, Any]:
        loaded = self.data_source.load_strategy_rows(self.wallet)
        activity = loaded.get("activity_rows", [])
        samples = loaded.get("market_state_samples", [])
        strategy = self.strategy or (
            WalletPathStrategy(
                PathStrategyConfig(
                    wallet=self.wallet,
                    checkpoints=self.config.checkpoints,
                    notional_usdc=self.config.notional_usdc,
                    first_bias_min_usdc=self.config.first_bias_min_usdc,
                    max_price=self.config.max_price,
                )
            )
        )
        written = 0
        with self.output_path.open("a", encoding="utf-8") as handle:
            for sample in sorted(samples, key=lambda row: (int(row.get("sampled_ts") or 0), str(row.get("market_slug") or ""))):
                intent = strategy.evaluate_snapshot(sample, activity)
                if not intent:
                    continue
                key = self._intent_key(intent)
                if key in self._emitted_keys:
                    continue
                self._emitted_keys.add(key)
                execution = self.adapter.submit(intent)
                payload = {"recorded_at": utc_iso(), "intent": intent.to_dict(), "execution": execution.to_dict()}
                handle.write(_json_dumps(payload) + "\n")
                written += 1
        return {"intents": written, "activity_rows": len(activity), "sample_rows": len(samples), "output_path": str(self.output_path)}

    def run(self, *, seconds: float | None = None) -> int:
        deadline = time.monotonic() + seconds if seconds is not None else None
        try:
            while deadline is None or time.monotonic() < deadline:
                self.tick()
                time.sleep(max(0.1, self.config.poll_sec))
        finally:
            self.close()
        return 0
