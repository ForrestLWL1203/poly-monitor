from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .path_strategy import ExecutionAdapter, PathStrategyConfig, SettlementPaperExecutionAdapter, TradeIntent, WalletPathStrategy, _signed_flow
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
    start_sampled_ts: int = 0
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
        self.store = ObserverStore(data_dir / "state" / "observer.sqlite")

    def load_strategy_rows(self, wallet: str) -> dict[str, list[dict[str, Any]]]:
        return {
            "activity_rows": self.store.wallet_activity_events(wallet),
            "market_state_samples": self.store.market_state_samples(),
            "settlements": {
                str(row["market_slug"]): str(row["winning_side"])
                for row in self.store.conn.execute(
                    "SELECT market_slug, winning_side FROM market_settlements WHERE completed=1 AND winning_side != ''"
                ).fetchall()
            },
        }

    def close(self) -> None:
        self.store.close()


def _json_dumps(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _net_side(value: float) -> str | None:
    if value > 0:
        return "Up"
    if value < 0:
        return "Down"
    return None


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
        self._settled_keys: set[tuple[str, int, str]] = set()
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
                if payload.get("record_type") == "settlement":
                    self._settled_keys.add(self._intent_key(intent))

    def _intent_key(self, intent: Any) -> tuple[str, int, str]:
        if hasattr(intent, "market_slug"):
            return (str(intent.market_slug), int(intent.checkpoint_sec), str(intent.outcome))
        return (str(intent.get("market_slug") or ""), int(intent.get("checkpoint_sec") or 0), str(intent.get("outcome") or ""))

    def _intent_from_dict(self, payload: dict[str, Any]) -> TradeIntent:
        return TradeIntent(
            wallet=str(payload.get("wallet") or self.wallet),
            market_slug=str(payload.get("market_slug") or ""),
            sampled_ts=int(payload.get("sampled_ts") or 0),
            checkpoint_sec=int(payload.get("checkpoint_sec") or 0),
            intent=str(payload.get("intent") or "BUY"),
            outcome=str(payload.get("outcome") or ""),
            notional_usdc=float(payload.get("notional_usdc") or 0.0),
            max_price=float(payload.get("max_price") or 0.0),
            expected_price=float(payload.get("expected_price") or 0.0),
            reason=str(payload.get("reason") or ""),
            features=payload.get("features") if isinstance(payload.get("features"), dict) else {},
        )

    def _settle_open_records(self, settlements: dict[str, str]) -> int:
        if not self.output_path.exists() or not settlements:
            return 0
        rows: list[dict[str, Any]] = []
        for line in self.output_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        written = 0
        with self.output_path.open("a", encoding="utf-8") as handle:
            for row in rows:
                if row.get("record_type") == "settlement":
                    continue
                execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
                if execution.get("status") != "paper_open":
                    continue
                intent_payload = row.get("intent") if isinstance(row.get("intent"), dict) else None
                if not intent_payload:
                    continue
                key = self._intent_key(intent_payload)
                if key in self._settled_keys:
                    continue
                slug = str(intent_payload.get("market_slug") or "")
                if slug not in settlements:
                    continue
                adapter = SettlementPaperExecutionAdapter({slug: settlements[slug]})
                settled = adapter.submit(self._intent_from_dict(intent_payload))
                payload = {"recorded_at": utc_iso(), "record_type": "settlement", "intent": intent_payload, "execution": settled.to_dict()}
                handle.write(_json_dumps(payload) + "\n")
                self._settled_keys.add(key)
                written += 1
        return written

    def _target_wallet_context(self, intent: TradeIntent, activity_rows: list[dict[str, Any]]) -> dict[str, Any]:
        rows = [
            row
            for row in activity_rows
            if str(row.get("wallet") or "").lower() == self.wallet
            and str(row.get("market_slug") or "") == intent.market_slug
            and str(row.get("activity_type") or "").upper() == "TRADE"
            and int(row.get("exchange_ts") or 0) <= intent.sampled_ts
        ]
        net_usdc = round(sum(_signed_flow(row) for row in rows), 6)
        return {
            "wallet": self.wallet,
            "market_slug": intent.market_slug,
            "sampled_ts": intent.sampled_ts,
            "trade_rows_seen": len(rows),
            "net_up_down_usdc_seen": net_usdc,
            "net_side_seen": _net_side(net_usdc),
            "total_usdc_seen": round(sum(float(row.get("usdc") or 0.0) for row in rows), 6),
        }

    def tick(self) -> dict[str, Any]:
        loaded = self.data_source.load_strategy_rows(self.wallet)
        activity = loaded.get("activity_rows", [])
        samples = loaded.get("market_state_samples", [])
        samples_by_market: dict[str, list[dict[str, Any]]] = {}
        for row in samples:
            samples_by_market.setdefault(str(row.get("market_slug") or ""), []).append(row)
        raw_settlements = loaded.get("settlements", {})
        settlements = raw_settlements if isinstance(raw_settlements, dict) else {}
        settled = self._settle_open_records({str(slug): str(side) for slug, side in settlements.items()})
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
                if int(sample.get("sampled_ts") or 0) < self.config.start_sampled_ts:
                    continue
                slug = str(sample.get("market_slug") or "")
                sample = dict(sample)
                sample["_market_state_history"] = samples_by_market.get(slug, [])
                intent = strategy.evaluate_snapshot(sample, activity)
                if not intent:
                    continue
                key = self._intent_key(intent)
                if key in self._emitted_keys:
                    continue
                self._emitted_keys.add(key)
                execution = self.adapter.submit(intent)
                payload = {
                    "recorded_at": utc_iso(),
                    "record_type": "execution",
                    "intent": intent.to_dict(),
                    "execution": execution.to_dict(),
                    "target_wallet_context": self._target_wallet_context(intent, activity),
                }
                handle.write(_json_dumps(payload) + "\n")
                written += 1
        return {"intents": written, "settlements": settled, "activity_rows": len(activity), "sample_rows": len(samples), "output_path": str(self.output_path)}

    def run(self, *, seconds: float | None = None) -> int:
        deadline = time.monotonic() + seconds if seconds is not None else None
        try:
            while deadline is None or time.monotonic() < deadline:
                self.tick()
                time.sleep(max(0.1, self.config.poll_sec))
        finally:
            self.close()
        return 0
