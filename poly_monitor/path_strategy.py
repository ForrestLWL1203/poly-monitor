from __future__ import annotations

import json
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class PathStrategyConfig:
    wallet: str
    checkpoints: tuple[int, ...] = (120, 180, 240)
    notional_usdc: float = 25.0
    first_bias_min_usdc: float = 25.0
    max_price: float = 0.95
    one_trade_per_market: bool = True


@dataclass(frozen=True)
class TradeIntent:
    wallet: str
    market_slug: str
    sampled_ts: int
    checkpoint_sec: int
    intent: str
    outcome: str
    notional_usdc: float
    max_price: float
    expected_price: float
    reason: str
    features: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    intent: TradeIntent
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "intent": self.intent.to_dict(), "detail": self.detail}


class ExecutionAdapter(Protocol):
    def submit(self, intent: TradeIntent) -> ExecutionResult:
        ...


class RecordingExecutionAdapter:
    def __init__(self) -> None:
        self.submitted: list[TradeIntent] = []

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        self.submitted.append(intent)
        return ExecutionResult(status="recorded", intent=intent)


class SettlementPaperExecutionAdapter:
    def __init__(self, winning_sides: dict[str, str]) -> None:
        self.winning_sides = {str(slug): str(side).capitalize() for slug, side in winning_sides.items()}
        self.submitted: list[TradeIntent] = []

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        self.submitted.append(intent)
        winning_side = self.winning_sides.get(intent.market_slug)
        shares = intent.notional_usdc / intent.expected_price if intent.expected_price > 0 else 0.0
        if not winning_side:
            return ExecutionResult(
                status="paper_open",
                intent=intent,
                detail={"filled_usdc": intent.notional_usdc, "avg_price": intent.expected_price, "shares": round(shares, 6)},
            )
        realized_pnl = shares - intent.notional_usdc if winning_side == intent.outcome else -intent.notional_usdc
        return ExecutionResult(
            status="paper_settled",
            intent=intent,
            detail={
                "filled_usdc": intent.notional_usdc,
                "avg_price": intent.expected_price,
                "shares": round(shares, 6),
                "winning_side": winning_side,
                "realized_pnl": round(realized_pnl, 6),
            },
        )


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


def _load_jsonl(bundle: zipfile.ZipFile, name: str) -> list[dict[str, Any]]:
    try:
        raw = bundle.read(name).decode("utf-8")
    except KeyError:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def load_deep_export_for_path_strategy(zip_path: Path) -> DeepExportReplayInput:
    with zipfile.ZipFile(Path(zip_path)) as bundle:
        pnl_rows = _load_jsonl(bundle, "wallet_market_pnl.jsonl")
        return DeepExportReplayInput(
            activity_rows=_load_jsonl(bundle, "wallet_activity.jsonl"),
            market_state_samples=_load_jsonl(bundle, "deep_collection/market_state_samples.jsonl"),
            winning_sides={
                str(row.get("market_slug") or ""): str(row.get("winning_side") or row.get("winner") or row.get("resolved_outcome") or "").capitalize()
                for row in pnl_rows
                if row.get("market_slug") and str(row.get("winning_side") or row.get("winner") or row.get("resolved_outcome") or "").lower() in {"up", "down"}
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


class WalletPathStrategy:
    def __init__(self, config: PathStrategyConfig) -> None:
        self.config = config

    def evaluate_snapshot(self, sample: dict[str, Any], activity_rows: list[dict[str, Any]]) -> TradeIntent | None:
        slug = str(sample.get("market_slug") or "")
        sampled_ts = _safe_int(sample.get("sampled_ts"))
        elapsed = _elapsed_sec(slug, sampled_ts)
        if elapsed is None or sample.get("book_stale"):
            return None
        checkpoint = _checkpoint_for_elapsed(elapsed, self.config.checkpoints)
        if checkpoint is None:
            return None
        rows = [
            row
            for row in activity_rows
            if str(row.get("wallet") or "").lower() == self.config.wallet.lower()
            and str(row.get("market_slug") or "") == slug
            and str(row.get("activity_type") or "").upper() == "TRADE"
            and _safe_int(row.get("exchange_ts")) <= sampled_ts
        ]
        net_usdc = round(sum(_signed_flow(row) for row in rows), 6)
        if abs(net_usdc) < self.config.first_bias_min_usdc:
            return None
        outcome = _net_side(net_usdc)
        if outcome is None:
            return None
        book = _book_for_outcome(sample, outcome)
        ask_targets = book.get("ask_targets") if isinstance(book, dict) else None
        if not isinstance(ask_targets, dict):
            return None
        target_key = f"{self.config.notional_usdc:g}"
        fill = ask_targets.get(target_key)
        if not isinstance(fill, dict) or not fill.get("ok"):
            return None
        expected_price = _safe_float(fill.get("avg"))
        if expected_price <= 0 or expected_price > self.config.max_price:
            return None
        return TradeIntent(
            wallet=self.config.wallet.lower(),
            market_slug=slug,
            sampled_ts=sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=float(self.config.notional_usdc),
            max_price=float(self.config.max_price),
            expected_price=round(expected_price, 6),
            reason=f"checkpoint_{checkpoint}_net_bias",
            features={
                "elapsed_sec": elapsed,
                "wallet_net_up_down_usdc": net_usdc,
                "wallet_trade_rows_seen": len(rows),
                "book_fill": fill,
            },
        )


class D950MarketPathStrategy:
    def __init__(self, config: PathStrategyConfig, *, min_reference_delta: float = 0.0) -> None:
        self.config = config
        self.min_reference_delta = float(min_reference_delta)

    def evaluate_snapshot(self, sample: dict[str, Any], activity_rows: list[dict[str, Any]]) -> TradeIntent | None:
        slug = str(sample.get("market_slug") or "")
        sampled_ts = _safe_int(sample.get("sampled_ts"))
        elapsed = _elapsed_sec(slug, sampled_ts)
        if elapsed is None or sample.get("book_stale"):
            return None
        checkpoint = _checkpoint_for_elapsed(elapsed, self.config.checkpoints)
        if checkpoint is None:
            return None
        history = sample.get("_market_state_history")
        if not isinstance(history, list):
            history = []
        current_ref = _safe_float(sample.get("reference_price"))
        refs = [
            row
            for row in history
            if str(row.get("market_slug") or "") == slug
            and _safe_int(row.get("sampled_ts")) <= sampled_ts
            and _safe_float(row.get("reference_price")) > 0
        ]
        first_ref = _safe_float(refs[0].get("reference_price")) if refs else current_ref
        reference_delta = round(current_ref - first_ref, 6)
        if abs(reference_delta) <= self.min_reference_delta:
            return None
        outcome = "Up" if reference_delta > 0 else "Down"
        book = _book_for_outcome(sample, outcome)
        ask_targets = book.get("ask_targets") if isinstance(book, dict) else None
        if not isinstance(ask_targets, dict):
            return None
        target_key = f"{self.config.notional_usdc:g}"
        fill = ask_targets.get(target_key)
        if not isinstance(fill, dict) or not fill.get("ok"):
            return None
        expected_price = _safe_float(fill.get("avg"))
        if expected_price <= 0 or expected_price > self.config.max_price:
            return None
        return TradeIntent(
            wallet=self.config.wallet.lower(),
            market_slug=slug,
            sampled_ts=sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=float(self.config.notional_usdc),
            max_price=float(self.config.max_price),
            expected_price=round(expected_price, 6),
            reason="d950_path_v0_reference_momentum",
            features={
                "elapsed_sec": elapsed,
                "reference_delta": reference_delta,
                "reference_price": current_ref,
                "reference_start_price": first_ref,
                "book_fill": fill,
            },
        )


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
    for sample in sorted(market_state_samples, key=lambda row: (_safe_int(row.get("sampled_ts")), str(row.get("market_slug") or ""))):
        slug = str(sample.get("market_slug") or "")
        if config.one_trade_per_market and slug in emitted_markets:
            continue
        intent = strategy.evaluate_snapshot(sample, activity_rows)
        if not intent:
            continue
        emitted_markets.add(slug)
        intents.append(intent)
        executions.append(execution_adapter.submit(intent))
    return ReplayResult(intents=intents, executions=executions)
