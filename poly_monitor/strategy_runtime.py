from __future__ import annotations

import datetime as dt
import json
import zipfile
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


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


def _coalesce(value: Any, default: Any) -> Any:
    return default if value is None else value


def _load_jsonl_from_zip(bundle: zipfile.ZipFile, name: str) -> list[dict[str, Any]]:
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


def normalize_winning_side(value: Any) -> str:
    side = str(value or "")
    return side.capitalize() if side.lower() in {"up", "down"} else ""


def winning_side_from_row(row: dict[str, Any]) -> str:
    return normalize_winning_side(row.get("winning_side") or row.get("winner") or row.get("resolved_outcome"))


def book_fill_source(features: dict[str, Any]) -> str:
    book_fill = features.get("book_fill")
    return str((book_fill or {}).get("source") or "") if isinstance(book_fill, dict) else ""


def _window_start_from_slug(slug: str) -> int | None:
    try:
        return int(str(slug).rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


@dataclass(frozen=True)
class BookSnapshot:
    bid: float | None = None
    ask: float | None = None
    spread: float | None = None
    book_age_ms: float | None = None
    ask_depth_usdc: float | None = None
    bid_depth_usdc: float | None = None
    ask_targets: dict[str, dict[str, Any]] = field(default_factory=dict)
    bid_targets: dict[str, dict[str, Any]] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_value(cls, value: Any) -> "BookSnapshot":
        data = _decode_json(value)
        return cls(
            bid=data.get("bid"),
            ask=data.get("ask"),
            spread=data.get("spread"),
            book_age_ms=data.get("book_age_ms"),
            ask_depth_usdc=data.get("ask_depth_usdc") or data.get("stable_depth_usd"),
            bid_depth_usdc=data.get("bid_depth_usdc"),
            ask_targets=data.get("ask_targets") if isinstance(data.get("ask_targets"), dict) else {},
            bid_targets=data.get("bid_targets") if isinstance(data.get("bid_targets"), dict) else {},
            raw=data,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
            "book_age_ms": self.book_age_ms,
            "ask_depth_usdc": self.ask_depth_usdc,
            "bid_depth_usdc": self.bid_depth_usdc,
            "ask_targets": self.ask_targets,
            "bid_targets": self.bid_targets,
        }


@dataclass(frozen=True)
class StrategySnapshot:
    market_slug: str
    sampled_ts: int = 0
    condition_id: str = ""
    symbol: str = ""
    window_start_ts: int | None = None
    window_end_ts: int | None = None
    observed_at: str = ""
    elapsed_sec: int = 0
    remaining_sec: float | None = None
    reference_price: float | None = None
    reference_price_age_sec: float | None = None
    up: BookSnapshot = field(default_factory=BookSnapshot)
    down: BookSnapshot = field(default_factory=BookSnapshot)
    book_stale: bool = False
    sample_reason: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_market_state_sample(cls, row: dict[str, Any]) -> "StrategySnapshot":
        slug = str(row.get("market_slug") or "")
        sampled_ts = _safe_int(row.get("sampled_ts"))
        start_ts = _window_start_from_slug(slug)
        remaining = row.get("window_remaining_sec")
        remaining_float = _safe_float(remaining) if remaining is not None else None
        return cls(
            market_slug=slug,
            sampled_ts=sampled_ts,
            condition_id=str(row.get("condition_id") or ""),
            symbol=str(row.get("symbol") or "").upper(),
            window_start_ts=start_ts,
            window_end_ts=start_ts + 300 if start_ts is not None else None,
            observed_at=str(row.get("observed_at") or ""),
            elapsed_sec=max(0, sampled_ts - start_ts) if start_ts is not None and sampled_ts else 0,
            remaining_sec=remaining_float,
            reference_price=row.get("reference_price"),
            reference_price_age_sec=row.get("reference_price_age_sec"),
            up=BookSnapshot.from_value(row.get("up_json") or row.get("up") or {}),
            down=BookSnapshot.from_value(row.get("down_json") or row.get("down") or {}),
            book_stale=bool(row.get("book_stale")),
            sample_reason=str(row.get("sample_reason") or ""),
            raw=dict(row),
        )

    def book_for_outcome(self, outcome: str) -> BookSnapshot:
        return self.up if str(outcome).lower() == "up" else self.down

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_slug": self.market_slug,
            "condition_id": self.condition_id,
            "symbol": self.symbol,
            "sampled_ts": self.sampled_ts,
            "observed_at": self.observed_at,
            "elapsed_sec": self.elapsed_sec,
            "remaining_sec": self.remaining_sec,
            "reference_price": self.reference_price,
            "reference_price_age_sec": self.reference_price_age_sec,
            "book_stale": self.book_stale,
            "sample_reason": self.sample_reason,
            "up": self.up.to_dict(),
            "down": self.down.to_dict(),
        }


@dataclass
class StrategyHistory:
    activity_rows: list[dict[str, Any]] = field(default_factory=list)
    snapshots_by_market: dict[str, list[StrategySnapshot]] = field(default_factory=dict)
    winning_sides: dict[str, str] = field(default_factory=dict)
    emitted_intents: list["TradeIntent"] = field(default_factory=list)
    pending_intents: list["TradeIntent"] = field(default_factory=list)

    def activity_for_market(self, market_slug: str) -> list[dict[str, Any]]:
        return [row for row in self.activity_rows if str(row.get("market_slug") or "") == market_slug]

    def snapshots_for_market(self, market_slug: str) -> list[StrategySnapshot]:
        return list(self.snapshots_by_market.get(market_slug, []))


@dataclass(frozen=True)
class TradeIntent:
    market_slug: str
    sampled_ts: int
    intent: str
    outcome: str
    notional_usdc: float
    max_price: float
    expected_price: float
    reason: str
    strategy_name: str = ""
    wallet: str = ""
    checkpoint_sec: int = 0
    symbol: str = ""
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


class StrategyPlugin(Protocol):
    strategy_name: str

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        ...


class ExecutionAdapter(Protocol):
    def submit(self, intent: TradeIntent) -> ExecutionResult:
        ...


class RecordingExecutionAdapter:
    def __init__(self) -> None:
        self.submitted: list[TradeIntent] = []

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        self.submitted.append(intent)
        return ExecutionResult(status="recorded", intent=intent)


class PaperExecutionAdapter:
    def __init__(self, winning_sides: dict[str, str] | None = None) -> None:
        self.winning_sides = {str(slug): str(side).capitalize() for slug, side in (winning_sides or {}).items()}
        self.submitted: list[TradeIntent] = []

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        self.submitted.append(intent)
        if intent.intent.upper() != "BUY":
            return ExecutionResult(
                status="paper_rejected_unsupported_intent",
                intent=intent,
                detail={"error": "PaperExecutionAdapter only supports BUY intents"},
            )
        winning_side = self.winning_sides.get(intent.market_slug)
        shares = intent.notional_usdc / intent.expected_price if intent.expected_price > 0 else 0.0
        base = {"filled_usdc": intent.notional_usdc, "avg_price": intent.expected_price, "shares": round(shares, 6)}
        if not winning_side:
            return ExecutionResult(status="paper_open", intent=intent, detail=base)
        realized_pnl = shares - intent.notional_usdc if winning_side == intent.outcome else -intent.notional_usdc
        return ExecutionResult(
            status="paper_settled",
            intent=intent,
            detail={**base, "winning_side": winning_side, "realized_pnl": round(realized_pnl, 6)},
        )


class RejectingLiveExecutionAdapter:
    def submit(self, intent: TradeIntent) -> ExecutionResult:
        return ExecutionResult(
            status="live_rejected",
            intent=intent,
            detail={"error": "live execution not implemented for poly-monitor"},
        )


def strategy_from_name(name: str, **kwargs: Any) -> StrategyPlugin:
    from .path_strategy import D950MarketPathStrategy, PathStrategyConfig, WalletPathStrategy

    normalized = str(name)
    checkpoints = kwargs.get("checkpoints")
    if checkpoints is None:
        checkpoints = (1,) if normalized in {"wallet_path", "wallet_path_v0"} else (120, 180, 240)
    config = PathStrategyConfig(
        wallet=str(_coalesce(kwargs.get("wallet"), normalized)),
        checkpoints=tuple(checkpoints),
        notional_usdc=float(_coalesce(kwargs.get("notional_usdc"), 25.0)),
        first_bias_min_usdc=float(_coalesce(kwargs.get("first_bias_min_usdc"), _coalesce(kwargs.get("bias_threshold"), 25.0))),
        max_price=float(_coalesce(kwargs.get("max_price"), 0.95)),
        target_pair_notional_usdc=float(_coalesce(kwargs.get("target_pair_notional_usdc"), 25.0)),
        target_pair_shares_per_side=(
            float(kwargs["target_pair_shares_per_side"])
            if kwargs.get("target_pair_shares_per_side") is not None
            else None
        ),
        max_pair_cost=float(_coalesce(kwargs.get("max_pair_cost"), 0.99)),
        max_unpaired_price=float(_coalesce(kwargs.get("max_unpaired_price"), 0.6)),
        max_inventory_imbalance_ratio=float(_coalesce(kwargs.get("max_inventory_imbalance_ratio"), 0.05)),
        early_inventory_imbalance_ratio=float(_coalesce(kwargs.get("early_inventory_imbalance_ratio"), 0.30)),
        mid_inventory_imbalance_ratio=float(_coalesce(kwargs.get("mid_inventory_imbalance_ratio"), 0.15)),
        late_inventory_imbalance_ratio=float(_coalesce(kwargs.get("late_inventory_imbalance_ratio"), 0.08)),
        final_inventory_imbalance_ratio=float(_coalesce(kwargs.get("final_inventory_imbalance_ratio"), 0.03)),
        rebalance_start_sec=int(_coalesce(kwargs.get("rebalance_start_sec"), 240)),
        maker_rebalance_ticks=int(_coalesce(kwargs.get("maker_rebalance_ticks"), 1)),
        tick_size=float(_coalesce(kwargs.get("tick_size"), 0.01)),
        min_order_usdc=float(_coalesce(kwargs.get("min_order_usdc"), 1.0)),
        execution_style=str(_coalesce(kwargs.get("execution_style"), "maker")),
        one_trade_per_market=bool(_coalesce(kwargs.get("one_trade_per_market"), normalized == "d950_path_v0")),
    )
    if normalized == "d950_path_v0":
        return D950MarketPathStrategy(config, min_reference_delta=float(_coalesce(kwargs.get("min_reference_delta"), 0.0)))
    if normalized in {"wallet_path", "wallet_path_v0"}:
        return WalletPathStrategy(config)
    raise ValueError(f"unknown strategy: {name}")


def utc_iso(value: dt.datetime | None = None) -> str:
    return (value or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).isoformat()
