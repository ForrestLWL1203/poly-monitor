from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .strategy_runtime import ExecutionResult, PaperExecutionAdapter, TradeIntent, book_fill_source


@dataclass
class PendingMakerOrder:
    order_id: str
    intent: TradeIntent
    remaining_usdc: float
    expires_ts: int
    submitted_ts: int
    filled_usdc: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "market_slug": self.intent.market_slug,
            "symbol": self.intent.symbol,
            "outcome": self.intent.outcome,
            "sampled_ts": self.intent.sampled_ts,
            "quote_price": self.intent.expected_price,
            "original_usdc": round(self.intent.notional_usdc, 6),
            "remaining_usdc": round(self.remaining_usdc, 6),
            "filled_usdc": round(self.filled_usdc, 6),
            "expires_ts": self.expires_ts,
            "submitted_ts": self.submitted_ts,
        }


@dataclass
class PendingMakerReplayConfig:
    fill_rate: float = 0.1
    order_ttl_sec: int = 5
    max_open_orders_per_market: int = 20
    rebalance_fill_multiplier: float = 2.0
    rebalance_ttl_multiplier: float = 1.0
    excess_ttl_multiplier: float = 1.0


@dataclass
class MakerCancel:
    order: PendingMakerOrder
    reason: str
    cancelled_ts: int
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class MakerFill:
    order: PendingMakerOrder
    intent: TradeIntent
    touch_trade: dict[str, Any]
    remaining_usdc: float

    def to_execution_row(self) -> dict[str, Any]:
        fill_shares = self.intent.notional_usdc / self.intent.expected_price if self.intent.expected_price > 0 else 0.0
        return {
            "record_type": "maker_fill",
            "order_id": self.order.order_id,
            "market_slug": self.intent.market_slug,
            "symbol": self.intent.symbol,
            "sampled_ts": self.intent.sampled_ts,
            "parent_sampled_ts": self.order.submitted_ts,
            "outcome": self.intent.outcome,
            "quote_price": self.intent.expected_price,
            "fill_usdc": round(self.intent.notional_usdc, 6),
            "fill_shares": round(fill_shares, 6),
            "remaining_usdc": round(self.remaining_usdc, 6),
            "touch_trade": {
                "market_slug": self.touch_trade.get("market_slug"),
                "exchange_ts": self.touch_trade.get("exchange_ts"),
                "outcome": self.touch_trade.get("outcome"),
                "side": self.touch_trade.get("side"),
                "price": self.touch_trade.get("price"),
                "size": self.touch_trade.get("size"),
                "usdc": self.touch_trade.get("usdc"),
                "tx_hash": self.touch_trade.get("tx_hash"),
                "fill_id": self.touch_trade.get("fill_id"),
            },
            "configured_fill_rate": self.intent.features.get("maker_fill_rate"),
            "realized_touch_fill_rate": round(self.intent.notional_usdc / float(self.touch_trade.get("usdc") or 0.0), 6) if float(self.touch_trade.get("usdc") or 0.0) > 0 else None,
        }


@dataclass
class PendingMakerReplay:
    winning_sides: dict[str, str] = field(default_factory=dict)
    config: PendingMakerReplayConfig = field(default_factory=PendingMakerReplayConfig)
    pending: list[PendingMakerOrder] = field(default_factory=list)
    filled_intents: list[TradeIntent] = field(default_factory=list)
    fill_rows: list[dict[str, Any]] = field(default_factory=list)
    submitted: int = 0
    expired: int = 0
    rejected: int = 0
    partial_fills: int = 0
    cancelled: int = 0
    _next_order_id: int = 1

    def submit(self, intent: TradeIntent) -> ExecutionResult:
        if intent.intent.upper() != "BUY":
            self.rejected += 1
            return ExecutionResult(
                status="maker_rejected_unsupported_intent",
                intent=intent,
                detail={"error": "PendingMakerReplay only supports BUY intents"},
            )
        same_market = [order for order in self.pending if order.intent.market_slug == intent.market_slug]
        if len(same_market) >= self.config.max_open_orders_per_market:
            self.rejected += 1
            return ExecutionResult(status="maker_rejected_open_order_limit", intent=intent, detail={"open_orders": len(same_market)})
        ttl = self.ttl_for_intent(intent)
        expires_ts = int(intent.sampled_ts) + ttl
        order = PendingMakerOrder(
            order_id=f"maker-{self._next_order_id}",
            intent=intent,
            remaining_usdc=float(intent.notional_usdc),
            expires_ts=expires_ts,
            submitted_ts=int(intent.sampled_ts),
        )
        self._next_order_id += 1
        self.pending.append(order)
        self.submitted += 1
        return ExecutionResult(
            status="maker_pending",
            intent=intent,
            detail={
                "order_id": order.order_id,
                "quote_price": intent.expected_price,
                "remaining_usdc": intent.notional_usdc,
                "ttl_sec": ttl,
                "expires_ts": expires_ts,
            },
        )

    def ttl_for_intent(self, intent: TradeIntent) -> int:
        source = book_fill_source(intent.features)
        if source == "maker_rebalance_quote":
            return max(1, int(round(self.config.order_ttl_sec * self.config.rebalance_ttl_multiplier)))
        if intent.features.get("deficit_side") not in {None, intent.outcome}:
            return max(1, int(round(self.config.order_ttl_sec * self.config.excess_ttl_multiplier)))
        return int(self.config.order_ttl_sec)

    def expire_before(self, ts: int) -> list[PendingMakerOrder]:
        expired: list[PendingMakerOrder] = []
        kept: list[PendingMakerOrder] = []
        for order in self.pending:
            if order.expires_ts < ts and order.remaining_usdc > 1e-9:
                expired.append(order)
                self.expired += 1
            else:
                kept.append(order)
        self.pending = kept
        return expired

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

    def cancel_orders(self, order_ids: set[str], *, reason: str, cancelled_ts: int, detail: dict[str, Any] | None = None) -> list[MakerCancel]:
        if not order_ids:
            return []
        cancelled: list[MakerCancel] = []
        kept: list[PendingMakerOrder] = []
        for order in self.pending:
            if order.order_id in order_ids and order.remaining_usdc > 1e-9:
                cancelled.append(MakerCancel(order=order, reason=reason, cancelled_ts=int(cancelled_ts), detail=dict(detail or {})))
                self.cancelled += 1
            else:
                kept.append(order)
        self.pending = kept
        return cancelled

    def process_trade(self, trade: dict[str, Any], *, expire_first: bool = True) -> list[MakerFill]:
        ts = int(trade.get("exchange_ts") or 0)
        if expire_first:
            self.expire_before(ts)
        market_slug = str(trade.get("market_slug") or "")
        outcome = str(trade.get("outcome") or "").capitalize()
        price = float(trade.get("price") or 0.0)
        trade_usdc = float(trade.get("usdc") or 0.0)
        if not market_slug or outcome not in {"Up", "Down"} or price <= 0 or trade_usdc <= 0:
            return []
        fills: list[MakerFill] = []
        for order in list(self.pending):
            intent = order.intent
            if ts < order.submitted_ts:
                continue
            if ts > order.expires_ts:
                continue
            if intent.market_slug != market_slug or intent.outcome != outcome:
                continue
            if price > intent.expected_price + 1e-9:
                continue
            fill_rate = max(0.0, self.config.fill_rate)
            if book_fill_source(intent.features) == "maker_rebalance_quote":
                fill_rate *= max(0.0, self.config.rebalance_fill_multiplier)
            fill_usdc = min(order.remaining_usdc, trade_usdc * fill_rate)
            if fill_usdc <= 1e-9:
                continue
            was_partial = fill_usdc + 1e-9 < order.remaining_usdc
            order.remaining_usdc -= fill_usdc
            order.filled_usdc += fill_usdc
            if was_partial:
                self.partial_fills += 1
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
                    "maker_order_id": order.order_id,
                    "maker_parent_sampled_ts": intent.sampled_ts,
                    "maker_touch_trade_price": price,
                    "maker_touch_trade_usdc": trade_usdc,
                    "maker_fill_rate": self.config.fill_rate,
                },
            )
            maker_fill = MakerFill(order=order, intent=fill_intent, touch_trade=dict(trade), remaining_usdc=order.remaining_usdc)
            fills.append(maker_fill)
            self.filled_intents.append(fill_intent)
            self.fill_rows.append(maker_fill.to_execution_row())
            if order.remaining_usdc <= 1e-9:
                self.pending.remove(order)
        return fills

    def settle(self, intent: TradeIntent) -> ExecutionResult:
        return PaperExecutionAdapter(self.winning_sides).submit(intent)
