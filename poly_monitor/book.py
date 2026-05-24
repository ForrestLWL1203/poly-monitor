from __future__ import annotations

import math
from typing import Any, Iterable


def compact_float(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return round(number, digits)


def fill_for_notional(levels: Iterable[tuple[float, float]], target_notional: float) -> dict[str, Any]:
    remaining = float(target_notional)
    spent = 0.0
    shares = 0.0
    limit_price: float | None = None
    for price_raw, size_raw in levels:
        price = float(price_raw)
        size = float(size_raw)
        if price <= 0 or size <= 0 or remaining <= 0:
            continue
        take_notional = min(remaining, price * size)
        spent += take_notional
        shares += take_notional / price
        remaining -= take_notional
        limit_price = price
        if remaining <= 1e-9:
            break
    return {
        "ok": spent >= float(target_notional) - 1e-9,
        "avg": compact_float(spent / shares if shares else None),
        "limit": compact_float(limit_price),
        "filled_usdc": compact_float(spent, 4),
    }


def token_book_summary(
    *,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    book_age_ms: int | None,
    targets: Iterable[float] = (5.0, 25.0, 100.0),
) -> dict[str, Any]:
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    return {
        "bid": compact_float(best_bid),
        "ask": compact_float(best_ask),
        "spread": compact_float(best_ask - best_bid if best_bid is not None and best_ask is not None else None),
        "book_age_ms": book_age_ms,
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "ask_depth_usdc": compact_float(sum(price * size for price, size in asks), 4),
        "bid_depth_usdc": compact_float(sum(price * size for price, size in bids), 4),
        "targets": {f"{target:g}": fill_for_notional(asks, float(target)) for target in targets},
    }
