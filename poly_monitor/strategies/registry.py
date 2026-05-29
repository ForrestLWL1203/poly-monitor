from __future__ import annotations

from typing import Any

from poly_monitor.path_strategy import PathStrategyConfig, WalletPathStrategy
from poly_monitor.strategy_runtime import _coalesce

from .pair_cost_inventory import X32PairCostInventoryStrategy, x32_default_config
from .parity_terminal_bias import ParityTerminalBiasStrategy
from .terminal_bias import D950MarketPathStrategy

STRATEGY_CHOICES = (
    "d950_path_v0",
    "d950_terminal_bias_v0",
    "wallet_path_v0",
    "wallet_path",
    "x32_pair_cost_inventory_v0",
    "parity_terminal_bias_v0",
)


def _base_config(name: str, **kwargs: Any) -> PathStrategyConfig:
    normalized = str(name)
    checkpoints = kwargs.get("checkpoints")
    if checkpoints is None:
        checkpoints = (1,) if normalized in {"wallet_path", "wallet_path_v0", "x32_pair_cost_inventory_v0"} else (120, 180, 240)
    return PathStrategyConfig(
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
        one_trade_per_market=bool(_coalesce(kwargs.get("one_trade_per_market"), normalized in {"d950_path_v0", "d950_terminal_bias_v0"})),
        terminal_bias_start_sec=int(_coalesce(kwargs.get("terminal_bias_start_sec"), 180)),
        terminal_strong_start_sec=int(_coalesce(kwargs.get("terminal_strong_start_sec"), 240)),
        terminal_max_price=float(_coalesce(kwargs.get("terminal_max_price"), 0.95)),
        bias_score_threshold=int(_coalesce(kwargs.get("bias_score_threshold"), 3)),
        min_reference_move_bps=float(_coalesce(kwargs.get("min_reference_move_bps"), 1.0)),
        min_recent_move_bps=float(_coalesce(kwargs.get("min_recent_move_bps"), 0.5)),
        terminal_favorite_bid=float(_coalesce(kwargs.get("terminal_favorite_bid"), 0.85)),
        terminal_favorite_mid=float(_coalesce(kwargs.get("terminal_favorite_mid"), 0.80)),
    )


def _x32_config(**kwargs: Any) -> PathStrategyConfig:
    wallet = str(_coalesce(kwargs.get("wallet"), "x32_pair_cost_inventory_v0"))
    checkpoints = kwargs.get("checkpoints")
    return x32_default_config(
        wallet,
        checkpoints=tuple(checkpoints) if checkpoints is not None else None,
        notional_usdc=float(kwargs["notional_usdc"]) if kwargs.get("notional_usdc") is not None else None,
        max_price=float(kwargs["max_price"]) if kwargs.get("max_price") is not None else None,
        target_pair_shares_per_side=(
            float(kwargs["target_pair_shares_per_side"])
            if kwargs.get("target_pair_shares_per_side") is not None
            else None
        ),
        target_pair_notional_usdc=(
            float(kwargs["target_pair_notional_usdc"])
            if kwargs.get("target_pair_notional_usdc") is not None
            else None
        ),
        max_pair_cost=float(kwargs["max_pair_cost"]) if kwargs.get("max_pair_cost") is not None else None,
        max_unpaired_price=float(kwargs["max_unpaired_price"]) if kwargs.get("max_unpaired_price") is not None else None,
        max_inventory_imbalance_ratio=(
            float(kwargs["max_inventory_imbalance_ratio"])
            if kwargs.get("max_inventory_imbalance_ratio") is not None
            else None
        ),
        early_inventory_imbalance_ratio=(
            float(kwargs["early_inventory_imbalance_ratio"])
            if kwargs.get("early_inventory_imbalance_ratio") is not None
            else None
        ),
        mid_inventory_imbalance_ratio=(
            float(kwargs["mid_inventory_imbalance_ratio"])
            if kwargs.get("mid_inventory_imbalance_ratio") is not None
            else None
        ),
        late_inventory_imbalance_ratio=(
            float(kwargs["late_inventory_imbalance_ratio"])
            if kwargs.get("late_inventory_imbalance_ratio") is not None
            else None
        ),
        final_inventory_imbalance_ratio=(
            float(kwargs["final_inventory_imbalance_ratio"])
            if kwargs.get("final_inventory_imbalance_ratio") is not None
            else None
        ),
        rebalance_start_sec=int(kwargs["rebalance_start_sec"]) if kwargs.get("rebalance_start_sec") is not None else None,
        maker_rebalance_ticks=int(kwargs["maker_rebalance_ticks"]) if kwargs.get("maker_rebalance_ticks") is not None else None,
        tick_size=float(kwargs["tick_size"]) if kwargs.get("tick_size") is not None else None,
        min_order_usdc=float(kwargs["min_order_usdc"]) if kwargs.get("min_order_usdc") is not None else None,
        max_quote_spread=float(kwargs["max_quote_spread"]) if kwargs.get("max_quote_spread") is not None else None,
        max_quote_book_age_ms=(
            float(kwargs["max_quote_book_age_ms"])
            if kwargs.get("max_quote_book_age_ms") is not None
            else None
        ),
        min_quote_bid_depth_usdc=(
            float(kwargs["min_quote_bid_depth_usdc"])
            if kwargs.get("min_quote_bid_depth_usdc") is not None
            else None
        ),
        dual_build_max_abs_bid_diff=(
            float(kwargs["dual_build_max_abs_bid_diff"])
            if kwargs.get("dual_build_max_abs_bid_diff") is not None
            else None
        ) if "dual_build_max_abs_bid_diff" in kwargs else "__use_default__",
        build_phase_until_sec=int(kwargs["build_phase_until_sec"]) if kwargs.get("build_phase_until_sec") is not None else None,
        execution_style=str(kwargs["execution_style"]) if kwargs.get("execution_style") is not None else None,
        one_trade_per_market=bool(kwargs["one_trade_per_market"]) if kwargs.get("one_trade_per_market") is not None else None,
    )


def strategy_from_name(name: str, **kwargs: Any):
    normalized = str(name)
    if normalized == "x32_pair_cost_inventory_v0":
        return X32PairCostInventoryStrategy(_x32_config(**kwargs))
    config = _base_config(normalized, **kwargs)
    if normalized in {"d950_path_v0", "d950_terminal_bias_v0"}:
        return D950MarketPathStrategy(config, min_reference_delta=float(_coalesce(kwargs.get("min_reference_delta"), 0.0)))
    if normalized in {"wallet_path", "wallet_path_v0"}:
        return WalletPathStrategy(config)
    if normalized == "parity_terminal_bias_v0":
        return ParityTerminalBiasStrategy(config)
    raise ValueError(f"unknown strategy: {name}")
