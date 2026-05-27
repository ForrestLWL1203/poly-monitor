#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.strategy_backtest import DeepExportBacktestEnvironment, PendingMakerReplayConfig, run_strategy_backtest, run_strategy_maker_replay_backtest
from poly_monitor.strategy_runtime import PaperExecutionAdapter, strategy_from_name


def _parse_checkpoints(value: str) -> tuple[int, ...]:
    checkpoints = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not checkpoints:
        raise argparse.ArgumentTypeError("at least one checkpoint is required")
    return checkpoints


def main() -> int:
    parser = argparse.ArgumentParser(description="Compatibility wrapper for run_strategy_backtest.py.")
    parser.add_argument("--zip", required=True, type=Path, help="Path to bundle.complete-windows.zip")
    parser.add_argument("--wallet", required=True, help="Target wallet address")
    parser.add_argument("--out", type=Path, help="Output JSON path")
    parser.add_argument("--strategy", choices=("wallet_path_v0", "wallet_path", "d950_path_v0", "parity_terminal_bias_v0"), default="wallet_path_v0")
    parser.add_argument("--notional", type=float, default=25.0, help="Paper notional per intent")
    parser.add_argument("--bias-threshold", type=float, default=25.0, help="Minimum wallet net Up-Down flow to trigger")
    parser.add_argument("--max-price", type=float, default=0.95, help="Maximum acceptable simulated buy price")
    parser.add_argument("--checkpoints", type=_parse_checkpoints, help="Comma-separated elapsed-second checkpoints")
    parser.add_argument("--min-reference-delta", type=float, default=0.0)
    parser.add_argument("--target-pair-notional", type=float, default=25.0)
    parser.add_argument("--target-pair-shares", type=float, help="Per-side target shares by window end; overrides --target-pair-notional for wallet_path_v0")
    parser.add_argument("--max-pair-cost", type=float, default=0.99)
    parser.add_argument("--max-unpaired-price", type=float, default=0.6)
    parser.add_argument("--max-inventory-imbalance-ratio", type=float, default=0.05)
    parser.add_argument("--min-order-usdc", type=float, default=1.0)
    parser.add_argument("--execution-style", choices=("maker", "taker"), default="maker")
    parser.add_argument("--maker-fill-rate", type=float, default=0.1)
    parser.add_argument("--maker-order-ttl-sec", type=int, default=30)
    parser.add_argument("--maker-max-open-orders-per-market", type=int, default=20)
    parser.add_argument("--maker-rebalance-fill-multiplier", type=float, default=2.0)
    parser.add_argument("--maker-rebalance-ttl-multiplier", type=float, default=1.5)
    parser.add_argument("--maker-excess-ttl-multiplier", type=float, default=0.5)
    parser.add_argument("--rebalance-start-sec", type=int, default=240)
    parser.add_argument("--maker-rebalance-ticks", type=int, default=1)
    parser.add_argument("--terminal-bias-start-sec", type=int, default=180)
    parser.add_argument("--terminal-strong-start-sec", type=int, default=240)
    parser.add_argument("--terminal-max-price", type=float, default=0.95)
    parser.add_argument("--bias-score-threshold", type=int, default=3)
    parser.add_argument("--min-reference-move-bps", type=float, default=1.0)
    parser.add_argument("--min-recent-move-bps", type=float, default=0.5)
    parser.add_argument("--terminal-favorite-bid", type=float, default=0.85)
    parser.add_argument("--terminal-favorite-mid", type=float, default=0.80)
    args = parser.parse_args()

    env = DeepExportBacktestEnvironment(args.zip)
    strategy = strategy_from_name(
        args.strategy,
        wallet=args.wallet,
        checkpoints=args.checkpoints,
        notional_usdc=args.notional,
        bias_threshold=args.bias_threshold,
        max_price=args.max_price,
        min_reference_delta=args.min_reference_delta,
        target_pair_notional_usdc=args.target_pair_notional,
        target_pair_shares_per_side=args.target_pair_shares,
        max_pair_cost=args.max_pair_cost,
        max_unpaired_price=args.max_unpaired_price,
        max_inventory_imbalance_ratio=args.max_inventory_imbalance_ratio,
        rebalance_start_sec=args.rebalance_start_sec,
        maker_rebalance_ticks=args.maker_rebalance_ticks,
        min_order_usdc=args.min_order_usdc,
        execution_style=args.execution_style,
        terminal_bias_start_sec=args.terminal_bias_start_sec,
        terminal_strong_start_sec=args.terminal_strong_start_sec,
        terminal_max_price=args.terminal_max_price,
        bias_score_threshold=args.bias_score_threshold,
        min_reference_move_bps=args.min_reference_move_bps,
        min_recent_move_bps=args.min_recent_move_bps,
        terminal_favorite_bid=args.terminal_favorite_bid,
        terminal_favorite_mid=args.terminal_favorite_mid,
    )
    if args.execution_style == "maker":
        result = run_strategy_maker_replay_backtest(
            strategy,
            env,
            config=PendingMakerReplayConfig(
                fill_rate=args.maker_fill_rate,
                order_ttl_sec=args.maker_order_ttl_sec,
                max_open_orders_per_market=args.maker_max_open_orders_per_market,
                rebalance_fill_multiplier=args.maker_rebalance_fill_multiplier,
                rebalance_ttl_multiplier=args.maker_rebalance_ttl_multiplier,
                excess_ttl_multiplier=args.maker_excess_ttl_multiplier,
            ),
        )
    else:
        result = run_strategy_backtest(strategy, env, PaperExecutionAdapter(env.winning_sides))
    payload = result.to_dict()
    payload["config"] = {
        "wallet": args.wallet.lower(),
        "strategy": args.strategy,
        "notional_usdc": args.notional,
        "bias_threshold": args.bias_threshold,
        "max_price": args.max_price,
        "checkpoints": list(strategy.config.checkpoints) if hasattr(strategy, "config") else args.checkpoints,
        "target_pair_notional_usdc": args.target_pair_notional,
        "target_pair_shares_per_side": args.target_pair_shares,
        "max_pair_cost": args.max_pair_cost,
        "max_unpaired_price": args.max_unpaired_price,
        "max_inventory_imbalance_ratio": args.max_inventory_imbalance_ratio,
        "min_order_usdc": args.min_order_usdc,
        "execution_style": args.execution_style,
        "maker_fill_rate": args.maker_fill_rate,
        "maker_order_ttl_sec": args.maker_order_ttl_sec,
        "maker_max_open_orders_per_market": args.maker_max_open_orders_per_market,
        "maker_rebalance_fill_multiplier": args.maker_rebalance_fill_multiplier,
        "maker_rebalance_ttl_multiplier": args.maker_rebalance_ttl_multiplier,
        "maker_excess_ttl_multiplier": args.maker_excess_ttl_multiplier,
        "rebalance_start_sec": args.rebalance_start_sec,
        "maker_rebalance_ticks": args.maker_rebalance_ticks,
        "terminal_bias_start_sec": args.terminal_bias_start_sec,
        "terminal_strong_start_sec": args.terminal_strong_start_sec,
        "terminal_max_price": args.terminal_max_price,
        "bias_score_threshold": args.bias_score_threshold,
        "min_reference_move_bps": args.min_reference_move_bps,
        "min_recent_move_bps": args.min_recent_move_bps,
        "terminal_favorite_bid": args.terminal_favorite_bid,
        "terminal_favorite_mid": args.terminal_favorite_mid,
        "source_zip": str(args.zip),
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(str(args.out))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
