#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.strategy_live import LivePaperEnvironment
from poly_monitor.strategy_runner import StrategyRunner, StrategyRunnerConfig
from poly_monitor.strategy_runtime import PaperExecutionAdapter, RejectingLiveExecutionAdapter, strategy_from_name


def _parse_checkpoints(value: str) -> tuple[int, ...]:
    checkpoints = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not checkpoints:
        raise argparse.ArgumentTypeError("at least one checkpoint is required")
    return checkpoints


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(item.strip().upper() for item in value.split(",") if item.strip())
    if not symbols:
        raise argparse.ArgumentTypeError("at least one symbol is required")
    return symbols


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an independent live paper strategy plugin.")
    parser.add_argument("--strategy", choices=("d950_path_v0", "wallet_path_v0", "wallet_path"), default="d950_path_v0")
    parser.add_argument("--wallet", default="strategy")
    parser.add_argument("--symbols", type=_parse_symbols, default=("BTC",))
    parser.add_argument("--mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--jsonl", type=Path, default=Path("data/paper/d950_path_v0/run.jsonl"))
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--notional", type=float, default=25.0)
    parser.add_argument("--bias-threshold", type=float, default=25.0)
    parser.add_argument("--max-price", type=float, default=0.95)
    parser.add_argument("--checkpoints", type=_parse_checkpoints)
    parser.add_argument("--min-reference-delta", type=float, default=0.0)
    parser.add_argument("--target-pair-notional", type=float, default=25.0)
    parser.add_argument("--target-pair-shares", type=float, help="Per-side target shares by window end; overrides --target-pair-notional for wallet_path_v0")
    parser.add_argument("--max-pair-cost", type=float, default=0.99)
    parser.add_argument("--max-unpaired-price", type=float, default=0.6)
    parser.add_argument("--max-inventory-imbalance-ratio", type=float, default=0.05)
    parser.add_argument("--min-order-usdc", type=float, default=1.0)
    parser.add_argument("--execution-style", choices=("maker", "taker"), default="maker")
    return parser


async def async_main() -> int:
    args = build_parser().parse_args()
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
        min_order_usdc=args.min_order_usdc,
        execution_style=args.execution_style,
    )
    adapter = RejectingLiveExecutionAdapter() if args.mode == "live" else PaperExecutionAdapter()
    runner = StrategyRunner(
        StrategyRunnerConfig(output_path=args.jsonl, mode=args.mode),
        strategy=strategy,
        execution_adapter=adapter,
    )
    env = LivePaperEnvironment(symbols=args.symbols)
    return await runner.run_live(env, seconds=args.seconds, poll_sec=args.poll_sec)


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
