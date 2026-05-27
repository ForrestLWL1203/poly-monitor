#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_strategy_paper import build_parser
from poly_monitor.strategy_live import LivePaperEnvironment
from poly_monitor.strategy_runner import StrategyRunner, StrategyRunnerConfig
from poly_monitor.strategy_runtime import PaperExecutionAdapter, RejectingLiveExecutionAdapter, strategy_from_name


async def async_main() -> int:
    parser = build_parser()
    parser.description = "Compatibility wrapper for run_strategy_paper.py; no observer.sqlite is read."
    args = parser.parse_args()
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
    return await runner.run_live(LivePaperEnvironment(symbols=args.symbols), seconds=args.seconds, poll_sec=args.poll_sec)


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
