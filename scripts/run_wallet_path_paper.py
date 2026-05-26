#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.path_paper_runner import PathPaperRunner, PathPaperRunnerConfig


def _parse_checkpoints(value: str) -> tuple[int, ...]:
    checkpoints = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not checkpoints:
        raise argparse.ArgumentTypeError("at least one checkpoint is required")
    return checkpoints


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run live read-only wallet path paper strategy.")
    parser.add_argument("--wallet", required=True, help="Target wallet address")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--seconds", type=float, help="Optional finite runtime for tests/manual probes")
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--notional", type=float, default=25.0)
    parser.add_argument("--bias-threshold", type=float, default=25.0)
    parser.add_argument("--max-price", type=float, default=0.95)
    parser.add_argument("--checkpoints", type=_parse_checkpoints, default=(120, 180, 240))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    runner = PathPaperRunner(
        PathPaperRunnerConfig(
            wallet=args.wallet,
            data_dir=args.data_dir,
            poll_sec=args.poll_sec,
            checkpoints=args.checkpoints,
            notional_usdc=args.notional,
            first_bias_min_usdc=args.bias_threshold,
            max_price=args.max_price,
        )
    )
    return runner.run(seconds=args.seconds)


if __name__ == "__main__":
    raise SystemExit(main())
