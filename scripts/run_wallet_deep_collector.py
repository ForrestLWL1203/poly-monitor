#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.deep_collection import WalletDeepCollector, WalletDeepCollectorConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one read-only watchlist wallet deep collector.")
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--sample-sec", type=float, default=1.0)
    parser.add_argument("--book-depth-levels", type=int, default=3)
    parser.add_argument("--activity-poll-sec", type=float, default=1.0)
    parser.add_argument("--activity-lookback-sec", type=int, default=600)
    return parser


async def async_main() -> int:
    args = build_parser().parse_args()
    collector = WalletDeepCollector(
        WalletDeepCollectorConfig(
            wallet=args.wallet,
            data_dir=args.data_dir,
            sample_sec=args.sample_sec,
            book_depth_levels=args.book_depth_levels,
            activity_poll_sec=args.activity_poll_sec,
            activity_lookback_sec=args.activity_lookback_sec,
        )
    )
    return await collector.run(seconds=args.seconds)


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
