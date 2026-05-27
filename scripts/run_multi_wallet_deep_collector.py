#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.deep_collection import MultiWalletDeepCollector, MultiWalletDeepCollectorConfig, normalize_wallet, read_deep_wallets, write_deep_wallets


def parse_wallets(raw: str) -> tuple[str, ...]:
    return tuple(normalize_wallet(item) for item in raw.split(",") if normalize_wallet(item))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one read-only multi-wallet deep collector with shared market sampling.")
    parser.add_argument("--wallet", action="append", default=[], help="Wallet address to deep collect. Can be repeated.")
    parser.add_argument("--wallets", type=parse_wallets, default=(), help="Comma-separated wallet addresses.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--sample-sec", type=float, default=1.0)
    parser.add_argument("--book-depth-levels", type=int, default=3)
    parser.add_argument("--activity-poll-sec", type=float, default=1.5)
    parser.add_argument("--activity-lookback-sec", type=int, default=600)
    parser.add_argument("--activity-pages", type=int, default=2)
    parser.add_argument("--max-concurrent-activity-polls", type=int, default=4)
    parser.add_argument("--symbols", default="BTC,ETH")
    parser.add_argument("--replace-list", action="store_true", help="Replace the persisted deep-collection wallet list with the CLI wallets.")
    return parser


def _symbols(raw: str) -> tuple[str, ...]:
    symbols = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    unsupported = [symbol for symbol in symbols if symbol not in {"BTC", "ETH"}]
    if unsupported:
        raise ValueError(f"unsupported symbols: {','.join(unsupported)}")
    return symbols or ("BTC", "ETH")


async def async_main() -> int:
    args = build_parser().parse_args()
    cli_wallets = tuple(normalize_wallet(wallet) for wallet in [*args.wallet, *args.wallets] if normalize_wallet(wallet))
    existing_wallets = tuple(read_deep_wallets(args.data_dir))
    if cli_wallets and args.replace_list:
        wallets = cli_wallets
        write_deep_wallets(args.data_dir, list(wallets))
    elif cli_wallets:
        wallets = tuple(dict.fromkeys([*existing_wallets, *cli_wallets]))
        write_deep_wallets(args.data_dir, list(wallets))
    else:
        wallets = tuple(read_deep_wallets(args.data_dir))
    if not wallets:
        raise SystemExit("at least one --wallet or --wallets address is required")
    collector = MultiWalletDeepCollector(
        MultiWalletDeepCollectorConfig(
            wallets=wallets,
            data_dir=args.data_dir,
            symbols=_symbols(args.symbols),
            sample_sec=args.sample_sec,
            book_depth_levels=args.book_depth_levels,
            activity_poll_sec=args.activity_poll_sec,
            activity_lookback_sec=args.activity_lookback_sec,
            activity_pages=args.activity_pages,
            max_concurrent_activity_polls=args.max_concurrent_activity_polls,
        )
    )
    return await collector.run(seconds=args.seconds)


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
