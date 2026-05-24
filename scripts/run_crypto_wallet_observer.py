#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.observer import CryptoWalletObserver, ObserverConfig, parse_seed_wallets

DEFAULT_SEEDS = ""


def parse_symbols(raw: str) -> tuple[str, ...]:
    symbols = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    unsupported = [symbol for symbol in symbols if symbol not in {"BTC", "ETH"}]
    if unsupported:
        raise argparse.ArgumentTypeError(f"unsupported symbols: {','.join(unsupported)}")
    return symbols or ("BTC", "ETH")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only BTC/ETH 5m Polymarket high-frequency wallet observer.")
    parser.add_argument("--symbols", type=parse_symbols, default=("BTC", "ETH"))
    parser.add_argument("--poll-sec", type=float, default=2.0)
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--max-candidates", type=int, default=15)
    parser.add_argument("--min-trade-usdc", type=float, default=1.0)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--raw-retention-days", type=int, default=3)
    parser.add_argument("--score-refresh-sec", type=float, default=60.0)
    parser.add_argument("--score-wallets-per-cycle", type=int, default=2)
    parser.add_argument("--score-wallet-pool-limit", type=int, default=50)
    parser.add_argument("--cleanup-interval-hours", type=float, default=6.0)
    parser.add_argument("--inactive-wallet-ttl-hours", type=float, default=12.0)
    parser.add_argument("--max-non-candidate-wallets", type=int, default=100)
    parser.add_argument("--report-refresh-sec", type=float, default=60.0)
    parser.add_argument("--book-max-age-sec", type=float, default=3.0)
    parser.add_argument("--open-price-min-age-sec", type=float, default=5.0)
    parser.add_argument("--settlement-delay-sec", type=float, default=90.0)
    parser.add_argument("--max-active-candidates", type=int, default=15)
    parser.add_argument("--max-dormant-candidates", type=int, default=10)
    parser.add_argument("--max-archive-candidates", type=int, default=0)
    parser.add_argument("--seed-wallet", default=DEFAULT_SEEDS, help="Comma-separated label=0xwallet entries.")
    return parser


async def async_main() -> int:
    args = build_parser().parse_args()
    config = ObserverConfig(
        symbols=args.symbols,
        poll_sec=args.poll_sec,
        seconds=args.seconds,
        max_candidates=args.max_candidates,
        min_trade_usdc=args.min_trade_usdc,
        data_dir=args.data_dir,
        raw_retention_days=args.raw_retention_days,
        score_refresh_sec=args.score_refresh_sec,
        score_wallets_per_cycle=args.score_wallets_per_cycle,
        score_wallet_pool_limit=args.score_wallet_pool_limit,
        cleanup_interval_hours=args.cleanup_interval_hours,
        inactive_wallet_ttl_hours=args.inactive_wallet_ttl_hours,
        max_non_candidate_wallets=args.max_non_candidate_wallets,
        report_refresh_sec=args.report_refresh_sec,
        book_max_age_sec=args.book_max_age_sec,
        open_price_min_age_sec=args.open_price_min_age_sec,
        settlement_delay_sec=args.settlement_delay_sec,
        max_active_candidates=args.max_active_candidates,
        max_dormant_candidates=args.max_dormant_candidates,
        max_archive_candidates=args.max_archive_candidates,
    )
    observer = CryptoWalletObserver(config, parse_seed_wallets(args.seed_wallet))
    return await observer.run()


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
