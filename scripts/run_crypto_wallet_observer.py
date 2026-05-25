#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.observer import CryptoWalletObserver, ObserverConfig


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
    parser.add_argument("--inactive-wallet-ttl-hours", type=float, default=48.0)
    parser.add_argument("--max-non-candidate-wallets", type=int, default=100)
    parser.add_argument("--report-refresh-sec", type=float, default=60.0)
    parser.add_argument("--book-max-age-sec", type=float, default=3.0)
    parser.add_argument("--window-refresh-sec", type=float, default=15.0)
    parser.add_argument("--open-price-refresh-sec", type=float, default=5.0)
    parser.add_argument("--settlement-check-sec", type=float, default=30.0)
    parser.add_argument("--raw-cleanup-interval-hours", type=float, default=1.0)
    parser.add_argument("--context-snapshot-cooldown-sec", type=float, default=15.0)
    parser.add_argument("--open-price-min-age-sec", type=float, default=5.0)
    parser.add_argument("--settlement-delay-sec", type=float, default=150.0)
    parser.add_argument("--settlement-retry-sec", type=float, default=30.0)
    parser.add_argument("--max-active-candidates", type=int, default=15)
    parser.add_argument("--max-dormant-candidates", type=int, default=10)
    parser.add_argument("--max-archive-candidates", type=int, default=100)
    parser.add_argument("--archive-revival-min-trades-24h", type=int, default=100)
    parser.add_argument("--archive-revival-min-markets-24h", type=int, default=20)
    parser.add_argument("--archive-revival-cooldown-sec", type=float, default=300.0)
    parser.add_argument("--watchlist-activity-poll-sec", type=float, default=180.0)
    parser.add_argument("--watchlist-activity-lookback-sec", type=int, default=6 * 3600)
    parser.add_argument("--watchlist-activity-safety-window-sec", type=int, default=300)
    parser.add_argument("--watchlist-activity-pages", type=int, default=2)
    parser.add_argument("--watchlist-activity-retention-days", type=int, default=30)
    parser.add_argument("--non-watchlist-activity-retention-days", type=int, default=7)
    parser.add_argument("--context-retention-days", type=int, default=30)
    parser.add_argument("--market-state-retention-days", type=int, default=7)
    parser.add_argument("--strategy-archive-interval-hours", type=float, default=6.0)
    parser.add_argument("--market-state-sample-sec", type=float, default=5.0)
    parser.add_argument("--market-state-terminal-sample-sec", type=float, default=2.0)
    parser.add_argument("--market-state-terminal-window-sec", type=float, default=60.0)
    parser.add_argument("--market-state-heartbeat-sec", type=float, default=15.0)
    parser.add_argument("--watched-market-state-sample-sec", type=float, default=1.0)
    parser.add_argument("--watched-market-state-terminal-sample-sec", type=float, default=1.0)
    parser.add_argument("--watched-market-state-heartbeat-sec", type=float, default=1.0)
    parser.add_argument("--watchlist-market-capture-after-end-sec", type=float, default=1800.0)
    parser.add_argument("--max-context-writes-per-cycle", type=int, default=200)
    parser.add_argument("--market-trade-pages", type=int, default=1)
    parser.add_argument("--watched-market-trade-pages", type=int, default=2)
    parser.add_argument("--market-trade-rate-limit-backoff-sec", type=float, default=60.0)
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
        window_refresh_sec=args.window_refresh_sec,
        open_price_refresh_sec=args.open_price_refresh_sec,
        settlement_check_sec=args.settlement_check_sec,
        raw_cleanup_interval_hours=args.raw_cleanup_interval_hours,
        context_snapshot_cooldown_sec=args.context_snapshot_cooldown_sec,
        open_price_min_age_sec=args.open_price_min_age_sec,
        settlement_delay_sec=args.settlement_delay_sec,
        settlement_retry_sec=args.settlement_retry_sec,
        max_active_candidates=args.max_active_candidates,
        max_dormant_candidates=args.max_dormant_candidates,
        max_archive_candidates=args.max_archive_candidates,
        archive_revival_min_trades_24h=args.archive_revival_min_trades_24h,
        archive_revival_min_markets_24h=args.archive_revival_min_markets_24h,
        archive_revival_cooldown_sec=args.archive_revival_cooldown_sec,
        watchlist_activity_poll_sec=args.watchlist_activity_poll_sec,
        watchlist_activity_lookback_sec=args.watchlist_activity_lookback_sec,
        watchlist_activity_safety_window_sec=args.watchlist_activity_safety_window_sec,
        watchlist_activity_pages=args.watchlist_activity_pages,
        watchlist_activity_retention_days=args.watchlist_activity_retention_days,
        non_watchlist_activity_retention_days=args.non_watchlist_activity_retention_days,
        context_retention_days=args.context_retention_days,
        market_state_retention_days=args.market_state_retention_days,
        strategy_archive_interval_hours=args.strategy_archive_interval_hours,
        market_state_sample_sec=args.market_state_sample_sec,
        market_state_terminal_sample_sec=args.market_state_terminal_sample_sec,
        market_state_terminal_window_sec=args.market_state_terminal_window_sec,
        market_state_heartbeat_sec=args.market_state_heartbeat_sec,
        watched_market_state_sample_sec=args.watched_market_state_sample_sec,
        watched_market_state_terminal_sample_sec=args.watched_market_state_terminal_sample_sec,
        watched_market_state_heartbeat_sec=args.watched_market_state_heartbeat_sec,
        watchlist_market_capture_after_end_sec=args.watchlist_market_capture_after_end_sec,
        max_context_writes_per_cycle=args.max_context_writes_per_cycle,
        market_trade_pages=args.market_trade_pages,
        watched_market_trade_pages=args.watched_market_trade_pages,
        market_trade_rate_limit_backoff_sec=args.market_trade_rate_limit_backoff_sec,
    )
    observer = CryptoWalletObserver(config)
    return await observer.run()


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
