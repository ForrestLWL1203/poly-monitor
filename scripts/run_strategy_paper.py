#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.crypto_price import fetch_crypto_price_api
from poly_monitor.data_api import AsyncDataApiClient, normalize_trade
from poly_monitor.maker_paper import PendingMakerReplayConfig
from poly_monitor.strategy_live import LivePaperEnvironment
from poly_monitor.strategy_runner import LivePaperRunConfig, LivePaperStrategyRunner
from poly_monitor.strategy_runtime import strategy_from_name
from poly_monitor.strategies import STRATEGY_CHOICES


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
    parser.add_argument("--strategy", choices=STRATEGY_CHOICES, default="d950_terminal_bias_v0")
    parser.add_argument("--wallet", default="strategy")
    parser.add_argument("--symbols", type=_parse_symbols, default=("BTC",))
    parser.add_argument("--mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--jsonl", type=Path)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--run-id")
    parser.add_argument("--start-sampled-ts", type=int, default=0)
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--market-trade-poll-sec", type=float, default=1.5)
    parser.add_argument("--settlement-delay-sec", type=float, default=150.0)
    parser.add_argument("--settlement-retry-sec", type=float, default=30.0)
    parser.add_argument("--notional", type=float)
    parser.add_argument("--bias-threshold", type=float)
    parser.add_argument("--max-price", type=float)
    parser.add_argument("--checkpoints", type=_parse_checkpoints)
    parser.add_argument("--min-reference-delta", type=float, default=0.0)
    parser.add_argument("--target-pair-notional", type=float)
    parser.add_argument("--target-pair-shares", type=float, help="Per-side target shares by window end; overrides --target-pair-notional for wallet_path_v0")
    parser.add_argument("--max-pair-cost", type=float)
    parser.add_argument("--max-unpaired-price", type=float)
    parser.add_argument("--max-inventory-imbalance-ratio", type=float)
    parser.add_argument("--min-order-usdc", type=float)
    parser.add_argument("--max-quote-spread", type=float)
    parser.add_argument("--max-quote-book-age-ms", type=float)
    parser.add_argument("--min-quote-bid-depth-usdc", type=float)
    parser.add_argument("--execution-style", choices=("maker", "taker"))
    parser.add_argument("--maker-fill-rate", type=float, default=0.1)
    parser.add_argument("--maker-order-ttl-sec", type=int, default=30)
    parser.add_argument("--maker-expiry-grace-sec", type=float)
    parser.add_argument("--maker-max-open-orders-per-market", type=int, default=20)
    parser.add_argument("--maker-rebalance-fill-multiplier", type=float, default=2.0)
    parser.add_argument("--maker-rebalance-ttl-multiplier", type=float, default=1.5)
    parser.add_argument("--maker-excess-ttl-multiplier", type=float, default=0.5)
    parser.add_argument("--terminal-bias-start-sec", type=int)
    parser.add_argument("--terminal-strong-start-sec", type=int)
    parser.add_argument("--terminal-max-price", type=float)
    parser.add_argument("--bias-score-threshold", type=int)
    parser.add_argument("--min-reference-move-bps", type=float)
    parser.add_argument("--min-recent-move-bps", type=float)
    parser.add_argument("--terminal-favorite-bid", type=float)
    parser.add_argument("--terminal-favorite-mid", type=float)
    return parser


async def async_main() -> int:
    args = build_parser().parse_args()
    if args.mode != "paper":
        raise SystemExit("run_strategy_paper.py is read-only paper mode; live order submission is not implemented")
    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = args.jsonl.parent if args.jsonl else args.data_dir / "paper_live" / args.strategy / run_id
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
        max_quote_spread=args.max_quote_spread,
        max_quote_book_age_ms=args.max_quote_book_age_ms,
        min_quote_bid_depth_usdc=args.min_quote_bid_depth_usdc,
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
    runner = LivePaperStrategyRunner(
        LivePaperRunConfig(
            run_dir=run_dir,
            run_id=run_id,
            mode="paper",
            start_sampled_ts=args.start_sampled_ts,
            expiry_grace_sec=args.maker_expiry_grace_sec if args.maker_expiry_grace_sec is not None else args.market_trade_poll_sec + 2.0,
            maker=PendingMakerReplayConfig(
                fill_rate=args.maker_fill_rate,
                order_ttl_sec=args.maker_order_ttl_sec,
                max_open_orders_per_market=args.maker_max_open_orders_per_market,
                rebalance_fill_multiplier=args.maker_rebalance_fill_multiplier,
                rebalance_ttl_multiplier=args.maker_rebalance_ttl_multiplier,
                excess_ttl_multiplier=args.maker_excess_ttl_multiplier,
            ),
        ),
        strategy=strategy,
    )
    env = LivePaperEnvironment(symbols=args.symbols)
    data_api = AsyncDataApiClient()
    last_trade_poll = 0.0
    known_windows = {}
    pending_settlements = {}
    settlement_next_retry = {}
    settled = set()
    poll_state = {}
    deadline = time.monotonic() + args.seconds if args.seconds is not None else None
    await env.start()
    try:
        while deadline is None or time.monotonic() < deadline:
            now = dt.datetime.now(dt.timezone.utc)
            for window in env.windows.values():
                known_windows[window.slug] = window
            before = dict(env.windows)
            await env.roll_window_if_needed(now=now)
            for symbol, window in before.items():
                if symbol not in env.windows or env.windows[symbol].slug != window.slug:
                    pending_settlements[window.slug] = window
            snapshots = env.snapshot(now=now)
            runner.tick(snapshots)
            if time.monotonic() - last_trade_poll >= args.market_trade_poll_sec:
                last_trade_poll = time.monotonic()
                trades = []
                for window in env.windows.values():
                    try:
                        raw_rows = await data_api.fetch_market_trades(window.condition_id, limit=100, pages=1)
                    except Exception:
                        raw_rows = []
                    observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
                    normalized = [normalize_trade(row, symbol=window.symbol, observed_at=observed_at) for row in raw_rows]
                    trades.extend(normalized)
                    last_ts = max((int(row.get("exchange_ts") or 0) for row in normalized), default=None)
                    poll_state[window.slug] = {
                        "condition_id": window.condition_id,
                        "last_poll_at": observed_at,
                        "last_trade_ts": last_ts,
                        "rows_seen": len(normalized),
                    }
                runner.process_market_trades(trades)
            due_settlements = [
                (slug, window)
                for slug, window in pending_settlements.items()
                if slug not in settled and now >= window.end_time + dt.timedelta(seconds=args.settlement_delay_sec)
                and now >= settlement_next_retry.get(slug, now)
            ]
            for slug, window in due_settlements:
                data = await asyncio.to_thread(fetch_crypto_price_api, window)
                if data and data.get("openPrice") is not None and data.get("closePrice") is not None:
                    winning_side = "Up" if float(data["closePrice"]) >= float(data["openPrice"]) else "Down"
                    runner.settle_market(slug, winning_side)
                    settled.add(slug)
                    settlement_next_retry.pop(slug, None)
                else:
                    settlement_next_retry[slug] = now + dt.timedelta(seconds=args.settlement_retry_sec)
            active_windows = [
                {
                    "symbol": window.symbol,
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "up_token": window.up_token,
                    "down_token": window.down_token,
                    "window_start_ts": window.start_epoch,
                    "window_end_ts": window.end_epoch,
                }
                for window in env.windows.values()
            ]
            diagnostics = env.stream.diagnostics(reset_counts=False) if hasattr(env.stream, "diagnostics") else {}
            runner.write_state(active_windows=active_windows, stream_diagnostics=diagnostics, poll_state=poll_state)
            await asyncio.sleep(max(0.1, args.poll_sec))
    finally:
        await data_api.close()
        await env.close()
    print(str(run_dir))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
