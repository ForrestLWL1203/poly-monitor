from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .book import compact_float, token_book_summary
from .clob_stream import ClobBookStream
from .crypto_price import fetch_crypto_price_api
from .data_api import fetch_market_trades, normalize_trade
from .market import MarketSeries, MarketWindow, find_current_or_next_window
from .price_feed import ChainlinkPriceFeed
from .scoring import CandidateThresholds, score_wallet
from .storage import JsonlEventWriter, ObserverStore, cleanup_raw_retention, write_latest_candidates
from .wallet_metrics import build_metrics_from_api


@dataclass(frozen=True)
class ObserverConfig:
    symbols: tuple[str, ...] = ("BTC", "ETH")
    poll_sec: float = 2.0
    seconds: float | None = None
    max_candidates: int = 15
    min_trade_usdc: float = 1.0
    data_dir: Path = Path("data")
    raw_retention_days: int = 7
    score_refresh_sec: float = 60.0
    score_wallets_per_cycle: int = 2
    score_wallet_pool_limit: int = 50
    cleanup_interval_hours: float = 6.0
    inactive_wallet_ttl_hours: float = 12.0
    max_non_candidate_wallets: int = 100
    report_refresh_sec: float = 60.0
    book_max_age_sec: float = 3.0
    open_price_min_age_sec: float = 5.0
    settlement_delay_sec: float = 90.0
    max_active_candidates: int = 15
    max_dormant_candidates: int = 10
    max_archive_candidates: int = 0


def should_persist_score(score, *, is_seed: bool = False, previous_status: str | None = None) -> bool:
    if score.status != "archive_candidate":
        return True
    if is_seed:
        return True
    if previous_status in {"active_candidate", "dormant_candidate"}:
        return True
    return False


def compact_score_event(score) -> dict[str, Any]:
    return {
        "event": "candidate_score",
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "wallet": score.wallet if hasattr(score, "wallet") else score["wallet"],
        "status": score.status if hasattr(score, "status") else score["status"],
        "rank_score": score.rank_score if hasattr(score, "rank_score") else score["rank_score"],
        "reason_count": len(score.reasons if hasattr(score, "reasons") else score.get("reasons", [])),
    }


def parse_seed_wallets(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    seeds: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            label, wallet = item.split("=", 1)
        else:
            label, wallet = item, item
        seeds[wallet.strip().lower()] = label.strip()
    return seeds


def context_snapshot(
    *,
    trade: dict[str, Any],
    window: MarketWindow,
    stream: ClobBookStream,
    feed: ChainlinkPriceFeed | None,
    window_open_reference_price: float | None = None,
    window_close_reference_price: float | None = None,
    targets: tuple[float, ...] = (5.0, 25.0, 100.0),
    max_book_age_sec: float = 3.0,
) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    up_bids, up_asks, up_age = stream.get_book(window.up_token, max_age_sec=max_book_age_sec)
    down_bids, down_asks, down_age = stream.get_book(window.down_token, max_age_sec=max_book_age_sec)
    return {
        "event": "context_snapshot",
        "observed_at": now.isoformat(),
        "wallet": trade["wallet"],
        "symbol": window.symbol,
        "market_slug": window.slug,
        "condition_id": window.condition_id,
        "trade_tx_hash": trade["tx_hash"],
        "trade_outcome": trade["outcome"],
        "trade_price": trade["price"],
        "trade_usdc": trade["usdc"],
        "window_age_sec": compact_float((now - window.start_time).total_seconds(), 3),
        "window_remaining_sec": compact_float((window.end_time - now).total_seconds(), 3),
        "reference_price": compact_float(feed.latest_price if feed else None, 6),
        "window_open_reference_price": compact_float(window_open_reference_price, 6),
        "window_close_reference_price": compact_float(window_close_reference_price, 6),
        "reference_price_age_sec": compact_float(feed.latest_age_sec() if feed else None, 3),
        "reference_return_1s_bps": compact_float(feed.return_bps(1.0) if feed else None, 3),
        "reference_return_3s_bps": compact_float(feed.return_bps(3.0) if feed else None, 3),
        "reference_return_5s_bps": compact_float(feed.return_bps(5.0) if feed else None, 3),
        "reference_return_10s_bps": compact_float(feed.return_bps(10.0) if feed else None, 3),
        "up": token_book_summary(bids=up_bids, asks=up_asks, book_age_ms=up_age, targets=targets),
        "down": token_book_summary(bids=down_bids, asks=down_asks, book_age_ms=down_age, targets=targets),
        "ws": stream.diagnostics(reset_counts=True),
    }


class CryptoWalletObserver:
    def __init__(self, config: ObserverConfig, seeds: dict[str, str]) -> None:
        self.config = config
        self.seeds = seeds
        self.store = ObserverStore(config.data_dir / "state" / "observer.sqlite")
        for wallet, label in seeds.items():
            self.store.add_seed(wallet, label)
        self.writer = JsonlEventWriter(config.data_dir)
        self.windows: dict[str, MarketWindow] = {}
        self.stream = ClobBookStream()
        self.feeds = {symbol: ChainlinkPriceFeed(f"{symbol.lower()}/usd") for symbol in config.symbols}
        self.window_reference_prices: dict[str, dict[str, float | None]] = {}
        self.pending_settlements: dict[str, tuple[MarketWindow, dt.datetime]] = {}
        self._last_score_refresh = 0.0
        self._last_report_refresh = 0.0
        self._last_data_cleanup = 0.0
        self._active_score_cursor = 0
        self._discovery_score_cursor = 0

    async def run(self) -> int:
        started = time.monotonic()
        try:
            for feed in self.feeds.values():
                await feed.start()
            await self._refresh_windows(initial=True)
            await self.stream.connect(self._all_tokens())
            while True:
                await self._refresh_windows()
                await self._refresh_window_open_prices()
                await self._write_pending_settlements()
                await self._poll_trades_once()
                await self._refresh_scores_if_due()
                self._write_report_if_due()
                self._cleanup_stale_data_if_due()
                cleanup_raw_retention(self.config.data_dir / "raw", retention_days=self.config.raw_retention_days)
                if self.config.seconds is not None and time.monotonic() - started >= self.config.seconds:
                    break
                await asyncio.sleep(self.config.poll_sec)
            self._write_report(force=True)
            return 0
        finally:
            self.writer.close()
            self.store.close()
            await self.stream.close()
            await asyncio.gather(*(feed.stop() for feed in self.feeds.values()), return_exceptions=True)

    async def _refresh_windows(self, *, initial: bool = False) -> None:
        changed = False
        now = dt.datetime.now(dt.timezone.utc)
        for symbol in self.config.symbols:
            current = self.windows.get(symbol)
            if current is not None and current.end_time > now:
                continue
            if current is not None:
                self.pending_settlements.setdefault(
                    current.slug,
                    (current, current.end_time + dt.timedelta(seconds=self.config.settlement_delay_sec)),
                )
            window = await asyncio.to_thread(find_current_or_next_window, MarketSeries.from_symbol(symbol), now=now)
            if window is None:
                continue
            if current is None or current.slug != window.slug:
                self.windows[symbol] = window
                self.window_reference_prices[window.slug] = {"open": None, "close": None}
                changed = True
                self.writer.write({
                    "event": "market_selected",
                    "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "symbol": symbol,
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "window_start": window.start_time.isoformat(),
                    "window_end": window.end_time.isoformat(),
                })
        if changed and not initial:
            await self.stream.switch_tokens(self._all_tokens())

    async def _refresh_window_open_prices(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        for window in list(self.windows.values()):
            refs = self.window_reference_prices.setdefault(window.slug, {"open": None, "close": None})
            if refs.get("open") is not None:
                continue
            age_sec = (now - window.start_time).total_seconds()
            if age_sec < self.config.open_price_min_age_sec:
                continue
            data = await asyncio.to_thread(fetch_crypto_price_api, window)
            if data is None or data.get("openPrice") is None:
                continue
            refs["open"] = float(data["openPrice"])
            self.writer.write({
                "event": "market_open_price",
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "symbol": window.symbol,
                "market_slug": window.slug,
                "condition_id": window.condition_id,
                "window_start": window.start_time.isoformat(),
                "window_end": window.end_time.isoformat(),
                "reference_open_price": compact_float(refs["open"], 6),
                "source": "polymarket_crypto_price_api",
                "cached": bool(data.get("cached")),
            })

    async def _write_pending_settlements(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        due = [(slug, item[0]) for slug, item in self.pending_settlements.items() if item[1] <= now]
        for slug, window in due:
            data = await asyncio.to_thread(fetch_crypto_price_api, window)
            refs = self.window_reference_prices.setdefault(slug, {"open": None, "close": None})
            open_price = data.get("openPrice") if data is not None else refs.get("open")
            close_price = data.get("closePrice") if data is not None else None
            if open_price is not None:
                refs["open"] = float(open_price)
            if close_price is not None:
                refs["close"] = float(close_price)
            winning_side = None
            if refs.get("open") is not None and refs.get("close") is not None:
                winning_side = "Up" if float(refs["close"]) >= float(refs["open"]) else "Down"
            self.writer.write({
                "event": "window_settlement",
                "observed_at": now.isoformat(),
                "symbol": window.symbol,
                "market_slug": window.slug,
                "condition_id": window.condition_id,
                "window_start": window.start_time.isoformat(),
                "window_end": window.end_time.isoformat(),
                "settlement_open_price": compact_float(refs.get("open"), 6),
                "settlement_close_price": compact_float(refs.get("close"), 6),
                "winning_side": winning_side,
                "settlement_completed": bool(data.get("completed")) if data is not None else None,
                "settlement_cached": bool(data.get("cached")) if data is not None else None,
                "settlement_source": "polymarket_crypto_price_api",
            })
            self.pending_settlements.pop(slug, None)

    def _all_tokens(self) -> list[str]:
        tokens: list[str] = []
        for window in self.windows.values():
            tokens.extend([window.up_token, window.down_token])
        return tokens

    async def _poll_trades_once(self) -> None:
        for symbol, window in list(self.windows.items()):
            try:
                raw_trades = await asyncio.to_thread(fetch_market_trades, window.condition_id, limit=500, offset=0)
            except Exception as exc:
                self.writer.write({"event": "api_error", "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source": "market_trades", "symbol": symbol, "error": str(exc)})
                continue
            observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            for raw in raw_trades:
                trade = normalize_trade(raw, symbol=symbol, observed_at=observed_at)
                if trade["usdc"] < self.config.min_trade_usdc:
                    continue
                if not self.store.insert_trade(trade):
                    continue
                should_snapshot = self._should_snapshot(trade)
                if self._should_write_raw_trade(trade):
                    self.writer.write(trade)
                if should_snapshot:
                    self.writer.write(context_snapshot(
                        trade=trade,
                        window=window,
                        stream=self.stream,
                        feed=self.feeds.get(symbol),
                        window_open_reference_price=self.window_reference_prices.get(window.slug, {}).get("open"),
                        window_close_reference_price=self.window_reference_prices.get(window.slug, {}).get("close"),
                        max_book_age_sec=self.config.book_max_age_sec,
                    ))

    def _should_snapshot(self, trade: dict[str, Any]) -> bool:
        wallet = trade["wallet"]
        if wallet in self.seeds or wallet in self.store.seed_wallets():
            return True
        candidates = self.store.candidate_rows(limit=self.config.max_candidates)
        active = {row["wallet"] for row in candidates.get("active_candidate", [])}
        return wallet in active

    def _should_write_raw_trade(self, trade: dict[str, Any]) -> bool:
        return self._should_snapshot(trade)

    async def _refresh_scores_if_due(self) -> None:
        if time.monotonic() - self._last_score_refresh < self.config.score_refresh_sec:
            return
        self._last_score_refresh = time.monotonic()
        batch = self._score_batch()
        if not batch:
            return
        for wallet in batch:
            try:
                metrics = await asyncio.to_thread(build_metrics_from_api, wallet)
            except Exception as exc:
                local_metrics = self.store.wallet_trade_metrics(wallet)
                local_metrics["score_error"] = str(exc)
                score = score_wallet(local_metrics, CandidateThresholds())
            else:
                local_metrics = self.store.wallet_trade_metrics(wallet)
                if float(local_metrics.get("markets_24h") or 0) > float(metrics.get("markets_24h") or 0):
                    metrics["markets_24h"] = local_metrics["markets_24h"]
                    metrics["trades_24h"] = local_metrics["trades_24h"]
                    metrics["markets_24h_source"] = "local_observed"
                    metrics["markets_24h_lower_bound"] = False
                else:
                    metrics.setdefault("markets_24h_source", "api_sample")
                score = score_wallet(metrics, CandidateThresholds())
            previous_status = self.store.candidate_status(wallet)
            if should_persist_score(
                score,
                is_seed=wallet in self.seeds or wallet in self.store.seed_wallets(),
                previous_status=previous_status,
            ):
                self.store.upsert_score(score)
                self.writer.write(compact_score_event(score))
        keep = self.store.seed_wallets()
        removed = 0
        removed += self.store.prune_low_sample_archives(keep_wallets=keep)
        removed += self.store.prune_candidate_scores("active_candidate", max_rows=self.config.max_active_candidates, keep_wallets=keep)
        removed += self.store.prune_candidate_scores("dormant_candidate", max_rows=self.config.max_dormant_candidates, keep_wallets=keep)
        removed += self.store.prune_archive_scores(max_archive=self.config.max_archive_candidates, keep_wallets=keep)
        if removed:
            self.writer.write({"event": "archive_pruned", "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "removed": removed})

    def _score_batch(self) -> list[str]:
        budget = max(1, self.config.score_wallets_per_cycle)
        active_wallets = self.store.candidate_wallets("active_candidate", limit=self.config.max_active_candidates)
        batch: list[str] = []
        if active_wallets:
            start = self._active_score_cursor % len(active_wallets)
            take = min(budget, len(active_wallets))
            batch.extend(active_wallets[(start + idx) % len(active_wallets)] for idx in range(take))
            self._active_score_cursor = (start + take) % len(active_wallets)

        if len(batch) < budget:
            discovery_wallets = list(
                dict.fromkeys(
                    list(self.seeds)
                    + self.store.candidate_wallets("dormant_candidate", limit=self.config.max_dormant_candidates)
                    + self.store.recent_wallets(limit=self.config.score_wallet_pool_limit)
                )
            )
            discovery_wallets = [wallet for wallet in discovery_wallets if wallet not in set(batch)]
            if discovery_wallets:
                start = self._discovery_score_cursor % len(discovery_wallets)
                take = min(budget - len(batch), len(discovery_wallets))
                batch.extend(discovery_wallets[(start + idx) % len(discovery_wallets)] for idx in range(take))
                self._discovery_score_cursor = (start + take) % len(discovery_wallets)
        return batch

    def _cleanup_stale_data_if_due(self) -> None:
        interval_sec = max(0.0, self.config.cleanup_interval_hours) * 3600.0
        if interval_sec <= 0:
            return
        if time.monotonic() - self._last_data_cleanup < interval_sec:
            return
        self._last_data_cleanup = time.monotonic()
        now = dt.datetime.now(dt.timezone.utc)
        cutoff_ts = int((now - dt.timedelta(hours=self.config.inactive_wallet_ttl_hours)).timestamp())
        keep = self.store.seed_wallets()
        result = self.store.cleanup_inactive_wallet_data(
            inactive_cutoff_ts=cutoff_ts,
            keep_wallets=keep,
            max_non_candidate_wallets=self.config.max_non_candidate_wallets,
        )
        if any(result.values()):
            self.writer.write({
                "event": "sqlite_cleanup",
                "observed_at": now.isoformat(),
                "inactive_wallet_ttl_hours": self.config.inactive_wallet_ttl_hours,
                "max_non_candidate_wallets": self.config.max_non_candidate_wallets,
                "inactive_cutoff_ts": cutoff_ts,
                **result,
            })

    def _write_report_if_due(self) -> None:
        if time.monotonic() - self._last_report_refresh >= self.config.report_refresh_sec:
            self._write_report(force=True)

    def _write_report(self, *, force: bool = False) -> None:
        if not force:
            return
        self._last_report_refresh = time.monotonic()
        keep = self.store.seed_wallets()
        self.store.prune_low_sample_archives(keep_wallets=keep)
        self.store.prune_candidate_scores("active_candidate", max_rows=self.config.max_active_candidates, keep_wallets=keep)
        self.store.prune_candidate_scores("dormant_candidate", max_rows=self.config.max_dormant_candidates, keep_wallets=keep)
        self.store.prune_archive_scores(max_archive=self.config.max_archive_candidates, keep_wallets=keep)
        write_latest_candidates(
            self.config.data_dir / "reports" / "latest_candidates.json",
            {
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "max_candidates": self.config.max_candidates,
                "symbols": list(self.config.symbols),
                "candidates": self.store.candidate_rows(limit=self.config.max_candidates),
            },
        )
