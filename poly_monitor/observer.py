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
from .data_api import AsyncDataApiClient, normalize_trade
from .market import MarketSeries, MarketWindow, find_current_or_next_window
from .price_feed import ChainlinkPriceFeed, ChainlinkPriceHub
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
    raw_retention_days: int = 3
    score_refresh_sec: float = 60.0
    score_wallets_per_cycle: int = 2
    score_wallet_pool_limit: int = 50
    cleanup_interval_hours: float = 6.0
    inactive_wallet_ttl_hours: float = 48.0
    max_non_candidate_wallets: int = 100
    report_refresh_sec: float = 60.0
    book_max_age_sec: float = 3.0
    window_refresh_sec: float = 15.0
    open_price_refresh_sec: float = 5.0
    settlement_check_sec: float = 30.0
    raw_cleanup_interval_hours: float = 1.0
    context_snapshot_cooldown_sec: float = 15.0
    open_price_min_age_sec: float = 5.0
    settlement_delay_sec: float = 150.0
    settlement_retry_sec: float = 30.0
    max_active_candidates: int = 15
    max_dormant_candidates: int = 10
    max_archive_candidates: int = 100
    active_metrics_ttl_sec: float = 60.0
    dormant_metrics_ttl_sec: float = 600.0
    archive_revival_min_trades_24h: int = 100
    archive_revival_min_markets_24h: int = 20
    archive_revival_cooldown_sec: float = 300.0


@dataclass
class _MetricsCacheEntry:
    metrics: dict[str, Any]
    fetched_at: float


def should_persist_score(score, *, previous_status: str | None = None) -> bool:
    return True


def compact_score_event(score) -> dict[str, Any]:
    return {
        "event": "candidate_score",
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "wallet": score.wallet if hasattr(score, "wallet") else score["wallet"],
        "status": score.status if hasattr(score, "status") else score["status"],
        "rank_score": score.rank_score if hasattr(score, "rank_score") else score["rank_score"],
        "reason_count": len(score.reasons if hasattr(score, "reasons") else score.get("reasons", [])),
    }


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
    def __init__(self, config: ObserverConfig) -> None:
        self.config = config
        self.store = ObserverStore(config.data_dir / "state" / "observer.sqlite")
        self.writer = JsonlEventWriter(config.data_dir)
        self.data_api = AsyncDataApiClient()
        self.windows: dict[str, MarketWindow] = {}
        self.stream = ClobBookStream()
        self.price_hub = ChainlinkPriceHub([f"{symbol.lower()}/usd" for symbol in config.symbols])
        self.feeds = {symbol: self.price_hub.feed(f"{symbol.lower()}/usd") for symbol in config.symbols}
        self.window_reference_prices: dict[str, dict[str, float | None]] = {}
        self.pending_settlements: dict[str, tuple[MarketWindow, dt.datetime]] = {}
        self._last_score_refresh = 0.0
        self._last_report_refresh = 0.0
        self._last_data_cleanup = 0.0
        self._last_raw_cleanup = 0.0
        self._last_window_refresh = 0.0
        self._last_open_price_refresh = 0.0
        self._last_settlement_check = 0.0
        self._last_trade_poll = 0.0
        self._single_score_bucket_cursor = 0
        self._watchlist_score_cursor = 0
        self._active_score_cursor = 0
        self._discovery_score_cursor = 0
        self._single_score_discovery_turn = True
        self._score_cycles_since_prune = 0
        self._active_snapshot_wallets: set[str] = set()
        self._metrics_cache: dict[str, _MetricsCacheEntry] = {}
        self._last_context_snapshot: dict[tuple[str, str], float] = {}
        self._last_score_event_state: dict[str, tuple[str, float]] = {}
        self._refresh_candidate_caches()

    async def run(self) -> int:
        started = time.monotonic()
        try:
            await self.price_hub.start()
            await self._refresh_windows(initial=True)
            self._last_window_refresh = time.monotonic()
            await self.stream.connect(self._all_tokens())
            while True:
                await self._refresh_windows_if_due()
                await self._refresh_window_open_prices_if_due()
                await self._write_pending_settlements_if_due()
                await self._poll_trades_if_due()
                await self._refresh_scores_if_due()
                self._write_report_if_due()
                self._cleanup_stale_data_if_due()
                self._cleanup_raw_retention_if_due()
                if self.config.seconds is not None and time.monotonic() - started >= self.config.seconds:
                    break
                await asyncio.sleep(self._next_sleep_delay(started))
            self._write_report(force=True)
            return 0
        finally:
            self.writer.close()
            self.store.close()
            await self.data_api.close()
            await self.stream.close()
            await self.price_hub.stop()

    async def _refresh_windows_if_due(self) -> None:
        if time.monotonic() - self._last_window_refresh < self.config.window_refresh_sec:
            return
        self._last_window_refresh = time.monotonic()
        await self._refresh_windows()

    async def _refresh_window_open_prices_if_due(self) -> None:
        if not self._has_missing_open_prices():
            return
        if time.monotonic() - self._last_open_price_refresh < self.config.open_price_refresh_sec:
            return
        self._last_open_price_refresh = time.monotonic()
        await self._refresh_window_open_prices()

    async def _write_pending_settlements_if_due(self) -> None:
        if time.monotonic() - self._last_settlement_check < self.config.settlement_check_sec:
            return
        self._last_settlement_check = time.monotonic()
        await self._write_pending_settlements()

    async def _poll_trades_if_due(self) -> None:
        if time.monotonic() - self._last_trade_poll < self.config.poll_sec:
            return
        self._last_trade_poll = time.monotonic()
        await self._poll_trades_once()

    def _next_sleep_delay(self, started: float) -> float:
        now = time.monotonic()
        due_times = [
            self._last_trade_poll + self.config.poll_sec,
            self._last_window_refresh + self.config.window_refresh_sec,
            self._last_score_refresh + self.config.score_refresh_sec,
            self._last_report_refresh + self.config.report_refresh_sec,
        ]
        if self._has_missing_open_prices():
            due_times.append(self._last_open_price_refresh + self.config.open_price_refresh_sec)
        if self.pending_settlements:
            due_times.append(self._last_settlement_check + self.config.settlement_check_sec)
        cleanup_interval = max(0.0, self.config.cleanup_interval_hours) * 3600.0
        if cleanup_interval > 0:
            due_times.append(self._last_data_cleanup + cleanup_interval)
        raw_cleanup_interval = max(0.0, self.config.raw_cleanup_interval_hours) * 3600.0
        if raw_cleanup_interval > 0:
            due_times.append(self._last_raw_cleanup + raw_cleanup_interval)
        if self.config.seconds is not None:
            due_times.append(started + self.config.seconds)
        delay = min(due_times) - now
        return max(0.05, min(delay, 5.0))

    def _has_missing_open_prices(self) -> bool:
        return any(
            refs.get("open") is None
            for window in self.windows.values()
            for refs in [self.window_reference_prices.setdefault(window.slug, {"open": None, "close": None})]
        )

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
                self.store.upsert_market_window(
                    symbol=symbol,
                    market_slug=window.slug,
                    condition_id=window.condition_id,
                    window_start=window.start_time.isoformat(),
                    window_end=window.end_time.isoformat(),
                )
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
                feed = self.feeds.get(window.symbol)
                fallback = feed.latest_price if feed else None
                if fallback is None:
                    continue
                refs["open"] = float(fallback)
                source = "chainlink_live_fallback"
                cached = None
            else:
                refs["open"] = float(data["openPrice"])
                source = "polymarket_crypto_price_api"
                cached = bool(data.get("cached"))
            self.writer.write({
                "event": "market_open_price",
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "symbol": window.symbol,
                "market_slug": window.slug,
                "condition_id": window.condition_id,
                "window_start": window.start_time.isoformat(),
                "window_end": window.end_time.isoformat(),
                "reference_open_price": compact_float(refs["open"], 6),
                "source": source,
                "cached": cached,
            })

    async def _write_pending_settlements(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        due = [(slug, item[0]) for slug, item in self.pending_settlements.items() if item[1] <= now]
        for slug, window in due:
            data = await asyncio.to_thread(fetch_crypto_price_api, window)
            refs = self.window_reference_prices.setdefault(slug, {"open": None, "close": None})
            open_price = data.get("openPrice") if data is not None else refs.get("open")
            close_price = data.get("closePrice") if data is not None else None
            source = "polymarket_crypto_price_api"
            if close_price is None:
                feed = self.feeds.get(window.symbol)
                fallback = feed.latest_price if feed else None
                if fallback is not None:
                    close_price = fallback
                    source = "chainlink_live_fallback"
            if open_price is not None:
                refs["open"] = float(open_price)
            if close_price is not None:
                refs["close"] = float(close_price)
            winning_side = None
            if refs.get("open") is not None and refs.get("close") is not None:
                winning_side = "Up" if float(refs["close"]) >= float(refs["open"]) else "Down"
            completed = bool(data.get("completed")) if data is not None else False
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
                "settlement_completed": completed,
                "settlement_cached": bool(data.get("cached")) if data is not None else None,
                "settlement_source": source,
            })
            self.store.upsert_market_settlement(
                {
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": window.symbol,
                    "winning_side": winning_side or "",
                    "settlement_open_price": compact_float(refs.get("open"), 6),
                    "settlement_close_price": compact_float(refs.get("close"), 6),
                    "settled_at": now.isoformat(),
                    "completed": completed,
                }
            )
            if completed:
                self.pending_settlements.pop(slug, None)
            else:
                self.pending_settlements[slug] = (window, now + dt.timedelta(seconds=self.config.settlement_retry_sec))

    def _all_tokens(self) -> list[str]:
        tokens: list[str] = []
        for window in self.windows.values():
            tokens.extend([window.up_token, window.down_token])
        return tokens

    async def _poll_trades_once(self) -> None:
        for symbol, window in list(self.windows.items()):
            try:
                raw_trades = await self.data_api.fetch_market_trades(window.condition_id, limit=500, offset=0)
            except Exception as exc:
                self.writer.write({"event": "api_error", "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source": "market_trades", "symbol": symbol, "error": str(exc)})
                continue
            observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            last_seen_ts = self.store.market_last_exchange_ts(window.condition_id)
            normalized: list[dict[str, Any]] = []
            for raw in raw_trades:
                trade = normalize_trade(raw, symbol=symbol, observed_at=observed_at)
                if trade["usdc"] < self.config.min_trade_usdc:
                    continue
                if last_seen_ts and int(trade["exchange_ts"]) < last_seen_ts:
                    continue
                normalized.append(trade)
            for trade in self.store.insert_trades(normalized):
                should_snapshot = self._should_snapshot(trade)
                if self._should_write_raw_trade(trade):
                    self.writer.write(trade)
                if should_snapshot and self._should_write_context_snapshot(trade):
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
        return wallet in self._active_snapshot_wallets

    def _should_write_context_snapshot(self, trade: dict[str, Any]) -> bool:
        key = (str(trade.get("wallet") or "").lower(), str(trade.get("market_slug") or ""))
        now = time.monotonic()
        last = self._last_context_snapshot.get(key)
        if last is not None and now - last < self.config.context_snapshot_cooldown_sec:
            return False
        self._last_context_snapshot[key] = now
        return True

    def _should_write_raw_trade(self, trade: dict[str, Any]) -> bool:
        return self._should_snapshot(trade)

    def _score_event_state(self, score) -> tuple[str, str, float]:
        wallet = str(score.wallet if hasattr(score, "wallet") else score["wallet"]).lower()
        status = str(score.status if hasattr(score, "status") else score["status"])
        rank_score = float(score.rank_score if hasattr(score, "rank_score") else score["rank_score"])
        return wallet, status, rank_score

    def _score_event_changed(self, score) -> bool:
        wallet, status, rank_score = self._score_event_state(score)
        previous = self._last_score_event_state.get(wallet)
        if previous is None:
            return True
        previous_status, previous_rank = previous
        if status != previous_status:
            return True
        return abs(rank_score - previous_rank) >= 1.0

    def _record_score_event(self, score) -> None:
        wallet, status, rank_score = self._score_event_state(score)
        self._last_score_event_state[wallet] = (status, rank_score)

    async def _refresh_scores_if_due(self) -> None:
        if time.monotonic() - self._last_score_refresh < self.config.score_refresh_sec:
            return
        self._last_score_refresh = time.monotonic()
        batch = self._score_batch()
        if not batch:
            return
        for wallet, previous_status in batch:
            try:
                metrics = await self._metrics_for_wallet(wallet, previous_status)
            except Exception as exc:
                self.writer.write({
                    "event": "score_error",
                    "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "wallet": wallet,
                    "error": str(exc),
                })
                continue
            else:
                if metrics is None:
                    continue
                self._apply_local_observed_24h_override(metrics)
                score = score_wallet(metrics, CandidateThresholds())
            if should_persist_score(
                score,
                previous_status=previous_status,
            ):
                self.store.upsert_score(score)
                if self._score_event_changed(score):
                    self.writer.write(compact_score_event(score))
                self._record_score_event(score)
        self._score_cycles_since_prune += 1
        if self._score_cycles_since_prune >= 5:
            self._score_cycles_since_prune = 0
            removed = self._prune_candidate_tables()
            if removed:
                self.writer.write({"event": "archive_pruned", "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "removed": removed})
        self._refresh_candidate_caches()

    def _prune_candidate_tables(self) -> int:
        removed = 0
        removed += self.store.prune_low_sample_archives(min_age_seconds=self.config.archive_revival_cooldown_sec)
        removed += self.store.prune_candidate_scores("active_candidate", max_rows=self.config.max_active_candidates)
        removed += self.store.prune_candidate_scores("dormant_candidate", max_rows=self.config.max_dormant_candidates)
        removed += self.store.prune_archive_scores(
            max_archive=self.config.max_archive_candidates,
            min_age_seconds=self.config.archive_revival_cooldown_sec,
        )
        return removed

    async def _metrics_for_wallet(self, wallet: str, previous_status: str | None) -> dict[str, Any] | None:
        ttl = self._metrics_cache_ttl(previous_status, wallet)
        now = time.monotonic()
        entry = self._metrics_cache.get(wallet)
        if entry is not None and now - entry.fetched_at < ttl:
            return dict(entry.metrics)
        local_metrics = self.store.wallet_trade_metrics(wallet)
        if (
            previous_status is not None
            and not int(local_metrics.get("historical_trades") or 0)
            and not int(local_metrics.get("settled_markets_total") or 0)
        ):
            self.store.delete_candidate_score(wallet)
            self._metrics_cache.pop(wallet, None)
            self._last_score_event_state.pop(wallet.lower(), None)
            return None
        try:
            api_metrics = build_metrics_from_api(wallet)
        except Exception:
            metrics = dict(local_metrics)
        else:
            metrics = self._merge_api_and_local_metrics(api_metrics, local_metrics)
        self._metrics_cache[wallet] = _MetricsCacheEntry(dict(metrics), now)
        return metrics

    def _merge_api_and_local_metrics(self, api_metrics: dict[str, Any], local_metrics: dict[str, Any]) -> dict[str, Any]:
        metrics = dict(api_metrics)
        local_fields = {
            "trades_24h",
            "markets_24h",
            "trades_7d",
            "markets_7d",
            "trades_30d",
            "markets_30d",
            "pnl_7d",
            "pnl_30d",
            "pnl_total",
            "wins_7d",
            "losses_7d",
            "settled_markets_7d",
            "settled_markets_30d",
            "settled_markets_total",
            "incomplete_settled_markets_7d",
            "incomplete_settled_markets_30d",
            "incomplete_settled_markets_total",
            "historical_trades",
            "historical_markets",
            "historical_pnl",
            "last_active_age_hours",
        }
        for key in local_fields:
            if key in local_metrics:
                metrics[f"local_observed_{key}"] = local_metrics[key]
        if int(local_metrics.get("settled_markets_7d") or 0) > 0:
            metrics["wins_7d"] = local_metrics.get("wins_7d", 0)
            metrics["losses_7d"] = local_metrics.get("losses_7d", 0)
        metrics["local_observed_pnl_source"] = local_metrics.get("pnl_source", "local_observed")
        return metrics

    def _apply_local_observed_24h_override(self, metrics: dict[str, Any]) -> None:
        api_markets = float(metrics.get("markets_24h") or 0)
        local_markets = float(metrics.get("local_observed_markets_24h") or 0)
        if local_markets > api_markets:
            metrics["markets_24h"] = int(local_markets)
            metrics["trades_24h"] = int(metrics.get("local_observed_trades_24h") or 0)
            metrics["markets_24h_source"] = "local_observed"
            metrics["markets_24h_lower_bound"] = False
        else:
            metrics.setdefault("markets_24h_source", "api_sample")

    def _metrics_cache_ttl(self, previous_status: str | None, wallet: str | None = None) -> float:
        if wallet and wallet.lower() in self.store.watchlist_wallets():
            return self.config.active_metrics_ttl_sec
        if previous_status in {"dormant_candidate", "archive_candidate"}:
            return self.config.dormant_metrics_ttl_sec
        return self.config.active_metrics_ttl_sec

    def _score_batch(self, *, now: dt.datetime | None = None) -> list[tuple[str, str | None]]:
        now = now or dt.datetime.now(dt.timezone.utc)
        budget = max(1, self.config.score_wallets_per_cycle)
        watchlist_wallets = self.store.watchlist_wallets()
        watchlist_set = set(watchlist_wallets)
        active_wallets = self.store.candidate_wallets("active_candidate", limit=self.config.max_active_candidates)
        active_wallets = [wallet for wallet in active_wallets if wallet not in watchlist_set]
        if budget == 1 and watchlist_wallets:
            buckets = ("watchlist", "discovery", "active")
            selected = buckets[self._single_score_bucket_cursor % len(buckets)]
            self._single_score_bucket_cursor += 1
            if selected == "active" and not active_wallets:
                selected = "discovery"
            watchlist_budget = 1 if selected == "watchlist" else 0
            discovery_budget = 1 if selected == "discovery" else 0
            active_budget = 1 if selected == "active" else 0
        elif budget == 1:
            use_discovery = self._single_score_discovery_turn or not active_wallets
            self._single_score_discovery_turn = not self._single_score_discovery_turn
            watchlist_budget = 0
            discovery_budget = 1 if use_discovery else 0
            active_budget = 0 if use_discovery else 1
        elif watchlist_wallets:
            watchlist_budget = max(1, budget // 3)
            remaining_budget = max(0, budget - watchlist_budget)
            if remaining_budget == 1:
                use_discovery = self._single_score_discovery_turn or not active_wallets
                self._single_score_discovery_turn = not self._single_score_discovery_turn
                discovery_budget = 1 if use_discovery else 0
                active_budget = 0 if use_discovery else 1
            else:
                discovery_budget = max(1, remaining_budget // 2) if remaining_budget > 0 else 0
                active_budget = max(0, remaining_budget - discovery_budget)
        else:
            watchlist_budget = 0
            discovery_budget = max(1, budget // 2)
            active_budget = budget - discovery_budget
        batch: list[str] = []
        if watchlist_wallets and watchlist_budget > 0:
            start = self._watchlist_score_cursor % len(watchlist_wallets)
            take = min(watchlist_budget, len(watchlist_wallets), budget - len(batch))
            batch.extend(watchlist_wallets[(start + idx) % len(watchlist_wallets)] for idx in range(take))
            self._watchlist_score_cursor = (start + take) % len(watchlist_wallets)
        if active_wallets and active_budget > 0 and len(batch) < budget:
            start = self._active_score_cursor % len(active_wallets)
            take = min(active_budget, len(active_wallets), budget - len(batch))
            batch.extend(active_wallets[(start + idx) % len(active_wallets)] for idx in range(take))
            self._active_score_cursor = (start + take) % len(active_wallets)

        if discovery_budget > 0 and len(batch) < budget:
            due_dormant = set(
                self.store.candidate_wallets_due(
                    "dormant_candidate",
                    limit=self.config.max_dormant_candidates,
                    min_age_seconds=self.config.dormant_metrics_ttl_sec,
                )
            )
            reactivatable_archives = set(
                self.store.reactivatable_archive_wallets(
                    limit=self.config.score_wallet_pool_limit,
                    now=now,
                    min_trades_24h=self.config.archive_revival_min_trades_24h,
                    min_markets_24h=self.config.archive_revival_min_markets_24h,
                    min_age_seconds=self.config.archive_revival_cooldown_sec,
                )
            )
            discovery_wallets = list(
                dict.fromkeys(
                    list(due_dormant)
                    + self.store.recent_wallets(limit=self.config.score_wallet_pool_limit)
                    + list(reactivatable_archives)
                )
            )
            discovery_wallets = [wallet for wallet in discovery_wallets if wallet not in set(batch)]
            statuses = self.store.candidate_statuses(discovery_wallets)
            discovery_wallets = [
                wallet
                for wallet in discovery_wallets
                if wallet not in watchlist_set
                and (
                    statuses.get(wallet) != "archive_candidate"
                    or wallet in reactivatable_archives
                )
                and (statuses.get(wallet) != "dormant_candidate" or wallet in due_dormant)
            ]
            if discovery_wallets:
                start = self._discovery_score_cursor % len(discovery_wallets)
                take = min(len(discovery_wallets), budget - len(batch))
                batch.extend(discovery_wallets[(start + idx) % len(discovery_wallets)] for idx in range(take))
                self._discovery_score_cursor = (start + take) % len(discovery_wallets)
        if not batch and active_wallets:
            start = self._active_score_cursor % len(active_wallets)
            take = min(budget, len(active_wallets))
            batch.extend(active_wallets[(start + idx) % len(active_wallets)] for idx in range(take))
            self._active_score_cursor = (start + take) % len(active_wallets)
        statuses = self.store.candidate_statuses(batch)
        return [(wallet, statuses.get(wallet)) for wallet in batch]

    def _cleanup_stale_data_if_due(self) -> None:
        interval_sec = max(0.0, self.config.cleanup_interval_hours) * 3600.0
        if interval_sec <= 0:
            return
        if time.monotonic() - self._last_data_cleanup < interval_sec:
            return
        self._last_data_cleanup = time.monotonic()
        now = dt.datetime.now(dt.timezone.utc)
        cutoff_ts = int((now - dt.timedelta(hours=self.config.inactive_wallet_ttl_hours)).timestamp())
        result = self.store.cleanup_inactive_wallet_data(
            inactive_cutoff_ts=cutoff_ts,
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
        context_cutoff = time.monotonic() - 600.0
        self._last_context_snapshot = {
            key: last_seen
            for key, last_seen in self._last_context_snapshot.items()
            if last_seen > context_cutoff
        }
        known_candidate_wallets = self._known_candidate_wallets()
        self._last_score_event_state = {
            wallet: state
            for wallet, state in self._last_score_event_state.items()
            if wallet in known_candidate_wallets
        }
        metrics_cutoff = time.monotonic() - max(
            self.config.active_metrics_ttl_sec,
            self.config.dormant_metrics_ttl_sec,
        ) * 10.0
        self._metrics_cache = {
            wallet: entry
            for wallet, entry in self._metrics_cache.items()
            if wallet in known_candidate_wallets and entry.fetched_at > metrics_cutoff
        }

    def _known_candidate_wallets(self) -> set[str]:
        rows = self.store.conn.execute("SELECT wallet FROM candidate_scores").fetchall()
        return {str(row["wallet"]).lower() for row in rows} | set(self.store.watchlist_wallets())

    def _cleanup_raw_retention_if_due(self) -> None:
        interval_sec = max(0.0, self.config.raw_cleanup_interval_hours) * 3600.0
        if interval_sec <= 0:
            return
        if time.monotonic() - self._last_raw_cleanup < interval_sec:
            return
        self._last_raw_cleanup = time.monotonic()
        cleanup_raw_retention(self.config.data_dir / "raw", retention_days=self.config.raw_retention_days)

    def _write_report_if_due(self) -> None:
        if time.monotonic() - self._last_report_refresh >= self.config.report_refresh_sec:
            self._write_report(force=True)

    def _write_report(self, *, force: bool = False) -> None:
        if not force:
            return
        self._last_report_refresh = time.monotonic()
        self._refresh_candidate_caches()
        write_latest_candidates(
            self.config.data_dir / "reports" / "latest_candidates.json",
            {
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "max_candidates": self.config.max_candidates,
                "symbols": list(self.config.symbols),
                "candidates": self.store.candidate_rows(limit=self.config.max_candidates),
            },
        )

    def _refresh_candidate_caches(self) -> None:
        watchlist_wallets = set(self.store.watchlist_wallets())
        self._active_snapshot_wallets = set(
            self.store.candidate_wallets("active_candidate", limit=self.config.max_active_candidates)
        ) | watchlist_wallets
        if not self._last_score_event_state:
            rows = self.store.conn.execute("SELECT wallet, status, rank_score FROM candidate_scores").fetchall()
            self._last_score_event_state = {
                str(row["wallet"]).lower(): (str(row["status"]), float(row["rank_score"] or 0.0))
                for row in rows
            }
