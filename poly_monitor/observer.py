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
from .data_api import AsyncDataApiClient, normalize_activity_event, normalize_trade
from .market import MarketSeries, MarketWindow, find_current_or_next_window
from .price_feed import ChainlinkPriceFeed, ChainlinkPriceHub
from .scoring import CandidateThresholds, score_wallet
from .storage import JsonlEventWriter, ObserverStore, cleanup_raw_retention, write_latest_candidates
from .wallet_metrics import build_metrics_from_api


def collector_status(data_dir: Path, wallet: str) -> dict[str, Any]:
    from .deep_collection import collector_status as _collector_status

    return _collector_status(data_dir, wallet)


@dataclass(frozen=True)
class ObserverConfig:
    symbols: tuple[str, ...] = ("BTC", "ETH")
    poll_sec: float = 2.0
    seconds: float | None = None
    max_candidates: int = 15
    min_trade_usdc: float = 1.0
    data_dir: Path = Path("data")
    raw_retention_days: int = 2
    score_refresh_sec: float = 60.0
    score_wallets_per_cycle: int = 2
    score_wallet_pool_limit: int = 50
    cleanup_interval_hours: float = 1.0
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
    max_dormant_candidates: int = 0
    max_archive_candidates: int = 100
    active_metrics_ttl_sec: float = 60.0
    dormant_metrics_ttl_sec: float = 600.0
    archive_revival_min_trades_24h: int = 100
    archive_revival_min_markets_24h: int = 20
    archive_revival_cooldown_sec: float = 300.0
    watchlist_activity_poll_sec: float = 180.0
    watchlist_activity_lookback_sec: int = 6 * 3600
    watchlist_activity_safety_window_sec: int = 300
    watchlist_activity_pages: int = 2
    watchlist_activity_retention_days: int = 2
    non_watchlist_activity_retention_days: int = 2
    context_retention_days: int = 2
    research_cleanup_dormant_wallets: int = 0
    market_state_retention_days: int = 2
    strategy_archive_interval_hours: float = 1.0
    market_state_sample_sec: float = 5.0
    market_state_terminal_sample_sec: float = 2.0
    market_state_terminal_window_sec: float = 60.0
    market_state_heartbeat_sec: float = 15.0
    watched_market_state_sample_sec: float = 1.0
    watched_market_state_terminal_sample_sec: float = 1.0
    watched_market_state_heartbeat_sec: float = 1.0
    watchlist_market_capture_after_end_sec: float = 1800.0
    max_context_writes_per_cycle: int = 200
    market_trade_pages: int = 1
    watched_market_trade_pages: int = 2
    market_trade_rate_limit_backoff_sec: float = 60.0


@dataclass
class _MetricsCacheEntry:
    metrics: dict[str, Any]
    fetched_at: float


def should_persist_score(score, *, previous_status: str | None = None) -> bool:
    if score.status != "active_candidate":
        return False
    if previous_status is not None:
        return True
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
    raw_trade_exchange_ts = trade.get("exchange_ts")
    try:
        trade_exchange_ts = int(raw_trade_exchange_ts) if raw_trade_exchange_ts not in (None, "") else None
    except (TypeError, ValueError):
        trade_exchange_ts = None
    up_bids, up_asks, up_age = stream.get_book(window.up_token, max_age_sec=max_book_age_sec)
    down_bids, down_asks, down_age = stream.get_book(window.down_token, max_age_sec=max_book_age_sec)
    up = token_book_summary(bids=up_bids, asks=up_asks, book_age_ms=up_age, targets=targets)
    down = token_book_summary(bids=down_bids, asks=down_asks, book_age_ms=down_age, targets=targets)
    max_age_ms = max_book_age_sec * 1000.0
    book_stale = (
        up_age is None
        or down_age is None
        or float(up_age) > max_age_ms
        or float(down_age) > max_age_ms
        or up.get("bid") is None
        or up.get("ask") is None
        or down.get("bid") is None
        or down.get("ask") is None
    )
    return {
        "event": "context_snapshot",
        "observed_at": now.isoformat(),
        "wallet": trade["wallet"],
        "symbol": window.symbol,
        "market_slug": window.slug,
        "condition_id": window.condition_id,
        "trade_tx_hash": trade["tx_hash"],
        "trade_exchange_ts": trade_exchange_ts,
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
        "book_stale": book_stale,
        "up": up,
        "down": down,
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
        now_monotonic = time.monotonic()
        self._last_data_cleanup = now_monotonic
        self._last_raw_cleanup = now_monotonic
        self._last_strategy_archive = now_monotonic
        self._last_window_refresh = 0.0
        self._last_open_price_refresh = 0.0
        self._last_settlement_check = 0.0
        self._last_trade_poll = 0.0
        self._last_watchlist_activity_poll = 0.0
        self._single_score_bucket_cursor = 0
        self._watchlist_score_cursor = 0
        self._active_score_cursor = 0
        self._discovery_score_cursor = 0
        self._single_score_discovery_turn = True
        self._score_cycles_since_prune = 0
        self._watchlist_wallets: set[str] = set()
        self._active_snapshot_wallets: set[str] = set()
        self._watchlist_settlement_backfilled: set[str] = set()
        self._metrics_cache: dict[str, _MetricsCacheEntry] = {}
        self._last_context_snapshot: dict[tuple[str, str], float] = {}
        self._last_market_state_sample: dict[str, tuple[dt.datetime, tuple[Any, ...]]] = {}
        self._last_score_event_state: dict[str, tuple[str, float]] = {}
        self._market_trade_backoff_until: dict[str, float] = {}
        self._cycle_watched_rows: list[dict[str, Any]] | None = None
        self._cycle_watched_slugs: set[str] | None = None
        self._refresh_candidate_caches()

    async def run(self) -> int:
        started = time.monotonic()
        try:
            await self.price_hub.start()
            await self._refresh_windows(initial=True)
            self._last_window_refresh = time.monotonic()
            await self.stream.connect(self._all_tokens())
            while True:
                self._begin_cycle()
                await self._refresh_windows_if_due()
                await self._refresh_window_open_prices_if_due()
                await self._write_pending_settlements_if_due()
                await self._poll_trades_if_due()
                await self._poll_watchlist_activity_if_due()
                self._sample_market_state_each_cycle()
                await self._refresh_scores_if_due()
                self._write_report_if_due()
                self._cleanup_stale_data_if_due()
                self._cleanup_raw_retention_if_due()
                self._archive_strategy_data_if_due()
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
        try:
            await self._refresh_windows()
        except Exception as exc:
            self.writer.write({
                "event": "observer_error",
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "stage": "refresh_windows",
                "error": str(exc),
            })

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

    async def _poll_watchlist_activity_if_due(self) -> None:
        if time.monotonic() - self._last_watchlist_activity_poll < self.config.watchlist_activity_poll_sec:
            return
        self._last_watchlist_activity_poll = time.monotonic()
        await self._poll_watchlist_activity_once()

    def _begin_cycle(self) -> None:
        self._cycle_watched_rows = None
        self._cycle_watched_slugs = None

    def _active_watched_rows(self, *, now: dt.datetime | None = None) -> list[dict[str, Any]]:
        if self._cycle_watched_rows is None:
            self._cycle_watched_rows = self.store.watched_market_windows(active_only=True, now=now)
        return self._cycle_watched_rows

    def _active_watched_slugs(self, *, now: dt.datetime | None = None) -> set[str]:
        if self._cycle_watched_slugs is None:
            self._cycle_watched_slugs = {str(row.get("market_slug") or "") for row in self._active_watched_rows(now=now)}
        return self._cycle_watched_slugs

    def _invalidate_active_watched_cache(self) -> None:
        self._cycle_watched_rows = None
        self._cycle_watched_slugs = None

    def _next_sleep_delay(self, started: float) -> float:
        now = time.monotonic()
        due_times = [
            self._last_trade_poll + self.config.poll_sec,
            self._last_window_refresh + self.config.window_refresh_sec,
            self._last_score_refresh + self.config.score_refresh_sec,
            self._last_report_refresh + self.config.report_refresh_sec,
            self._last_watchlist_activity_poll + self.config.watchlist_activity_poll_sec,
        ]
        if self._has_active_watched_market():
            watched_cadence = min(
                max(0.05, float(self.config.watched_market_state_sample_sec)),
                max(0.05, float(self.config.watched_market_state_terminal_sample_sec)),
                max(0.05, float(self.config.watched_market_state_heartbeat_sec)),
            )
            due_times.append(now + watched_cadence)
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
        archive_interval = max(0.0, self.config.strategy_archive_interval_hours) * 3600.0
        if archive_interval > 0:
            due_times.append(self._last_strategy_archive + archive_interval)
        if self.config.seconds is not None:
            due_times.append(started + self.config.seconds)
        delay = min(due_times) - now
        return max(0.05, min(delay, 5.0))

    def _has_active_watched_market(self) -> bool:
        return bool(self._active_watched_rows())

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
        due: dict[str, MarketWindow] = {
            slug: item[0]
            for slug, item in self.pending_settlements.items()
            if item[1] <= now
        }
        for row in self._active_watched_rows(now=now):
            slug = str(row.get("market_slug") or "")
            if not slug or slug in due:
                continue
            window = self._market_window_from_watched_row(row)
            if window is None:
                continue
            if window.end_time + dt.timedelta(seconds=self.config.settlement_delay_sec) > now:
                continue
            existing = self.store.conn.execute(
                "SELECT 1 FROM market_settlements WHERE market_slug=? AND completed=1 AND winning_side != ''",
                (slug,),
            ).fetchone()
            if existing is None:
                due[slug] = window
        for slug, window in due.items():
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

    async def _poll_trades_once(self, *, now_monotonic: float | None = None) -> None:
        monotonic_now = time.monotonic() if now_monotonic is None else now_monotonic
        for symbol, window in self._market_trade_poll_windows():
            backoff_until = self._market_trade_backoff_until.get(window.condition_id, 0.0)
            if monotonic_now < backoff_until:
                continue
            is_watched = window.slug in self._active_watched_slugs()
            pages = self.config.watched_market_trade_pages if is_watched else self.config.market_trade_pages
            page_size = max(1, min(100, int(pages) * 100))
            try:
                raw_trades = await self.data_api.fetch_market_trades(
                    window.condition_id,
                    limit=page_size,
                    offset=0,
                    pages=max(1, int(pages)),
                )
            except Exception as exc:
                error = str(exc)
                if "429" in error or "Too Many Requests" in error:
                    self._market_trade_backoff_until[window.condition_id] = (
                        monotonic_now + max(1.0, float(self.config.market_trade_rate_limit_backoff_sec))
                    )
                self.writer.write({"event": "api_error", "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source": "market_trades", "symbol": symbol, "error": error})
                continue
            self._market_trade_backoff_until.pop(window.condition_id, None)
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

    def _market_trade_poll_windows(self) -> list[tuple[str, MarketWindow]]:
        by_condition: dict[str, tuple[str, MarketWindow]] = {}
        by_slug: dict[str, MarketWindow] = {}
        for symbol, window in list(self.windows.items()):
            if window.condition_id:
                by_condition[window.condition_id] = (symbol, window)
            by_slug[window.slug] = window
        for row in self._active_watched_rows():
            condition_id = str(row.get("condition_id") or "")
            if not condition_id or condition_id in by_condition:
                continue
            current = by_slug.get(str(row.get("market_slug") or ""))
            if current is not None:
                by_condition[condition_id] = (current.symbol, current)
                continue
            window = self._market_window_from_watched_row(row)
            if window is not None:
                by_condition[condition_id] = (window.symbol, window)
        return list(by_condition.values())

    def _market_window_from_watched_row(self, row: dict[str, Any]) -> MarketWindow | None:
        start = self._parse_iso_dt(row.get("window_start"))
        end = self._parse_iso_dt(row.get("window_end"))
        if start is None or end is None:
            return None
        return MarketWindow(
            symbol=str(row.get("symbol") or "").upper(),
            slug=str(row.get("market_slug") or ""),
            condition_id=str(row.get("condition_id") or ""),
            question="",
            up_token="",
            down_token="",
            start_time=start,
            end_time=end,
        )

    async def _poll_watchlist_activity_once(self) -> None:
        wallets = self.store.watchlist_wallets()
        if not wallets:
            return
        now_ts = int(dt.datetime.now(dt.timezone.utc).timestamp())
        end_ts = now_ts + 60
        observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        page_size = 500
        page_count = max(1, int(self.config.watchlist_activity_pages))
        touched_activity_markets: set[str] = set()
        for wallet in wallets:
            try:
                if collector_status(self.config.data_dir, wallet).get("running"):
                    continue
            except Exception:
                pass
            lookback_start = now_ts - max(60, int(self.config.watchlist_activity_lookback_sec))
            last_seen = self.store.last_wallet_activity_ts(wallet)
            start_ts = max(lookback_start, last_seen - max(0, int(self.config.watchlist_activity_safety_window_sec))) if last_seen else lookback_start
            normalized: list[dict[str, Any]] = []
            try:
                for page in range(page_count):
                    rows = await self.data_api.fetch_user_activity(
                        wallet,
                        limit=page_size,
                        offset=page * page_size,
                        start=start_ts,
                        end=end_ts,
                    )
                    if not rows:
                        break
                    for raw in rows:
                        activity_type = str(raw.get("type") or "").upper()
                        if activity_type not in {"TRADE", "MERGE", "REDEEM", "SPLIT"}:
                            continue
                        event = normalize_activity_event(raw, wallet=wallet, observed_at=observed_at)
                        if event["market_slug"] and "-updown-5m-" not in event["market_slug"]:
                            continue
                        normalized.append(event)
                    if len(rows) < page_size:
                        break
            except Exception as exc:
                self.writer.write({
                    "event": "api_error",
                    "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "source": "watchlist_activity",
                    "wallet": wallet,
                    "error": str(exc),
                })
                continue
            inserted = self.store.insert_wallet_activity_events(normalized, recompute=False)
            touched_activity_markets.update(str(event.get("market_slug") or "") for event in inserted)
            self._register_watchlist_market_windows(inserted)
            await self._backfill_watchlist_activity_settlements(inserted)
            trade_rows = [
                self._trade_from_activity_event(event)
                for event in inserted
                if event.get("activity_type") == "TRADE"
            ]
            if trade_rows:
                self.store.insert_trades(trade_rows)
                context_rows = self._trade_context_rows_for_activity(inserted)
                if context_rows:
                    capped_context_rows = context_rows[: max(0, self.config.max_context_writes_per_cycle)]
                    self.store.insert_wallet_trade_contexts(capped_context_rows)
            for event in inserted:
                warning = self._activity_value_warning(event)
                if warning:
                    self.writer.write(warning)
                self.writer.write({
                    "event": "watchlist_activity_observed",
                    "observed_at": event["observed_at"],
                    "wallet": event["wallet"],
                    "activity_type": event["activity_type"],
                    "market_slug": event["market_slug"],
                    "condition_id": event["condition_id"],
                    "exchange_ts": event["exchange_ts"],
                    "side": event["side"],
                    "outcome": event["outcome"],
                    "price": event["price"],
                    "size": event["size"],
                    "usdc": event["usdc"],
                    "tx_hash": event["tx_hash"],
                })
        self.store.recompute_market_pnl_for_markets(touched_activity_markets)

    def _register_watchlist_market_windows(self, events: list[dict[str, Any]]) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        for event in events:
            activity_type = str(event.get("activity_type") or "").upper()
            if activity_type not in {"TRADE", "MERGE", "REDEEM", "SPLIT"}:
                continue
            window = self._activity_window_from_slug(event)
            if window is None:
                continue
            if window.end_time <= now:
                continue
            capture_after_end_sec = max(0.0, self.config.watchlist_market_capture_after_end_sec)
            capture_until = window.end_time + dt.timedelta(seconds=capture_after_end_sec)
            created = self.store.upsert_watched_market_window(
                {
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": window.symbol,
                    "first_seen_at": now.isoformat(),
                    "window_start": window.start_time.isoformat(),
                    "window_end": window.end_time.isoformat(),
                    "tracking_reason": f"watchlist_{activity_type.lower()}",
                    "source_wallet": str(event.get("wallet") or "").lower(),
                    "capture_until": capture_until.isoformat(),
                    "status": "tracking",
                }
            )
            self._invalidate_active_watched_cache()
            if created:
                self.writer.write(
                    {
                        "event": "watched_market_window_registered",
                        "observed_at": now.isoformat(),
                        "wallet": str(event.get("wallet") or "").lower(),
                        "activity_type": activity_type,
                        "symbol": window.symbol,
                        "market_slug": window.slug,
                        "condition_id": window.condition_id,
                        "window_start": window.start_time.isoformat(),
                        "window_end": window.end_time.isoformat(),
                        "capture_until": capture_until.isoformat(),
                    }
                )

    def _trade_context_rows_for_activity(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for event in events:
            if str(event.get("activity_type") or "").upper() != "TRADE":
                continue
            window = self._window_for_event(event)
            if window is None:
                continue
            feed = self.feeds.get(window.symbol)
            context = context_snapshot(
                trade=self._trade_from_activity_event(event),
                window=window,
                stream=self.stream,
                feed=feed,
                window_open_reference_price=self.window_reference_prices.get(window.slug, {}).get("open"),
                window_close_reference_price=self.window_reference_prices.get(window.slug, {}).get("close"),
                max_book_age_sec=self.config.book_max_age_sec,
            )
            rows.append(
                {
                    "wallet": str(event.get("wallet") or "").lower(),
                    "tx_hash": str(event.get("tx_hash") or ""),
                    "fill_id": str(event.get("fill_id") or ""),
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": window.symbol,
                    "exchange_ts": int(event.get("exchange_ts") or 0),
                    "observed_at": context["observed_at"],
                    "context_json": context,
                    "book_stale": bool(context.get("book_stale")),
                }
            )
        return rows

    def _window_for_event(self, event: dict[str, Any]) -> MarketWindow | None:
        slug = str(event.get("market_slug") or "")
        for window in self.windows.values():
            if window.slug == slug:
                return window
        return None

    async def _backfill_watchlist_activity_settlements(self, events: list[dict[str, Any]]) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        windows: dict[str, MarketWindow] = {}
        for event in events:
            window = self._activity_window_from_slug(event)
            if window is None:
                continue
            if window.slug in self._watchlist_settlement_backfilled:
                continue
            if window.end_time + dt.timedelta(seconds=self.config.settlement_delay_sec) > now:
                continue
            existing = self.store.conn.execute(
                "SELECT 1 FROM market_settlements WHERE market_slug=? AND completed=1 AND winning_side != ''",
                (window.slug,),
            ).fetchone()
            if existing is not None:
                self._watchlist_settlement_backfilled.add(window.slug)
                continue
            windows[window.slug] = window
        for window in windows.values():
            data = await asyncio.to_thread(fetch_crypto_price_api, window)
            if data is None or data.get("openPrice") is None or data.get("closePrice") is None:
                continue
            open_price = float(data["openPrice"])
            close_price = float(data["closePrice"])
            winning_side = "Up" if close_price >= open_price else "Down"
            completed = bool(data.get("completed"))
            changed = self.store.upsert_market_settlement(
                {
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": window.symbol,
                    "winning_side": winning_side,
                    "settlement_open_price": compact_float(open_price, 6),
                    "settlement_close_price": compact_float(close_price, 6),
                    "settled_at": now.isoformat(),
                    "completed": completed,
                }
            )
            if completed:
                self._watchlist_settlement_backfilled.add(window.slug)
            if changed:
                self.writer.write({
                    "event": "watchlist_activity_settlement_backfill",
                    "observed_at": now.isoformat(),
                    "symbol": window.symbol,
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "settlement_open_price": compact_float(open_price, 6),
                    "settlement_close_price": compact_float(close_price, 6),
                    "winning_side": winning_side,
                    "settlement_completed": completed,
                    "settlement_cached": bool(data.get("cached")),
                })

    def _activity_window_from_slug(self, event: dict[str, Any]) -> MarketWindow | None:
        slug = str(event.get("market_slug") or "")
        marker = "-updown-5m-"
        if marker not in slug:
            return None
        prefix, raw_epoch = slug.rsplit(marker, 1)
        try:
            start_epoch = int(raw_epoch)
        except ValueError:
            return None
        if start_epoch < 1_600_000_000:
            return None
        symbol = str(event.get("symbol") or prefix).upper()
        start_time = dt.datetime.fromtimestamp(start_epoch, dt.timezone.utc)
        end_time = start_time + dt.timedelta(seconds=300)
        return MarketWindow(
            symbol=symbol,
            slug=slug,
            condition_id=str(event.get("condition_id") or ""),
            question="",
            up_token="",
            down_token="",
            start_time=start_time,
            end_time=end_time,
        )

    def _activity_value_warning(self, event: dict[str, Any]) -> dict[str, Any] | None:
        activity_type = str(event.get("activity_type") or "").upper()
        if activity_type not in {"SPLIT", "MERGE", "REDEEM"}:
            return None
        size = float(event.get("size") or 0.0)
        usdc = float(event.get("usdc") or 0.0)
        delta = abs(usdc - size)
        if delta <= 0.01:
            return None
        return {
            "event": "watchlist_activity_value_warning",
            "observed_at": event.get("observed_at") or dt.datetime.now(dt.timezone.utc).isoformat(),
            "wallet": str(event.get("wallet") or "").lower(),
            "activity_type": activity_type,
            "market_slug": event.get("market_slug"),
            "condition_id": event.get("condition_id"),
            "exchange_ts": event.get("exchange_ts"),
            "size": size,
            "usdc": usdc,
            "delta": round(delta, 6),
            "tx_hash": event.get("tx_hash"),
            "message": "activity cashflow invariant mismatch: expected usdcSize to match size",
        }

    def _sample_market_state_each_cycle(self, *, now: dt.datetime | None = None) -> bool:
        now = now or dt.datetime.now(dt.timezone.utc)
        rows: list[dict[str, Any]] = []
        for window in self.windows.values():
            row = self._market_state_sample_row(window, now=now)
            if row is None:
                continue
            rows.append(row)
        if not rows:
            return False
        self.store.insert_market_state_samples(rows)
        return True

    def _market_state_sample_row(self, window: MarketWindow, *, now: dt.datetime) -> dict[str, Any] | None:
        up_bids, up_asks, up_age = self.stream.get_book(window.up_token, max_age_sec=self.config.book_max_age_sec)
        down_bids, down_asks, down_age = self.stream.get_book(window.down_token, max_age_sec=self.config.book_max_age_sec)
        up = token_book_summary(bids=up_bids, asks=up_asks, book_age_ms=up_age)
        down = token_book_summary(bids=down_bids, asks=down_asks, book_age_ms=down_age)
        feed = self.feeds.get(window.symbol)
        reference_price = compact_float(feed.latest_price if feed else None, 6)
        reference_age = compact_float(feed.latest_age_sec() if feed else None, 3)
        signature = (
            up.get("bid"),
            up.get("ask"),
            up.get("spread"),
            down.get("bid"),
            down.get("ask"),
            down.get("spread"),
            reference_price,
        )
        last = self._last_market_state_sample.get(window.slug)
        remaining_sec = (window.end_time - now).total_seconds()
        watched = window.slug in self._active_watched_slugs(now=now)
        if watched:
            cadence = (
                self.config.watched_market_state_terminal_sample_sec
                if remaining_sec <= self.config.market_state_terminal_window_sec
                else self.config.watched_market_state_sample_sec
            )
            heartbeat = self.config.watched_market_state_heartbeat_sec
        else:
            cadence = (
                self.config.market_state_terminal_sample_sec
                if remaining_sec <= self.config.market_state_terminal_window_sec
                else self.config.market_state_sample_sec
            )
            heartbeat = self.config.market_state_heartbeat_sec
        reason = "initial"
        if last is not None:
            last_time, last_signature = last
            elapsed = (now - last_time).total_seconds()
            if elapsed < cadence:
                return None
            if signature != last_signature:
                reason = "watched_changed" if watched else "changed"
            elif elapsed >= heartbeat:
                reason = "watched_heartbeat" if watched else "heartbeat"
            else:
                return None
        max_age_ms = self.config.book_max_age_sec * 1000.0
        book_stale = (
            up_age is None
            or down_age is None
            or float(up_age) > max_age_ms
            or float(down_age) > max_age_ms
            or up.get("bid") is None
            or up.get("ask") is None
            or down.get("bid") is None
            or down.get("ask") is None
        )
        self._last_market_state_sample[window.slug] = (now, signature)
        return {
            "market_slug": window.slug,
            "condition_id": window.condition_id,
            "symbol": window.symbol,
            "sampled_ts": int(now.timestamp()),
            "observed_at": now.isoformat(),
            "window_remaining_sec": compact_float(remaining_sec, 3),
            "reference_price": reference_price,
            "reference_price_age_sec": reference_age,
            "up_json": up,
            "down_json": down,
            "book_stale": book_stale,
            "sample_reason": reason,
        }

    def _parse_iso_dt(self, value: Any) -> dt.datetime | None:
        if not value:
            return None
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)

    def _trade_from_activity_event(self, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "tx_hash": str(event.get("tx_hash") or ""),
            "fill_id": str(event.get("fill_id") or f"activity:{event.get('activity_type') or ''}:{event.get('outcome_index')}"),
            "wallet": str(event.get("wallet") or "").lower(),
            "market_slug": str(event.get("market_slug") or ""),
            "condition_id": str(event.get("condition_id") or ""),
            "symbol": str(event.get("symbol") or "").upper(),
            "exchange_ts": int(event.get("exchange_ts") or 0),
            "outcome": str(event.get("outcome") or ""),
            "side": str(event.get("side") or "").upper(),
            "price": float(event.get("price") or 0.0),
            "size": float(event.get("size") or 0.0),
            "usdc": float(event.get("usdc") or 0.0),
            "name": str(event.get("name") or ""),
            "pseudonym": str(event.get("pseudonym") or ""),
        }

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
            watchlist_set = set(self.store.watchlist_wallets())
            if score.status != "active_candidate" and score.wallet not in watchlist_set:
                self._purge_rejected_wallet(score.wallet, score)
                continue
            if should_persist_score(
                score,
                previous_status=previous_status,
            ) or score.wallet in watchlist_set:
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

    def _purge_rejected_wallet(self, wallet: str, score) -> None:
        result = self.store.purge_wallet_data(wallet, preserve_watchlist=True)
        self._metrics_cache.pop(wallet.lower(), None)
        self._last_score_event_state.pop(wallet.lower(), None)
        if any(value for value in result.values()):
            self.writer.write(
                {
                    "event": "rejected_wallet_purged",
                    "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "wallet": wallet.lower(),
                    "status": score.status,
                    "rank_score": score.rank_score,
                    "reasons": score.reasons[:5],
                    **result,
                }
            )

    def _prune_candidate_tables(self) -> int:
        removed = 0
        removed += self.store.prune_low_sample_archives(min_age_seconds=self.config.archive_revival_cooldown_sec)
        removed += self.store.prune_candidate_scores("active_candidate", max_rows=self.config.max_active_candidates)
        removed += self.store.prune_candidate_scores("dormant_candidate", max_rows=0)
        archive_budget = 0
        removed += self.store.prune_archive_scores(
            max_archive=archive_budget,
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
            "max_trades_per_market_24h",
            "max_trades_per_market_7d",
            "max_trades_per_market_30d",
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
            "activity_ledger_markets_7d",
            "activity_ledger_markets_30d",
            "activity_ledger_markets_total",
            "merge_or_split_markets_7d",
            "merge_or_split_markets_30d",
            "merge_or_split_markets_total",
            "historical_trades",
            "historical_markets",
            "historical_pnl",
            "last_active_age_hours",
            "observed_span_hours",
        }
        for key in local_fields:
            if key in local_metrics:
                target = "local_observed_span_hours" if key == "observed_span_hours" else f"local_observed_{key}"
                metrics[target] = local_metrics[key]
        local_span_hours = float(local_metrics.get("observed_span_hours") or 0.0)
        if int(local_metrics.get("settled_markets_7d") or 0) > 0 and local_span_hours >= 24.0:
            metrics["wins_7d"] = local_metrics.get("wins_7d", 0)
            metrics["losses_7d"] = local_metrics.get("losses_7d", 0)
        metrics["local_observed_pnl_source"] = local_metrics.get("pnl_source", "local_observed")
        if int(local_metrics.get("activity_ledger_markets_total") or 0) > 0:
            metrics.setdefault("profile_reference_pnl_7d", api_metrics.get("pnl_7d"))
            metrics.setdefault("profile_reference_pnl_30d", api_metrics.get("pnl_30d"))
            metrics.setdefault("profile_reference_historical_pnl", api_metrics.get("historical_pnl"))
        return metrics

    def _apply_local_observed_24h_override(self, metrics: dict[str, Any]) -> None:
        api_markets = float(metrics.get("markets_24h") or 0)
        local_markets = float(metrics.get("local_observed_markets_24h") or 0)
        if local_markets > api_markets:
            metrics["markets_24h"] = int(local_markets)
            metrics["trades_24h"] = int(metrics.get("local_observed_trades_24h") or 0)
            metrics["btc_markets_24h"] = int(metrics.get("local_observed_btc_markets_24h") or 0)
            metrics["eth_markets_24h"] = int(metrics.get("local_observed_eth_markets_24h") or 0)
            metrics["markets_24h_source"] = "local_observed"
            metrics["markets_24h_lower_bound"] = False
        else:
            metrics.setdefault("markets_24h_source", "api_sample")

    def _metrics_cache_ttl(self, previous_status: str | None, wallet: str | None = None) -> float:
        if wallet and wallet.lower() in self._watchlist_wallets:
            return self.config.active_metrics_ttl_sec
        if previous_status == "archive_candidate":
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
            due_dormant: set[str] = set()
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
                    self.store.high_activity_wallets_24h(
                        now_ts=int(now.timestamp()),
                        limit=self.config.score_wallet_pool_limit,
                    )
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
        if not batch and watchlist_wallets:
            start = self._watchlist_score_cursor % len(watchlist_wallets)
            take = min(budget, len(watchlist_wallets))
            batch.extend(watchlist_wallets[(start + idx) % len(watchlist_wallets)] for idx in range(take))
            self._watchlist_score_cursor = (start + take) % len(watchlist_wallets)
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
        activity_cleanup = self.store.cleanup_wallet_activity_events(
            watchlist_cutoff_ts=int((now - dt.timedelta(days=self.config.watchlist_activity_retention_days)).timestamp()),
            non_watchlist_cutoff_ts=int((now - dt.timedelta(days=self.config.non_watchlist_activity_retention_days)).timestamp()),
        )
        result.update(activity_cleanup)
        hot_cleanup = self.store.cleanup_hot_research_rows(
            cutoff_ts=int((now - dt.timedelta(days=self.config.watchlist_activity_retention_days)).timestamp()),
        )
        result.update(hot_cleanup)
        research_cleanup = self.store.cleanup_non_focus_research_data(
            dormant_limit=self.config.research_cleanup_dormant_wallets
        )
        result.update(research_cleanup)
        compact_result = self.store.compact_database_if_needed()
        result.update(compact_result)
        cleanup_changed = any(
            value
            for key, value in result.items()
            if key != "research_cleanup_keep_wallets"
        )
        if cleanup_changed:
            self.writer.write({
                "event": "sqlite_cleanup",
                "observed_at": now.isoformat(),
                "inactive_wallet_ttl_hours": self.config.inactive_wallet_ttl_hours,
                "max_non_candidate_wallets": self.config.max_non_candidate_wallets,
                "research_cleanup_dormant_wallets": self.config.research_cleanup_dormant_wallets,
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

    def _archive_strategy_data_if_due(self) -> None:
        interval_sec = max(0.0, self.config.strategy_archive_interval_hours) * 3600.0
        if interval_sec <= 0:
            return
        if time.monotonic() - self._last_strategy_archive < interval_sec:
            return
        self._last_strategy_archive = time.monotonic()
        now = dt.datetime.now(dt.timezone.utc)
        result = self.store.archive_strategy_rows(
            self.config.data_dir / "archive",
            activity_cutoff_ts=int((now - dt.timedelta(days=self.config.watchlist_activity_retention_days)).timestamp()),
            context_cutoff_ts=int((now - dt.timedelta(days=self.config.context_retention_days)).timestamp()),
            sample_cutoff_ts=int((now - dt.timedelta(days=self.config.market_state_retention_days)).timestamp()),
        )
        if any(result.values()):
            self.writer.write({"event": "strategy_archive", "observed_at": now.isoformat(), **result})

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
        self._watchlist_wallets = watchlist_wallets
        self._active_snapshot_wallets = set(
            self.store.candidate_wallets("active_candidate", limit=self.config.max_active_candidates)
        ) | watchlist_wallets
        if not self._last_score_event_state:
            rows = self.store.conn.execute("SELECT wallet, status, rank_score FROM candidate_scores").fetchall()
            self._last_score_event_state = {
                str(row["wallet"]).lower(): (str(row["status"]), float(row["rank_score"] or 0.0))
                for row in rows
            }
