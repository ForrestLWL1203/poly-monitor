from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Iterable

from .book import token_book_summary
from .clob_stream import ClobBookStream
from .market import MarketSeries, MarketWindow, fetch_market_by_slug, find_current_or_next_window, build_window
from .price_feed import ChainlinkPriceHub
from .strategy_runtime import BookSnapshot, StrategySnapshot, utc_iso


def _following_window(window: MarketWindow) -> MarketWindow | None:
    series = MarketSeries.from_symbol(window.symbol)
    for offset in range(4):
        slug = series.epoch_to_slug(window.end_epoch + offset * series.slug_step)
        raw = fetch_market_by_slug(slug)
        if raw is None or raw.get("closed"):
            continue
        candidate = build_window(raw, series)
        if candidate is not None and candidate.start_epoch >= window.end_epoch:
            return candidate
    return None


class LivePaperEnvironment:
    def __init__(
        self,
        *,
        symbols: Iterable[str] = ("BTC", "ETH"),
        sample_targets: tuple[float, ...] = (5.0, 25.0, 100.0),
        book_max_age_sec: float = 3.0,
        window_finder: Callable[..., MarketWindow | None] | None = None,
        following_window_finder: Callable[[MarketWindow], MarketWindow | None] | None = None,
        stream: Any | None = None,
        price_hub: Any | None = None,
    ) -> None:
        self.symbols = tuple(str(symbol).upper() for symbol in symbols)
        self.sample_targets = sample_targets
        self.book_max_age_sec = book_max_age_sec
        self.window_finder = window_finder or self._default_window_finder
        self.following_window_finder = following_window_finder or _following_window
        self.stream = stream or ClobBookStream()
        self.price_hub = price_hub or ChainlinkPriceHub([f"{symbol}/USD" for symbol in self.symbols], max_history_sec=120.0)
        self.windows: dict[str, MarketWindow] = {}
        self._started = False

    def _default_window_finder(self, symbol: str, now: dt.datetime | None = None) -> MarketWindow | None:
        return find_current_or_next_window(MarketSeries.from_symbol(symbol), now=now)

    async def start(self) -> None:
        await self.price_hub.start()
        await self.refresh_windows(force=True)
        tokens = self._tokens()
        if tokens:
            await self.stream.connect(tokens)
        self._started = True

    async def close(self) -> None:
        await self.stream.close()
        await self.price_hub.stop()
        self._started = False

    def _tokens(self) -> list[str]:
        return [token for window in self.windows.values() for token in (window.up_token, window.down_token) if token]

    async def refresh_windows(self, *, force: bool = False, now: dt.datetime | None = None) -> bool:
        windows: dict[str, MarketWindow] = {}
        for symbol in self.symbols:
            window = self.window_finder(symbol, now=now)
            if window is not None:
                windows[window.symbol] = window
        changed = {window.slug for window in windows.values()} != {window.slug for window in self.windows.values()}
        self.windows = windows
        if self._started and changed:
            tokens = self._tokens()
            if tokens:
                await self.stream.switch_tokens(tokens)
        return changed

    async def roll_window_if_needed(self, *, now: dt.datetime | None = None) -> bool:
        now_dt = now or dt.datetime.now(dt.timezone.utc)
        changed = False
        for symbol, window in list(self.windows.items()):
            if now_dt < window.end_time:
                continue
            next_window = self.following_window_finder(window)
            if next_window is not None:
                self.windows[symbol] = next_window
                changed = True
        if changed:
            tokens = self._tokens()
            if tokens:
                await self.stream.switch_tokens(tokens)
        return changed

    def snapshot(self, *, now: dt.datetime | None = None) -> list[StrategySnapshot]:
        now_dt = now or dt.datetime.now(dt.timezone.utc)
        return [self._snapshot_for_window(window, now=now_dt) for window in self.windows.values()]

    def _book(self, token_id: str) -> tuple[BookSnapshot, bool]:
        bids, asks, age_ms = self.stream.get_book(token_id, max_age_sec=self.book_max_age_sec)
        summary = token_book_summary(bids=bids, asks=asks, book_age_ms=age_ms, targets=self.sample_targets)
        stale = summary.get("bid") is None or summary.get("ask") is None
        return BookSnapshot.from_value(summary), stale

    def _snapshot_for_window(self, window: MarketWindow, *, now: dt.datetime) -> StrategySnapshot:
        up, up_stale = self._book(window.up_token)
        down, down_stale = self._book(window.down_token)
        feed = self.price_hub.feed(f"{window.symbol}/USD")
        return StrategySnapshot(
            market_slug=window.slug,
            condition_id=window.condition_id,
            symbol=window.symbol,
            sampled_ts=int(now.timestamp()),
            observed_at=utc_iso(now),
            window_start_ts=window.start_epoch,
            window_end_ts=window.end_epoch,
            elapsed_sec=max(0, int((now - window.start_time).total_seconds())),
            remaining_sec=max(0.0, (window.end_time - now).total_seconds()),
            reference_price=getattr(feed, "latest_price", None),
            reference_price_age_sec=feed.latest_age_sec(),
            up=up,
            down=down,
            book_stale=up_stale or down_stale,
            sample_reason="live_paper",
        )
