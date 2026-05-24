from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from typing import Any

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None

CLOB_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class ClobBookStream:
    def __init__(self, *, idle_reconnect_sec: float = 20.0) -> None:
        self.idle_reconnect_sec = idle_reconnect_sec
        self._tokens: list[str] = []
        self._books: dict[str, dict[str, Any]] = {}
        self._running = False
        self._ws = None
        self._recv_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._last_message_at = 0.0
        self._event_counts: Counter[str] = Counter()

    async def connect(self, token_ids: list[str]) -> None:
        if websockets is None:
            raise RuntimeError("websockets package is required for CLOB market stream")
        self._tokens = list(token_ids)
        self._running = True
        await self._connect_once()
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def switch_tokens(self, token_ids: list[str]) -> None:
        self._tokens = list(token_ids)
        self._books.clear()
        if self._ws is not None:
            await self._reconnect()

    async def close(self) -> None:
        self._running = False
        tasks = [task for task in (self._recv_task, self._ping_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._ws is not None:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=3.0)
            except Exception:
                pass
            self._ws = None

    def get_book(self, token_id: str, *, max_age_sec: float | None = None) -> tuple[list[tuple[float, float]], list[tuple[float, float]], int | None]:
        book = self._books.get(token_id)
        if book is None:
            return [], [], None
        age_sec = time.monotonic() - float(book["received_at"])
        if max_age_sec is not None and age_sec > max_age_sec:
            return [], [], round(age_sec * 1000)
        bids = sorted(book["bids"].items(), key=lambda pair: pair[0], reverse=True)
        asks = sorted(book["asks"].items(), key=lambda pair: pair[0])
        return bids, asks, round(age_sec * 1000)

    def diagnostics(self, *, reset_counts: bool = False) -> dict[str, Any]:
        age_ms = None if self._last_message_at <= 0 else round((time.monotonic() - self._last_message_at) * 1000)
        row = {"last_message_age_ms": age_ms, "subscribed_tokens": len(self._tokens), "event_counts": dict(self._event_counts)}
        if reset_counts:
            self._event_counts.clear()
        return row

    async def _connect_once(self) -> None:
        self._ws = await websockets.connect(CLOB_MARKET_WS_URL)
        await self._subscribe()
        self._last_message_at = time.monotonic()

    async def _subscribe(self) -> None:
        await self._ws.send(json.dumps({
            "type": "market",
            "assets_ids": self._tokens,
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }))

    async def _reconnect(self) -> None:
        if self._ws is not None:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=3.0)
            except Exception:
                pass
            self._ws = None
        self._books.clear()
        await self._connect_once()

    async def _ping_loop(self) -> None:
        while self._running:
            await asyncio.sleep(10.0)
            if self._ws is None:
                continue
            try:
                await self._ws.send("{}")
            except Exception:
                await self._reconnect()

    async def _recv_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                if self._ws is None:
                    await self._reconnect()
                raw = await asyncio.wait_for(self._ws.recv(), timeout=self.idle_reconnect_sec)
                self._last_message_at = time.monotonic()
                self._dispatch(raw)
                backoff = 1.0
            except asyncio.TimeoutError:
                await self._reconnect()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._running:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                    self._ws = None

    def _dispatch(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        events = data if isinstance(data, list) else [data]
        for event in events:
            if isinstance(event, dict):
                self._handle_event(event)

    def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or "")
        self._event_counts[event_type or "missing"] += 1
        if event_type == "book":
            self._handle_book(event)
        elif event_type == "price_change":
            self._handle_price_change(event)

    @staticmethod
    def _parse_side(levels: list[dict[str, Any]], *, reverse: bool) -> dict[float, float]:
        parsed: list[tuple[float, float]] = []
        for item in levels:
            try:
                price = float(item.get("price"))
                size = float(item.get("size", 0))
            except (AttributeError, TypeError, ValueError):
                continue
            if price > 0 and size > 0:
                parsed.append((price, size))
        if len(parsed) > 1 and not _is_sorted_prices(parsed, reverse=reverse):
            parsed.sort(key=lambda pair: pair[0], reverse=reverse)
        return dict(parsed)

    def _handle_book(self, event: dict[str, Any]) -> None:
        token = str(event.get("asset_id") or "")
        if not token:
            return
        self._books[token] = {
            "bids": self._parse_side(event.get("bids", []), reverse=True),
            "asks": self._parse_side(event.get("asks", []), reverse=False),
            "received_at": time.monotonic(),
        }

    def _handle_price_change(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or ([event] if event.get("price") else [])
        for change in changes:
            token = str(change.get("asset_id") or "")
            book = self._books.get(token)
            if not book:
                continue
            try:
                price = float(change.get("price"))
                size = float(change.get("size"))
            except (TypeError, ValueError):
                continue
            side_key = "bids" if change.get("side") == "BUY" else "asks" if change.get("side") == "SELL" else None
            if side_key is None:
                continue
            if size > 0:
                book[side_key][price] = size
            else:
                book[side_key].pop(price, None)
            book["received_at"] = time.monotonic()


def _is_sorted_prices(levels: list[tuple[float, float]], *, reverse: bool) -> bool:
    if len(levels) < 2:
        return True
    prices = [price for price, _size in levels]
    if reverse:
        return all(left >= right for left, right in zip(prices, prices[1:]))
    return all(left <= right for left, right in zip(prices, prices[1:]))
