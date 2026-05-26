from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .book import compact_float, token_book_summary
from .clob_stream import ClobBookStream
from .data_api import AsyncDataApiClient, normalize_activity_event
from .market import MarketSeries, MarketWindow, find_current_or_next_window
from .observer import context_snapshot
from .price_feed import ChainlinkPriceHub
from .storage import ObserverStore


DEEP_COLLECTOR_SCRIPT = "scripts/run_wallet_deep_collector.py"
DEFAULT_SAMPLE_SEC = 1.0
DEFAULT_BOOK_DEPTH_LEVELS = 3
DEFAULT_HEARTBEAT_SEC = 5.0
DEFAULT_MAX_ACTIVE_COLLECTORS = 3
STALE_HEARTBEAT_SEC = 20.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def normalize_wallet(wallet: str) -> str:
    return str(wallet or "").lower().strip()


def status_dir(data_dir: Path) -> Path:
    return data_dir / "state" / "deep_collectors"


def status_path(data_dir: Path, wallet: str) -> Path:
    return status_dir(data_dir) / f"{normalize_wallet(wallet)}.json"


def write_status(data_dir: Path, wallet: str, payload: dict[str, Any]) -> None:
    target_dir = status_dir(data_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = status_path(data_dir, wallet)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_status(data_dir: Path, wallet: str) -> dict[str, Any] | None:
    path = status_path(data_dir, wallet)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_iso(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def process_cmdline(pid: int) -> str | None:
    if pid <= 0:
        return None
    proc_cmdline = Path("/proc") / str(pid) / "cmdline"
    if proc_cmdline.exists():
        try:
            raw = proc_cmdline.read_bytes()
        except OSError:
            raw = b""
        text = raw.replace(b"\x00", b" ").decode(errors="replace").strip()
        if text:
            return text
    try:
        output = subprocess.check_output(["ps", "-p", str(pid), "-o", "args="], text=True)
    except (OSError, subprocess.SubprocessError):
        return None
    text = output.strip()
    return text or None


def process_matches_wallet(pid: int, wallet: str) -> bool:
    cmdline = process_cmdline(pid)
    if not cmdline:
        return False
    normalized = normalize_wallet(wallet)
    try:
        args = shlex.split(cmdline)
    except ValueError:
        args = cmdline.split()
    script_matches = any(arg.endswith(DEEP_COLLECTOR_SCRIPT) or Path(arg).name == Path(DEEP_COLLECTOR_SCRIPT).name for arg in args)
    if not script_matches:
        return False
    for idx, arg in enumerate(args):
        if arg == "--wallet" and idx + 1 < len(args) and normalize_wallet(args[idx + 1]) == normalized:
            return True
        if arg.startswith("--wallet=") and normalize_wallet(arg.split("=", 1)[1]) == normalized:
            return True
    return False


def collector_status(data_dir: Path, wallet: str, *, now: dt.datetime | None = None) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    payload = read_status(data_dir, normalized) or {}
    current = now or utc_now()
    pid = int(payload.get("pid") or 0)
    last_heartbeat = _parse_iso(payload.get("last_heartbeat_at"))
    heartbeat_age_sec = None
    if last_heartbeat is not None:
        heartbeat_age_sec = max(0.0, (current - last_heartbeat).total_seconds())
    pid_matches = process_matches_wallet(pid, normalized)
    healthy = bool(pid_matches and heartbeat_age_sec is not None and heartbeat_age_sec <= STALE_HEARTBEAT_SEC)
    if healthy:
        state = "running"
    elif pid_matches:
        state = "stale"
    elif payload:
        state = "stopped"
    else:
        state = "none"
    return {
        "wallet": normalized,
        "state": state,
        "running": healthy,
        "pid": pid if pid > 0 else None,
        "started_at": payload.get("started_at"),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "heartbeat_age_sec": round(heartbeat_age_sec, 3) if heartbeat_age_sec is not None else None,
        "sample_sec": payload.get("sample_sec"),
        "book_depth_levels": payload.get("book_depth_levels"),
    }


def active_collector_statuses(data_dir: Path) -> list[dict[str, Any]]:
    directory = status_dir(data_dir)
    if not directory.exists():
        return []
    statuses: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        wallet = path.stem.lower()
        status = collector_status(data_dir, wallet)
        if status.get("running"):
            statuses.append(status)
    return statuses


def start_collector(
    data_dir: Path,
    wallet: str,
    *,
    python: str | None = None,
    max_active_collectors: int = DEFAULT_MAX_ACTIVE_COLLECTORS,
) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    current = collector_status(data_dir, normalized)
    if current["running"]:
        return {"ok": True, "already_running": True, **current}
    active = active_collector_statuses(data_dir)
    if max_active_collectors >= 0 and len(active) >= max_active_collectors:
        return {
            "ok": False,
            "error": "too_many_collectors",
            "running": len(active),
            "max_active_collectors": max_active_collectors,
            "wallet": normalized,
        }
    root = Path(__file__).resolve().parents[1]
    script = root / DEEP_COLLECTOR_SCRIPT
    cmd = [
        python or sys.executable,
        str(script),
        "--wallet",
        normalized,
        "--data-dir",
        str(data_dir),
    ]
    logs_dir = data_dir / ".." / "logs"
    if str(data_dir).endswith("/data"):
        logs_dir = data_dir.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"deep_collector_{normalized}.log"
    log = log_path.open("ab")
    try:
        proc = subprocess.Popen(cmd, cwd=str(root), stdout=log, stderr=subprocess.STDOUT, close_fds=True)
    finally:
        log.close()
    now = utc_now().isoformat()
    payload = {
        "wallet": normalized,
        "pid": proc.pid,
        "status": "running",
        "started_at": now,
        "last_heartbeat_at": now,
        "sample_sec": DEFAULT_SAMPLE_SEC,
        "book_depth_levels": DEFAULT_BOOK_DEPTH_LEVELS,
        "command": cmd,
        "log_path": str(log_path),
    }
    write_status(data_dir, normalized, payload)
    return {"ok": True, "already_running": False, **collector_status(data_dir, normalized)}


def stop_collector(data_dir: Path, wallet: str) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    status = collector_status(data_dir, normalized)
    pid = int(status.get("pid") or 0)
    stopped = False
    if pid > 0 and process_matches_wallet(pid, normalized):
        try:
            os.kill(pid, signal.SIGTERM)
            stopped = True
        except ProcessLookupError:
            stopped = False
    payload = read_status(data_dir, normalized) or {"wallet": normalized}
    payload["status"] = "stopped"
    payload["stopped_at"] = utc_now().isoformat()
    write_status(data_dir, normalized, payload)
    return {"ok": True, "stopped": stopped, **collector_status(data_dir, normalized)}


def l3_book_summary(
    *,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    book_age_ms: int | None,
    depth_levels: int = DEFAULT_BOOK_DEPTH_LEVELS,
) -> dict[str, Any]:
    summary = token_book_summary(
        bids=bids,
        asks=asks,
        book_age_ms=book_age_ms,
        targets=(5.0, 25.0),
        depth_levels=depth_levels,
    )
    summary["bids"] = [[compact_float(price), compact_float(size)] for price, size in bids[:depth_levels]]
    summary["asks"] = [[compact_float(price), compact_float(size)] for price, size in asks[:depth_levels]]
    return summary


@dataclass
class WalletDeepCollectorConfig:
    wallet: str
    data_dir: Path
    symbols: tuple[str, ...] = ("BTC", "ETH")
    sample_sec: float = DEFAULT_SAMPLE_SEC
    heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC
    book_depth_levels: int = DEFAULT_BOOK_DEPTH_LEVELS
    activity_poll_sec: float = 1.0
    activity_lookback_sec: int = 600
    activity_pages: int = 2
    book_max_age_sec: float = 3.0


class WalletDeepCollector:
    def __init__(self, config: WalletDeepCollectorConfig) -> None:
        self.config = config
        self.wallet = normalize_wallet(config.wallet)
        self.store = ObserverStore(config.data_dir / "state" / "observer.sqlite")
        self.data_api = AsyncDataApiClient()
        self.stream = ClobBookStream()
        self.price_hub = ChainlinkPriceHub(config.symbols, max_history_sec=120.0)
        self.windows: dict[str, MarketWindow] = {}
        self._last_activity_poll = 0.0
        self._last_sample = 0.0
        self._last_window_refresh = 0.0
        self._last_heartbeat = 0.0
        self._running = True

    async def run(self, *, seconds: float | None = None) -> int:
        start = time.monotonic()
        self._write_heartbeat()
        await self.price_hub.start()
        try:
            await self._refresh_windows(force=True)
            while self._running:
                if seconds is not None and time.monotonic() - start >= seconds:
                    break
                await self._refresh_windows()
                await self._poll_activity_if_due()
                self._sample_if_due()
                self._heartbeat_if_due()
                await asyncio.sleep(0.1)
        finally:
            await self.stream.close()
            await self.price_hub.stop()
            self.store.close()
        return 0

    async def _refresh_windows(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_window_refresh < 15.0:
            return
        self._last_window_refresh = now
        windows: dict[str, MarketWindow] = {}
        for symbol in self.config.symbols:
            window = await asyncio.to_thread(find_current_or_next_window, MarketSeries.from_symbol(symbol))
            if window is not None:
                windows[window.slug] = window
                self.store.upsert_market_window(
                    symbol=window.symbol,
                    market_slug=window.slug,
                    condition_id=window.condition_id,
                    window_start=window.start_time.isoformat(),
                    window_end=window.end_time.isoformat(),
                )
        tokens = [token for window in windows.values() for token in (window.up_token, window.down_token) if token]
        if tokens and {window.slug for window in windows.values()} != set(self.windows):
            if self.windows:
                await self.stream.switch_tokens(tokens)
            else:
                await self.stream.connect(tokens)
        self.windows = windows

    async def _poll_activity_if_due(self) -> None:
        if time.monotonic() - self._last_activity_poll < self.config.activity_poll_sec:
            return
        self._last_activity_poll = time.monotonic()
        now_ts = int(utc_now().timestamp())
        last_seen = self.store.last_wallet_activity_ts(self.wallet)
        start_ts = max(now_ts - self.config.activity_lookback_sec, last_seen - 30) if last_seen else now_ts - self.config.activity_lookback_sec
        observed_at = utc_now().isoformat()
        normalized: list[dict[str, Any]] = []
        page_size = 500
        for page in range(max(1, self.config.activity_pages)):
            rows = await self.data_api.fetch_user_activity(
                self.wallet,
                limit=page_size,
                offset=page * page_size,
                start=start_ts,
                end=now_ts + 30,
            )
            if not rows:
                break
            for raw in rows:
                activity_type = str(raw.get("type") or "").upper()
                if activity_type not in {"TRADE", "MERGE", "REDEEM", "SPLIT"}:
                    continue
                event = normalize_activity_event(raw, wallet=self.wallet, observed_at=observed_at)
                if event["symbol"] not in self.config.symbols or "-updown-5m-" not in event["market_slug"]:
                    continue
                normalized.append(event)
            if len(rows) < page_size:
                break
        inserted = self.store.insert_wallet_activity_events(normalized, recompute=False)
        if inserted:
            self.store.insert_trades([self._trade_from_activity(event) for event in inserted if event.get("activity_type") == "TRADE"])
            contexts = self._context_rows(inserted)
            if contexts:
                self.store.insert_wallet_trade_contexts(contexts)
            self.store.recompute_market_pnl_for_markets({str(row.get("market_slug") or "") for row in inserted})

    def _sample_if_due(self) -> None:
        if time.monotonic() - self._last_sample < self.config.sample_sec:
            return
        self._last_sample = time.monotonic()
        now = utc_now()
        rows = [row for row in (self._sample_row(window, now=now) for window in self.windows.values()) if row is not None]
        if rows:
            self.store.insert_market_state_samples(rows)

    def _sample_row(self, window: MarketWindow, *, now: dt.datetime) -> dict[str, Any] | None:
        up_bids, up_asks, up_age = self.stream.get_book(window.up_token, max_age_sec=self.config.book_max_age_sec)
        down_bids, down_asks, down_age = self.stream.get_book(window.down_token, max_age_sec=self.config.book_max_age_sec)
        up = l3_book_summary(bids=up_bids, asks=up_asks, book_age_ms=up_age, depth_levels=self.config.book_depth_levels)
        down = l3_book_summary(bids=down_bids, asks=down_asks, book_age_ms=down_age, depth_levels=self.config.book_depth_levels)
        feed = self.price_hub.feed(window.symbol)
        book_stale = up.get("bid") is None or up.get("ask") is None or down.get("bid") is None or down.get("ask") is None
        return {
            "market_slug": window.slug,
            "condition_id": window.condition_id,
            "symbol": window.symbol,
            "sampled_ts": int(now.timestamp()),
            "observed_at": now.isoformat(),
            "window_remaining_sec": compact_float((window.end_time - now).total_seconds(), 3),
            "reference_price": compact_float(feed.latest_price, 6),
            "reference_price_age_sec": compact_float(feed.latest_age_sec(), 3),
            "up_json": up,
            "down_json": down,
            "book_stale": book_stale,
            "sample_reason": "deep_collector",
        }

    def _context_rows(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        by_slug = {window.slug: window for window in self.windows.values()}
        for event in events:
            if str(event.get("activity_type") or "").upper() != "TRADE":
                continue
            window = by_slug.get(str(event.get("market_slug") or ""))
            if window is None:
                continue
            trade = self._trade_from_activity(event)
            context = context_snapshot(
                trade=trade,
                window=window,
                stream=self.stream,
                feed=self.price_hub.feed(window.symbol),
                targets=(5.0, 25.0),
                max_book_age_sec=self.config.book_max_age_sec,
            )
            rows.append(
                {
                    "wallet": self.wallet,
                    "tx_hash": trade["tx_hash"],
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

    def _trade_from_activity(self, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "tx_hash": str(event.get("tx_hash") or ""),
            "fill_id": str(event.get("fill_id") or ""),
            "wallet": self.wallet,
            "market_slug": str(event.get("market_slug") or ""),
            "condition_id": str(event.get("condition_id") or ""),
            "symbol": str(event.get("symbol") or "").upper(),
            "exchange_ts": int(event.get("exchange_ts") or 0),
            "outcome": str(event.get("outcome") or ""),
            "side": str(event.get("side") or "").upper(),
            "price": float(event.get("price") or 0.0),
            "size": float(event.get("size") or 0.0),
            "usdc": float(event.get("usdc") or 0.0),
        }

    def _heartbeat_if_due(self) -> None:
        if time.monotonic() - self._last_heartbeat >= self.config.heartbeat_sec:
            self._write_heartbeat()

    def _write_heartbeat(self) -> None:
        now = utc_now().isoformat()
        existing = read_status(self.config.data_dir, self.wallet) or {}
        payload = {
            **existing,
            "wallet": self.wallet,
            "pid": os.getpid(),
            "status": "running",
            "started_at": existing.get("started_at") or now,
            "last_heartbeat_at": now,
            "sample_sec": self.config.sample_sec,
            "book_depth_levels": self.config.book_depth_levels,
        }
        write_status(self.config.data_dir, self.wallet, payload)
        self._last_heartbeat = time.monotonic()
