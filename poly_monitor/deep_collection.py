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
import traceback
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


DEEP_LEGACY_COLLECTOR_SCRIPT = "scripts/run_wallet_deep_collector.py"
DEEP_MULTI_COLLECTOR_SCRIPT = "scripts/run_multi_wallet_deep_collector.py"
DEEP_MULTI_STATUS_WALLET = "multi_wallet"
DEFAULT_SAMPLE_SEC = 1.0
DEFAULT_BOOK_DEPTH_LEVELS = 3
DEFAULT_HEARTBEAT_SEC = 5.0
STALE_HEARTBEAT_SEC = 20.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def normalize_wallet(wallet: str) -> str:
    return str(wallet or "").lower().strip()


def status_dir(data_dir: Path) -> Path:
    return data_dir / "state" / "deep_collectors"


def status_path(data_dir: Path, wallet: str) -> Path:
    name = "_group" if normalize_wallet(wallet) == DEEP_MULTI_STATUS_WALLET else normalize_wallet(wallet)
    return status_dir(data_dir) / f"{name}.json"


def deep_list_path(data_dir: Path) -> Path:
    return data_dir / "state" / "deep_collection_wallets.json"


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


def read_deep_wallets(data_dir: Path) -> list[str]:
    path = deep_list_path(data_dir)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    raw_wallets = payload.get("wallets") if isinstance(payload, dict) else payload
    if not isinstance(raw_wallets, list):
        return []
    seen: set[str] = set()
    wallets: list[str] = []
    for wallet in raw_wallets:
        normalized = normalize_wallet(str(wallet))
        if normalized and normalized not in seen:
            seen.add(normalized)
            wallets.append(normalized)
    return wallets


def write_deep_wallets(data_dir: Path, wallets: list[str]) -> None:
    path = deep_list_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = []
    seen: set[str] = set()
    for wallet in wallets:
        item = normalize_wallet(wallet)
        if item and item not in seen:
            seen.add(item)
            normalized.append(item)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({"wallets": normalized, "updated_at": utc_now().isoformat()}, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def install_stop_signal_handlers(stop_callback) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_callback)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda _signum, _frame: stop_callback())


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
    script_name = Path(DEEP_LEGACY_COLLECTOR_SCRIPT).name
    multi_script_name = Path(DEEP_MULTI_COLLECTOR_SCRIPT).name
    script_matches = any(
        arg.endswith(DEEP_LEGACY_COLLECTOR_SCRIPT)
        or Path(arg).name == script_name
        or arg.endswith(DEEP_MULTI_COLLECTOR_SCRIPT)
        or Path(arg).name == multi_script_name
        for arg in args
    )
    if not script_matches:
        return False
    matched_wallets: set[str] = set()
    for idx, arg in enumerate(args):
        if arg == "--wallet" and idx + 1 < len(args) and normalize_wallet(args[idx + 1]) == normalized:
            return True
        if arg == "--wallet" and idx + 1 < len(args):
            matched_wallets.add(normalize_wallet(args[idx + 1]))
        if arg.startswith("--wallet=") and normalize_wallet(arg.split("=", 1)[1]) == normalized:
            return True
        if arg.startswith("--wallet="):
            matched_wallets.add(normalize_wallet(arg.split("=", 1)[1]))
        if arg == "--wallets" and idx + 1 < len(args):
            matched_wallets.update(normalize_wallet(item) for item in args[idx + 1].split(",") if normalize_wallet(item))
        if arg.startswith("--wallets="):
            matched_wallets.update(normalize_wallet(item) for item in arg.split("=", 1)[1].split(",") if normalize_wallet(item))
    if normalized in matched_wallets:
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
    if not pid_matches and payload.get("collector_mode") == "multi_wallet" and normalized in set(read_deep_wallets(data_dir)):
        group = multi_collector_status(data_dir, now=current)
        pid_matches = bool(group.get("running"))
        if group.get("heartbeat_age_sec") is not None:
            heartbeat_age_sec = float(group["heartbeat_age_sec"])
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
        "listed": normalized in set(read_deep_wallets(data_dir)),
        "pid": pid if pid > 0 else None,
        "started_at": payload.get("started_at"),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "heartbeat_age_sec": round(heartbeat_age_sec, 3) if heartbeat_age_sec is not None else None,
        "sample_sec": payload.get("sample_sec"),
        "book_depth_levels": payload.get("book_depth_levels"),
    }


def multi_collector_status(data_dir: Path, *, now: dt.datetime | None = None) -> dict[str, Any]:
    payload = read_status(data_dir, DEEP_MULTI_STATUS_WALLET) or {}
    wallets = read_deep_wallets(data_dir)
    current = now or utc_now()
    pid = int(payload.get("pid") or 0)
    last_heartbeat = _parse_iso(payload.get("last_heartbeat_at"))
    heartbeat_age_sec = None
    if last_heartbeat is not None:
        heartbeat_age_sec = max(0.0, (current - last_heartbeat).total_seconds())
    pid_matches = False
    if pid > 0:
        cmdline = process_cmdline(pid) or ""
        pid_matches = Path(DEEP_MULTI_COLLECTOR_SCRIPT).name in cmdline or DEEP_MULTI_COLLECTOR_SCRIPT in cmdline
    running = bool(pid_matches and heartbeat_age_sec is not None and heartbeat_age_sec <= STALE_HEARTBEAT_SEC)
    return {
        "wallet": DEEP_MULTI_STATUS_WALLET,
        "state": "running" if running else "stale" if pid_matches else "stopped" if payload else "none",
        "running": running,
        "pid": pid if pid > 0 else None,
        "wallets": wallets,
        "wallet_count": len(wallets),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "heartbeat_age_sec": round(heartbeat_age_sec, 3) if heartbeat_age_sec is not None else None,
    }


def stop_collector(data_dir: Path, wallet: str) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    payload = read_status(data_dir, normalized) or {"wallet": normalized}
    if payload.get("collector_mode") == "multi_wallet":
        payload["status"] = "stopped"
        payload["stopped_at"] = utc_now().isoformat()
        write_status(data_dir, normalized, payload)
        return {"ok": True, "stopped": False, "skipped_multi_wallet": True, **collector_status(data_dir, normalized)}
    status = collector_status(data_dir, normalized)
    pid = int(status.get("pid") or 0)
    stopped = False
    if pid > 0 and process_matches_wallet(pid, normalized):
        try:
            os.kill(pid, signal.SIGTERM)
            stopped = True
        except ProcessLookupError:
            stopped = False
    payload["status"] = "stopped"
    payload["stopped_at"] = utc_now().isoformat()
    write_status(data_dir, normalized, payload)
    return {"ok": True, "stopped": stopped, **collector_status(data_dir, normalized)}


def _logs_dir_for_data_dir(data_dir: Path) -> Path:
    logs_dir = data_dir.parent / "logs" if data_dir.name == "data" else data_dir / ".." / "logs"
    if data_dir.name == "data":
        logs_dir = data_dir.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def ensure_multi_collector(data_dir: Path, wallets: list[str], *, python: str | None = None) -> dict[str, Any]:
    normalized = read_deep_wallets(data_dir)
    for wallet in wallets:
        item = normalize_wallet(wallet)
        if item and item not in normalized:
            normalized.append(item)
    write_deep_wallets(data_dir, normalized)
    current = multi_collector_status(data_dir)
    if current["running"]:
        return {"ok": True, "already_running": True, **current}
    if not normalized:
        return {"ok": True, "already_running": False, **current}
    root = Path(__file__).resolve().parents[1]
    script = root / DEEP_MULTI_COLLECTOR_SCRIPT
    cmd = [python or sys.executable, str(script), "--data-dir", str(data_dir), "--wallets", ",".join(normalized)]
    log_path = _logs_dir_for_data_dir(data_dir) / "deep_collector_multi_wallet.log"
    log = log_path.open("ab")
    try:
        proc = subprocess.Popen(cmd, cwd=str(root), stdout=log, stderr=subprocess.STDOUT, close_fds=True)
    finally:
        log.close()
    now = utc_now().isoformat()
    payload = {
        "wallet": DEEP_MULTI_STATUS_WALLET,
        "pid": proc.pid,
        "status": "running",
        "collector_mode": "multi_wallet",
        "wallets": normalized,
        "started_at": now,
        "last_heartbeat_at": now,
        "sample_sec": DEFAULT_SAMPLE_SEC,
        "book_depth_levels": DEFAULT_BOOK_DEPTH_LEVELS,
        "command": cmd,
        "log_path": str(log_path),
    }
    write_status(data_dir, DEEP_MULTI_STATUS_WALLET, payload)
    for wallet in normalized:
        existing = read_status(data_dir, wallet) or {}
        write_status(
            data_dir,
            wallet,
            {
                **existing,
                "wallet": wallet,
                "pid": proc.pid,
                "status": "running",
                "collector_mode": "multi_wallet",
                "wallets": normalized,
                "started_at": existing.get("started_at") or now,
                "last_heartbeat_at": now,
                "sample_sec": DEFAULT_SAMPLE_SEC,
                "book_depth_levels": DEFAULT_BOOK_DEPTH_LEVELS,
            },
        )
    return {"ok": True, "already_running": False, **multi_collector_status(data_dir)}


def add_multi_collector_wallet(data_dir: Path, wallet: str, *, python: str | None = None) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    wallets = read_deep_wallets(data_dir)
    if normalized and normalized not in wallets:
        wallets.append(normalized)
    return ensure_multi_collector(data_dir, wallets, python=python)


def remove_multi_collector_wallet(data_dir: Path, wallet: str) -> dict[str, Any]:
    normalized = normalize_wallet(wallet)
    wallets = [item for item in read_deep_wallets(data_dir) if item != normalized]
    write_deep_wallets(data_dir, wallets)
    payload = read_status(data_dir, normalized) or {"wallet": normalized}
    payload["status"] = "stopped"
    payload["listed"] = False
    payload["stopped_at"] = utc_now().isoformat()
    write_status(data_dir, normalized, payload)
    return {"ok": True, "wallet": normalized, "listed": False, **multi_collector_status(data_dir)}


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


@dataclass
class MultiWalletDeepCollectorConfig:
    wallets: tuple[str, ...]
    data_dir: Path
    symbols: tuple[str, ...] = ("BTC", "ETH")
    sample_sec: float = DEFAULT_SAMPLE_SEC
    heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC
    book_depth_levels: int = DEFAULT_BOOK_DEPTH_LEVELS
    activity_poll_sec: float = 1.5
    activity_lookback_sec: int = 600
    activity_pages: int = 2
    book_max_age_sec: float = 3.0
    max_concurrent_activity_polls: int = 4

    def normalized_wallets(self) -> tuple[str, ...]:
        seen: set[str] = set()
        wallets: list[str] = []
        for wallet in self.wallets:
            normalized = normalize_wallet(wallet)
            if normalized and normalized not in seen:
                seen.add(normalized)
                wallets.append(normalized)
        return tuple(wallets)


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

    def _record_error(self, stage: str, exc: BaseException) -> None:
        now = utc_now().isoformat()
        existing = read_status(self.config.data_dir, self.wallet) or {}
        payload = {
            **existing,
            "wallet": self.wallet,
            "pid": os.getpid(),
            "status": "running",
            "last_error": {
                "stage": stage,
                "type": exc.__class__.__name__,
                "message": str(exc),
                "observed_at": now,
            },
            "last_error_traceback": "".join(traceback.format_exception_only(type(exc), exc)).strip(),
        }
        write_status(self.config.data_dir, self.wallet, payload)

    async def run(self, *, seconds: float | None = None) -> int:
        start = time.monotonic()
        install_stop_signal_handlers(lambda: setattr(self, "_running", False))
        self._write_heartbeat()
        await self.price_hub.start()
        try:
            await self._refresh_windows(force=True)
            while self._running:
                if seconds is not None and time.monotonic() - start >= seconds:
                    break
                try:
                    await self._refresh_windows()
                    await self._poll_activity_if_due()
                    self._sample_if_due()
                except Exception as exc:
                    self._record_error("run_loop", exc)
                self._heartbeat_if_due()
                await asyncio.sleep(0.1)
        finally:
            await self.stream.close()
            await self.data_api.close()
            await self.price_hub.stop()
            self.store.close()
        return 0

    async def _refresh_windows(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_window_refresh < 15.0:
            return
        self._last_window_refresh = now
        windows: dict[str, MarketWindow] = dict(self.windows)
        for symbol in self.config.symbols:
            try:
                window = await asyncio.to_thread(find_current_or_next_window, MarketSeries.from_symbol(symbol))
            except Exception as exc:
                self._record_error(f"refresh_windows:{symbol}", exc)
                continue
            if window is not None:
                windows = {slug: item for slug, item in windows.items() if item.symbol != symbol}
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
        try:
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
        except Exception as exc:
            self._record_error("poll_activity", exc)
            return
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


class MultiWalletDeepCollector:
    def __init__(self, config: MultiWalletDeepCollectorConfig) -> None:
        self.config = config
        self.wallets = config.normalized_wallets()
        if not self.wallets:
            raise ValueError("at least one wallet is required")
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
        self._activity_sem = asyncio.Semaphore(max(1, int(config.max_concurrent_activity_polls)))

    def _reload_wallets(self) -> None:
        listed = read_deep_wallets(self.config.data_dir)
        if listed:
            self.wallets = tuple(listed)

    def _record_error(self, stage: str, exc: BaseException, *, wallet: str | None = None) -> None:
        now = utc_now().isoformat()
        targets = (normalize_wallet(wallet),) if wallet else (DEEP_MULTI_STATUS_WALLET,)
        for target in targets:
            existing = read_status(self.config.data_dir, target) or {}
            payload = {
                **existing,
                "wallet": target,
                "pid": os.getpid(),
                "status": "running",
                "collector_mode": "multi_wallet",
                "wallets": list(self.wallets),
                "last_error": {
                    "stage": stage,
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                    "observed_at": now,
                },
                "last_error_traceback": "".join(traceback.format_exception_only(type(exc), exc)).strip(),
            }
            write_status(self.config.data_dir, target, payload)

    async def run(self, *, seconds: float | None = None) -> int:
        start = time.monotonic()
        install_stop_signal_handlers(lambda: setattr(self, "_running", False))
        self._write_heartbeat()
        await self.price_hub.start()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._refresh_windows(force=True)
            while self._running:
                if seconds is not None and time.monotonic() - start >= seconds:
                    break
                try:
                    self._reload_wallets()
                    await self._refresh_windows()
                    await self._poll_activity_if_due()
                    self._sample_if_due()
                except Exception as exc:
                    self._record_error("run_loop", exc)
                self._heartbeat_if_due()
                await asyncio.sleep(0.1)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            await self.stream.close()
            await self.data_api.close()
            await self.price_hub.stop()
            self.store.close()
        return 0

    async def _heartbeat_loop(self) -> None:
        while self._running:
            self._write_heartbeat()
            await asyncio.sleep(max(0.5, float(self.config.heartbeat_sec)))

    async def _refresh_windows(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_window_refresh < 15.0:
            return
        self._last_window_refresh = now
        windows: dict[str, MarketWindow] = dict(self.windows)
        for symbol in self.config.symbols:
            try:
                window = await asyncio.to_thread(find_current_or_next_window, MarketSeries.from_symbol(symbol))
            except Exception as exc:
                self._record_error(f"refresh_windows:{symbol}", exc)
                continue
            if window is not None:
                windows = {slug: item for slug, item in windows.items() if item.symbol != symbol}
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
        await asyncio.gather(*(self._poll_wallet_activity(wallet) for wallet in self.wallets))

    async def _poll_wallet_activity(self, wallet: str) -> None:
        async with self._activity_sem:
            now_ts = int(utc_now().timestamp())
            last_seen = self.store.last_wallet_activity_ts(wallet)
            start_ts = max(now_ts - self.config.activity_lookback_sec, last_seen - 30) if last_seen else now_ts - self.config.activity_lookback_sec
            observed_at = utc_now().isoformat()
            normalized: list[dict[str, Any]] = []
            page_size = 500
            try:
                for page in range(max(1, self.config.activity_pages)):
                    rows = await self.data_api.fetch_user_activity(
                        wallet,
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
                        event = normalize_activity_event(raw, wallet=wallet, observed_at=observed_at)
                        if event["symbol"] not in self.config.symbols or "-updown-5m-" not in event["market_slug"]:
                            continue
                        normalized.append(event)
                    if len(rows) < page_size:
                        break
            except Exception as exc:
                self._record_error("poll_activity", exc, wallet=wallet)
                return
        inserted = self.store.insert_wallet_activity_events(normalized, recompute=False)
        if inserted:
            self.store.insert_trades([self._trade_from_activity(event, wallet=wallet) for event in inserted if event.get("activity_type") == "TRADE"])
            contexts = self._context_rows(inserted, wallet=wallet)
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
            "sample_reason": "multi_wallet_deep_collector",
        }

    def _context_rows(self, events: list[dict[str, Any]], *, wallet: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        by_slug = {window.slug: window for window in self.windows.values()}
        for event in events:
            if str(event.get("activity_type") or "").upper() != "TRADE":
                continue
            event_wallet = normalize_wallet(str(event.get("wallet") or wallet))
            window = by_slug.get(str(event.get("market_slug") or ""))
            if window is None:
                continue
            trade = self._trade_from_activity(event, wallet=event_wallet)
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
                    "wallet": event_wallet,
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

    def _trade_from_activity(self, event: dict[str, Any], *, wallet: str) -> dict[str, Any]:
        return {
            "tx_hash": str(event.get("tx_hash") or ""),
            "fill_id": str(event.get("fill_id") or ""),
            "wallet": normalize_wallet(str(event.get("wallet") or wallet)),
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
        for wallet in self.wallets:
            existing = read_status(self.config.data_dir, wallet) or {}
            payload = {
                **existing,
                "wallet": wallet,
                "pid": os.getpid(),
                "status": "running",
                "collector_mode": "multi_wallet",
                "wallets": list(self.wallets),
                "started_at": existing.get("started_at") or now,
                "last_heartbeat_at": now,
                "sample_sec": self.config.sample_sec,
                "book_depth_levels": self.config.book_depth_levels,
            }
            write_status(self.config.data_dir, wallet, payload)
        self._write_group_status(now)
        self._last_heartbeat = time.monotonic()

    def _write_group_status(self, now: str) -> None:
        existing = read_status(self.config.data_dir, DEEP_MULTI_STATUS_WALLET) or {}
        payload = {
            "wallet": "multi_wallet",
            "pid": os.getpid(),
            "status": "running",
            "collector_mode": "multi_wallet",
            "wallets": list(self.wallets),
            "started_at": existing.get("started_at") or now,
            "last_heartbeat_at": now,
            "sample_sec": self.config.sample_sec,
            "book_depth_levels": self.config.book_depth_levels,
        }
        write_status(self.config.data_dir, DEEP_MULTI_STATUS_WALLET, payload)
