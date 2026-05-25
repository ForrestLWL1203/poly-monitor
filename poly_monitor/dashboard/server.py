from __future__ import annotations

import base64
import hashlib
import hmac
import json
import mimetypes
import os
import re
import signal
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from ..storage import WatchlistStore
from ..wallet_export import export_watchlist_wallet
from .status import build_dashboard_status, recent_trades, wallet_detail


COOKIE_NAME = "poly_monitor_session"
FAILED_LOGINS: dict[str, list[float]] = {}
_STATUS_TTL = 2.0
_status_cache: tuple[str, float, dict[str, Any]] | None = None
_status_lock = threading.Lock()
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")


@dataclass(frozen=True)
class DashboardConfig:
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 8787
    username: str = "admin"
    password: str = ""
    cookie_secret: str = ""
    session_ttl_seconds: int = 12 * 3600
    status_cache_ttl_seconds: float = 2.0
    static_dir: Path | None = None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def make_session_token(username: str, secret: str, *, now: int | None = None) -> str:
    issued_at = int(now if now is not None else time.time())
    payload = _b64(json.dumps({"u": username, "iat": issued_at}, separators=(",", ":")).encode())
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return f"{payload}.{_b64(sig)}"


def verify_session_token(token: str, secret: str, *, max_age_seconds: int, now: int | None = None) -> str | None:
    if not token or "." not in token or not secret:
        return None
    payload, sig = token.rsplit(".", 1)
    expected = _b64(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        data = json.loads(_unb64(payload))
    except (ValueError, json.JSONDecodeError):
        return None
    issued_at = int(data.get("iat", 0))
    current = int(now if now is not None else time.time())
    if issued_at <= 0 or current - issued_at > max_age_seconds:
        return None
    username = data.get("u")
    return str(username) if username else None


def create_server(config: DashboardConfig) -> ThreadingHTTPServer:
    if not config.password:
        raise ValueError("POLY_MONITOR_DASH_PASSWORD is required")
    cookie_secret = config.cookie_secret or os.environ.get("POLY_MONITOR_DASH_COOKIE_SECRET") or ""
    if not cookie_secret:
        raise ValueError("POLY_MONITOR_DASH_COOKIE_SECRET is required")
    static_dir = config.static_dir or Path(__file__).with_name("static")
    resolved = DashboardConfig(
        data_dir=config.data_dir,
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        cookie_secret=cookie_secret,
        session_ttl_seconds=config.session_ttl_seconds,
        status_cache_ttl_seconds=config.status_cache_ttl_seconds,
        static_dir=static_dir,
    )

    class Handler(DashboardHandler):
        dashboard_config = resolved

    return ThreadingHTTPServer((resolved.host, resolved.port), Handler)


def _cached_status(data_dir: Path, *, ttl: float = _STATUS_TTL) -> dict[str, Any]:
    global _status_cache
    ttl = max(0.0, float(ttl))
    if ttl <= 0:
        return build_dashboard_status(data_dir)
    data_dir_key = str(data_dir.expanduser().resolve())
    now = time.monotonic()
    if _status_cache and _status_cache[0] == data_dir_key and now - _status_cache[1] < ttl:
        return _status_cache[2]
    with _status_lock:
        now = time.monotonic()
        if _status_cache and _status_cache[0] == data_dir_key and now - _status_cache[1] < ttl:
            return _status_cache[2]
        payload = build_dashboard_status(data_dir)
        _status_cache = (data_dir_key, now, payload)
        return payload


def _clear_status_cache() -> None:
    global _status_cache
    with _status_lock:
        _status_cache = None


def _stop_observer_processes() -> list[int]:
    try:
        output = subprocess.check_output(["ps", "-eo", "pid=,args="], text=True)
    except (OSError, subprocess.SubprocessError):
        return []
    killed: list[int] = []
    current_pid = os.getpid()
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, _, args = stripped.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid == current_pid or "run_crypto_wallet_observer.py" not in args:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        killed.append(pid)
    return killed


class DashboardHandler(BaseHTTPRequestHandler):
    dashboard_config: DashboardConfig

    server_version = "PolyMonitorDashboard/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            if not self._authenticated():
                self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            self._handle_api_get(parsed)
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/login":
            self._login()
            return
        if parsed.path == "/api/logout":
            self._logout()
            return
        if parsed.path == "/api/stop-observer":
            if not self._authenticated():
                self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            killed = _stop_observer_processes()
            self._json({"ok": True, "killed_pids": killed, "count": len(killed)})
            return
        if parsed.path == "/api/watchlist":
            if not self._authenticated():
                self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            self._watchlist_mutation()
            return
        if parsed.path == "/api/wallet-export":
            if not self._authenticated():
                self._json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            self._wallet_export()
            return
        self._json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, parsed: urllib.parse.ParseResult) -> None:
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/status":
            self._json(self._cached_status())
            return
        if parsed.path == "/api/recent-trades":
            limit = _int_param(query.get("limit", ["100"])[0], default=100, minimum=1, maximum=500)
            self._json({"recent_trades": recent_trades(self.dashboard_config.data_dir, limit=limit), "limit": limit})
            return
        if parsed.path == "/api/wallet":
            address = str(query.get("address", [""])[0]).lower()
            detail = wallet_detail(self.dashboard_config.data_dir, address)
            if detail is None:
                self._json({"error": "wallet_not_found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._json(detail)
            return
        if parsed.path == "/api/watchlist":
            store = WatchlistStore(self.dashboard_config.data_dir / "state" / "observer.sqlite")
            try:
                self._json({"watchlist": store.rows()})
            finally:
                store.close()
            return
        self._json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _cached_status(self) -> dict[str, Any]:
        return _cached_status(self.dashboard_config.data_dir, ttl=self.dashboard_config.status_cache_ttl_seconds)

    def _login(self) -> None:
        body = self.rfile.read(_int_param(self.headers.get("Content-Length"), default=0, minimum=0, maximum=16384))
        content_type = self.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                form = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                form = {}
        else:
            form = {key: values[0] for key, values in urllib.parse.parse_qs(body.decode()).items()}
        username = str(form.get("username") or "")
        password = str(form.get("password") or "")
        config = self.dashboard_config
        client = self.client_address[0] if self.client_address else "unknown"
        failures = [stamp for stamp in FAILED_LOGINS.get(client, []) if time.time() - stamp < 60.0]
        if len(failures) >= 5:
            time.sleep(min(2.0, 0.25 * len(failures)))
        if not (hmac.compare_digest(username, config.username) and hmac.compare_digest(password, config.password)):
            failures.append(time.time())
            FAILED_LOGINS[client] = failures[-10:]
            self._json({"error": "invalid_login"}, status=HTTPStatus.UNAUTHORIZED)
            return
        FAILED_LOGINS.pop(client, None)
        token = make_session_token(username, config.cookie_secret)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header(
            "Set-Cookie",
            f"{COOKIE_NAME}={token}; Path=/; Max-Age={config.session_ttl_seconds}; HttpOnly; SameSite=Lax",
        )
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def _logout(self) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Set-Cookie", f"{COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def _watchlist_mutation(self) -> None:
        body = self.rfile.read(_int_param(self.headers.get("Content-Length"), default=0, minimum=0, maximum=16384))
        content_type = self.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                form = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                form = {}
        else:
            form = {key: values[0] for key, values in urllib.parse.parse_qs(body.decode()).items()}
        wallet = str(form.get("wallet") or form.get("address") or "").lower().strip()
        action = str(form.get("action") or "add").lower()
        note = str(form.get("note") or "")
        if not ADDRESS_RE.match(wallet):
            self._json({"error": "invalid_wallet", "hint": "expected 0x + 40 hex chars"}, status=HTTPStatus.BAD_REQUEST)
            return
        store = WatchlistStore(self.dashboard_config.data_dir / "state" / "observer.sqlite")
        try:
            if action == "remove":
                removed = store.remove_wallet(wallet)
                payload = {"ok": True, "wallet": wallet, "watchlisted": False, "removed": removed}
            elif action == "add":
                store.add_wallet(wallet, note=note)
                payload = {"ok": True, "wallet": wallet, "watchlisted": True}
            else:
                self._json({"error": "invalid_action"}, status=HTTPStatus.BAD_REQUEST)
                return
        finally:
            store.close()
        _clear_status_cache()
        self._json(payload)

    def _wallet_export(self) -> None:
        body = self.rfile.read(_int_param(self.headers.get("Content-Length"), default=0, minimum=0, maximum=16384))
        content_type = self.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                form = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                form = {}
        else:
            form = {key: values[0] for key, values in urllib.parse.parse_qs(body.decode()).items()}
        wallet = str(form.get("wallet") or form.get("address") or "").lower().strip()
        if not ADDRESS_RE.match(wallet):
            self._json({"error": "invalid_wallet", "hint": "expected 0x + 40 hex chars"}, status=HTTPStatus.BAD_REQUEST)
            return
        store = WatchlistStore(self.dashboard_config.data_dir / "state" / "observer.sqlite")
        try:
            if wallet not in set(store.wallets()):
                self._json({"error": "wallet_not_watchlisted"}, status=HTTPStatus.NOT_FOUND)
                return
        finally:
            store.close()
        try:
            result = export_watchlist_wallet(wallet, data_dir=self.dashboard_config.data_dir)
        except ValueError as exc:
            if str(exc) == "wallet_not_watchlisted":
                self._json({"error": "wallet_not_watchlisted"}, status=HTTPStatus.NOT_FOUND)
                return
            self._json({"error": "export_failed", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:
            self._json({"error": "export_failed", "message": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._json({"ok": True, **result})

    def _authenticated(self) -> bool:
        token = _cookie_value(self.headers.get("Cookie", ""), COOKIE_NAME)
        username = verify_session_token(
            token,
            self.dashboard_config.cookie_secret,
            max_age_seconds=self.dashboard_config.session_ttl_seconds,
        )
        return username == self.dashboard_config.username

    def _serve_static(self, path: str) -> None:
        static_dir = self.dashboard_config.static_dir or Path(__file__).with_name("static")
        name = "index.html" if path in {"", "/"} else path.lstrip("/")
        if "/" in name and name != "index.html":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        target = (static_dir / name).resolve()
        try:
            target.relative_to(static_dir.resolve())
        except ValueError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        payload = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mimetypes.guess_type(str(target))[0] or "application/octet-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, payload: Any, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _cookie_value(raw: str, key: str) -> str:
    for part in raw.split(";"):
        if "=" not in part:
            continue
        name, value = part.strip().split("=", 1)
        if name == key:
            return value
    return ""


def _int_param(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))
