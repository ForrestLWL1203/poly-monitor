from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any

DATA_API = "https://data-api.polymarket.com"
LB_API = "https://lb-api.polymarket.com"


def _get_url_json(base_url: str, path: str, params: dict[str, Any], *, timeout: float = 10.0, retries: int = 3) -> Any:
    url = base_url + path + "?" + urllib.parse.urlencode(params, doseq=True)
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "poly-monitor/0.1", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(0.25 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _get_json(path: str, params: dict[str, Any], *, timeout: float = 10.0, retries: int = 3) -> Any:
    return _get_url_json(DATA_API, path, params, timeout=timeout, retries=retries)


def normalize_trade(raw: dict[str, Any], *, symbol: str, observed_at: str) -> dict[str, Any]:
    price = float(raw.get("price") or 0.0)
    size = float(raw.get("size") or 0.0)
    return {
        "event": "trade_observed",
        "observed_at": observed_at,
        "exchange_ts": int(raw.get("timestamp") or 0),
        "symbol": symbol.upper(),
        "market_slug": str(raw.get("slug") or raw.get("eventSlug") or ""),
        "condition_id": str(raw.get("conditionId") or ""),
        "wallet": str(raw.get("proxyWallet") or "").lower(),
        "name": str(raw.get("name") or ""),
        "outcome": str(raw.get("outcome") or ""),
        "price": price,
        "size": size,
        "usdc": round(price * size, 6),
        "tx_hash": str(raw.get("transactionHash") or ""),
    }


def fetch_market_trades(condition_id: str, *, limit: int = 100, offset: int = 0, pages: int = 4) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    page_size = min(limit, 100)
    for page in range(max(1, pages)):
        data = _get_json("/trades", {"market": condition_id, "limit": page_size, "offset": offset + page * page_size})
        if not isinstance(data, list):
            break
        for row in data:
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("transactionHash") or ""),
                str(row.get("proxyWallet") or ""),
                str(row.get("outcome") or ""),
                str(row.get("price") or ""),
                str(row.get("size") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
        if not data:
            break
    return rows


def fetch_user_activity(wallet: str, *, limit: int = 500, offset: int = 0, start: int | None = None, end: int | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"user": wallet, "limit": limit, "offset": offset}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    data = _get_json("/activity", params)
    return data if isinstance(data, list) else []


def fetch_closed_positions(wallet: str, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
    data = _get_json("/closed-positions", {"user": wallet, "limit": limit, "offset": offset})
    return data if isinstance(data, list) else []


def fetch_user_profit(wallet: str, *, window: str) -> dict[str, Any] | None:
    data = _get_url_json(LB_API, "/profit", {"address": wallet, "window": window, "limit": 1})
    if isinstance(data, list) and data:
        row = data[0]
        return row if isinstance(row, dict) else None
    return None
