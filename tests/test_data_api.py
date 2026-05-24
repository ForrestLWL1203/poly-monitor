import asyncio
import unittest
from unittest.mock import patch

from poly_monitor.data_api import AsyncDataApiClient, fetch_market_trades, normalize_trade


class DataApiTests(unittest.TestCase):
    def test_fetch_market_trades_merges_multiple_pages_by_tx_hash(self):
        calls = []

        def fake_get_json(path, params):
            calls.append((path, dict(params)))
            offset = params["offset"]
            if offset == 0:
                return [
                    {"transactionHash": "0x1", "proxyWallet": "0xa"},
                    {"transactionHash": "0x2", "proxyWallet": "0xb"},
                ]
            if offset == 100:
                return [
                    {"transactionHash": "0x2", "proxyWallet": "0xb"},
                    {"transactionHash": "0x3", "proxyWallet": "0xc"},
                ]
            return []

        with patch("poly_monitor.data_api._get_json", side_effect=fake_get_json):
            rows = fetch_market_trades("0xcond", limit=100, pages=3)

        self.assertEqual([row["transactionHash"] for row in rows], ["0x1", "0x2", "0x3"])
        self.assertEqual([params["offset"] for _path, params in calls], [0, 100, 200])

    def test_normalize_trade_keeps_only_compact_fields(self):
        raw = {
            "proxyWallet": "0xabc",
            "side": "BUY",
            "asset": "token",
            "conditionId": "0xcond",
            "size": 12.5,
            "price": 0.42,
            "timestamp": 1779598796,
            "title": "Bitcoin Up or Down - May 24, 1:00AM-1:05AM ET",
            "slug": "btc-updown-5m-1779598800",
            "eventSlug": "btc-updown-5m-1779598800",
            "outcome": "Up",
            "outcomeIndex": 0,
            "name": "sample",
            "pseudonym": "Alias",
            "bio": "drop-me",
            "profileImage": "drop-me",
            "profileImageOptimized": "drop-me",
            "transactionHash": "0xtx",
            "logIndex": 7,
        }

        row = normalize_trade(raw, symbol="BTC", observed_at="2026-05-24T05:00:01+00:00")

        self.assertEqual(row, {
            "event": "trade_observed",
            "observed_at": "2026-05-24T05:00:01+00:00",
            "exchange_ts": 1779598796,
            "symbol": "BTC",
            "market_slug": "btc-updown-5m-1779598800",
            "condition_id": "0xcond",
            "wallet": "0xabc",
            "name": "sample",
            "outcome": "Up",
            "price": 0.42,
            "size": 12.5,
            "usdc": 5.25,
            "tx_hash": "0xtx",
            "fill_id": "7",
        })

    def test_async_client_uses_tuned_tcp_connector(self):
        async def run_case():
            connector_kwargs = {}
            session_kwargs = {}

            class FakeResponse:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return None

                def raise_for_status(self):
                    return None

                async def json(self, content_type=None):
                    return []

            class FakeSession:
                def __init__(self, **kwargs):
                    session_kwargs.update(kwargs)

                def get(self, *_args, **_kwargs):
                    return FakeResponse()

                async def close(self):
                    return None

            class FakeTimeout:
                def __init__(self, **kwargs):
                    self.kwargs = kwargs

            class FakeConnector:
                def __init__(self, **kwargs):
                    connector_kwargs.update(kwargs)

            class FakeAiohttp:
                ClientTimeout = FakeTimeout
                TCPConnector = FakeConnector
                ClientSession = FakeSession

            with patch("poly_monitor.data_api.aiohttp", FakeAiohttp):
                client = AsyncDataApiClient(base_url="https://example.test", timeout=7)
                await client._get_json("/trades", {})
                await client.close()

            return connector_kwargs, session_kwargs

        connector_kwargs, session_kwargs = asyncio.run(run_case())

        self.assertEqual(connector_kwargs, {
            "limit": 10,
            "limit_per_host": 5,
            "ttl_dns_cache": 300,
            "keepalive_timeout": 30,
        })
        self.assertIn("connector", session_kwargs)


if __name__ == "__main__":
    unittest.main()
