import unittest
from unittest.mock import patch

from poly_monitor.data_api import fetch_market_trades, normalize_trade


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


if __name__ == "__main__":
    unittest.main()
