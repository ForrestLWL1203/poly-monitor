from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.storage import ObserverStore
from poly_monitor.wallet_export import export_watchlist_wallet


class WalletExportTests(unittest.TestCase):
    def test_export_watchlist_wallet_writes_window_bundle_and_manifest_gaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            slug = "btc-updown-5m-1770000000"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
                store.upsert_watched_market_window(
                    {
                        "market_slug": slug,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "first_seen_at": dt.datetime.fromtimestamp(1770000120, dt.timezone.utc).isoformat(),
                        "window_start": dt.datetime.fromtimestamp(1770000000, dt.timezone.utc).isoformat(),
                        "window_end": dt.datetime.fromtimestamp(1770000300, dt.timezone.utc).isoformat(),
                        "tracking_reason": "watchlist_activity",
                        "source_wallet": wallet,
                        "capture_until": dt.datetime.fromtimestamp(1770002100, dt.timezone.utc).isoformat(),
                        "status": "tracking",
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xtrade",
                            "wallet": wallet,
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 1770000121,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.6,
                            "size": 10,
                            "usdc": 6,
                            "asset": "up",
                            "observed_at": dt.datetime.fromtimestamp(1770000122, dt.timezone.utc).isoformat(),
                        }
                    ]
                )
                store.insert_trades(
                    [
                        {
                            "tx_hash": "0xtrade",
                            "wallet": wallet,
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 1770000121,
                            "outcome": "Up",
                            "side": "BUY",
                            "price": 0.6,
                            "size": 10,
                            "usdc": 6,
                        },
                        {
                            "tx_hash": "0xother",
                            "wallet": "0xother",
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 1770000125,
                            "outcome": "Down",
                            "side": "BUY",
                            "price": 0.4,
                            "size": 5,
                            "usdc": 2,
                        },
                    ]
                )
                store.insert_market_state_samples(
                    [
                        {
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "sampled_ts": 1770000122,
                            "observed_at": dt.datetime.fromtimestamp(1770000122, dt.timezone.utc).isoformat(),
                            "window_remaining_sec": 178,
                            "reference_price": 100000,
                            "reference_price_age_sec": 0.5,
                            "up_json": {"bid": 0.59, "ask": 0.61},
                            "down_json": {"bid": 0.39, "ask": 0.41},
                            "book_stale": False,
                            "sample_reason": "initial",
                        }
                    ]
                )
            finally:
                store.close()

            result = export_watchlist_wallet(wallet, data_dir=data_dir)
            manifest_path = Path(result["manifest_path"])
            manifest = json.loads(manifest_path.read_text())

            self.assertTrue(Path(result["zip_path"]).exists())
            self.assertEqual(manifest["wallet"], wallet)
            self.assertEqual(manifest["window_count"], 1)
            self.assertTrue(manifest["windows"][0]["insufficient_market_capture"])
            self.assertEqual(manifest["windows"][0]["market_trade_rows"], 2)
            self.assertEqual(manifest["windows"][0]["market_state_sample_rows"], 1)
            self.assertFalse(manifest["windows"][0]["settlement_complete"])
            self.assertTrue((manifest_path.parent / "markets" / slug / "market_trades.jsonl").exists())
            with zipfile.ZipFile(result["zip_path"]) as bundle:
                self.assertIn("manifest.json", bundle.namelist())
                self.assertIn(f"markets/{slug}/market_trades.jsonl", bundle.namelist())


if __name__ == "__main__":
    unittest.main()
