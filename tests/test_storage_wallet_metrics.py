import tempfile
import unittest
from pathlib import Path

from poly_monitor.storage import ObserverStore


def trade_row(wallet: str, market_slug: str, exchange_ts: int, *, tx_hash: str) -> dict:
    return {
        "tx_hash": tx_hash,
        "fill_id": "",
        "wallet": wallet.lower(),
        "market_slug": market_slug,
        "condition_id": f"cond-{market_slug}",
        "symbol": "BTC",
        "exchange_ts": exchange_ts,
        "outcome": "Up",
        "price": 0.5,
        "size": 10,
        "usdc": 5,
    }


class StorageWalletMetricsTests(unittest.TestCase):
    def test_wallet_trade_metrics_counts_time_windows_without_loading_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-1", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "btc-1", now_ts - 200, tx_hash="tx-2"),
                        trade_row("0xabc", "eth-1", now_ts - 2 * 86400, tx_hash="tx-3"),
                        trade_row("0xabc", "sol-1", now_ts - 20 * 86400, tx_hash="tx-4"),
                        trade_row("0xabc", "xrp-1", now_ts - 40 * 86400, tx_hash="tx-5"),
                        trade_row("0xdef", "btc-1", now_ts - 50, tx_hash="tx-6"),
                    ]
                )

                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)

                self.assertEqual(metrics["wallet"], "0xabc")
                self.assertEqual(metrics["trades_24h"], 2)
                self.assertEqual(metrics["markets_24h"], 1)
                self.assertEqual(metrics["trades_7d"], 3)
                self.assertEqual(metrics["markets_7d"], 2)
                self.assertEqual(metrics["trades_30d"], 4)
                self.assertEqual(metrics["markets_30d"], 3)
                self.assertEqual(metrics["historical_trades"], 5)
                self.assertEqual(metrics["historical_markets"], 4)
                self.assertAlmostEqual(metrics["last_active_age_hours"], round(100 / 3600, 3))
            finally:
                store.close()

    def test_wallet_24h_counts_returns_observer_override_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-1", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "btc-2", now_ts - 200, tx_hash="tx-2"),
                        trade_row("0xabc", "btc-3", now_ts - 2 * 86400, tx_hash="tx-3"),
                    ]
                )

                self.assertEqual(store.wallet_24h_counts("0xABC", now_ts=now_ts), {"trades_24h": 2, "markets_24h": 2})
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
