import datetime as dt
import tempfile
import unittest
from pathlib import Path

from poly_monitor.market import MarketWindow
from poly_monitor.observer import CryptoWalletObserver, ObserverConfig, context_snapshot


class FakeStream:
    def get_book(self, token, max_age_sec=3.0):
        return [(0.49, 20.0)], [(0.51, 20.0)], 100

    def diagnostics(self, reset_counts=False):
        return {"messages": 1}


class FakeFeed:
    latest_price = 100_000.0

    def latest_age_sec(self):
        return 0.25

    def return_bps(self, lookback_sec):
        return lookback_sec


class ObserverContextTests(unittest.TestCase):
    def test_context_snapshot_includes_window_reference_prices(self):
        now = dt.datetime.now(dt.timezone.utc)
        window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xcond",
            question="Bitcoin Up or Down",
            up_token="up",
            down_token="down",
            start_time=now - dt.timedelta(seconds=30),
            end_time=now + dt.timedelta(seconds=270),
        )
        trade = {
            "wallet": "0xabc",
            "tx_hash": "0xtx",
            "outcome": "Up",
            "price": 0.51,
            "usdc": 25.5,
        }

        row = context_snapshot(
            trade=trade,
            window=window,
            stream=FakeStream(),
            feed=FakeFeed(),
            window_open_reference_price=99_900.0,
            window_close_reference_price=100_100.0,
        )

        self.assertEqual(row["window_open_reference_price"], 99_900.0)
        self.assertEqual(row["window_close_reference_price"], 100_100.0)

    def test_raw_trade_events_are_limited_to_followed_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(data_dir=Path(tmp)),
                {"0xseed": "seed"},
            )
            self.assertTrue(observer._should_write_raw_trade({"wallet": "0xseed"}))
            self.assertFalse(observer._should_write_raw_trade({"wallet": "0xstranger"}))
            observer.store.close()
            observer.writer.close()


if __name__ == "__main__":
    unittest.main()
