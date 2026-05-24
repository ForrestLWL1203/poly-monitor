import datetime as dt
import unittest

from poly_monitor.market import MarketSeries, build_window, current_epoch_start


class MarketTests(unittest.TestCase):
    def test_series_builds_btc_and_eth_5m_slugs(self):
        self.assertEqual(MarketSeries.from_symbol("BTC").epoch_to_slug(1779598800), "btc-updown-5m-1779598800")
        self.assertEqual(MarketSeries.from_symbol("eth").epoch_to_slug(1779598800), "eth-updown-5m-1779598800")

    def test_current_epoch_start_floors_to_five_minutes(self):
        now = dt.datetime.fromtimestamp(1779598999, tz=dt.timezone.utc)

        self.assertEqual(current_epoch_start(now, 300), 1779598800)

    def test_build_window_parses_gamma_market_tokens_and_times(self):
        raw = {
            "slug": "eth-updown-5m-1779598800",
            "question": "Ethereum Up or Down - May 24, 1:00AM-1:05AM ET",
            "conditionId": "0xabc",
            "clobTokenIds": '["up-token","down-token"]',
            "eventStartTime": "2026-05-24T05:00:00Z",
            "endDate": "2026-05-24T05:05:00Z",
            "active": True,
            "closed": False,
        }

        window = build_window(raw, MarketSeries.from_symbol("ETH"))

        self.assertIsNotNone(window)
        assert window is not None
        self.assertEqual(window.symbol, "ETH")
        self.assertEqual(window.up_token, "up-token")
        self.assertEqual(window.down_token, "down-token")
        self.assertEqual(window.start_epoch, 1779598800)
        self.assertEqual(window.end_epoch, 1779599100)


if __name__ == "__main__":
    unittest.main()
