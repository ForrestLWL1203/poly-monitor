import datetime as dt
import unittest

from poly_monitor.crypto_price import crypto_price_api_url, parse_crypto_price_response
from poly_monitor.market import MarketWindow


class CryptoPriceTests(unittest.TestCase):
    def test_crypto_price_url_uses_symbol_and_window_times(self):
        window = MarketWindow(
            symbol="ETH",
            slug="eth-updown-5m-1770000000",
            condition_id="0xcond",
            question="Ethereum Up or Down",
            up_token="up",
            down_token="down",
            start_time=dt.datetime(2026, 2, 2, 2, 40, tzinfo=dt.timezone.utc),
            end_time=dt.datetime(2026, 2, 2, 2, 45, tzinfo=dt.timezone.utc),
        )

        url = crypto_price_api_url(window)

        self.assertIn("symbol=ETH", url)
        self.assertIn("variant=fiveminute", url)
        self.assertIn("eventStartTime=2026-02-02T02%3A40%3A00Z", url)
        self.assertIn("endDate=2026-02-02T02%3A45%3A00Z", url)

    def test_parse_crypto_price_response_keeps_open_close_and_flags(self):
        parsed = parse_crypto_price_response({
            "openPrice": "100.5",
            "closePrice": 101.25,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })

        self.assertEqual(parsed, {
            "openPrice": 100.5,
            "closePrice": 101.25,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })


if __name__ == "__main__":
    unittest.main()
