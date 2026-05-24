import unittest

from poly_monitor.price_feed import price_ticks_from_message, subscribe_message


class PriceFeedTests(unittest.TestCase):
    def test_subscribe_message_uses_compact_symbol_filter(self):
        self.assertEqual(subscribe_message("ETH/USD"), {
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "filters": '{"symbol":"eth/usd"}',
            }],
        })

    def test_price_ticks_from_batch_and_live_update_messages(self):
        self.assertEqual(price_ticks_from_message({
            "payload": {
                "data": [
                    {"timestamp": 1779598954000, "value": "2117.38"},
                    {"timestamp": 1779598955000, "value": 2117.39},
                ]
            }
        }), [(1779598954.0, 2117.38), (1779598955.0, 2117.39)])

        self.assertEqual(price_ticks_from_message({
            "payload": {"timestamp": 1779599013000, "value": 2117.3879125852773}
        }), [(1779599013.0, 2117.3879125852773)])


if __name__ == "__main__":
    unittest.main()
