import asyncio
import time
import unittest

from poly_monitor.price_feed import ChainlinkPriceHub, price_ticks_by_symbol_from_message, price_ticks_from_message, subscribe_message


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

    def test_subscribe_message_accepts_multiple_symbols(self):
        self.assertEqual(subscribe_message(["BTC/USD", "ETH/USD"]), {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "filters": '{"symbol":"btc/usd"}',
                },
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "filters": '{"symbol":"eth/usd"}',
                },
            ],
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

    def test_price_ticks_by_symbol_routes_batch_updates(self):
        self.assertEqual(price_ticks_by_symbol_from_message({
            "payload": {
                "data": [
                    {"symbol": "BTC/USD", "timestamp": 1779598954000, "value": "100000.01"},
                    {"symbol": "ETH/USD", "timestamp": 1779598955000, "value": 2117.39},
                ]
            }
        }), {
            "btc/usd": [(1779598954.0, 100000.01)],
            "eth/usd": [(1779598955.0, 2117.39)],
        })

    def test_price_hub_updates_symbol_feeds_from_one_message(self):
        hub = ChainlinkPriceHub(["BTC/USD", "ETH/USD"])
        now_ms = int(time.time() * 1000)

        hub._handle_message({
            "payload": {
                "data": [
                    {"symbol": "BTC/USD", "timestamp": now_ms, "value": "100000.01"},
                    {"symbol": "ETH/USD", "timestamp": now_ms + 1000, "value": 2117.39},
                ]
            }
        })

        self.assertEqual(hub.feed("btc/usd").latest_price, 100000.01)
        self.assertEqual(hub.feed("ETH/USD").latest_price, 2117.39)

    def test_price_feed_inject_tick_updates_history(self):
        feed = ChainlinkPriceHub(["BTC/USD"]).feed("btc/usd")
        now = time.time()

        feed.inject_tick(now, 100000.01)

        self.assertEqual(feed.latest_price, 100000.01)

    def test_price_hub_start_is_single_task_for_multiple_symbols(self):
        async def run_test():
            hub = ChainlinkPriceHub(["BTC/USD", "ETH/USD"])
            calls = 0

            async def fake_loop():
                nonlocal calls
                calls += 1

            hub._recv_loop = fake_loop
            await hub.start()
            await hub.start()
            await asyncio.sleep(0)
            await hub.stop()
            self.assertEqual(calls, 1)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
