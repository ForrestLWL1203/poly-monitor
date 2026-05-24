from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from poly_monitor.clob_stream import ClobBookStream


class ClobStreamTests(unittest.TestCase):
    def test_book_snapshots_are_returned_sorted_from_price_maps(self):
        stream = ClobBookStream()
        stream._handle_book(
            {
                "asset_id": "token",
                "bids": [{"price": "0.50", "size": "2"}, {"price": "0.49", "size": "3"}],
                "asks": [{"price": "0.51", "size": "4"}, {"price": "0.52", "size": "5"}],
            }
        )

        bids, asks, age = stream.get_book("token")

        self.assertEqual(bids, [(0.5, 2.0), (0.49, 3.0)])
        self.assertEqual(asks, [(0.51, 4.0), (0.52, 5.0)])
        self.assertIsInstance(age, int)

    def test_price_change_updates_price_map_without_rebuilding_side(self):
        stream = ClobBookStream()
        stream._handle_book(
            {
                "asset_id": "token",
                "bids": [{"price": "0.50", "size": "2"}, {"price": "0.49", "size": "3"}],
                "asks": [{"price": "0.51", "size": "4"}],
            }
        )
        stream._handle_price_change({"asset_id": "token", "side": "BUY", "price": "0.50", "size": "0"})
        stream._handle_price_change({"asset_id": "token", "side": "SELL", "price": "0.53", "size": "6"})

        bids, asks, _age = stream.get_book("token")

        self.assertEqual(bids, [(0.49, 3.0)])
        self.assertEqual(asks, [(0.51, 4.0), (0.53, 6.0)])

    def test_connect_uses_protocol_ping_without_extra_ping_task(self):
        class FakeWebSocket:
            async def send(self, _payload):
                return None

            async def close(self):
                return None

        async def run_case():
            stream = ClobBookStream()

            async def fake_recv_loop():
                await asyncio.Event().wait()

            async def fake_connect(*_args, **kwargs):
                return FakeWebSocket()

            stream._recv_loop = fake_recv_loop
            with mock.patch("poly_monitor.clob_stream.websockets.connect", side_effect=fake_connect) as connect:
                await stream.connect(["token"])
                recv_task = stream._recv_task
                try:
                    self.assertIsNotNone(recv_task)
                    self.assertFalse(hasattr(stream, "_ping_task"))
                    self.assertEqual(connect.call_args.kwargs["ping_interval"], 10)
                    self.assertEqual(connect.call_args.kwargs["ping_timeout"], 15)
                finally:
                    await stream.close()

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
