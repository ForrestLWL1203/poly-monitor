from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
