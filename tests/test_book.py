import unittest

from poly_monitor.book import fill_for_notional, token_book_summary


class BookTests(unittest.TestCase):
    def test_fill_for_notional_reports_average_limit_and_partial_fill(self):
        self.assertEqual(fill_for_notional([(0.5, 10.0), (0.6, 10.0)], 8.0), {
            "ok": True,
            "avg": 0.533333,
            "limit": 0.6,
            "filled_usdc": 8.0,
        })

        self.assertEqual(fill_for_notional([(0.5, 2.0)], 5.0), {
            "ok": False,
            "avg": 0.5,
            "limit": 0.5,
            "filled_usdc": 1.0,
        })

    def test_token_book_summary_is_compact_and_has_no_full_depth(self):
        row = token_book_summary(
            bids=[(0.49, 20.0), (0.48, 10.0)],
            asks=[(0.51, 10.0), (0.52, 20.0)],
            book_age_ms=150,
            targets=(5.0, 25.0),
        )

        self.assertEqual(row["bid"], 0.49)
        self.assertEqual(row["ask"], 0.51)
        self.assertEqual(row["book_age_ms"], 150)
        self.assertEqual(row["depth_levels"], 20)
        self.assertTrue(row["ask_targets"]["5"]["ok"])
        self.assertEqual(row["ask_targets"]["25"]["limit"], 0.52)
        self.assertIn("bid_targets", row)
        self.assertNotIn("bids", row)
        self.assertNotIn("asks", row)

    def test_token_book_depth_sums_are_limited_to_top_levels(self):
        row = token_book_summary(
            bids=[(0.49, 10.0), (0.48, 1000.0)],
            asks=[(0.51, 10.0), (0.52, 1000.0)],
            book_age_ms=1,
            depth_levels=1,
        )

        self.assertEqual(row["ask_depth_usdc"], 5.1)
        self.assertEqual(row["bid_depth_usdc"], 4.9)


if __name__ == "__main__":
    unittest.main()
