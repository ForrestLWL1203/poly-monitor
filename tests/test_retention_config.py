from __future__ import annotations

import unittest

from poly_monitor.observer import ObserverConfig
from scripts.run_crypto_wallet_observer import build_parser


class RetentionConfigTests(unittest.TestCase):
    def test_observer_defaults_keep_only_48h_hot_research_data(self):
        config = ObserverConfig()

        self.assertEqual(config.raw_retention_days, 2)
        self.assertEqual(config.cleanup_interval_hours, 1.0)
        self.assertEqual(config.watchlist_activity_retention_days, 2)
        self.assertEqual(config.non_watchlist_activity_retention_days, 2)
        self.assertEqual(config.context_retention_days, 2)
        self.assertEqual(config.market_state_retention_days, 2)
        self.assertEqual(config.strategy_archive_interval_hours, 1.0)

    def test_observer_cli_defaults_match_48h_hot_research_data_policy(self):
        args = build_parser().parse_args([])

        self.assertEqual(args.raw_retention_days, 2)
        self.assertEqual(args.cleanup_interval_hours, 1.0)
        self.assertEqual(args.watchlist_activity_retention_days, 2)
        self.assertEqual(args.non_watchlist_activity_retention_days, 2)
        self.assertEqual(args.context_retention_days, 2)
        self.assertEqual(args.market_state_retention_days, 2)
        self.assertEqual(args.strategy_archive_interval_hours, 1.0)


if __name__ == "__main__":
    unittest.main()
