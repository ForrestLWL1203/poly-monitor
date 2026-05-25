import unittest
from unittest.mock import patch

from poly_monitor.wallet_metrics import behavior_metrics, build_metrics_from_api


class WalletMetricsTests(unittest.TestCase):
    def test_behavior_metrics_detects_dual_side_and_late_bias(self):
        trades = [
            {"slug": "btc-updown-5m-1", "timestamp": 1, "outcome": "Up", "usdcSize": 10},
            {"slug": "btc-updown-5m-1", "timestamp": 2, "outcome": "Down", "usdcSize": 10},
            {"slug": "btc-updown-5m-1", "timestamp": 3, "outcome": "Down", "usdcSize": 30},
            {"slug": "btc-updown-5m-1", "timestamp": 4, "outcome": "Down", "usdcSize": 30},
        ]
        closed = [{"slug": "btc-updown-5m-1", "outcome": "Down", "realizedPnl": 20}]

        metrics = behavior_metrics(trades, closed)

        self.assertEqual(metrics["dual_side_rate"], 1.0)
        self.assertEqual(metrics["late_bias_shift"], 1.0)
        self.assertEqual(metrics["winner_add_rate"], 1.0)

    def test_build_metrics_uses_recent_activity_markets_for_activity_counts(self):
        activity = [
            {"type": "TRADE", "slug": "btc-updown-5m-100", "timestamp": 1000, "outcome": "Up", "usdcSize": 10},
            {"type": "TRADE", "slug": "btc-updown-5m-100", "timestamp": 1001, "outcome": "Down", "usdcSize": 10},
        ]
        closed = [
            {"slug": "btc-updown-5m-100", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 1, "avgPrice": 0.5, "outcome": "Up"},
            {"slug": "eth-updown-5m-200", "endDate": "1970-01-01T00:16:45+00:00", "realizedPnl": 2, "avgPrice": 0.5, "outcome": "Down"},
        ]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=[activity, []]), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 12.5, "name": "profile"},
                {"amount": 34.5, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=2, closed_pages=2)

        self.assertEqual(metrics["trades_7d"], 2)
        self.assertEqual(metrics["markets_24h"], 1)
        self.assertEqual(metrics["markets_7d"], 1)
        self.assertEqual(metrics["markets_30d"], 1)
        self.assertEqual(metrics["historical_markets"], 1)
        self.assertEqual(metrics["wins_7d"], 0)
        self.assertEqual(metrics["losses_7d"], 0)
        self.assertEqual(metrics["closed_position_wins_7d"], 2)
        self.assertEqual(metrics["closed_position_losses_7d"], 0)
        self.assertEqual(metrics["pnl_7d"], 12.5)
        self.assertEqual(metrics["pnl_30d"], 34.5)
        self.assertEqual(metrics["pnl_source"], "profile_profit")
        self.assertEqual(metrics["profile_pnl_7d"], 12.5)
        self.assertEqual(metrics["profile_pnl_30d"], 34.5)
        self.assertEqual(metrics["crypto_closed_pnl_estimate_30d"], 3)

    def test_closed_positions_do_not_drive_win_loss_counts(self):
        activity = [
            {"type": "TRADE", "slug": "btc-updown-5m-100", "timestamp": 1000, "outcome": "Up", "usdcSize": 10},
        ]
        closed = [
            {"slug": "btc-updown-5m-100", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 50, "avgPrice": 0.2, "outcome": "Up"},
            {"slug": "btc-updown-5m-200", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 25, "avgPrice": 0.3, "outcome": "Down"},
        ]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=[activity, []]), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": -100, "name": "profile"},
                {"amount": -100, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=2, closed_pages=2)

        self.assertEqual(metrics["wins_7d"], 0)
        self.assertEqual(metrics["losses_7d"], 0)
        self.assertEqual(metrics["closed_position_wins_7d"], 2)
        self.assertEqual(metrics["closed_position_losses_7d"], 0)

    def test_build_metrics_uses_profile_profit_when_settled_positions_are_unavailable(self):
        activity = [
            {"type": "TRADE", "slug": "btc-updown-5m-100", "timestamp": 1000, "outcome": "Up", "usdcSize": 10},
        ]
        closed = [
            {
                "slug": "btc-updown-5m-100",
                "endDate": "1970-01-01T00:16:40+00:00",
                "realizedPnl": 100_000,
                "avgPrice": 0.2,
                "outcome": "Up",
            },
        ]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=[activity, []]), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 1_234.5, "name": "profile"},
                {"amount": 3_456.7, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=2, closed_pages=2)

        self.assertEqual(metrics["pnl_7d"], 1_234.5)
        self.assertEqual(metrics["pnl_30d"], 3_456.7)
        self.assertEqual(metrics["pnl_source"], "profile_profit")
        self.assertEqual(metrics["profile_pnl_7d"], 1_234.5)
        self.assertEqual(metrics["profile_pnl_30d"], 3_456.7)
        self.assertEqual(metrics["crypto_closed_pnl_estimate_30d"], 100_000)

    def test_negative_profile_profit_overrides_positive_closed_position_estimate_when_positions_are_unavailable(self):
        activity = [
            {"type": "TRADE", "slug": "btc-updown-5m-100", "timestamp": 1000, "outcome": "Up", "usdcSize": 10},
        ]
        closed = [
            {
                "slug": "btc-updown-5m-100",
                "endDate": "1970-01-01T00:16:40+00:00",
                "realizedPnl": 5_000,
                "avgPrice": 0.2,
                "outcome": "Up",
            },
        ]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=[activity, []]), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 25.0, "name": "profile"},
                {"amount": -31.0, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=2, closed_pages=2)

        self.assertEqual(metrics["pnl_7d"], 25.0)
        self.assertEqual(metrics["pnl_30d"], -31.0)
        self.assertEqual(metrics["pnl_source"], "profile_profit")
        self.assertEqual(metrics["crypto_closed_pnl_estimate_30d"], 5_000)

    def test_profile_profit_overrides_incomplete_position_diagnostics(self):
        activity = [
            {"type": "TRADE", "slug": "btc-updown-5m-1000", "timestamp": 1000, "outcome": "Up", "usdcSize": 10},
        ]
        closed = [
            {"slug": "btc-updown-5m-1000", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 5_000, "avgPrice": 0.2, "outcome": "Up"},
        ]
        positions = [
            {
                "slug": "btc-updown-5m-1000",
                "eventSlug": "btc-updown-5m-1000",
                "cashPnl": -50.0,
                "curPrice": 0,
                "outcome": "Down",
            },
            {
                "slug": "btc-updown-5m-1000",
                "eventSlug": "btc-updown-5m-1000",
                "cashPnl": 10.0,
                "curPrice": 1,
                "outcome": "Up",
            },
            {
                "slug": "eth-updown-5m-1500",
                "eventSlug": "eth-updown-5m-1500",
                "cashPnl": 30.0,
                "curPrice": 1,
                "outcome": "Up",
            },
            {
                "slug": "btc-updown-5m-1600",
                "eventSlug": "btc-updown-5m-1600",
                "cashPnl": 999.0,
                "curPrice": 0.44,
                "outcome": "Up",
            },
        ]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=[activity, []]), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", side_effect=[positions, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 25.0, "name": "profile"},
                {"amount": -31.0, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=2, closed_pages=2)

        self.assertEqual(metrics["pnl_7d"], 25.0)
        self.assertEqual(metrics["pnl_30d"], -31.0)
        self.assertEqual(metrics["pnl_source"], "profile_profit")
        self.assertEqual(metrics["crypto_settled_positions_pnl_30d"], -10.0)
        self.assertEqual(metrics["crypto_settled_positions_markets_30d"], 2)
        self.assertEqual(metrics["crypto_closed_pnl_estimate_30d"], 5_000)

    def test_activity_metrics_saturate_24h_windows_for_high_frequency_page_cap(self):
        first_page = [
            {"type": "TRADE", "slug": f"btc-updown-5m-{100 + idx}", "timestamp": 1000 + idx, "outcome": "Up", "usdcSize": 1}
            for idx in range(500)
        ]
        closed = [{"slug": "btc-updown-5m-100", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 1, "avgPrice": 0.5, "outcome": "Up"}]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", return_value=first_page), patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 1, "name": "profile"},
                {"amount": 1, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, activity_pages=1, closed_pages=2)

        self.assertEqual(metrics["markets_24h"], 288)
        self.assertTrue(metrics["markets_24h_lower_bound"])

    def test_default_activity_paging_marks_high_frequency_wallets_as_saturated(self):
        pages = [
            [
                {"type": "TRADE", "slug": f"btc-updown-5m-{page}", "timestamp": 2000 - page, "outcome": "Up", "usdcSize": 1}
                for idx in range(500)
            ]
            for page in range(3)
        ]
        closed = [{"slug": "btc-updown-5m-0-0", "endDate": "1970-01-01T00:16:40+00:00", "realizedPnl": 1, "avgPrice": 0.5, "outcome": "Up"}]

        with patch("poly_monitor.wallet_metrics.fetch_user_activity", side_effect=pages) as activity, patch(
            "poly_monitor.wallet_metrics.fetch_closed_positions", side_effect=[closed, []]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_positions", return_value=[]
        ), patch(
            "poly_monitor.wallet_metrics.fetch_user_profit",
            side_effect=[
                {"amount": 1, "name": "profile"},
                {"amount": 1, "name": "profile"},
            ],
        ):
            metrics = build_metrics_from_api("0xabc", now_ts=2000, closed_pages=2)

        self.assertEqual(activity.call_count, 3)
        self.assertEqual(metrics["trades_24h"], 1500)
        self.assertEqual(metrics["markets_24h"], 288)
        self.assertTrue(metrics["markets_24h_lower_bound"])


if __name__ == "__main__":
    unittest.main()
