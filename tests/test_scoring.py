import unittest

from poly_monitor.scoring import CandidateThresholds, score_wallet


class ScoringTests(unittest.TestCase):
    def test_score_wallet_promotes_only_high_frequency_profitable_wallets(self):
        thresholds = CandidateThresholds()
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 500,
            "markets_24h": 100,
            "markets_7d": 100,
            "trades_30d": 800,
            "markets_30d": 100,
            "pnl_7d": 10.0,
            "pnl_30d": 100.0,
            "wins_7d": 51,
            "losses_7d": 49,
            "top1_concentration": 0.25,
            "top3_concentration": 0.5,
            "longshot_profit_share": 0.8,
            "longshot_profit_markets": 6,
            "last_active_age_hours": 48,
            "historical_trades": 800,
            "historical_markets": 100,
            "historical_pnl": 100.0,
        }

        score = score_wallet(metrics, thresholds)

        self.assertEqual(score.status, "active_candidate")
        self.assertEqual(score.reasons, [])

    def test_score_wallet_does_not_reject_repeatable_longshot_edge(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1047,
            "markets_24h": 100,
            "markets_7d": 32,
            "trades_30d": 1047,
            "markets_30d": 32,
            "pnl_7d": 30662.75,
            "pnl_30d": 97107.40,
            "wins_7d": 8,
            "losses_7d": 0,
            "top1_concentration": 0.122,
            "top3_concentration": 0.263,
            "longshot_profit_share": 0.668,
            "longshot_profit_markets": 8,
            "last_active_age_hours": 0.1,
            "historical_trades": 1047,
            "historical_markets": 32,
            "historical_pnl": 97107.40,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "active_candidate")

    def test_rank_prefers_consistent_active_win_rate_over_raw_pnl_size(self):
        quality_small_bankroll = {
            "wallet": "0xquality",
            "trades_7d": 1800,
            "markets_24h": 120,
            "trades_30d": 3000,
            "pnl_7d": 120.0,
            "pnl_30d": 300.0,
            "wins_7d": 100,
            "losses_7d": 0,
            "top1_concentration": 0.05,
            "top3_concentration": 0.15,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 3000,
            "historical_markets": 80,
            "historical_pnl": 300.0,
        }
        bigger_pnl_weaker_sample = {
            **quality_small_bankroll,
            "wallet": "0xbigger",
            "trades_7d": 500,
            "markets_24h": 100,
            "trades_30d": 800,
            "pnl_7d": 10_000.0,
            "pnl_30d": 30_000.0,
            "wins_7d": 51,
            "losses_7d": 49,
            "top1_concentration": 0.24,
            "top3_concentration": 0.49,
        }

        quality = score_wallet(quality_small_bankroll, CandidateThresholds())
        bigger = score_wallet(bigger_pnl_weaker_sample, CandidateThresholds())

        self.assertEqual(quality.status, "active_candidate")
        self.assertEqual(bigger.status, "active_candidate")
        self.assertGreater(quality.rank_score, bigger.rank_score)

    def test_score_wallet_downgrades_historically_good_inactive_wallet_to_dormant(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 0,
            "markets_24h": 0,
            "markets_7d": 0,
            "trades_30d": 1200,
            "markets_30d": 180,
            "pnl_7d": 0.0,
            "pnl_30d": 50.0,
            "wins_7d": 0,
            "losses_7d": 0,
            "top1_concentration": 0.2,
            "top3_concentration": 0.45,
            "longshot_profit_share": 0.2,
            "longshot_profit_markets": 2,
            "last_active_age_hours": 96,
            "historical_trades": 1000,
            "historical_markets": 20,
            "historical_pnl": 50.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("inactive_for_active", score.reasons)

    def test_score_wallet_archives_long_inactive_wallets(self):
        metrics = {
            "wallet": "0xabc",
            "last_active_age_hours": 24 * 15,
            "historical_trades": 2000,
            "historical_markets": 200,
            "historical_pnl": 100.0,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
        }

        self.assertEqual(score_wallet(metrics, CandidateThresholds()).status, "archive_candidate")

    def test_score_wallet_allows_equal_win_loss_after_min_sample(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 500,
            "markets_24h": 100,
            "trades_30d": 800,
            "pnl_7d": 10.0,
            "pnl_30d": 100.0,
            "wins_7d": 20,
            "losses_7d": 20,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 1,
            "historical_trades": 800,
            "historical_markets": 100,
            "historical_pnl": 100.0,
        }

        self.assertEqual(score_wallet(metrics, CandidateThresholds()).status, "active_candidate")

    def test_score_wallet_treats_saturated_24h_activity_as_enough_activity(self):
        metrics = {
            "wallet": "0xabc",
            "trades_24h": 1000,
            "trades_7d": 1000,
            "markets_24h": 288,
            "markets_24h_lower_bound": True,
            "trades_30d": 1000,
            "pnl_7d": 10.0,
            "pnl_30d": 100.0,
            "wins_7d": 2,
            "losses_7d": 0,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 1,
            "historical_trades": 1000,
            "historical_markets": 1,
            "historical_pnl": 100.0,
        }

        self.assertEqual(score_wallet(metrics, CandidateThresholds()).status, "active_candidate")

    def test_score_wallet_does_not_promote_small_24h_lower_bound_to_active(self):
        metrics = {
            "wallet": "0xabc",
            "trades_24h": 124,
            "markets_24h": 24,
            "markets_24h_lower_bound": True,
            "activity_page_cap_hit": True,
            "trades_7d": 918,
            "trades_30d": 1269,
            "pnl_7d": 100.0,
            "pnl_30d": 500.0,
            "pnl_source": "crypto_closed_positions",
            "wins_7d": 30,
            "losses_7d": 0,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 1269,
            "historical_markets": 225,
            "historical_pnl": 500.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("markets_24h_below_threshold", score.reasons)

    def test_score_wallet_requires_high_24h_market_activity_for_active(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1000,
            "markets_24h": 20,
            "trades_30d": 1500,
            "markets_30d": 200,
            "pnl_7d": 100.0,
            "pnl_30d": 500.0,
            "pnl_source": "crypto_closed_positions",
            "wins_7d": 30,
            "losses_7d": 0,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 1500,
            "historical_markets": 200,
            "historical_pnl": 500.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("markets_24h_below_threshold", score.reasons)

    def test_score_wallet_rejects_negative_7d_pnl_on_api_metrics(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1000,
            "markets_24h": 20,
            "trades_30d": 1500,
            "markets_30d": 200,
            "pnl_7d": -100.0,
            "pnl_30d": 50.0,
            "pnl_source": "crypto_closed_positions",
            "wins_7d": 30,
            "losses_7d": 10,
            "top1_concentration": 0.1,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 1500,
            "historical_markets": 200,
            "historical_pnl": 50.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertNotEqual(score.status, "active_candidate")
        self.assertIn("pnl_7d_not_positive", score.reasons)

    def test_score_wallet_waits_for_local_settled_ledger_before_active(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1000,
            "markets_24h": 20,
            "trades_30d": 1000,
            "pnl_7d": 0.0,
            "pnl_30d": 0.0,
            "pnl_source": "local_observed_ledger",
            "wins_7d": 0,
            "losses_7d": 0,
            "settled_markets_7d": 0,
            "settled_markets_30d": 0,
            "top1_concentration": 1.0,
            "top3_concentration": 1.0,
            "longshot_profit_share": 0.0,
            "last_active_age_hours": 0.1,
            "historical_trades": 1000,
            "historical_markets": 20,
            "historical_pnl": 0.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("settled_markets_7d_below_threshold", score.reasons)
        self.assertNotIn("pnl_7d_not_positive", score.reasons)

    def test_score_wallet_archives_lossy_local_ledger_dormant_candidate(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1000,
            "markets_24h": 20,
            "trades_30d": 1000,
            "pnl_7d": -50.0,
            "pnl_30d": -50.0,
            "pnl_source": "local_observed_ledger",
            "wins_7d": 0,
            "losses_7d": 1,
            "settled_markets_7d": 1,
            "settled_markets_30d": 1,
            "top1_concentration": 0.0,
            "top3_concentration": 0.0,
            "longshot_profit_share": 0.0,
            "last_active_age_hours": 0.1,
            "historical_trades": 1000,
            "historical_markets": 20,
            "historical_pnl": -50.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "archive_candidate")
        self.assertIn("pnl_30d_not_positive", score.reasons)

    def test_score_wallet_archives_concentrated_local_ledger_dormant_candidate(self):
        metrics = {
            "wallet": "0xabc",
            "trades_7d": 1000,
            "markets_24h": 20,
            "trades_30d": 1000,
            "pnl_7d": 10.0,
            "pnl_30d": 10.0,
            "pnl_source": "local_observed_ledger",
            "wins_7d": 1,
            "losses_7d": 0,
            "settled_markets_7d": 1,
            "settled_markets_30d": 1,
            "top1_concentration": 0.90,
            "top3_concentration": 0.90,
            "longshot_profit_share": 0.0,
            "last_active_age_hours": 0.1,
            "historical_trades": 1000,
            "historical_markets": 20,
            "historical_pnl": 10.0,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "archive_candidate")
        self.assertIn("top1_concentration_high", score.reasons)


if __name__ == "__main__":
    unittest.main()
