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
            "last_active_age_hours": 1,
            "historical_trades": 800,
            "historical_markets": 100,
            "historical_pnl": 100.0,
        }

        score = score_wallet(metrics, thresholds)

        self.assertEqual(score.status, "active_candidate")
        self.assertEqual(score.reasons, [])

    def test_score_wallet_downgrades_wallet_inactive_for_more_than_one_hour(self):
        metrics = {
            "wallet": "0x1adbccaf449aa1f84b48e1f1ec689bdacefc1495",
            "trades_24h": 259,
            "markets_24h": 103,
            "markets_24h_lower_bound": True,
            "trades_7d": 1040,
            "trades_30d": 1040,
            "pnl_7d": 143.29879,
            "pnl_30d": 303.787292,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 0,
            "losses_7d": 0,
            "top1_concentration": 0.006985,
            "top3_concentration": 0.019501,
            "longshot_profit_share": 0.0,
            "longshot_profit_markets": 0,
            "last_active_age_hours": 1.075,
            "historical_trades": 1040,
            "historical_markets": 400,
            "historical_pnl": 303.787292,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("inactive_for_active", score.reasons)

    def test_score_wallet_archives_terminal_thin_edge_wallets(self):
        metrics = {
            "wallet": "0x562a11bcd7354ea82a09ed803cb1739d60862ad4",
            "trades_24h": 120,
            "markets_24h": 120,
            "trades_7d": 800,
            "trades_30d": 1200,
            "pnl_7d": 20.0,
            "pnl_30d": 100.0,
            "wins_7d": 90,
            "losses_7d": 10,
            "top1_concentration": 0.05,
            "top3_concentration": 0.15,
            "longshot_profit_share": 0.0,
            "longshot_profit_markets": 0,
            "last_active_age_hours": 0.1,
            "historical_trades": 1200,
            "historical_markets": 300,
            "historical_pnl": 100.0,
            "terminal_near_certain_trades_30d": 100,
            "terminal_near_certain_trade_share_30d": 0.9,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "archive_candidate")
        self.assertIn("uncopyable_terminal_thin_edge", score.reasons)

    def test_active_rank_uses_local_observed_quality_not_cancelled_api_wins(self):
        metrics = {
            "wallet": "0x25f4707c93e4bfdf26cd6c5cc46c5464691cf88e",
            "trades_24h": 3224,
            "markets_24h": 346,
            "trades_7d": 1365,
            "trades_30d": 1365,
            "pnl_7d": 5950.883,
            "pnl_30d": 14926.985,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 0,
            "losses_7d": 0,
            "closed_position_wins_7d": 174,
            "closed_position_losses_7d": 0,
            "local_observed_pnl_7d": 171.959363,
            "local_observed_settled_markets_7d": 191,
            "local_observed_wins_7d": 110,
            "local_observed_losses_7d": 81,
            "local_observed_span_hours": 22.503,
            "local_observed_max_trades_per_market_24h": 72,
            "top1_concentration": 0.017674,
            "top3_concentration": 0.046623,
            "longshot_profit_share": 0.0,
            "longshot_profit_markets": 0,
            "last_active_age_hours": 0.004,
            "historical_trades": 1365,
            "historical_markets": 60,
            "historical_pnl": 14926.985,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "active_candidate")
        self.assertGreater(score.rank_score, 200)

    def test_active_quality_gate_ignores_cancelled_api_wins(self):
        metrics = {
            "wallet": "0xstaleapi",
            "trades_7d": 1000,
            "markets_24h": 120,
            "trades_30d": 2000,
            "pnl_7d": 500,
            "pnl_30d": 1000,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 0,
            "losses_7d": 99,
            "closed_position_wins_7d": 30,
            "closed_position_losses_7d": 10,
            "top1_concentration": 0.05,
            "top3_concentration": 0.15,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 2000,
            "historical_markets": 200,
            "historical_pnl": 1000,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "active_candidate")
        self.assertNotIn("wins_7d_below_losses", score.reasons)

    def test_quality_gate_uses_only_crypto_closed_position_win_loss_fallback(self):
        metrics = {
            "wallet": "0xmixedprofile",
            "trades_7d": 1000,
            "markets_24h": 120,
            "trades_30d": 2000,
            "pnl_7d": 500,
            "pnl_30d": 1000,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 0,
            "losses_7d": 0,
            "closed_position_wins_7d": 200,
            "closed_position_losses_7d": 0,
            "crypto_closed_position_wins_7d": 1,
            "crypto_closed_position_losses_7d": 20,
            "top1_concentration": 0.05,
            "top3_concentration": 0.15,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 2000,
            "historical_markets": 200,
            "historical_pnl": 1000,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("wins_7d_below_losses", score.reasons)

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

    def test_local_observed_losses_downgrade_high_frequency_wallet(self):
        metrics = {
            "wallet": "0x95fa",
            "trades_24h": 1349,
            "markets_24h": 288,
            "markets_24h_lower_bound": True,
            "trades_7d": 1349,
            "markets_7d": 182,
            "trades_30d": 1349,
            "markets_30d": 182,
            "pnl_7d": 13705.24,
            "pnl_30d": 24602.39,
            "pnl_source": "crypto_closed_positions",
            "wins_7d": 6,
            "losses_7d": 7,
            "local_observed_pnl_7d": -7.09,
            "local_observed_settled_markets_7d": 13,
            "local_observed_wins_7d": 6,
            "local_observed_losses_7d": 7,
            "top1_concentration": 0.014,
            "top3_concentration": 0.038,
            "longshot_profit_share": 0.003,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.05,
            "historical_trades": 1349,
            "historical_markets": 182,
            "historical_pnl": 24602.39,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("local_observed_pnl_7d_not_positive", score.reasons)
        self.assertIn("local_observed_wins_7d_not_above_losses", score.reasons)

    def test_local_observed_quality_can_promote_moderate_frequency_wallet(self):
        metrics = {
            "wallet": "0xcbcf",
            "trades_24h": 229,
            "markets_24h": 73,
            "markets_24h_lower_bound": True,
            "trades_7d": 568,
            "markets_7d": 170,
            "trades_30d": 1057,
            "markets_30d": 359,
            "pnl_7d": 6486.59,
            "pnl_30d": 23841.49,
            "pnl_source": "crypto_closed_positions",
            "wins_7d": 9,
            "losses_7d": 2,
            "local_observed_pnl_7d": 529.42,
            "local_observed_settled_markets_7d": 11,
            "local_observed_wins_7d": 9,
            "local_observed_losses_7d": 2,
            "top1_concentration": 0.039,
            "top3_concentration": 0.101,
            "longshot_profit_share": 0.023,
            "longshot_profit_markets": 3,
            "last_active_age_hours": 0.8,
            "historical_trades": 1057,
            "historical_markets": 359,
            "historical_pnl": 23841.49,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "active_candidate")
        self.assertNotIn("markets_24h_below_threshold", score.reasons)

    def test_reliable_profile_30d_loss_blocks_local_recovery_from_active(self):
        metrics = {
            "wallet": "0xcbcf",
            "trades_24h": 229,
            "markets_24h": 74,
            "trades_7d": 569,
            "markets_7d": 171,
            "trades_30d": 1057,
            "markets_30d": 359,
            "pnl_7d": 455.23,
            "pnl_30d": -8283.84,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 9,
            "losses_7d": 2,
            "local_observed_pnl_7d": 529.42,
            "local_observed_settled_markets_7d": 11,
            "local_observed_wins_7d": 9,
            "local_observed_losses_7d": 2,
            "local_observed_span_hours": 12,
            "top1_concentration": 0.039,
            "top3_concentration": 0.101,
            "longshot_profit_share": 0.023,
            "longshot_profit_markets": 3,
            "last_active_age_hours": 1.4,
            "historical_trades": 1057,
            "historical_markets": 359,
            "historical_pnl": -8283.84,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "dormant_candidate")
        self.assertIn("pnl_30d_not_positive", score.reasons)

    def test_immature_local_sample_does_not_relax_activity_gate(self):
        metrics = {
            "wallet": "0xyoung",
            "trades_24h": 200,
            "markets_24h": 50,
            "trades_7d": 700,
            "markets_7d": 140,
            "trades_30d": 1200,
            "markets_30d": 260,
            "pnl_7d": 500,
            "pnl_30d": 1500,
            "pnl_source": "profile_portfolio_pnl",
            "wins_7d": 11,
            "losses_7d": 1,
            "local_observed_pnl_7d": 120,
            "local_observed_settled_markets_7d": 12,
            "local_observed_wins_7d": 11,
            "local_observed_losses_7d": 1,
            "local_observed_span_hours": 10,
            "top1_concentration": 0.08,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 1200,
            "historical_markets": 260,
            "historical_pnl": 1500,
        }

        immature = score_wallet(metrics, CandidateThresholds())
        mature = score_wallet({**metrics, "local_observed_span_hours": 30}, CandidateThresholds())

        self.assertEqual(immature.status, "dormant_candidate")
        self.assertIn("markets_24h_below_threshold", immature.reasons)
        self.assertEqual(mature.status, "active_candidate")

    def test_local_observed_quality_can_override_negative_historical_positions(self):
        metrics = {
            "wallet": "0xrecovering",
            "trades_24h": 900,
            "markets_24h": 120,
            "trades_7d": 1200,
            "markets_7d": 180,
            "trades_30d": 1500,
            "markets_30d": 260,
            "pnl_7d": -500,
            "pnl_30d": -3000,
            "pnl_source": "crypto_settled_positions",
            "wins_7d": 30,
            "losses_7d": 10,
            "local_observed_pnl_7d": 250,
            "local_observed_settled_markets_7d": 40,
            "local_observed_wins_7d": 30,
            "local_observed_losses_7d": 10,
            "top1_concentration": 0.9,
            "top3_concentration": 0.95,
            "longshot_profit_share": 0.8,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.2,
            "historical_trades": 1500,
            "historical_markets": 260,
            "historical_pnl": -3000,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertEqual(score.status, "active_candidate")
        self.assertNotIn("pnl_7d_not_positive", score.reasons)
        self.assertNotIn("pnl_30d_not_positive", score.reasons)
        self.assertNotIn("top1_concentration_high", score.reasons)

    def test_rank_penalizes_extreme_frequency_when_quality_is_similar(self):
        moderate_frequency = {
            "wallet": "0xmoderate",
            "trades_24h": 220,
            "markets_24h": 60,
            "markets_24h_lower_bound": True,
            "trades_7d": 700,
            "trades_30d": 1200,
            "pnl_7d": 500,
            "pnl_30d": 1500,
            "wins_7d": 18,
            "losses_7d": 6,
            "local_observed_pnl_7d": 500,
            "local_observed_settled_markets_7d": 24,
            "local_observed_wins_7d": 18,
            "local_observed_losses_7d": 6,
            "top1_concentration": 0.08,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 1200,
            "historical_markets": 220,
            "historical_pnl": 1500,
        }
        extreme_frequency = {
            **moderate_frequency,
            "wallet": "0xextreme",
            "trades_24h": 1800,
            "markets_24h": 288,
            "trades_7d": 4000,
            "trades_30d": 8000,
            "historical_trades": 8000,
        }

        moderate = score_wallet(moderate_frequency, CandidateThresholds())
        extreme = score_wallet(extreme_frequency, CandidateThresholds())

        self.assertEqual(moderate.status, "active_candidate")
        self.assertEqual(extreme.status, "active_candidate")
        self.assertGreater(moderate.rank_score, extreme.rank_score)

    def test_score_wallet_downgrades_uncopyable_single_window_frequency(self):
        metrics = {
            "wallet": "0xrobot",
            "trades_24h": 900,
            "markets_24h": 120,
            "trades_7d": 1300,
            "trades_30d": 4000,
            "pnl_7d": 200,
            "pnl_30d": 600,
            "pnl_source": "profile_profit",
            "wins_7d": 80,
            "losses_7d": 20,
            "top1_concentration": 0.05,
            "top3_concentration": 0.2,
            "longshot_profit_share": 0.1,
            "longshot_profit_markets": 1,
            "last_active_age_hours": 0.1,
            "historical_trades": 4000,
            "historical_markets": 200,
            "historical_pnl": 600,
            "local_observed_max_trades_per_market_24h": 603,
        }

        score = score_wallet(metrics, CandidateThresholds())

        self.assertNotEqual(score.status, "active_candidate")
        self.assertIn("uncopyable_single_window_frequency", score.reasons)

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
