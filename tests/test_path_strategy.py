from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.path_strategy import (
    D950MarketPathStrategy,
    PathStrategyConfig,
    RecordingExecutionAdapter,
    SettlementPaperExecutionAdapter,
    WalletPathStrategy,
    load_deep_export_for_path_strategy,
    replay_path_strategy,
)


class PathStrategyTests(unittest.TestCase):
    def test_emits_intent_when_checkpoint_bias_and_book_are_fillable(self):
        strategy = WalletPathStrategy(PathStrategyConfig(wallet="0xabc", checkpoints=(120,), notional_usdc=25, max_price=0.7))
        sample = {
            "market_slug": "btc-updown-5m-1770000000",
            "sampled_ts": 1770000120,
            "book_stale": 0,
            "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.61, "filled_usdc": 25}}},
            "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.45, "filled_usdc": 25}}},
        }
        activity = [
            {
                "wallet": "0xabc",
                "market_slug": "btc-updown-5m-1770000000",
                "activity_type": "TRADE",
                "side": "BUY",
                "outcome": "Up",
                "exchange_ts": 1770000040,
                "usdc": 42.0,
            }
        ]

        intent = strategy.evaluate_snapshot(sample, activity)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.wallet, "0xabc")
        self.assertEqual(intent.market_slug, "btc-updown-5m-1770000000")
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.intent, "BUY")
        self.assertEqual(intent.notional_usdc, 25)
        self.assertEqual(intent.expected_price, 0.61)
        self.assertEqual(intent.reason, "checkpoint_120_net_bias")
        self.assertEqual(intent.features["wallet_net_up_down_usdc"], 42.0)

    def test_rejects_expensive_or_unfillable_book(self):
        strategy = WalletPathStrategy(PathStrategyConfig(wallet="0xabc", checkpoints=(120,), notional_usdc=25, max_price=0.7))
        activity = [
            {
                "wallet": "0xabc",
                "market_slug": "btc-updown-5m-1770000000",
                "activity_type": "TRADE",
                "side": "BUY",
                "outcome": "Up",
                "exchange_ts": 1770000040,
                "usdc": 42.0,
            }
        ]

        expensive = strategy.evaluate_snapshot(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.91}}},
            },
            activity,
        )
        unfillable = strategy.evaluate_snapshot(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "up_json": {"ask_targets": {"25": {"ok": False}}},
            },
            activity,
        )

        self.assertIsNone(expensive)
        self.assertIsNone(unfillable)

    def test_replay_submits_once_per_market_to_pluggable_adapter(self):
        adapter = RecordingExecutionAdapter()
        activity = [
            {
                "wallet": "0xabc",
                "market_slug": "btc-updown-5m-1770000000",
                "activity_type": "TRADE",
                "side": "BUY",
                "outcome": "Down",
                "exchange_ts": 1770000040,
                "usdc": 50.0,
            }
        ]
        samples = [
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.52}}},
            },
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000180,
                "book_stale": 0,
                "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.54}}},
            },
        ]

        result = replay_path_strategy(
            activity,
            samples,
            PathStrategyConfig(wallet="0xabc", checkpoints=(120, 180), notional_usdc=25),
            adapter=adapter,
        )

        self.assertEqual(len(result.intents), 1)
        self.assertEqual(len(adapter.submitted), 1)
        self.assertEqual(adapter.submitted[0].outcome, "Down")
        self.assertEqual(result.executions[0].status, "recorded")

    def test_settlement_paper_adapter_computes_simulated_pnl_without_live_orders(self):
        adapter = SettlementPaperExecutionAdapter({"btc-updown-5m-1770000000": "Up"})
        strategy = WalletPathStrategy(PathStrategyConfig(wallet="0xabc", checkpoints=(120,), notional_usdc=25, max_price=0.7))
        intent = strategy.evaluate_snapshot(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5, "filled_usdc": 25}}},
            },
            [
                {
                    "wallet": "0xabc",
                    "market_slug": "btc-updown-5m-1770000000",
                    "activity_type": "TRADE",
                    "side": "BUY",
                    "outcome": "Up",
                    "exchange_ts": 1770000040,
                    "usdc": 42.0,
                }
            ],
        )
        assert intent is not None

        result = adapter.submit(intent)

        self.assertEqual(result.status, "paper_settled")
        self.assertEqual(result.detail["winning_side"], "Up")
        self.assertEqual(result.detail["shares"], 50.0)
        self.assertEqual(result.detail["realized_pnl"], 25.0)

    def test_loads_replay_inputs_from_deep_export_zip(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr("wallet_activity.jsonl", json.dumps({"wallet": "0xabc"}) + "\n")
                bundle.writestr("deep_collection/market_state_samples.jsonl", json.dumps({"market_slug": "btc-updown-5m-1"}) + "\n")

            loaded = load_deep_export_for_path_strategy(zip_path)

        self.assertEqual(loaded.activity_rows, [{"wallet": "0xabc"}])
        self.assertEqual(loaded.market_state_samples, [{"market_slug": "btc-updown-5m-1"}])

    def test_d950_market_strategy_uses_market_history_not_wallet_activity(self):
        strategy = D950MarketPathStrategy(PathStrategyConfig(wallet="strategy", checkpoints=(120,), notional_usdc=25, max_price=0.7))
        sample = {
            "market_slug": "btc-updown-5m-1770000000",
            "sampled_ts": 1770000120,
            "book_stale": 0,
            "reference_price": 101.2,
            "_market_state_history": [
                {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000001, "reference_price": 100.0},
                {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000120, "reference_price": 101.2},
            ],
            "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.55}}},
            "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.45}}},
        }

        intent = strategy.evaluate_snapshot(sample, [])

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.reason, "d950_path_v0_reference_momentum")
        self.assertEqual(intent.features["reference_delta"], 1.2)


if __name__ == "__main__":
    unittest.main()
