from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.path_strategy import (
    D950MarketPathStrategy,
    ParityTerminalBiasStrategy,
    PathStrategyConfig,
    RecordingExecutionAdapter,
    SettlementPaperExecutionAdapter,
    WalletPathStrategy,
    load_deep_export_for_path_strategy,
    replay_path_strategy,
)
from poly_monitor.strategies.pair_cost_inventory import X32PairCostInventoryStrategy
from poly_monitor.strategy_backtest import BacktestResult
from poly_monitor.strategy_runtime import StrategyHistory, StrategySnapshot, TradeIntent


class PathStrategyTests(unittest.TestCase):
    def test_legacy_path_strategy_imports_remain_available(self):
        self.assertEqual(WalletPathStrategy.strategy_name, "wallet_path_v0")
        self.assertEqual(X32PairCostInventoryStrategy.strategy_name, "x32_pair_cost_inventory_v0")
        self.assertTrue(issubclass(X32PairCostInventoryStrategy, WalletPathStrategy))

    def test_x32_strategy_stops_new_inventory_after_terminal_stop(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                notional_usdc=5,
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                min_order_usdc=1,
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000305,
                "book_stale": 0,
                "up_json": {"bid": 0.48, "ask": 0.49, "ask_depth_usdc": 100, "ask_targets": {"5": {"ok": True, "avg": 0.49, "filled_usdc": 5}}},
                "down_json": {"bid": 0.49, "ask": 0.50, "ask_depth_usdc": 100, "ask_targets": {"5": {"ok": True, "avg": 0.50, "filled_usdc": 5}}},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNone(intent)

    def test_x32_strategy_sizes_inventory_from_own_budget_not_observed_wallet_shares(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                notional_usdc=5,
                max_price=0.95,
                target_pair_notional_usdc=20,
                target_pair_shares_per_side=999,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000005,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.50},
                "down_json": {"bid": 0.50, "ask": 0.51},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.intent, "BUY")
        self.assertEqual(intent.reason, "x32_pair_cost_inventory")
        self.assertEqual(intent.expected_price, 0.49)
        self.assertEqual(intent.notional_usdc, 4.9)
        self.assertEqual(intent.features["sizing_mode"], "fixed_share_clip")
        self.assertEqual(intent.features["order_shares"], 10.0)
        self.assertAlmostEqual(intent.features["target_pair_notional_usdc"], 20.0)
        self.assertAlmostEqual(intent.features["target_pair_shares_per_side"], 20.20202, places=5)
        self.assertEqual(intent.features["book_fill"]["source"], "maker_quote_at_best_bid")

    def test_x32_strategy_targets_pair_cost_below_one_not_large_share_counts(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=30,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000011,
                "book_stale": 0,
                "up_json": {"bid": 0.83, "ask": 0.84},
                "down_json": {"bid": 0.16, "ask": 0.17},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.features["maker_pair_cost"], 0.99)
        self.assertEqual(intent.features["target_pair_notional_usdc"], 30.0)
        self.assertAlmostEqual(intent.features["target_pair_shares_per_side"], 30.30303, places=5)

    def test_x32_strategy_rejects_wide_or_stale_or_shallow_maker_books(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                target_pair_notional_usdc=30,
                max_price=0.95,
                max_pair_cost=0.995,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
                max_quote_spread=0.02,
                max_quote_book_age_ms=50,
                min_quote_bid_depth_usdc=20,
            )
        )

        def evaluate(up_json: dict, down_json: dict) -> TradeIntent | None:
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": up_json,
                    "down_json": down_json,
                }
            )
            return strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        clean_up = {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 10, "bid_depth_usdc": 40}
        clean_down = {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 10, "bid_depth_usdc": 40}

        self.assertIsNotNone(evaluate(clean_up, clean_down))
        self.assertIsNone(evaluate({**clean_up, "spread": 0.03}, clean_down))
        self.assertIsNone(evaluate({**clean_up, "book_age_ms": 60}, clean_down))
        self.assertIsNone(evaluate({**clean_up, "bid_depth_usdc": 10}, clean_down))

    def test_x32_strategy_reports_clip_shares_from_winning_candidate(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                target_pair_notional_usdc=30,
                max_price=0.95,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000011,
                "book_stale": 0,
                "up_json": {"bid": 0.40, "ask": 0.41},
                "down_json": {"bid": 0.58, "ask": 0.59},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.features["order_shares"], 10.0)
        self.assertEqual(intent.features["clip_shares"], 10.0)

    def test_x32_strategy_rejects_pair_cost_above_one_as_active_target(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=30,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000011,
                "book_stale": 0,
                "up_json": {"bid": 0.50, "ask": 0.51},
                "down_json": {"bid": 0.51, "ask": 0.52},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNone(intent)

    def test_x32_strategy_prioritizes_inventory_rebalance_side(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000065,
                "book_stale": 0,
                "up_json": {"bid": 0.40, "ask": 0.41},
                "down_json": {"bid": 0.58, "ask": 0.59},
            }
        )
        history = StrategyHistory(
            emitted_intents=[
                TradeIntent(
                    strategy_name="x32_pair_cost_inventory_v0",
                    wallet="0x32",
                    market_slug=snapshot.market_slug,
                    sampled_ts=1770000005,
                    checkpoint_sec=1,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=20.0,
                    max_price=0.95,
                    expected_price=0.40,
                    symbol="BTC",
                    reason="seed_inventory",
                )
            ],
            snapshots_by_market={snapshot.market_slug: [snapshot]},
        )

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Down")
        self.assertEqual(intent.expected_price, 0.59)
        self.assertEqual(intent.features["deficit_side"], "Down")
        self.assertEqual(intent.features["book_fill"]["source"], "maker_rebalance_quote")

    def test_x32_strategy_keeps_ten_share_clip_for_high_price_mid_window_building(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000125,
                "book_stale": 0,
                "up_json": {"bid": 0.82, "ask": 0.83},
                "down_json": {"bid": 0.16, "ask": 0.17},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Down")
        self.assertEqual(intent.features["order_shares"], 10.0)
        self.assertEqual(intent.features["clip_shares"], 10.0)

    def test_x32_strategy_uses_small_clip_for_final_rebalance(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000275,
                "book_stale": 0,
                "up_json": {"bid": 0.52, "ask": 0.53},
                "down_json": {"bid": 0.47, "ask": 0.48},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.features["order_shares"], 5.0)
        self.assertEqual(intent.features["clip_shares"], 5.0)

    def test_x32_strategy_chases_deficit_side_after_rebalance_start(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
                rebalance_start_sec=240,
                maker_rebalance_ticks=1,
                tick_size=0.01,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000245,
                "book_stale": 0,
                "up_json": {"bid": 0.40, "ask": 0.41},
                "down_json": {"bid": 0.58, "ask": 0.60},
            }
        )
        history = StrategyHistory(
            emitted_intents=[
                TradeIntent(
                    strategy_name="x32_pair_cost_inventory_v0",
                    wallet="0x32",
                    market_slug=snapshot.market_slug,
                    sampled_ts=1770000005,
                    checkpoint_sec=1,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=20.0,
                    max_price=0.95,
                    expected_price=0.40,
                    symbol="BTC",
                    reason="seed_inventory",
                )
            ],
            snapshots_by_market={snapshot.market_slug: [snapshot]},
        )

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Down")
        self.assertEqual(intent.expected_price, 0.60)
        self.assertEqual(intent.features["book_fill"]["source"], "maker_rebalance_quote")
        self.assertEqual(intent.features["paired_recovery_side"], "Down")

    def test_x32_absolute_unpaired_cap_forces_lagging_recovery(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=140,
                max_pair_cost=0.995,
                max_pair_cost_recovery=1.03,
                max_unpaired_price=0.70,
                max_unpaired_shares=10.0,
                paired_balance_min_ratio=0.80,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000042,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.50},
                "down_json": {"bid": 0.49, "ask": 0.51},
            }
        )
        history = StrategyHistory(
            emitted_intents=[
                TradeIntent(
                    strategy_name="x32_pair_cost_inventory_v0",
                    wallet="0x32",
                    market_slug=snapshot.market_slug,
                    sampled_ts=1770000005,
                    checkpoint_sec=1,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=49.0,
                    max_price=0.95,
                    expected_price=0.49,
                    symbol="BTC",
                    reason="seed_inventory",
                ),
                TradeIntent(
                    strategy_name="x32_pair_cost_inventory_v0",
                    wallet="0x32",
                    market_slug=snapshot.market_slug,
                    sampled_ts=1770000006,
                    checkpoint_sec=1,
                    intent="BUY",
                    outcome="Down",
                    notional_usdc=41.65,
                    max_price=0.95,
                    expected_price=0.49,
                    symbol="BTC",
                    reason="seed_inventory",
                ),
            ],
            snapshots_by_market={snapshot.market_slug: [snapshot]},
        )

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Down")
        self.assertEqual(intent.expected_price, 0.51)
        self.assertEqual(intent.features["paired_recovery_side"], "Down")
        self.assertAlmostEqual(intent.features["filled_balance_ratio"], 0.85)
        self.assertAlmostEqual(intent.features["filled_unpaired_shares"], 15.0)
        self.assertEqual(intent.features["max_unpaired_shares"], 10.0)
        self.assertEqual(intent.features["pair_cost_cap"], 1.03)
        self.assertTrue(intent.features["quote_forced_to_ask"])

    def test_x32_strategy_treats_pending_quotes_as_working_inventory_for_sizing(self):
        strategy = X32PairCostInventoryStrategy(
            PathStrategyConfig(
                wallet="0x32",
                checkpoints=(1,),
                max_price=0.95,
                target_pair_notional_usdc=55,
                max_pair_cost=0.995,
                max_unpaired_price=0.70,
                min_order_usdc=1,
                execution_style="maker",
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000012,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.50},
                "down_json": {"bid": 0.50, "ask": 0.51},
            }
        )
        pending_quote = TradeIntent(
            strategy_name="x32_pair_cost_inventory_v0",
            wallet="0x32",
            market_slug=snapshot.market_slug,
            sampled_ts=1770000011,
            checkpoint_sec=1,
            intent="BUY",
            outcome="Down",
            notional_usdc=55.0,
            max_price=0.95,
            expected_price=0.49,
            symbol="BTC",
            reason="pending_quote",
        )
        history = StrategyHistory(
            pending_intents=[pending_quote],
            snapshots_by_market={snapshot.market_slug: [snapshot]},
        )

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.features["current_down_shares"], 0.0)
        self.assertGreater(intent.features["working_down_shares"], 0.0)

    def test_wallet_path_builds_scaled_pair_inventory_without_wallet_activity(self):
        strategy = WalletPathStrategy(
            PathStrategyConfig(
                wallet="0xabc",
                checkpoints=(1,),
                notional_usdc=10,
                max_price=0.7,
                target_pair_notional_usdc=100,
                max_pair_cost=1.01,
                min_order_usdc=1,
                one_trade_per_market=False,
            )
        )
        first = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
            }
        )
        second = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000121,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
            }
        )
        history = StrategyHistory(snapshots_by_market={"btc-updown-5m-1770000000": [first, second]})

        first_intent = strategy.evaluate(first, history)
        assert first_intent is not None
        history.emitted_intents.append(first_intent)
        second_intent = strategy.evaluate(second, history)

        self.assertEqual(first_intent.outcome, "Up")
        self.assertEqual(first_intent.notional_usdc, 10)
        self.assertEqual(first_intent.features["target_up_shares"], 40)
        self.assertEqual(first_intent.features["current_up_shares"], 0)
        self.assertIsNotNone(second_intent)
        assert second_intent is not None
        self.assertEqual(second_intent.outcome, "Down")
        self.assertEqual(second_intent.notional_usdc, 10)
        self.assertEqual(second_intent.features["target_down_shares"], 40.333333)
        self.assertEqual(second_intent.features["pair_cost"], 1.0)

    def test_wallet_path_inventory_logic_is_symbol_agnostic(self):
        strategy = WalletPathStrategy(
            PathStrategyConfig(
                wallet="0xabc",
                checkpoints=(1,),
                notional_usdc=10,
                max_price=0.7,
                target_pair_notional_usdc=100,
                max_pair_cost=1.01,
                min_order_usdc=1,
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000010,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
            }
        )
        history = StrategyHistory(snapshots_by_market={"eth-updown-5m-1770000000": [snapshot]})

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.symbol, "ETH")
        self.assertEqual(intent.outcome, "Up")

    def test_wallet_path_rejects_expensive_pair_cost(self):
        strategy = WalletPathStrategy(
            PathStrategyConfig(
                wallet="0xabc",
                checkpoints=(1,),
                notional_usdc=10,
                max_price=0.7,
                target_pair_notional_usdc=100,
                max_pair_cost=0.99,
                min_order_usdc=1,
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000010,
                "book_stale": 0,
                "up_json": {"bid": 0.50, "ask": 0.51, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.51, "filled_usdc": 10}}},
                "down_json": {"bid": 0.49, "ask": 0.50, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.50, "filled_usdc": 10}}},
            }
        )

        history = StrategyHistory(
            snapshots_by_market={"eth-updown-5m-1770000000": [snapshot]},
            emitted_intents=[
                TradeIntent(
                    market_slug="eth-updown-5m-1770000000",
                    sampled_ts=1770000009,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=10,
                    max_price=0.7,
                    expected_price=0.51,
                    reason="seed",
                )
            ],
        )

        intent = strategy.evaluate(snapshot, history)

        self.assertIsNone(intent)

    def test_wallet_path_can_size_by_target_shares_per_side(self):
        strategy = WalletPathStrategy(
            PathStrategyConfig(
                wallet="0xabc",
                checkpoints=(1,),
                notional_usdc=10,
                max_price=0.7,
                target_pair_notional_usdc=1000,
                target_pair_shares_per_side=40,
                max_pair_cost=1.01,
                min_order_usdc=1,
                one_trade_per_market=False,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000150,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5, "filled_usdc": 10}}},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={"btc-updown-5m-1770000000": [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.notional_usdc, 9.8)
        self.assertEqual(intent.features["sizing_mode"], "shares_per_side")
        self.assertEqual(intent.features["target_pair_shares_per_side"], 40)
        self.assertEqual(intent.features["target_up_shares"], 20)

    def test_wallet_path_replay_can_emit_multiple_inventory_orders_in_same_market(self):
        adapter = RecordingExecutionAdapter()
        samples = [
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000010,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
            },
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000020,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
            },
        ]

        result = replay_path_strategy(
            [],
            samples,
            PathStrategyConfig(
                wallet="0xabc",
                checkpoints=(1,),
                notional_usdc=10,
                target_pair_notional_usdc=100,
                max_pair_cost=1.01,
                min_order_usdc=1,
                one_trade_per_market=False,
            ),
            adapter=adapter,
        )

        self.assertEqual([intent.outcome for intent in result.intents], ["Up", "Down"])

    def test_emits_intent_when_checkpoint_bias_and_book_are_fillable(self):
        strategy = WalletPathStrategy(PathStrategyConfig(wallet="0xabc", checkpoints=(120,), notional_usdc=25, max_price=0.7, target_pair_notional_usdc=100, max_pair_cost=1.1))
        sample = {
            "market_slug": "btc-updown-5m-1770000000",
            "sampled_ts": 1770000120,
            "book_stale": 0,
            "up_json": {"bid": 0.60, "ask": 0.61, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.61, "filled_usdc": 25}}},
            "down_json": {"bid": 0.44, "ask": 0.45, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.45, "filled_usdc": 25}}},
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
        self.assertEqual(intent.outcome, "Down")
        self.assertEqual(intent.intent, "BUY")
        self.assertEqual(intent.notional_usdc, 16.603774)
        self.assertEqual(intent.expected_price, 0.44)
        self.assertEqual(intent.reason, "checkpoint_120_pair_cost_inventory")
        self.assertEqual(intent.features["pair_cost"], 1.06)
        self.assertEqual(intent.features["execution_style"], "maker")
        self.assertEqual(intent.features["book_fill"]["source"], "maker_quote_at_best_bid")

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
                "up_json": {"bid": 0.47, "ask": 0.48, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.48}}},
                "down_json": {"bid": 0.51, "ask": 0.52, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.52}}},
            },
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000180,
                "book_stale": 0,
                "up_json": {"bid": 0.45, "ask": 0.46, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.46}}},
                "down_json": {"bid": 0.53, "ask": 0.54, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.54}}},
            },
        ]

        result = replay_path_strategy(
            activity,
            samples,
            PathStrategyConfig(wallet="0xabc", checkpoints=(120, 180), notional_usdc=25, target_pair_notional_usdc=100, max_pair_cost=1.1),
            adapter=adapter,
        )

        self.assertEqual(len(result.intents), 1)
        self.assertEqual(len(adapter.submitted), 1)
        self.assertEqual(adapter.submitted[0].outcome, "Up")
        self.assertEqual(result.executions[0].status, "recorded")

    def test_settlement_paper_adapter_computes_simulated_pnl_without_live_orders(self):
        adapter = SettlementPaperExecutionAdapter({"btc-updown-5m-1770000000": "Up"})
        strategy = WalletPathStrategy(PathStrategyConfig(wallet="0xabc", checkpoints=(120,), notional_usdc=25, max_price=0.7, target_pair_notional_usdc=100, max_pair_cost=1.1))
        intent = strategy.evaluate_snapshot(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "sampled_ts": 1770000120,
                "book_stale": 0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.5, "filled_usdc": 25}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"25": {"ok": True, "avg": 0.5, "filled_usdc": 25}}},
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
        self.assertEqual(result.detail["shares"], 40.0)
        self.assertEqual(result.detail["realized_pnl"], 20.4)

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

    def test_d950_market_strategy_rejects_zero_reference_price(self):
        strategy = D950MarketPathStrategy(PathStrategyConfig(wallet="strategy", checkpoints=(120,), notional_usdc=25, max_price=0.7))
        sample = {
            "market_slug": "btc-updown-5m-1770000000",
            "sampled_ts": 1770000120,
            "book_stale": 0,
            "reference_price": 0,
            "_market_state_history": [
                {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000001, "reference_price": 100.0},
                {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000120, "reference_price": 0},
            ],
            "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.05}}},
        }

        intent = strategy.evaluate_snapshot(sample, [])

        self.assertIsNone(intent)

    def test_parity_terminal_bias_builds_pair_inventory_before_terminal_phase(self):
        strategy = ParityTerminalBiasStrategy(
            PathStrategyConfig(
                wallet="strategy",
                checkpoints=(1,),
                notional_usdc=10,
                target_pair_notional_usdc=60,
                max_pair_cost=1.01,
                max_price=0.7,
                min_order_usdc=1,
            )
        )
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000060,
                "book_stale": 0,
                "reference_price": 100.0,
                "up_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
                "down_json": {"bid": 0.49, "ask": 0.5, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.5}}},
            }
        )

        intent = strategy.evaluate(snapshot, StrategyHistory(snapshots_by_market={snapshot.market_slug: [snapshot]}))

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.reason, "parity_terminal_bias_pair_inventory")
        self.assertEqual(intent.features["phase"], "pair_inventory")
        self.assertEqual(intent.features["symbol"], "ETH")

    def test_parity_terminal_bias_adds_terminal_overlay_for_strong_reference_and_book_signal(self):
        strategy = ParityTerminalBiasStrategy(
            PathStrategyConfig(
                wallet="strategy",
                checkpoints=(1,),
                notional_usdc=10,
                target_pair_notional_usdc=20,
                max_pair_cost=1.01,
                max_price=0.95,
                min_order_usdc=1,
            )
        )
        first = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000210,
                "book_stale": 0,
                "reference_price": 100.0,
                "up_json": {"bid": 0.51, "ask": 0.52, "ask_depth_usdc": 100},
                "down_json": {"bid": 0.47, "ask": 0.48, "ask_depth_usdc": 100},
            }
        )
        terminal = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000250,
                "book_stale": 0,
                "reference_price": 101.0,
                "up_json": {"bid": 0.62, "ask": 0.64, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.64}}},
                "down_json": {"bid": 0.35, "ask": 0.37, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.37}}},
            }
        )
        history = StrategyHistory(
            snapshots_by_market={terminal.market_slug: [first, terminal]},
            emitted_intents=[
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000120,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000121,
                    intent="BUY",
                    outcome="Down",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
            ],
        )

        intent = strategy.evaluate(terminal, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.reason, "parity_terminal_bias_overlay")
        self.assertEqual(intent.features["phase"], "terminal_bias")
        self.assertGreaterEqual(intent.features["bias_score"], 3)

    def test_parity_terminal_bias_skips_weak_terminal_signal_after_pair_target_is_met(self):
        strategy = ParityTerminalBiasStrategy(
            PathStrategyConfig(
                wallet="strategy",
                checkpoints=(1,),
                notional_usdc=10,
                target_pair_notional_usdc=20,
                max_pair_cost=1.01,
                max_price=0.95,
                min_order_usdc=1,
            )
        )
        first = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000210,
                "book_stale": 0,
                "reference_price": 100.0,
                "up_json": {"bid": 0.5, "ask": 0.51, "ask_depth_usdc": 100},
                "down_json": {"bid": 0.48, "ask": 0.49, "ask_depth_usdc": 100},
            }
        )
        terminal = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000250,
                "book_stale": 0,
                "reference_price": 100.004,
                "up_json": {"bid": 0.51, "ask": 0.52, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.52}}},
                "down_json": {"bid": 0.47, "ask": 0.48, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.48}}},
            }
        )
        history = StrategyHistory(
            snapshots_by_market={terminal.market_slug: [first, terminal]},
            emitted_intents=[
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000120,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000121,
                    intent="BUY",
                    outcome="Down",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
            ],
        )

        intent = strategy.evaluate(terminal, history)

        self.assertIsNone(intent)

    def test_parity_terminal_bias_can_trigger_from_terminal_book_favorite_without_reference_price(self):
        strategy = ParityTerminalBiasStrategy(
            PathStrategyConfig(
                wallet="strategy",
                checkpoints=(1,),
                notional_usdc=10,
                target_pair_notional_usdc=20,
                max_pair_cost=1.01,
                max_price=0.95,
                min_order_usdc=1,
            )
        )
        terminal = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "eth-updown-5m-1770000000",
                "symbol": "ETH",
                "sampled_ts": 1770000270,
                "book_stale": 0,
                "up_json": {"bid": 0.91, "ask": 0.93, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.93}}},
                "down_json": {"bid": 0.06, "ask": 0.08, "ask_depth_usdc": 100, "ask_targets": {"10": {"ok": True, "avg": 0.08}}},
            }
        )
        history = StrategyHistory(
            snapshots_by_market={terminal.market_slug: [terminal]},
            emitted_intents=[
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000120,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
                TradeIntent(
                    market_slug=terminal.market_slug,
                    sampled_ts=1770000121,
                    intent="BUY",
                    outcome="Down",
                    notional_usdc=10,
                    max_price=0.95,
                    expected_price=0.5,
                    reason="seed",
                ),
            ],
        )

        intent = strategy.evaluate(terminal, history)

        self.assertIsNotNone(intent)
        assert intent is not None
        self.assertEqual(intent.outcome, "Up")
        self.assertEqual(intent.features["phase"], "terminal_bias")
        self.assertEqual(intent.features["book_favorite_side"], "Up")

    def test_backtest_result_groups_paper_pnl_by_symbol(self):
        result = BacktestResult(
            summary={"paper_total_pnl": 10.0},
            trades=[
                {
                    "intent": {"symbol": "ETH"},
                    "execution": {"status": "paper_settled", "detail": {"realized_pnl": 7.0}},
                },
                {
                    "intent": {"symbol": "BTC"},
                    "execution": {"status": "paper_settled", "detail": {"realized_pnl": -2.0}},
                },
                {
                    "intent": {"symbol": "ETH"},
                    "execution": {"status": "paper_settled", "detail": {"realized_pnl": 5.0}},
                },
            ],
        )

        payload = result.to_dict()

        self.assertEqual(payload["summary_by_symbol"]["ETH"]["paper_total_pnl"], 12.0)
        self.assertEqual(payload["summary_by_symbol"]["ETH"]["paper_win_rate"], 1.0)
        self.assertEqual(payload["summary_by_symbol"]["BTC"]["paper_total_pnl"], -2.0)
        self.assertEqual(payload["summary_by_symbol"]["all_symbols"]["paper_settled"], 3)


if __name__ == "__main__":
    unittest.main()
