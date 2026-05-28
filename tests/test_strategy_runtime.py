from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.market import MarketWindow
from poly_monitor.maker_paper import PendingMakerReplay, PendingMakerReplayConfig
from poly_monitor.strategy_backtest import DeepExportBacktestEnvironment, run_strategy_backtest, run_strategy_maker_replay_backtest
from poly_monitor.strategy_live import LivePaperEnvironment
from poly_monitor.strategy_runtime import (
    PaperExecutionAdapter,
    RejectingLiveExecutionAdapter,
    StrategyHistory,
    StrategySnapshot,
    TradeIntent,
    strategy_from_name,
)
from poly_monitor.clob_stream import ClobBookStream
from poly_monitor.strategy_runner import StrategyRunner, StrategyRunnerConfig
from poly_monitor.strategy_runner import LivePaperRunConfig, LivePaperStrategyRunner
from poly_monitor.strategies import X32PairCostInventoryStrategy
from scripts.archive_paper_live_run import archive_run
from scripts.run_strategy_paper import require_single_symbol


class FakeStream:
    def __init__(self) -> None:
        self.switches: list[list[str]] = []
        self.books = {
            "up-token": ([(0.49, 50.0)], [(0.51, 50.0)], 25),
            "down-token": ([(0.48, 50.0)], [(0.52, 50.0)], 25),
            "up-token-2": ([(0.45, 50.0)], [(0.55, 50.0)], 25),
            "down-token-2": ([(0.44, 50.0)], [(0.56, 50.0)], 25),
        }

    async def connect(self, tokens):
        self.switches.append(list(tokens))

    async def switch_tokens(self, tokens):
        self.switches.append(list(tokens))

    async def close(self):
        pass

    def get_book(self, token_id, *, max_age_sec=None):
        return self.books.get(token_id, ([], [], None))

    def pop_trade_events(self):
        return [
            {
                "event": "ws_trade_observed",
                "asset_id": "up-token",
                "exchange_ts": 1770000011,
                "observed_at": "2026-05-27T12:00:11+00:00",
                "price": 0.49,
                "size": 10,
                "usdc": 4.9,
                "side": "SELL",
                "tx_hash": "0xws",
                "fill_id": "0xws:up-token:1770000011:0.49:10",
            }
        ]


class FakeFeed:
    latest_price = 101.0

    def latest_age_sec(self):
        return 0.5


class FakeHub:
    async def start(self):
        pass

    async def stop(self):
        pass

    def feed(self, symbol):
        return FakeFeed()


class RuntimeStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_environment_builds_snapshot_and_switches_tokens_on_rollover(self):
        start = dt.datetime(2026, 5, 27, 12, 0, tzinfo=dt.timezone.utc)
        first = MarketWindow("BTC", "btc-updown-5m-1779883200", "cond-1", "q1", "up-token", "down-token", start, start + dt.timedelta(minutes=5))
        second = MarketWindow("BTC", "btc-updown-5m-1779883500", "cond-2", "q2", "up-token-2", "down-token-2", start + dt.timedelta(minutes=5), start + dt.timedelta(minutes=10))
        stream = FakeStream()
        env = LivePaperEnvironment(
            symbols=("BTC",),
            window_finder=lambda symbol, now=None: first,
            following_window_finder=lambda window: second,
            stream=stream,
            price_hub=FakeHub(),
        )

        await env.start()
        snapshot = env.snapshot(now=start + dt.timedelta(seconds=120))[0]
        rolled = await env.roll_window_if_needed(now=start + dt.timedelta(minutes=5, seconds=1))

        self.assertIsInstance(snapshot, StrategySnapshot)
        self.assertEqual(snapshot.market_slug, first.slug)
        self.assertEqual(snapshot.elapsed_sec, 120)
        self.assertFalse(snapshot.book_stale)
        self.assertEqual(snapshot.up.ask_targets["25"]["avg"], 0.51)
        self.assertTrue(rolled)
        self.assertEqual(stream.switches, [["up-token", "down-token"], ["up-token-2", "down-token-2"]])

    async def test_missing_live_book_marks_snapshot_stale(self):
        start = dt.datetime(2026, 5, 27, 12, 0, tzinfo=dt.timezone.utc)
        window = MarketWindow("BTC", "btc-updown-5m-1779883200", "cond-1", "q1", "missing-up", "missing-down", start, start + dt.timedelta(minutes=5))
        env = LivePaperEnvironment(
            symbols=("BTC",),
            window_finder=lambda symbol, now=None: window,
            stream=FakeStream(),
            price_hub=FakeHub(),
        )

        await env.start()
        snapshot = env.snapshot(now=start + dt.timedelta(seconds=10))[0]

        self.assertTrue(snapshot.book_stale)

    def test_clob_stream_buffers_last_trade_price_events(self):
        stream = ClobBookStream()
        stream._handle_event(
            {
                "event_type": "last_trade_price",
                "asset_id": "up-token",
                "timestamp": "1770000011234",
                "price": "0.49",
                "size": "10",
                "side": "SELL",
                "hash": "0xtrade",
            }
        )

        rows = stream.pop_trade_events()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event"], "ws_trade_observed")
        self.assertEqual(rows[0]["asset_id"], "up-token")
        self.assertEqual(rows[0]["exchange_ts"], 1770000011)
        self.assertEqual(rows[0]["price"], 0.49)
        self.assertEqual(rows[0]["usdc"], 4.9)
        self.assertEqual(stream.pop_trade_events(), [])

    async def test_live_environment_maps_ws_trade_events_to_current_window(self):
        start = dt.datetime(2026, 5, 27, 12, 0, tzinfo=dt.timezone.utc)
        window = MarketWindow("BTC", "btc-updown-5m-1770000000", "cond-1", "q1", "up-token", "down-token", start, start + dt.timedelta(minutes=5))
        env = LivePaperEnvironment(
            symbols=("BTC",),
            window_finder=lambda symbol, now=None: window,
            stream=FakeStream(),
            price_hub=FakeHub(),
        )

        await env.start()
        rows = env.pop_trade_events()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_slug"], window.slug)
        self.assertEqual(rows[0]["condition_id"], window.condition_id)
        self.assertEqual(rows[0]["symbol"], "BTC")
        self.assertEqual(rows[0]["outcome"], "Up")
        self.assertEqual(rows[0]["source"], "clob_ws")

    def test_deep_export_environment_loads_per_market_trade_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            trade = {
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "exchange_ts": 1770000121,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.49,
                "size": 10,
                "usdc": 4.9,
            }
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr("deep_collection/market_state_samples.jsonl", "")
                bundle.writestr("wallet_activity.jsonl", "")
                bundle.writestr("wallet_market_pnl.jsonl", "")
                bundle.writestr("markets/btc-updown-5m-1770000000/market_trades.jsonl", json.dumps(trade) + "\n")

            env = DeepExportBacktestEnvironment(zip_path)

        self.assertEqual(len(env.market_trade_rows), 1)
        self.assertEqual(env.market_trade_rows[0]["market_slug"], "btc-updown-5m-1770000000")

    def test_rejecting_live_adapter_does_not_execute(self):
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=1,
            intent="BUY",
            outcome="Up",
            notional_usdc=5.0,
            max_price=0.8,
            expected_price=0.5,
            reason="test",
        )

        result = RejectingLiveExecutionAdapter().submit(intent)

        self.assertEqual(result.status, "live_rejected")
        self.assertIn("not implemented", result.detail["error"])

    def test_paper_execution_rejects_non_buy_intents(self):
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=1,
            intent="SELL",
            outcome="Up",
            notional_usdc=5.0,
            max_price=0.8,
            expected_price=0.5,
            reason="test",
        )

        result = PaperExecutionAdapter({"btc-updown-5m-1": "Up"}).submit(intent)

        self.assertEqual(result.status, "paper_rejected_unsupported_intent")
        self.assertNotIn("realized_pnl", result.detail)

    def test_pending_maker_replay_expires_orders_and_settles_fills(self):
        replay = PendingMakerReplay(
            winning_sides={"btc-updown-5m-1": "Up"},
            config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=5),
        )
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=10,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test",
        )

        pending = replay.submit(intent)
        expired = replay.expire_before(16)

        self.assertEqual(pending.status, "maker_pending")
        self.assertEqual(expired[0].order_id, "maker-1")
        self.assertEqual(replay.expired, 1)

        replay = PendingMakerReplay(
            winning_sides={"btc-updown-5m-1": "Up"},
            config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=5),
        )
        replay.submit(intent)
        fills = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 11, "outcome": "Up", "price": 0.5, "usdc": 5})
        settlement = replay.settle(fills[0].intent)

        self.assertEqual(fills[0].intent.notional_usdc, 5)
        self.assertEqual(settlement.status, "paper_settled")
        self.assertEqual(settlement.detail["realized_pnl"], 5.0)

    def test_pending_maker_replay_ignores_trades_before_order_submission(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=30))
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=20,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test",
        )
        replay.submit(intent)

        stale_fills = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 19, "outcome": "Up", "price": 0.5, "usdc": 5})
        live_fills = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 21, "outcome": "Up", "price": 0.5, "usdc": 5})

        self.assertEqual(stale_fills, [])
        self.assertEqual(len(live_fills), 1)
        self.assertEqual(live_fills[0].intent.sampled_ts, 21)

    def test_pending_maker_replay_ignores_trades_after_order_expiry(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=5))
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=20,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test",
        )
        replay.submit(intent)

        fills = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 26, "outcome": "Up", "price": 0.5, "usdc": 5})

        self.assertEqual(fills, [])

    def test_pending_maker_replay_defaults_to_short_ttl(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig())
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=20,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test",
        )

        result = replay.submit(intent)

        self.assertEqual(result.detail["ttl_sec"], 5)

    def test_pending_maker_replay_uses_dynamic_ttl_by_elapsed_phase(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig(order_ttl_sec=5, early_ttl_sec=4, mid_ttl_sec=3, late_ttl_sec=2, final_ttl_sec=1))

        def intent_at(elapsed: int) -> TradeIntent:
            return TradeIntent(
                strategy_name="demo",
                market_slug=f"btc-updown-5m-{elapsed}",
                sampled_ts=20,
                intent="BUY",
                outcome="Up",
                notional_usdc=5,
                max_price=0.9,
                expected_price=0.5,
                reason="test",
                features={"elapsed_sec": elapsed},
            )

        self.assertEqual(replay.ttl_for_intent(intent_at(1)), 4)
        self.assertEqual(replay.ttl_for_intent(intent_at(60)), 3)
        self.assertEqual(replay.ttl_for_intent(intent_at(180)), 2)
        self.assertEqual(replay.ttl_for_intent(intent_at(240)), 1)

    def test_pending_maker_replay_consumes_queue_ahead_before_fill(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=30, queue_position_ratio=1.0))
        intent = TradeIntent(
            strategy_name="demo",
            market_slug="btc-updown-5m-1",
            sampled_ts=20,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test",
            features={"quote_level_size_shares": 10},
        )
        replay.submit(intent)

        first = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 21, "outcome": "Up", "price": 0.5, "size": 6, "usdc": 3})
        second = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 22, "outcome": "Up", "price": 0.5, "size": 6, "usdc": 3})

        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0].intent.notional_usdc, 1.0)

    def test_pending_maker_replay_does_not_reuse_one_trade_across_orders(self):
        replay = PendingMakerReplay(config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=30, queue_position_ratio=0.0))
        for idx in range(2):
            replay.submit(
                TradeIntent(
                    strategy_name="demo",
                    market_slug="btc-updown-5m-1",
                    sampled_ts=20 + idx,
                    intent="BUY",
                    outcome="Up",
                    notional_usdc=5,
                    max_price=0.9,
                    expected_price=0.5,
                    reason="test",
                    features={"quote_level_size_shares": 0},
                )
            )

        fills = replay.process_trade({"market_slug": "btc-updown-5m-1", "exchange_ts": 23, "outcome": "Up", "price": 0.5, "size": 6, "usdc": 3})

        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].intent.notional_usdc, 3.0)
        self.assertEqual(len(replay.filled_intents), 1)

    def test_x32_trace_explains_pair_cost_skip(self):
        strategy = strategy_from_name("x32_pair_cost_inventory_v0", wallet="0x32")
        snapshot = StrategySnapshot.from_market_state_sample(
            {
                "market_slug": "btc-updown-5m-1770000000",
                "symbol": "BTC",
                "sampled_ts": 1770000010,
                "book_stale": 0,
                "up_json": {"bid": 0.60, "ask": 0.61, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                "down_json": {"bid": 0.40, "ask": 0.41, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
            }
        )

        trace = strategy.evaluate_with_trace(snapshot, StrategyHistory())

        self.assertEqual(trace.decision, "skip")
        self.assertEqual(trace.skip_reason, "pair_cost_above_max")
        self.assertIsNone(trace.intent)
        self.assertEqual(trace.features["maker_pair_cost"], 1.0)

    def test_deep_export_backtest_replays_strategy_with_standard_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            first_sample = {
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "sampled_ts": 1770000120,
                "observed_at": "2026-05-26T00:00:01+00:00",
                "window_remaining_sec": 299,
                "reference_price": 100.0,
                "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.50}}},
                "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.50}}},
                "book_stale": 0,
            }
            sample = {
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "sampled_ts": 1770000120,
                "observed_at": "2026-05-26T00:02:00+00:00",
                "window_remaining_sec": 180,
                "reference_price": 101.2,
                "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.55}}},
                "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.45}}},
                "book_stale": 0,
            }
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr(
                    "deep_collection/market_state_samples.jsonl",
                    json.dumps(first_sample) + "\n" + json.dumps(sample) + "\n",
                )
                bundle.writestr("wallet_activity.jsonl", "")
                bundle.writestr("wallet_market_pnl.jsonl", json.dumps({
                    "market_slug": "btc-updown-5m-1770000000",
                    "winning_side": "Up",
                    "realized_pnl": 1.0,
                }) + "\n")

            env = DeepExportBacktestEnvironment(zip_path)
            strategy = strategy_from_name("d950_path_v0", wallet="strategy", checkpoints=(120,), notional_usdc=25, max_price=0.7)
            result = run_strategy_backtest(strategy, env, PaperExecutionAdapter(env.winning_sides))

        self.assertEqual(result.summary["intents"], 1)
        self.assertEqual(result.summary["paper_settled"], 1)
        self.assertEqual(result.summary["paper_wins"], 1)
        self.assertEqual(result.trades[0]["intent"]["outcome"], "Up")

    def test_maker_replay_fills_pending_order_from_later_market_trade(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            sample = {
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "sampled_ts": 1770000120,
                "observed_at": "2026-05-26T00:00:01+00:00",
                "window_remaining_sec": 299,
                "up_json": {"bid": 0.49, "ask": 0.50, "ask_targets": {"5": {"ok": True, "avg": 0.50}}},
                "down_json": {"bid": 0.49, "ask": 0.50, "ask_targets": {"5": {"ok": True, "avg": 0.50}}},
                "book_stale": 0,
            }
            trade = {
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "exchange_ts": 1770000121,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.49,
                "size": 10,
                "usdc": 4.9,
            }
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr("deep_collection/market_state_samples.jsonl", json.dumps(sample) + "\n")
                bundle.writestr("market_trades.jsonl", json.dumps(trade) + "\n")
                bundle.writestr("wallet_activity.jsonl", "")
                bundle.writestr("wallet_market_pnl.jsonl", json.dumps({"market_slug": "btc-updown-5m-1770000000", "winning_side": "Up"}) + "\n")

            env = DeepExportBacktestEnvironment(zip_path)
            strategy = strategy_from_name(
                "wallet_path_v0",
                wallet="strategy",
                target_pair_shares_per_side=10,
                notional_usdc=5,
                max_pair_cost=1.0,
                max_unpaired_price=0.6,
                min_order_usdc=1,
                execution_style="maker",
            )
            result = run_strategy_maker_replay_backtest(strategy, env, config=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=30))

        self.assertEqual(result.summary["maker_orders"], 1)
        self.assertEqual(result.summary["maker_fills"], 1)
        self.assertEqual(result.summary["paper_settled"], 1)
        self.assertEqual(result.trades[1]["record_type"], "maker_fill")
        self.assertEqual(result.trades[1]["execution"]["status"], "paper_settled")


class StrategyHistoryTests(unittest.TestCase):
    def test_history_exposes_market_samples_and_wallet_activity_without_sqlite(self):
        history = StrategyHistory(
            activity_rows=[{"wallet": "0xabc", "market_slug": "btc-updown-5m-1"}],
            snapshots_by_market={"btc-updown-5m-1": [StrategySnapshot(market_slug="btc-updown-5m-1", sampled_ts=1)]},
        )

        self.assertEqual(len(history.activity_for_market("btc-updown-5m-1")), 1)
        self.assertEqual(len(history.snapshots_for_market("btc-updown-5m-1")), 1)


class StrategyFactoryTests(unittest.TestCase):
    def test_live_paper_requires_one_symbol(self):
        self.assertEqual(require_single_symbol(("BTC",)), "BTC")
        with self.assertRaises(SystemExit):
            require_single_symbol(("BTC", "ETH"))

    def test_x32_pair_cost_strategy_uses_address_specific_defaults(self):
        strategy = strategy_from_name("x32_pair_cost_inventory_v0", wallet="0x32")

        self.assertIsInstance(strategy, X32PairCostInventoryStrategy)
        self.assertEqual(strategy.strategy_name, "x32_pair_cost_inventory_v0")
        self.assertEqual(strategy.config.checkpoints, (1,))
        self.assertIsNone(strategy.config.target_pair_shares_per_side)
        self.assertEqual(strategy.config.target_pair_notional_usdc, 55)
        self.assertEqual(strategy.config.notional_usdc, 5)
        self.assertEqual(strategy.config.max_pair_cost, 0.995)
        self.assertEqual(strategy.config.max_unpaired_price, 0.70)
        self.assertEqual(strategy.config.max_quote_spread, 0.02)
        self.assertEqual(strategy.config.max_quote_book_age_ms, 50.0)
        self.assertEqual(strategy.config.min_quote_bid_depth_usdc, 20.0)
        self.assertEqual(strategy.config.rebalance_start_sec, 240)
        self.assertEqual(strategy.config.early_inventory_imbalance_ratio, 0.30)
        self.assertEqual(strategy.config.mid_inventory_imbalance_ratio, 0.12)
        self.assertEqual(strategy.config.late_inventory_imbalance_ratio, 0.06)
        self.assertEqual(strategy.config.final_inventory_imbalance_ratio, 0.05)
        self.assertFalse(strategy.one_trade_per_market)

    def test_x32_pair_cost_strategy_accepts_quote_quality_overrides(self):
        strategy = strategy_from_name(
            "x32_pair_cost_inventory_v0",
            wallet="0x32",
            max_quote_spread=0.01,
            max_quote_book_age_ms=25,
            min_quote_bid_depth_usdc=50,
        )

        self.assertEqual(strategy.config.max_quote_spread, 0.01)
        self.assertEqual(strategy.config.max_quote_book_age_ms, 25.0)
        self.assertEqual(strategy.config.min_quote_bid_depth_usdc, 50.0)

    def test_d950_terminal_bias_alias_matches_legacy_name(self):
        legacy = strategy_from_name("d950_path_v0", wallet="strategy")
        renamed = strategy_from_name("d950_terminal_bias_v0", wallet="strategy")

        self.assertEqual(legacy.strategy_name, "d950_terminal_bias_v0")
        self.assertEqual(renamed.strategy_name, "d950_terminal_bias_v0")
        self.assertEqual(legacy.config.checkpoints, renamed.config.checkpoints)
        self.assertTrue(legacy.config.one_trade_per_market)

    def test_wallet_path_defaults_to_independent_multi_order_inventory(self):
        strategy = strategy_from_name("wallet_path_v0", wallet="0xabc")

        self.assertEqual(strategy.config.checkpoints, (1,))
        self.assertFalse(strategy.config.one_trade_per_market)
        self.assertFalse(strategy.one_trade_per_market)

    def test_wallet_path_factory_accepts_share_target_sizing(self):
        strategy = strategy_from_name("wallet_path_v0", wallet="0xabc", target_pair_shares_per_side=40)

        self.assertEqual(strategy.config.target_pair_shares_per_side, 40)


class LivePaperRunnerTests(unittest.TestCase):
    def test_ws_trades_drive_fills_and_data_api_trades_are_audit_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            strategy = strategy_from_name(
                "wallet_path_v0",
                wallet="strategy",
                target_pair_shares_per_side=10,
                notional_usdc=5,
                max_pair_cost=1.0,
                max_unpaired_price=0.6,
                min_order_usdc=1,
                execution_style="maker",
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(
                    run_dir=Path(tmp),
                    run_id="test-run",
                    maker=PendingMakerReplayConfig(fill_rate=1.0, order_ttl_sec=30),
                ),
                strategy=strategy,
            )
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000120,
                    "window_remaining_sec": 180,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "book_stale": 0,
                }
            )
            trade = {
                "event": "ws_trade_observed",
                "source": "clob_ws",
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "cond",
                "symbol": "BTC",
                "exchange_ts": 1770000121,
                "outcome": "Up",
                "side": "SELL",
                "price": 0.49,
                "size": 10,
                "usdc": 4.9,
                "tx_hash": "0xws",
                "fill_id": "fill-1",
            }

            runner.tick([snapshot])
            audit_result = runner.process_market_trades([{**trade, "event": "trade_observed", "source": "data_api", "tx_hash": "0xapi"}])
            fill_result = runner.process_ws_trades([trade])

            self.assertEqual(audit_result, {"market_trades": 1})
            self.assertEqual(fill_result["fills"], 1)
            execution_rows = [json.loads(line) for line in (Path(tmp) / "executions.jsonl").read_text().splitlines()]
            self.assertEqual(sum(1 for row in execution_rows if row["record_type"] == "maker_fill"), 1)
            self.assertTrue((Path(tmp) / "market_trades.jsonl").exists())
            self.assertTrue((Path(tmp) / "ws_trades.jsonl").exists())

    def test_d950_path_keeps_single_trade_checkpoint_defaults(self):
        strategy = strategy_from_name("d950_path_v0", wallet="strategy")

        self.assertEqual(strategy.config.checkpoints, (120, 180, 240))
        self.assertTrue(strategy.config.one_trade_per_market)

    def test_factory_preserves_explicit_zero_values(self):
        strategy = strategy_from_name(
            "wallet_path_v0",
            wallet="0xabc",
            notional_usdc=0,
            max_pair_cost=0,
            max_inventory_imbalance_ratio=0,
            maker_rebalance_ticks=0,
            min_order_usdc=0,
        )
        d950 = strategy_from_name("d950_path_v0", wallet="strategy", min_reference_delta=0)

        self.assertEqual(strategy.config.notional_usdc, 0)
        self.assertEqual(strategy.config.max_pair_cost, 0)
        self.assertEqual(strategy.config.max_inventory_imbalance_ratio, 0)
        self.assertEqual(strategy.config.maker_rebalance_ticks, 0)
        self.assertEqual(strategy.config.min_order_usdc, 0)
        self.assertEqual(d950.min_reference_delta, 0)


class StrategyRunnerTests(unittest.TestCase):
    def test_runner_writes_strategy_execution_once_per_market(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "paper.jsonl"
            snapshot = StrategySnapshot(
                market_slug="btc-updown-5m-1770000000",
                sampled_ts=1770000120,
                elapsed_sec=120,
                reference_price=101.0,
            )

            class AlwaysBuy:
                strategy_name = "always_buy"

                def evaluate(self, snapshot, history):
                    return TradeIntent(
                        strategy_name=self.strategy_name,
                        market_slug=snapshot.market_slug,
                        sampled_ts=snapshot.sampled_ts,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=5,
                        max_price=0.9,
                        expected_price=0.5,
                        reason="test",
                    )

            runner = StrategyRunner(StrategyRunnerConfig(output_path=output), strategy=AlwaysBuy(), execution_adapter=PaperExecutionAdapter({"btc-updown-5m-1770000000": "Up"}))

            first = runner.tick([snapshot])
            second = runner.tick([snapshot])

            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(first["intents"], 1)
            self.assertEqual(second["intents"], 0)
            self.assertEqual(rows[0]["strategy_name"], "always_buy")
            self.assertEqual(rows[0]["execution"]["status"], "paper_settled")
            self.assertEqual(rows[0]["snapshot"]["market_slug"], snapshot.market_slug)

    def test_runner_records_live_rejection_without_secret_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "live.jsonl"
            snapshot = StrategySnapshot(market_slug="btc-updown-5m-1770000000", sampled_ts=1770000120, elapsed_sec=120)

            class AlwaysBuy:
                strategy_name = "always_buy"

                def evaluate(self, snapshot, history):
                    return TradeIntent(
                        strategy_name=self.strategy_name,
                        market_slug=snapshot.market_slug,
                        sampled_ts=snapshot.sampled_ts,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=5,
                        max_price=0.9,
                        expected_price=0.5,
                        reason="test",
                    )

            runner = StrategyRunner(StrategyRunnerConfig(output_path=output, mode="live"), strategy=AlwaysBuy(), execution_adapter=RejectingLiveExecutionAdapter())

            result = runner.tick([snapshot])

            row = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["intents"], 1)
            self.assertEqual(row["execution"]["status"], "live_rejected")

    def test_runner_loads_existing_jsonl_keys_for_restart_dedupe(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "paper.jsonl"
            output.write_text(json.dumps({
                "intent": {
                    "market_slug": "btc-updown-5m-1770000000",
                    "intent": "BUY",
                }
            }) + "\n", encoding="utf-8")
            snapshot = StrategySnapshot(market_slug="btc-updown-5m-1770000000", sampled_ts=1770000120, elapsed_sec=120)

            class AlwaysBuy:
                strategy_name = "always_buy"

                def evaluate(self, snapshot, history):
                    return TradeIntent(
                        strategy_name=self.strategy_name,
                        market_slug=snapshot.market_slug,
                        sampled_ts=snapshot.sampled_ts,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=5,
                        max_price=0.9,
                        expected_price=0.5,
                        reason="test",
                    )

            runner = StrategyRunner(StrategyRunnerConfig(output_path=output), strategy=AlwaysBuy(), execution_adapter=PaperExecutionAdapter())

            result = runner.tick([snapshot])

            self.assertEqual(result["intents"], 0)

    def test_live_paper_runner_writes_decisions_executions_trades_state_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000010,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )
            touch_trade = {
                "market_slug": snapshot.market_slug,
                "condition_id": "cond",
                "symbol": "BTC",
                "exchange_ts": 1770000011,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.49,
                "size": 20,
                "usdc": 9.8,
                "tx_hash": "0xt",
                "fill_id": "1",
                "observed_at": "2026-05-26T00:00:11+00:00",
                "source": "clob_ws",
            }

            runner.tick([snapshot])
            runner.process_ws_trades([touch_trade, dict(touch_trade)])
            runner.process_market_trades([{**touch_trade, "source": "data_api", "tx_hash": "0xapi"}])
            runner.settle_market(snapshot.market_slug, "Up")
            runner.write_state(active_windows=[], stream_diagnostics={"subscribed_tokens": 0})

            decisions = [json.loads(line) for line in (run_dir / "decisions.jsonl").read_text().splitlines()]
            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]
            trades = [json.loads(line) for line in (run_dir / "market_trades.jsonl").read_text().splitlines()]
            ws_trades = [json.loads(line) for line in (run_dir / "ws_trades.jsonl").read_text().splitlines()]
            state = json.loads((run_dir / "state.json").read_text())
            summary = json.loads((run_dir / "summary.json").read_text())

        self.assertEqual(decisions[0]["record_type"], "decision")
        self.assertEqual(decisions[0]["decision"], "intent")
        self.assertEqual(decisions[0]["maker_pair_cost"], 0.99)
        self.assertIn("book_fill", decisions[0]["intent"]["features"])
        self.assertEqual(executions[0]["record_type"], "maker_order_submitted")
        self.assertEqual(executions[0]["market_slug"], snapshot.market_slug)
        self.assertNotIn("execution", executions[0])
        self.assertEqual(executions[1]["record_type"], "maker_fill")
        self.assertEqual(executions[1]["parent_sampled_ts"], snapshot.sampled_ts)
        self.assertNotIn("parent_intent", executions[1])
        self.assertEqual(executions[1]["configured_fill_rate"], 0.1)
        self.assertEqual(executions[1]["realized_touch_fill_rate"], 0.1)
        self.assertEqual(executions[2]["record_type"], "settled")
        self.assertEqual(executions[2]["winning_side"], "Up")
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["tx_hash"], "0xapi")
        self.assertEqual(len(ws_trades), 1)
        self.assertEqual(ws_trades[0]["tx_hash"], "0xt")
        self.assertEqual(state["run"]["run_id"], "test-run")
        self.assertEqual(summary["orders_submitted"], 1)
        self.assertEqual(summary["fills"], 1)
        self.assertEqual(summary["partial_fill_events"], 1)

    def test_live_paper_runner_logs_trade_driven_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(
                    run_dir=run_dir,
                    run_id="test-run",
                    mode="paper",
                    maker=PendingMakerReplayConfig(order_ttl_sec=1),
                ),
                strategy=strategy,
            )
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000010,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )
            stale_touch = {
                "market_slug": snapshot.market_slug,
                "condition_id": "cond",
                "symbol": "BTC",
                "exchange_ts": 1770000013,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.49,
                "size": 20,
                "usdc": 9.8,
                "tx_hash": "0xt",
                "fill_id": "late",
                "observed_at": "2026-05-26T00:00:13+00:00",
                "source": "clob_ws",
            }

            runner.tick([snapshot])
            runner.process_ws_trades([stale_touch])
            runner.write_state(active_windows=[])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]
            summary = json.loads((run_dir / "summary.json").read_text())

        self.assertEqual([row["record_type"] for row in executions], ["maker_order_submitted", "maker_expired"])
        self.assertIn("lifetime_sec", executions[1])
        self.assertIn("wallclock_at_log_sec", executions[1])
        self.assertNotIn("age_sec", executions[1])
        self.assertEqual(summary["expired"], 1)
        self.assertEqual(summary["fills"], 0)

    def test_live_paper_runner_replaces_quote_when_bid_improves(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            first = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000010,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )
            moved = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": first.market_slug,
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )

            runner.tick([first])
            runner.tick([moved])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]

        self.assertIn("maker_cancelled", [row["record_type"] for row in executions])
        cancel = next(row for row in executions if row["record_type"] == "maker_cancelled")
        self.assertEqual(cancel["cancel_reason"], "quote_improved_replace")

    def test_live_paper_runner_keeps_pending_quote_when_bid_worsens(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            first = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000010,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )
            moved = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": first.market_slug,
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": {"bid": 0.48, "ask": 0.49, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )

            runner.tick([first])
            runner.tick([moved])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]

        self.assertNotIn("maker_cancelled", [row["record_type"] for row in executions])
        submitted = [row for row in executions if row["record_type"] == "maker_order_submitted"]
        self.assertEqual(submitted[0]["outcome"], "Up")
        self.assertEqual(submitted[1]["outcome"], "Down")

    def test_live_paper_runner_cancels_side_when_balance_is_reconciled(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            runner.history.emitted_intents.extend(
                [
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug="btc-updown-5m-1770000000",
                        sampled_ts=1770000008,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=9.31,
                        max_price=0.95,
                        expected_price=0.49,
                        symbol="BTC",
                        reason="filled",
                    ),
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug="btc-updown-5m-1770000000",
                        sampled_ts=1770000009,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Down",
                        notional_usdc=9.5,
                        max_price=0.95,
                        expected_price=0.50,
                        symbol="BTC",
                        reason="filled",
                    ),
                ]
            )
            pending = TradeIntent(
                strategy_name="x32_pair_cost_inventory_v0",
                wallet="0x32",
                market_slug="btc-updown-5m-1770000000",
                sampled_ts=1770000010,
                checkpoint_sec=1,
                intent="BUY",
                outcome="Up",
                notional_usdc=4.9,
                max_price=0.95,
                expected_price=0.49,
                symbol="BTC",
                reason="pending",
            )
            runner.maker.submit(pending)
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": pending.market_slug,
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )

            runner.tick([snapshot])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]

        cancel = next(row for row in executions if row["record_type"] == "maker_cancelled")
        self.assertEqual(cancel["cancel_reason"], "balance_reconciled")

    def test_live_paper_runner_keeps_balanced_pending_while_below_pair_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            market_slug = "btc-updown-5m-1770000000"
            runner.history.emitted_intents.extend(
                [
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug=market_slug,
                        sampled_ts=1770000008,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=2.45,
                        max_price=0.95,
                        expected_price=0.49,
                        symbol="BTC",
                        reason="filled",
                    ),
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug=market_slug,
                        sampled_ts=1770000009,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Down",
                        notional_usdc=2.50,
                        max_price=0.95,
                        expected_price=0.50,
                        symbol="BTC",
                        reason="filled",
                    ),
                ]
            )
            for outcome, price, notional in (("Up", 0.49, 4.9), ("Down", 0.50, 5.0)):
                runner.maker.submit(
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug=market_slug,
                        sampled_ts=1770000010,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome=outcome,
                        notional_usdc=notional,
                        max_price=0.95,
                        expected_price=price,
                        symbol="BTC",
                        reason="pending",
                    )
                )
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": market_slug,
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )

            runner.tick([snapshot])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]

        self.assertNotIn("maker_cancelled", [row["record_type"] for row in executions])

    def test_live_paper_runner_cancels_surplus_side_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            market_slug = "btc-updown-5m-1770000000"
            runner.history.emitted_intents.extend(
                [
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug=market_slug,
                        sampled_ts=1770000248,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Up",
                        notional_usdc=9.8,
                        max_price=0.95,
                        expected_price=0.49,
                        symbol="BTC",
                        reason="filled",
                    ),
                    TradeIntent(
                        strategy_name="x32_pair_cost_inventory_v0",
                        wallet="0x32",
                        market_slug=market_slug,
                        sampled_ts=1770000248,
                        checkpoint_sec=1,
                        intent="BUY",
                        outcome="Down",
                        notional_usdc=5.0,
                        max_price=0.95,
                        expected_price=0.50,
                        symbol="BTC",
                        reason="filled",
                    ),
                ]
            )
            pending = TradeIntent(
                strategy_name="x32_pair_cost_inventory_v0",
                wallet="0x32",
                market_slug=market_slug,
                sampled_ts=1770000249,
                checkpoint_sec=1,
                intent="BUY",
                outcome="Up",
                notional_usdc=4.9,
                max_price=0.95,
                expected_price=0.49,
                symbol="BTC",
                reason="pending",
            )
            runner.maker.submit(pending)
            snapshot = StrategySnapshot(
                market_slug=market_slug,
                condition_id="cond",
                symbol="BTC",
                sampled_ts=1770000250,
                elapsed_sec=250,
                book_stale=False,
                up=StrategySnapshot.from_market_state_sample({"up_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100}}).up,
                down=StrategySnapshot.from_market_state_sample({"down_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100}}).down,
            )

            runner.tick([snapshot])

            executions = [json.loads(line) for line in (run_dir / "executions.jsonl").read_text().splitlines()]

        cancel = next(row for row in executions if row["record_type"] == "maker_cancelled")
        self.assertEqual(cancel["cancel_reason"], "side_no_longer_needed")

    def test_live_paper_runner_summary_counts_cancel_reasons(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "paper_live" / "x32"
            strategy = strategy_from_name(
                "x32_pair_cost_inventory_v0",
                wallet="0x32",
                target_pair_notional_usdc=20,
                max_pair_cost=0.995,
                max_quote_book_age_ms=100,
                min_quote_bid_depth_usdc=1,
            )
            runner = LivePaperStrategyRunner(
                LivePaperRunConfig(run_dir=run_dir, run_id="test-run", mode="paper"),
                strategy=strategy,
            )
            pending = TradeIntent(
                strategy_name="x32_pair_cost_inventory_v0",
                wallet="0x32",
                market_slug="btc-updown-5m-1770000000",
                sampled_ts=1770000010,
                checkpoint_sec=1,
                intent="BUY",
                outcome="Up",
                notional_usdc=4.9,
                max_price=0.95,
                expected_price=0.49,
                symbol="BTC",
                reason="pending",
            )
            runner.maker.submit(pending)
            snapshot = StrategySnapshot.from_market_state_sample(
                {
                    "market_slug": pending.market_slug,
                    "condition_id": "cond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000011,
                    "book_stale": 0,
                    "up_json": {"bid": 0.50, "ask": 0.51, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                    "down_json": {"bid": 0.49, "ask": 0.50, "spread": 0.01, "book_age_ms": 5, "bid_depth_usdc": 100},
                }
            )

            runner.tick([snapshot])

            summary = json.loads((run_dir / "summary.json").read_text())

        self.assertEqual(summary["cancel_counts_by_reason"], {"quote_improved_replace": 1})

    def test_archive_paper_live_run_compresses_core_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            for name in ("decisions.jsonl", "executions.jsonl", "market_trades.jsonl", "ws_trades.jsonl"):
                (run_dir / name).write_text('{"ok":true}\n', encoding="utf-8")
            (run_dir / "summary.json").write_text("{}\n", encoding="utf-8")

            manifest = archive_run(run_dir)

            self.assertEqual(len(manifest["core_logs"]), 4)
            self.assertTrue((run_dir / "decisions.jsonl.gz").exists())
            self.assertTrue((run_dir / "archive_manifest.json").exists())
            self.assertTrue((run_dir / "decisions.jsonl").exists())
            self.assertIn("gzip_sha256", manifest["core_logs"][0])


if __name__ == "__main__":
    unittest.main()
