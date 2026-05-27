from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.market import MarketWindow
from poly_monitor.strategy_backtest import DeepExportBacktestEnvironment, PendingMakerReplayConfig, run_strategy_backtest, run_strategy_maker_replay_backtest
from poly_monitor.strategy_live import LivePaperEnvironment
from poly_monitor.strategy_runtime import (
    PaperExecutionAdapter,
    RejectingLiveExecutionAdapter,
    StrategyHistory,
    StrategySnapshot,
    TradeIntent,
    strategy_from_name,
)
from poly_monitor.strategy_runner import StrategyRunner, StrategyRunnerConfig
from poly_monitor.strategies import X32PairCostInventoryStrategy


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
    def test_x32_pair_cost_strategy_uses_address_specific_defaults(self):
        strategy = strategy_from_name("x32_pair_cost_inventory_v0", wallet="0x32")

        self.assertIsInstance(strategy, X32PairCostInventoryStrategy)
        self.assertEqual(strategy.strategy_name, "x32_pair_cost_inventory_v0")
        self.assertEqual(strategy.config.checkpoints, (1,))
        self.assertEqual(strategy.config.target_pair_shares_per_side, 100)
        self.assertEqual(strategy.config.notional_usdc, 5)
        self.assertEqual(strategy.config.max_pair_cost, 0.995)
        self.assertEqual(strategy.config.rebalance_start_sec, 180)
        self.assertEqual(strategy.config.mid_inventory_imbalance_ratio, 0.12)
        self.assertFalse(strategy.one_trade_per_market)

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


if __name__ == "__main__":
    unittest.main()
