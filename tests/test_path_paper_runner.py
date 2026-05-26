from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from poly_monitor.path_paper_runner import PathPaperRunner, PathPaperRunnerConfig
from poly_monitor.path_strategy import ExecutionResult
from poly_monitor.path_strategy import TradeIntent
from poly_monitor.storage import ObserverStore


class MemoryDataSource:
    def __init__(self, activity_rows, market_state_samples, settlements=None):
        self.activity_rows = activity_rows
        self.market_state_samples = market_state_samples
        self.settlements = settlements or {}

    def load_strategy_rows(self, wallet: str):
        return {
            "activity_rows": self.activity_rows,
            "market_state_samples": self.market_state_samples,
            "settlements": self.settlements,
        }

    def close(self):
        pass


class CountingAdapter:
    def __init__(self):
        self.calls = 0

    def submit(self, intent):
        self.calls += 1
        return ExecutionResult(status="counted", intent=intent)


class AlwaysBuyUpStrategy:
    def evaluate_snapshot(self, sample, activity_rows):
        return TradeIntent(
            wallet="0xabc",
            market_slug=sample["market_slug"],
            sampled_ts=sample["sampled_ts"],
            checkpoint_sec=1,
            intent="BUY",
            outcome="Up",
            notional_usdc=5,
            max_price=0.9,
            expected_price=0.5,
            reason="test_strategy",
            features={},
        )


class PathPaperRunnerTests(unittest.TestCase):
    def test_tick_reads_live_db_samples_and_appends_paper_execution_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "observer.db")
            wallet = "0xabc"
            store.insert_wallet_activity_events(
                [
                    {
                        "tx_hash": "0x1",
                        "fill_id": "1",
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "cond",
                        "symbol": "BTC",
                        "exchange_ts": 1770000040,
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 84,
                        "usdc": 42.0,
                        "observed_at": "2026-05-26T00:00:00+00:00",
                    }
                ]
            )
            store.insert_market_state_samples(
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "cond",
                        "symbol": "BTC",
                        "sampled_ts": 1770000120,
                        "observed_at": "2026-05-26T00:02:00+00:00",
                        "window_remaining_sec": 180,
                        "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                        "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                        "book_stale": 0,
                        "sample_reason": "deep_collection",
                    }
                ]
            )
            store.close()
            runner = PathPaperRunner(
                PathPaperRunnerConfig(
                    wallet=wallet,
                    data_dir=data_dir,
                    poll_sec=0.01,
                    checkpoints=(120,),
                    notional_usdc=25,
                    winning_sides={"btc-updown-5m-1770000000": "Up"},
                )
            )

            first = runner.tick()
            second = runner.tick()

            self.assertEqual(first["intents"], 1)
            self.assertEqual(second["intents"], 0)
            out_path = data_dir / "paper" / "path_strategy" / wallet / "executions.jsonl"
            rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["execution"]["status"], "paper_settled")
            self.assertEqual(rows[0]["execution"]["detail"]["realized_pnl"], 25.0)

    def test_runner_uses_pluggable_data_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [
                    {
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Down",
                        "exchange_ts": 1770000040,
                        "usdc": 42.0,
                    }
                ],
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "sampled_ts": 1770000120,
                        "book_stale": 0,
                        "down_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                    }
                ],
            )
            runner = PathPaperRunner(
                PathPaperRunnerConfig(
                    wallet=wallet,
                    data_dir=Path(tmp),
                    checkpoints=(120,),
                    winning_sides={"btc-updown-5m-1770000000": "Down"},
                ),
                data_source=data_source,
                execution_adapter=CountingAdapter(),
            )

            result = runner.tick()

            self.assertEqual(result["intents"], 1)
            out_path = Path(result["output_path"])
            row = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(row["intent"]["outcome"], "Down")

    def test_duplicate_ticks_do_not_resubmit_to_execution_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [
                    {
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "exchange_ts": 1770000040,
                        "usdc": 42.0,
                    }
                ],
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "sampled_ts": 1770000120,
                        "book_stale": 0,
                        "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                    }
                ],
            )
            adapter = CountingAdapter()
            runner = PathPaperRunner(
                PathPaperRunnerConfig(wallet=wallet, data_dir=Path(tmp), checkpoints=(120,)),
                data_source=data_source,
                execution_adapter=adapter,
            )

            runner.tick()
            runner.tick()

            self.assertEqual(adapter.calls, 1)

    def test_runner_accepts_any_strategy_with_evaluate_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [],
                [{"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000001}],
            )
            runner = PathPaperRunner(
                PathPaperRunnerConfig(wallet=wallet, data_dir=Path(tmp), strategy_name="strategy_b"),
                data_source=data_source,
                execution_adapter=CountingAdapter(),
                strategy=AlwaysBuyUpStrategy(),
            )

            result = runner.tick()

            self.assertEqual(result["intents"], 1)
            self.assertIn("strategy_b", result["output_path"])

    def test_open_paper_execution_is_settled_when_data_source_later_has_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [
                    {
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "exchange_ts": 1770000040,
                        "usdc": 42.0,
                    }
                ],
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "sampled_ts": 1770000120,
                        "book_stale": 0,
                        "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                    }
                ],
            )
            runner = PathPaperRunner(PathPaperRunnerConfig(wallet=wallet, data_dir=Path(tmp), checkpoints=(120,)), data_source=data_source)

            first = runner.tick()
            data_source.settlements = {"btc-updown-5m-1770000000": "Up"}
            second = runner.tick()

            self.assertEqual(first["intents"], 1)
            self.assertEqual(second["settlements"], 1)
            rows = [json.loads(line) for line in Path(second["output_path"]).read_text(encoding="utf-8").splitlines()]
            self.assertEqual([row["record_type"] for row in rows], ["execution", "settlement"])
            self.assertEqual(rows[1]["execution"]["status"], "paper_settled")
            self.assertEqual(rows[1]["execution"]["detail"]["realized_pnl"], 25.0)

    def test_start_sampled_ts_skips_existing_historical_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [
                    {
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "exchange_ts": 1770000040,
                        "usdc": 42.0,
                    }
                ],
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "sampled_ts": 1770000120,
                        "book_stale": 0,
                        "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                    }
                ],
            )
            runner = PathPaperRunner(
                PathPaperRunnerConfig(wallet=wallet, data_dir=Path(tmp), checkpoints=(120,), start_sampled_ts=1770000121),
                data_source=data_source,
            )

            result = runner.tick()

            self.assertEqual(result["intents"], 0)

    def test_execution_record_includes_target_wallet_comparison_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            wallet = "0xabc"
            data_source = MemoryDataSource(
                [
                    {
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1770000000",
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "exchange_ts": 1770000100,
                        "usdc": 42.0,
                    }
                ],
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "sampled_ts": 1770000120,
                        "book_stale": 0,
                        "up_json": {"ask_targets": {"25": {"ok": True, "avg": 0.5}}},
                    }
                ],
            )
            runner = PathPaperRunner(PathPaperRunnerConfig(wallet=wallet, data_dir=Path(tmp), checkpoints=(120,)), data_source=data_source)

            result = runner.tick()

            row = json.loads(Path(result["output_path"]).read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(row["target_wallet_context"]["wallet"], wallet)
            self.assertEqual(row["target_wallet_context"]["trade_rows_seen"], 1)
            self.assertEqual(row["target_wallet_context"]["net_side_seen"], "Up")
            self.assertEqual(row["target_wallet_context"]["net_up_down_usdc_seen"], 42.0)


if __name__ == "__main__":
    unittest.main()
