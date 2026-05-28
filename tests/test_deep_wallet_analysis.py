from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from poly_monitor.deep_wallet_analysis import analyze_deep_wallet_export, render_markdown_report


def _write_jsonl(zipf: zipfile.ZipFile, name: str, rows: list[dict]) -> None:
    zipf.writestr(name, "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))


class DeepWalletAnalysisTests(unittest.TestCase):
    def test_analyze_deep_wallet_export_summarizes_reusable_zip_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            manifest = {
                "wallet": wallet,
                "policy": "complete_deep_windows_only",
                "window_count": 2,
                "windows": [
                    {"market_slug": "btc-updown-5m-1770000000", "coverage": 1.0, "sample_rows": 3},
                    {"market_slug": "eth-updown-5m-1770000000", "coverage": 1.0, "sample_rows": 2},
                ],
            }
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr("manifest.json", json.dumps(manifest))
                _write_jsonl(
                    bundle,
                    "coverage_windows_complete.jsonl",
                    [
                        {"market_slug": "btc-updown-5m-1770000000", "coverage": 1.0, "sample_rows": 3},
                        {"market_slug": "eth-updown-5m-1770000000", "coverage": 1.0, "sample_rows": 2},
                    ],
                )
                _write_jsonl(
                    bundle,
                    "wallet_activity.jsonl",
                    [
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "price": 0.42,
                            "size": 100,
                            "usdc": 42.0,
                            "exchange_ts": 1770000010,
                        },
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "price": 0.48,
                            "size": 12,
                            "usdc": 5.76,
                            "exchange_ts": 1770000060,
                        },
                        {
                            "wallet": wallet,
                            "market_slug": "eth-updown-5m-1770000000",
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "price": 0.88,
                            "size": 5,
                            "usdc": 4.4,
                            "exchange_ts": 1770000280,
                        },
                    ],
                )
                _write_jsonl(
                    bundle,
                    "deep_collection/wallet_trade_contexts.jsonl",
                    [
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "tx_hash": "0x1",
                            "exchange_ts": 1770000010,
                            "context_json": {
                                "window_remaining_sec": 290,
                                "up": {"spread": 0.02, "book_age_ms": 100, "ask_targets": {"25": {"ok": True, "avg": 0.43}}},
                            },
                        },
                        {
                            "wallet": wallet,
                            "market_slug": "eth-updown-5m-1770000000",
                            "tx_hash": "0x3",
                            "exchange_ts": 1770000280,
                            "context_json": json.dumps(
                                {
                                    "window_remaining_sec": 20,
                                    "up": {"spread": 0.01, "book_age_ms": 200, "ask_targets": {"25": {"ok": False}}},
                                }
                            ),
                        },
                    ],
                )
                _write_jsonl(
                    bundle,
                    "wallet_market_pnl.jsonl",
                    [
                        {
                            "market_slug": "btc-updown-5m-1770000000",
                            "symbol": "BTC",
                            "winning_side": "Up",
                            "realized_pnl": 120.5,
                            "incomplete": 0,
                        },
                        {
                            "market_slug": "eth-updown-5m-1770000000",
                            "symbol": "ETH",
                            "winning_side": "Down",
                            "realized_pnl": -150.0,
                            "incomplete": 0,
                        },
                    ],
                )
                _write_jsonl(
                    bundle,
                    "deep_collection/market_state_samples.jsonl",
                    [
                        {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000000},
                        {"market_slug": "btc-updown-5m-1770000000", "sampled_ts": 1770000001},
                        {"market_slug": "eth-updown-5m-1770000000", "sampled_ts": 1770000000},
                    ],
                )

            report = analyze_deep_wallet_export(zip_path)
            markdown = render_markdown_report(report)

        self.assertEqual(report["wallet"], wallet)
        self.assertEqual(report["coverage"]["complete_windows"], 2)
        self.assertEqual(report["activity"]["trade_rows"], 3)
        self.assertEqual(report["pnl"]["total_realized_pnl"], -29.5)
        self.assertEqual(report["pnl"]["by_symbol"]["BTC"]["realized_pnl"], 120.5)
        self.assertEqual(report["market_behavior"]["dual_side_markets"], 1)
        self.assertEqual(report["timing"]["buckets"]["240s+"]["trades"], 1)
        self.assertEqual(report["timing"]["buckets"]["0-30s"]["trades"], 1)
        self.assertEqual(report["copyability"]["matched_contexts"], 2)
        self.assertEqual(report["copyability"]["targets"]["25"]["ok_rate"], 50.0)
        self.assertEqual(report["path_analysis"]["summary"]["windows"], 2)
        self.assertEqual(report["path_analysis"]["summary"]["final_bias_correct"], 1)
        self.assertEqual(report["path_analysis"]["summary"]["large_win_count"], 1)
        self.assertEqual(report["path_analysis"]["summary"]["large_loss_count"], 1)
        btc_path = next(row for row in report["path_analysis"]["windows"] if row["market_slug"].startswith("btc-"))
        self.assertEqual(btc_path["final_net_side"], "Up")
        self.assertEqual(btc_path["final_pair_cost"], 0.9)
        self.assertEqual(btc_path["paired_shares"], 12.0)
        self.assertEqual(btc_path["first_bias_bucket"], "0-30s")
        self.assertTrue(btc_path["final_bias_correct"])
        self.assertEqual(btc_path["bucket_flow"]["0-30s"]["up_usdc"], 42.0)
        self.assertIn("Window Path Analysis", markdown)
        self.assertIn("possible_strategy_hypotheses", report)
        self.assertIn("Wallet Deep Analysis", markdown)

    def test_execution_analysis_compares_trade_price_to_context_and_sampled_books(self):
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "bundle.zip"
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            with zipfile.ZipFile(zip_path, "w") as bundle:
                bundle.writestr("manifest.json", json.dumps({"wallet": wallet, "policy": "complete_deep_windows_only"}))
                _write_jsonl(bundle, "coverage_windows_complete.jsonl", [])
                _write_jsonl(
                    bundle,
                    "wallet_activity.jsonl",
                    [
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "price": 0.42,
                            "size": 10,
                            "usdc": 4.2,
                            "exchange_ts": 1770000010,
                            "tx_hash": "0x1",
                        },
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "price": 0.57,
                            "size": 10,
                            "usdc": 5.7,
                            "exchange_ts": 1770000012,
                            "tx_hash": "0x2",
                        },
                    ],
                )
                _write_jsonl(bundle, "wallet_market_pnl.jsonl", [])
                _write_jsonl(
                    bundle,
                    "deep_collection/wallet_trade_contexts.jsonl",
                    [
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "tx_hash": "0x1",
                            "exchange_ts": 1770000010,
                            "context_json": {
                                "trade_exchange_ts": 1770000010,
                                "trade_outcome": "Up",
                                "trade_price": 0.42,
                                "up": {"bid": 0.42, "ask": 0.43, "book_age_ms": 11, "spread": 0.01},
                                "down": {"bid": 0.56, "ask": 0.58, "book_age_ms": 11, "spread": 0.02},
                            },
                        },
                        {
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1770000000",
                            "tx_hash": "0x2",
                            "exchange_ts": 1770000012,
                            "context_json": {
                                "trade_exchange_ts": 1770000012,
                                "trade_outcome": "Down",
                                "trade_price": 0.57,
                                "up": {"bid": 0.41, "ask": 0.42, "book_age_ms": 12, "spread": 0.01},
                                "down": {"bid": 0.56, "ask": 0.58, "book_age_ms": 12, "spread": 0.02},
                            },
                        },
                    ],
                )
                _write_jsonl(
                    bundle,
                    "deep_collection/market_state_samples.jsonl",
                    [
                        {
                            "market_slug": "btc-updown-5m-1770000000",
                            "sampled_ts": 1770000009,
                            "up_json": json.dumps({"bid": 0.41, "ask": 0.42, "book_age_ms": 5, "spread": 0.01}),
                            "down_json": json.dumps({"bid": 0.57, "ask": 0.58, "book_age_ms": 5, "spread": 0.01}),
                        },
                        {
                            "market_slug": "btc-updown-5m-1770000000",
                            "sampled_ts": 1770000010,
                            "up_json": json.dumps({"bid": 0.42, "ask": 0.43, "book_age_ms": 6, "spread": 0.01}),
                            "down_json": json.dumps({"bid": 0.56, "ask": 0.58, "book_age_ms": 6, "spread": 0.02}),
                        },
                        {
                            "market_slug": "btc-updown-5m-1770000000",
                            "sampled_ts": 1770000013,
                            "up_json": json.dumps({"bid": 0.40, "ask": 0.41, "book_age_ms": 6, "spread": 0.01}),
                            "down_json": json.dumps({"bid": 0.56, "ask": 0.57, "book_age_ms": 6, "spread": 0.01}),
                        },
                    ],
                )

            report = analyze_deep_wallet_export(zip_path)

        execution = report["execution_analysis"]
        self.assertEqual(execution["context_snapshot"]["total"], 2)
        self.assertEqual(execution["context_snapshot"]["classification_counts"]["bid_match"], 1)
        self.assertEqual(execution["context_snapshot"]["classification_counts"]["inside_spread"], 1)
        self.assertEqual(execution["sample_exact"]["classification_counts"]["bid_match"], 1)
        self.assertEqual(execution["sample_exact"]["classification_counts"]["no_sample"], 1)
        self.assertEqual(execution["sample_nearest_2s"]["classification_counts"]["ask_match"], 1)
        self.assertEqual(execution["sample_before_2s"]["classification_counts"]["ask_match"], 1)
        self.assertEqual(execution["sample_after_2s"]["classification_counts"]["ask_match"], 1)
        self.assertEqual(execution["summary"]["simple_taker_ask_match_rate_pct"], 50.0)
        self.assertIn("Execution Analysis", render_markdown_report(report))


if __name__ == "__main__":
    unittest.main()
