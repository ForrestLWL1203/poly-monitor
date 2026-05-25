from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from poly_monitor.storage import JsonlEventWriter, ObserverStore


WALLET = "0xabc0000000000000000000000000000000000000"


def _trade(tx: str, *, ts: int, market: str, outcome: str = "Up", side: str = "BUY", price: float = 0.6, size: float = 10.0) -> dict:
    return {
        "tx_hash": tx,
        "fill_id": "",
        "wallet": WALLET,
        "market_slug": market,
        "condition_id": f"0x{market[-4:]}",
        "symbol": "BTC",
        "exchange_ts": ts,
        "outcome": outcome,
        "side": side,
        "price": price,
        "size": size,
        "usdc": round(price * size, 6),
        "name": "sample-wallet",
    }


class WalletResearchTests(unittest.TestCase):
    def test_report_uses_local_trades_and_context_snapshots(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0x1", ts=now_ts - 60, market="btc-updown-5m-1770000000", outcome="Up", price=0.62, size=20))
            store.insert_trade(_trade("0x2", ts=now_ts - 180, market="btc-updown-5m-1770000300", outcome="Down", price=0.42, size=10))
            store.insert_wallet_activity_events(
                [
                    {
                        "tx_hash": "0x1",
                        "wallet": WALLET,
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "0x0000",
                        "symbol": "BTC",
                        "exchange_ts": now_ts - 60,
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "outcome_index": 0,
                        "price": 0.62,
                        "size": 20,
                        "usdc": 12.4,
                        "observed_at": now.isoformat(),
                    }
                ]
            )
            store.insert_wallet_trade_contexts(
                [
                    {
                        "wallet": WALLET,
                        "tx_hash": "0x1",
                        "fill_id": "",
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "0x0000",
                        "symbol": "BTC",
                        "exchange_ts": now_ts - 60,
                        "observed_at": now.isoformat(),
                        "context_json": {"event": "context_snapshot"},
                        "book_stale": False,
                    }
                ]
            )
            store.insert_market_state_samples(
                [
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "0x0000",
                        "symbol": "BTC",
                        "sampled_ts": now_ts - 60,
                        "observed_at": now.isoformat(),
                        "window_remaining_sec": 45,
                        "reference_price": 100000,
                        "reference_price_age_sec": 0.25,
                        "up_json": {"bid": 0.61},
                        "down_json": {"ask": 0.39},
                        "book_stale": False,
                        "sample_reason": "initial",
                    }
                ]
            )
            store.close()

            writer = JsonlEventWriter(data_dir)
            writer.write(
                {
                    "event": "context_snapshot",
                    "observed_at": now.isoformat(),
                    "wallet": WALLET,
                    "symbol": "BTC",
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "0x0000",
                    "trade_tx_hash": "0x1",
                    "trade_outcome": "Up",
                    "trade_price": 0.62,
                    "trade_usdc": 12.4,
                    "window_remaining_sec": 45,
                    "reference_return_1s_bps": 1.1,
                    "reference_return_3s_bps": 2.2,
                    "reference_return_5s_bps": 3.3,
                    "reference_return_10s_bps": 4.4,
                    "up": {
                        "spread": 0.03,
                        "book_age_ms": 100,
                        "ask_targets": {"5": {"ok": True, "avg": 0.63}, "25": {"ok": True, "avg": 0.64}, "100": {"ok": False}},
                    },
                    "down": {
                        "spread": 0.04,
                        "book_age_ms": 100,
                        "ask_targets": {"5": {"ok": True, "avg": 0.39}, "25": {"ok": False}, "100": {"ok": False}},
                    },
                },
                now=now,
            )
            writer.close()

            report = build_wallet_research_report(
                WALLET,
                data_dir=data_dir,
                days=30,
                now=now,
                api_backfill="never",
                min_local_trades=100,
                min_local_markets=20,
            )

        self.assertEqual(report["wallet"], WALLET)
        self.assertEqual(report["data_coverage"]["local_trades"], 2)
        self.assertEqual(report["data_coverage"]["watchlist_activity_events"], 1)
        self.assertEqual(report["data_coverage"]["wallet_trade_context_rows"], 1)
        self.assertEqual(report["data_coverage"]["market_state_samples"], 1)
        self.assertEqual(report["data_coverage"]["context_snapshots"], 1)
        self.assertEqual(report["data_coverage"]["context_coverage_pct"], 50.0)
        self.assertEqual(report["frequency_profile"]["trades_24h"], 2)
        self.assertEqual(report["frequency_profile"]["distinct_markets_30d"], 2)
        self.assertEqual(report["price_behavior"]["buckets"]["0.55-0.75"]["trades"], 1)
        self.assertEqual(report["timing_behavior"]["buckets"]["30-60s"]["trades"], 1)
        self.assertEqual(report["book_copyability"]["targets"]["25"]["ok_rate"], 100.0)
        self.assertIn(report["recommendation"]["action"], {"monitor_more", "insufficient_local_data"})

    def test_api_backfill_auto_marks_api_rows_separately_when_local_sample_is_small(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        api_rows = [
            {
                "type": "TRADE",
                "timestamp": int(now.timestamp()) - idx,
                "slug": f"btc-updown-5m-{idx}",
                "eventSlug": f"btc-updown-5m-{idx}",
                "conditionId": f"0x{idx}",
                "outcome": "Up",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0xapi{idx}",
                "proxyWallet": WALLET,
            }
            for idx in range(3)
        ]

        with tempfile.TemporaryDirectory() as tmp, patch("poly_monitor.wallet_research.fetch_user_activity", side_effect=[api_rows, []]) as activity:
            report = build_wallet_research_report(
                WALLET,
                data_dir=Path(tmp),
                days=30,
                now=now,
                api_backfill="auto",
                min_local_trades=100,
                min_local_markets=20,
            )

        self.assertEqual(activity.call_count, 1)
        self.assertEqual(report["data_coverage"]["local_trades"], 0)
        self.assertEqual(report["data_coverage"]["api_backfill_trades"], 3)
        self.assertTrue(report["data_coverage"]["api_backfill_used"])
        self.assertEqual(report["frequency_profile"]["trades_30d"], 3)
        self.assertEqual(report["data_coverage"]["context_coverage_pct"], 0.0)
        self.assertFalse(report["data_coverage"]["api_backfill_truncated"])

    def test_api_backfill_marks_truncation_when_page_cap_is_hit(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        full_page = [
            {
                "type": "TRADE",
                "timestamp": int(now.timestamp()) - idx,
                "slug": f"btc-updown-5m-{idx}",
                "eventSlug": f"btc-updown-5m-{idx}",
                "conditionId": f"0x{idx}",
                "outcome": "Up",
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0xapi{idx}",
                "proxyWallet": WALLET,
            }
            for idx in range(500)
        ]

        with tempfile.TemporaryDirectory() as tmp, patch("poly_monitor.wallet_research.fetch_user_activity", return_value=full_page):
            report = build_wallet_research_report(
                WALLET,
                data_dir=Path(tmp),
                days=30,
                now=now,
                api_backfill="always",
            )

        self.assertEqual(report["data_coverage"]["api_backfill_trades"], 500)
        self.assertTrue(report["data_coverage"]["api_backfill_truncated"])

    def test_high_frequency_wallet_gets_too_high_frequency_recommendation(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            for idx in range(300):
                store.insert_trade(
                    _trade(
                        f"0x{idx}",
                        ts=now_ts - idx,
                        market=f"btc-updown-5m-{idx}",
                        outcome="Up" if idx % 2 else "Down",
                        price=0.5,
                    )
                )
            store.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, days=30, now=now, api_backfill="never")

        self.assertTrue(report["frequency_profile"]["markets_24h_saturated"])
        self.assertEqual(report["recommendation"]["action"], "too_high_frequency")
        self.assertGreater(report["distillation"]["overtrading_penalty"], 0)

    def test_sell_side_uses_bid_targets_and_inverse_success_direction(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0xsell", ts=now_ts - 60, market="btc-updown-5m-1770000000", outcome="Up", side="SELL", price=0.62, size=20))
            store.close()
            writer = JsonlEventWriter(data_dir)
            writer.write(
                {
                    "event": "context_snapshot",
                    "observed_at": now.isoformat(),
                    "wallet": WALLET,
                    "market_slug": "btc-updown-5m-1770000000",
                    "trade_tx_hash": "0xsell",
                    "trade_outcome": "Up",
                    "trade_price": 0.62,
                    "window_open_reference_price": 100.0,
                    "window_close_reference_price": 101.0,
                    "window_remaining_sec": 45,
                    "up": {
                        "spread": 0.03,
                        "book_age_ms": 100,
                        "ask_targets": {"25": {"ok": False, "avg": 0.70}},
                        "bid_targets": {"25": {"ok": True, "avg": 0.61}},
                    },
                    "down": {"ask_targets": {}, "bid_targets": {}},
                },
                now=now,
            )
            writer.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, now=now, api_backfill="never")

        self.assertEqual(report["book_copyability"]["targets"]["25"]["ok_rate"], 100.0)
        self.assertEqual(report["book_copyability"]["targets"]["25"]["avg_slippage_cents"], 1.0)
        self.assertEqual(report["success_vs_failure"]["labeled_trades"], 1)
        self.assertEqual(report["success_vs_failure"]["failure_trades"], 1)

    def test_saturated_frequency_keeps_actual_distinct_market_count(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            for idx in range(1000):
                store.insert_trade(_trade(f"0xbulk{idx}", ts=now_ts - idx, market=f"btc-updown-5m-{idx % 200}", price=0.5))
            store.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, days=30, now=now, api_backfill="never")

        self.assertTrue(report["frequency_profile"]["markets_24h_saturated"])
        self.assertEqual(report["frequency_profile"]["distinct_markets_24h"], 200)

    def test_volume_quality_fields_do_not_pretend_to_be_pnl(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0xv1", ts=now_ts - 10, market="btc-updown-5m-1", price=0.5, size=100))
            store.insert_trade(_trade("0xv2", ts=now_ts - 20, market="btc-updown-5m-2", price=0.5, size=10))
            store.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, now=now, api_backfill="never")

        self.assertIn("volume_quality", report)
        self.assertNotIn("pnl_quality", report)
        self.assertEqual(report["volume_quality"]["top1_volume_concentration"], round(50 / 55, 6))
        self.assertEqual(report["volume_quality"]["longshot_volume_share"], 0.0)

    def test_fallback_context_matching_consumes_each_context_once(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0xmissing1", ts=now_ts - 10, market="btc-updown-5m-1", price=0.5))
            store.insert_trade(_trade("0xmissing2", ts=now_ts - 20, market="btc-updown-5m-1", price=0.5))
            store.close()
            writer = JsonlEventWriter(data_dir)
            writer.write(
                {
                    "event": "context_snapshot",
                    "observed_at": (now - dt.timedelta(seconds=10)).isoformat(),
                    "wallet": WALLET,
                    "market_slug": "btc-updown-5m-1",
                    "trade_tx_hash": "",
                    "trade_outcome": "Up",
                    "window_remaining_sec": 45,
                    "up": {"ask_targets": {"25": {"ok": True, "avg": 0.51}}},
                    "down": {"ask_targets": {}},
                },
                now=now,
            )
            writer.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, now=now, api_backfill="never")

        self.assertEqual(report["data_coverage"]["context_matched_trades"], 1)
        self.assertEqual(report["data_coverage"]["context_coverage_pct"], 50.0)
        self.assertEqual(report["book_copyability"]["targets"]["25"]["seen"], 1)

    def test_local_observed_range_ignores_zero_timestamps(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(1970, 1, 2, 12, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0xzero", ts=0, market="btc-updown-5m-0"))
            store.insert_trade(_trade("0xvalid", ts=60, market="btc-updown-5m-60"))
            store.close()

            report = build_wallet_research_report(WALLET, data_dir=data_dir, days=30, now=now, api_backfill="never")

        self.assertEqual(report["data_coverage"]["local_observed_start"], "1970-01-01T00:01:00+00:00")
        self.assertEqual(report["data_coverage"]["local_observed_end"], "1970-01-01T00:01:00+00:00")

    def test_unreadable_raw_event_file_is_skipped(self):
        from poly_monitor.wallet_research import build_wallet_research_report

        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            path = data_dir / "raw" / now.date().isoformat() / "events.jsonl"
            path.parent.mkdir(parents=True)
            path.write_text("{}\n", encoding="utf-8")
            with patch("pathlib.Path.open", side_effect=OSError("broken")):
                report = build_wallet_research_report(WALLET, data_dir=data_dir, days=30, now=now, api_backfill="never")

        self.assertEqual(report["data_coverage"]["context_snapshots"], 0)

    def test_cli_writes_json_report(self):
        now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
        now_ts = int(now.timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            out = Path(tmp) / "report.json"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(_trade("0xcli", ts=now_ts, market="btc-updown-5m-1770000000"))
            store.close()

            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/research_wallet.py",
                    "--wallet",
                    WALLET,
                    "--data-dir",
                    str(data_dir),
                    "--api-backfill",
                    "never",
                    "--out",
                    str(out),
                    "--markdown",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(payload["wallet"], WALLET)
            self.assertEqual(payload["data_coverage"]["local_trades"], 1)
            self.assertIn("Wallet Research", out.with_suffix(".md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
