from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from poly_monitor.scoring import CandidateScore
from poly_monitor.storage import JsonlEventWriter, ObserverStore, write_latest_candidates


class DashboardStatusTests(unittest.TestCase):
    def test_empty_data_dir_returns_healthy_empty_status(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            status = build_dashboard_status(Path(tmp), now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertTrue(status["health"]["ok"])
        self.assertEqual(status["sqlite"]["trade_count"], 0)
        self.assertEqual(status["events"]["counts"], {})
        self.assertEqual(status["candidates"]["seed_watchlist"], [])
        self.assertEqual(status["candidates"]["active_candidate"], [])
        self.assertEqual(status["recent_trades"], [])

    def test_seed_wallets_show_as_pending_watchlist(self):
        from poly_monitor.dashboard.status import build_dashboard_status, wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x2222222222222222222222222222222222222222"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.add_seed(wallet, "manual-seed")
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))
            detail = wallet_detail(data_dir, wallet)

        self.assertEqual(len(status["candidates"]["seed_watchlist"]), 1)
        seed = status["candidates"]["seed_watchlist"][0]
        self.assertEqual(seed["wallet"], wallet)
        self.assertEqual(seed["name"], "manual-seed")
        self.assertEqual(seed["score_state"], "pending")
        self.assertEqual(seed["metrics"]["trades_7d"], 0)
        self.assertEqual(detail["status"], "seed_watchlist")
        self.assertEqual(detail["score_state"], "pending")

    def test_sqlite_scores_are_used_even_without_latest_report(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x3333333333333333333333333333333333333333"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.add_seed(wallet, "scored-seed")
            store.upsert_score(
                CandidateScore(
                    wallet=wallet,
                    status="dormant_candidate",
                    rank_score=0.0,
                    reasons=["markets_30d_below_threshold"],
                    metrics={
                        "wallet": wallet,
                        "trades_7d": 777,
                        "markets_7d": 70,
                        "trades_30d": 1777,
                        "markets_30d": 170,
                        "pnl_7d": 12.3,
                        "pnl_30d": 45.6,
                        "wins_7d": 12,
                        "losses_7d": 3,
                    },
                )
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(status["candidate_counts"]["dormant_candidate"], 1)
        dormant = status["candidates"]["dormant_candidate"][0]
        self.assertEqual(dormant["name"], "scored-seed")
        self.assertEqual(dormant["metrics"]["trades_30d"], 1777)
        self.assertEqual(status["candidates"]["seed_watchlist"][0]["metrics"]["trades_30d"], 1777)
        self.assertEqual(status["candidates"]["seed_watchlist"][0]["score_state"], "ready")

    def test_candidate_names_fall_back_to_latest_trade_name(self):
        from poly_monitor.dashboard.status import build_dashboard_status, wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x4444444444444444444444444444444444444444"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(
                {
                    "tx_hash": "0xname",
                    "wallet": wallet,
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 1770000010,
                    "outcome": "Up",
                    "price": 0.5,
                    "size": 10,
                    "usdc": 5,
                    "name": "frontrow-user",
                }
            )
            store.upsert_score(
                CandidateScore(
                    wallet=wallet,
                    status="active_candidate",
                    rank_score=10,
                    reasons=[],
                    metrics={
                        "wallet": wallet,
                        "trades_7d": 10,
                        "trades_30d": 10,
                        "pnl_7d": 1,
                        "pnl_30d": 1,
                        "wins_7d": 1,
                        "losses_7d": 0,
                    },
                )
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))
            detail = wallet_detail(data_dir, wallet)

        self.assertEqual(status["candidates"]["active_candidate"][0]["name"], "frontrow-user")
        self.assertEqual(status["candidates"]["active_candidate"][0]["metrics"]["name"], "frontrow-user")
        self.assertEqual(detail["metrics"]["name"], "frontrow-user")

    def test_dashboard_caps_archive_candidates(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            for idx in range(55):
                wallet = f"0x{idx:040x}"
                store.upsert_score(
                    CandidateScore(
                        wallet=wallet,
                        status="archive_candidate",
                        rank_score=float(idx),
                        reasons=["test"],
                        metrics={"wallet": wallet},
                    )
                )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(status["candidate_counts"]["archive_candidate"], 0)
        self.assertEqual(len(status["candidates"]["archive_candidate"]), 0)

    def test_dashboard_caps_and_sorts_active_and_dormant_candidates(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            for status_name in ("active_candidate", "dormant_candidate"):
                for idx in range(35):
                    wallet = f"0x{status_name[:2]}{idx:038d}"
                    store.upsert_score(
                        CandidateScore(
                            wallet=wallet,
                            status=status_name,
                            rank_score=float(idx),
                            reasons=[],
                            metrics={"wallet": wallet, "pnl_7d": idx, "pnl_30d": idx, "wins_7d": 1, "losses_7d": 0},
                        )
                    )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(status["candidate_counts"]["active_candidate"], 15)
        self.assertEqual(status["candidate_counts"]["dormant_candidate"], 10)
        self.assertEqual(status["candidates"]["active_candidate"][0]["rank_score"], 34.0)
        self.assertEqual(status["candidates"]["dormant_candidate"][0]["rank_score"], 34.0)

    def test_seed_watchlist_uses_score_even_when_scored_row_is_hidden_by_display_cap(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            seed_wallet = "0x9999999999999999999999999999999999999999"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.add_seed(seed_wallet, "older-seed")
            for idx in range(35):
                wallet = f"0x{idx:040x}"
                store.upsert_score(
                    CandidateScore(
                        wallet=wallet,
                        status="active_candidate",
                        rank_score=1000.0 + idx,
                        reasons=[],
                        metrics={"wallet": wallet, "pnl_7d": idx, "pnl_30d": idx, "wins_7d": 1, "losses_7d": 0},
                    )
                )
            store.upsert_score(
                CandidateScore(
                    wallet=seed_wallet,
                    status="active_candidate",
                    rank_score=1.0,
                    reasons=[],
                    metrics={
                        "wallet": seed_wallet,
                        "trades_7d": 806,
                        "pnl_7d": 82005.7,
                        "pnl_30d": 242963.9,
                        "wins_7d": 24,
                        "losses_7d": 0,
                    },
                )
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(len(status["candidates"]["active_candidate"]), 15)
        seed = status["candidates"]["seed_watchlist"][0]
        self.assertEqual(seed["wallet"], seed_wallet)
        self.assertEqual(seed["name"], "older-seed")
        self.assertEqual(seed["score_state"], "ready")
        self.assertEqual(seed["metrics"]["trades_7d"], 806)

    def test_candidate_score_raw_event_is_compact(self):
        from poly_monitor.observer import compact_score_event

        row = compact_score_event(
            {
                "wallet": "0xabc",
                "status": "active_candidate",
                "rank_score": 12.3456,
                "reasons": [],
                "metrics": {"trades_7d": 1000, "pnl_7d": 5, "huge": "x" * 1000},
            }
        )

        self.assertEqual(row["event"], "candidate_score")
        self.assertEqual(row["wallet"], "0xabc")
        self.assertEqual(row["rank_score"], 12.3456)
        self.assertNotIn("metrics", row)
        self.assertNotIn("reasons", row)

    def test_recent_events_hide_repetitive_candidate_scores(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            writer = JsonlEventWriter(data_dir)
            now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
            for idx in range(20):
                writer.write(
                    {
                        "event": "candidate_score",
                        "observed_at": (now + dt.timedelta(seconds=idx)).isoformat(),
                        "wallet": f"0x{idx:040x}",
                        "status": "archive_candidate",
                        "rank_score": 0,
                        "metrics": {"trades_7d": idx},
                        "reasons": ["trades_7d_below_threshold"],
                    },
                    now=now,
                )
            writer.write(
                {
                    "event": "trade_observed",
                    "observed_at": (now + dt.timedelta(seconds=30)).isoformat(),
                    "wallet": "0xabc",
                    "symbol": "BTC",
                    "market_slug": "btc-updown-5m-1",
                    "outcome": "Up",
                    "price": 0.51,
                    "size": 10,
                    "usdc": 5.1,
                    "tx_hash": "0xtx",
                },
                now=now,
            )
            writer.close()

            status = build_dashboard_status(data_dir, now=now + dt.timedelta(seconds=40))

        self.assertEqual(status["events"]["counts"]["candidate_score"], 20)
        self.assertEqual(len(status["events"]["recent"]), 1)
        self.assertEqual(status["events"]["recent"][0]["event_label"], "成交")

    def test_recent_events_hide_market_selected_noise(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            writer = JsonlEventWriter(data_dir)
            now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
            for idx in range(2):
                writer.write(
                    {
                        "event": "market_selected",
                        "observed_at": (now + dt.timedelta(seconds=idx)).isoformat(),
                        "symbol": "BTC",
                        "market_slug": "btc-updown-5m-1",
                    },
                    now=now,
                )
            writer.write(
                {
                    "event": "sqlite_cleanup",
                    "observed_at": (now + dt.timedelta(seconds=5)).isoformat(),
                    "removed_wallets": 3,
                    "removed_trades": 10,
                },
                now=now,
            )
            writer.close()

            status = build_dashboard_status(data_dir, now=now + dt.timedelta(seconds=10))

        self.assertEqual(status["events"]["counts"]["market_selected"], 2)
        self.assertEqual(len(status["events"]["recent"]), 1)
        self.assertEqual(status["events"]["recent"][0]["event_label"], "数据清理")

    def test_status_summarizes_sqlite_report_and_raw_jsonl(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            trade = {
                "tx_hash": "0xabc",
                "wallet": "0x1111111111111111111111111111111111111111",
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": 1770000010,
                "outcome": "Up",
                "price": 0.61,
                "size": 12.0,
                "usdc": 7.32,
                "name": "sharp-ish",
                "pseudonym": "s",
            }
            store.insert_trade(trade)
            score = CandidateScore(
                wallet=trade["wallet"],
                status="active_candidate",
                rank_score=123.45,
                reasons=[],
                metrics={
                    "wallet": trade["wallet"],
                    "trades_7d": 600,
                    "markets_7d": 90,
                    "trades_30d": 1600,
                    "markets_30d": 260,
                    "pnl_7d": 41.2,
                    "pnl_30d": 140.0,
                    "wins_7d": 52,
                    "losses_7d": 31,
                    "top1_concentration": 0.12,
                    "top3_concentration": 0.33,
                    "longshot_profit_share": 0.05,
                    "last_active_age_hours": 0.25,
                    "dual_side_rate": 0.28,
                    "late_bias_shift": 0.41,
                    "winner_add_rate": 0.37,
                },
            )
            store.upsert_score(score)
            store.close()

            write_latest_candidates(
                data_dir / "reports" / "latest_candidates.json",
                {
                    "generated_at": "2026-05-24T12:00:00+00:00",
                    "max_candidates": 30,
                    "symbols": ["BTC", "ETH"],
                    "candidates": {"active_candidate": [score.__dict__], "dormant_candidate": [], "archive_candidate": []},
                },
            )
            writer = JsonlEventWriter(data_dir)
            observed = dt.datetime(2026, 5, 24, 12, 0, 3, tzinfo=dt.timezone.utc)
            writer.write(
                {
                    "event": "market_selected",
                    "observed_at": observed.isoformat(),
                    "symbol": "BTC",
                    "market_slug": trade["market_slug"],
                    "condition_id": trade["condition_id"],
                    "window_start": "2026-02-02T02:40:00+00:00",
                    "window_end": "2026-02-02T02:45:00+00:00",
                },
                now=observed,
            )
            writer.write({"event": "trade_observed", "observed_at": observed.isoformat(), **trade}, now=observed)
            writer.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, 0, 10, tzinfo=dt.timezone.utc))

        self.assertEqual(status["sqlite"]["trade_count"], 1)
        self.assertGreater(status["health"]["raw_today_bytes"], 0)
        self.assertEqual(status["events"]["counts"]["trade_observed"], 1)
        self.assertEqual(status["events"]["last_event_age_seconds"], 7)
        self.assertEqual(status["markets"]["current"]["BTC"]["market_slug"], trade["market_slug"])
        self.assertEqual(status["markets"]["current"]["BTC"]["trade_count"], 1)
        self.assertEqual(len(status["candidates"]["active_candidate"]), 1)
        self.assertEqual(status["recent_trades"][0]["tx_hash"], "0xabc")

    def test_current_market_trade_count_excludes_pre_window_trades(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            base_trade = {
                "wallet": "0x1111111111111111111111111111111111111111",
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
                "name": "",
            }
            store.insert_trade({**base_trade, "tx_hash": "0xbefore", "exchange_ts": 1769999999})
            store.insert_trade({**base_trade, "tx_hash": "0xduring", "exchange_ts": 1770000010})
            store.close()
            writer = JsonlEventWriter(data_dir)
            observed = dt.datetime(2026, 5, 24, 12, 0, 3, tzinfo=dt.timezone.utc)
            writer.write(
                {
                    "event": "market_selected",
                    "observed_at": observed.isoformat(),
                    "symbol": "BTC",
                    "market_slug": base_trade["market_slug"],
                    "condition_id": base_trade["condition_id"],
                    "window_start": "2026-02-02T02:40:00+00:00",
                    "window_end": "2026-02-02T02:45:00+00:00",
                },
                now=observed,
            )
            writer.close()

            status = build_dashboard_status(data_dir, now=observed)

        self.assertEqual(status["markets"]["current"]["BTC"]["trade_count"], 1)


if __name__ == "__main__":
    unittest.main()
