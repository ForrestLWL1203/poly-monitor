from __future__ import annotations

import datetime as dt
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from poly_monitor.scoring import CandidateScore
from poly_monitor.storage import JsonlEventWriter, ObserverStore, write_latest_candidates


class DashboardStatusTests(unittest.TestCase):
    def test_tail_lines_reads_last_lines_from_gzip_file(self):
        from poly_monitor.dashboard.status import _tail_lines

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl.gz"
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                for idx in range(10):
                    handle.write(json.dumps({"idx": idx}) + "\n")

            lines = _tail_lines(path, max_lines=3)

        self.assertEqual([json.loads(line)["idx"] for line in lines], [7, 8, 9])

    def test_empty_data_dir_returns_healthy_empty_status(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            status = build_dashboard_status(Path(tmp), now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertTrue(status["health"]["ok"])
        self.assertEqual(status["sqlite"]["trade_count"], 0)
        self.assertEqual(status["events"]["counts"], {})
        self.assertEqual(set(status["candidates"]), {"watchlist", "active_candidate", "dormant_candidate", "archive_candidate"})
        self.assertEqual(status["candidates"]["active_candidate"], [])
        self.assertEqual(status["recent_trades"], [])

    def test_sqlite_scores_are_used_even_without_latest_report(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x3333333333333333333333333333333333333333"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
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
                        "profile_name": "scored-wallet",
                    },
                )
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(status["candidate_counts"]["dormant_candidate"], 1)
        dormant = status["candidates"]["dormant_candidate"][0]
        self.assertEqual(dormant["name"], "scored-wallet")
        self.assertEqual(dormant["metrics"]["trades_30d"], 1777)

    def test_watchlist_is_first_tab_and_excluded_from_candidate_tabs(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            watched = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            active = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.add_watchlist_wallet(watched, note="manual")
            store.upsert_score(
                CandidateScore(
                    watched,
                    "active_candidate",
                    100,
                    [],
                    {"wallet": watched, "trades_7d": 10, "trades_30d": 20, "pnl_7d": 1, "pnl_30d": 2, "wins_7d": 2, "losses_7d": 0},
                )
            )
            store.upsert_score(
                CandidateScore(
                    active,
                    "active_candidate",
                    90,
                    [],
                    {"wallet": active, "trades_7d": 10, "trades_30d": 20, "pnl_7d": 1, "pnl_30d": 2, "wins_7d": 2, "losses_7d": 0},
                )
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(list(status["candidates"].keys())[0], "watchlist")
        self.assertEqual(status["candidate_counts"]["watchlist"], 1)
        self.assertEqual(status["candidates"]["watchlist"][0]["wallet"], watched)
        self.assertTrue(status["candidates"]["watchlist"][0]["watchlisted"])
        self.assertEqual(status["candidates"]["watchlist"][0]["status"], "watchlist")
        self.assertEqual([row["wallet"] for row in status["candidates"]["active_candidate"]], [active])

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
        self.assertEqual(detail["name"], "frontrow-user")
        self.assertEqual(detail["metrics"]["name"], "frontrow-user")

    def test_wallet_detail_uses_profile_name_without_recent_trades(self):
        from poly_monitor.dashboard.status import wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x5555555555555555555555555555555555555555"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.upsert_score(
                CandidateScore(
                    wallet=wallet,
                    status="active_candidate",
                    rank_score=10,
                    reasons=[],
                    metrics={
                        "wallet": wallet,
                        "profile_name": "GODPROVIDES",
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

            detail = wallet_detail(data_dir, wallet)

        self.assertEqual(detail["name"], "GODPROVIDES")
        self.assertEqual(detail["metrics"]["name"], "GODPROVIDES")

    def test_wallet_detail_includes_observed_settled_market_pnl_rows(self):
        from poly_monitor.dashboard.status import wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x5555555555555555555555555555555555555555"
            market = "btc-updown-5m-1770000000"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(
                {
                    "tx_hash": "0xledger",
                    "wallet": wallet,
                    "market_slug": market,
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 1770000010,
                    "outcome": "Up",
                    "side": "BUY",
                    "price": 0.4,
                    "size": 10,
                    "usdc": 4,
                    "name": "ledger-wallet",
                }
            )
            store.upsert_market_settlement(
                {
                    "market_slug": market,
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "winning_side": "Up",
                    "settled_at": "2026-05-24T12:00:00+00:00",
                    "completed": True,
                }
            )
            store.close()

            detail = wallet_detail(data_dir, wallet)

        self.assertEqual(detail["ledger_summary"]["settled_markets"], 1)
        self.assertAlmostEqual(detail["ledger_summary"]["realized_pnl"], 6.0)
        self.assertEqual(len(detail["settled_markets"]), 1)
        row = detail["settled_markets"][0]
        self.assertEqual(row["market_slug"], market)
        self.assertAlmostEqual(row["realized_pnl"], 6.0)
        self.assertEqual(row["trades"], 1)
        self.assertFalse(row["incomplete"])

    def test_wallet_detail_marks_incomplete_ledger_rows(self):
        from poly_monitor.dashboard.status import wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x1111111111111111111111111111111111111111"
            market = "btc-updown-5m-1770000000"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.insert_trade(
                {
                    "tx_hash": "0xincomplete",
                    "wallet": wallet,
                    "market_slug": market,
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 1770000010,
                    "outcome": "Up",
                    "side": "SELL",
                    "price": 0.7,
                    "size": 10,
                    "usdc": 7,
                    "name": "ledger-wallet",
                }
            )
            store.upsert_market_settlement(
                {
                    "market_slug": market,
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "winning_side": "Up",
                    "settled_at": "2026-05-24T12:00:00+00:00",
                    "completed": True,
                }
            )
            store.close()

            detail = wallet_detail(data_dir, wallet)

        self.assertEqual(detail["ledger_summary"]["incomplete_markets"], 1)
        self.assertAlmostEqual(detail["ledger_summary"]["realized_pnl"], 0.0)
        self.assertTrue(detail["settled_markets"][0]["incomplete"])

    def test_watchlist_dashboard_uses_activity_ledger_pnl(self):
        from poly_monitor.dashboard.status import build_dashboard_status, wallet_detail

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
            market = "btc-updown-5m-1779694200"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.add_watchlist_wallet(wallet)
            store.upsert_score(
                CandidateScore(
                    wallet=wallet,
                    status="active_candidate",
                    rank_score=10,
                    reasons=[],
                    metrics={"wallet": wallet, "pnl_total": -999, "pnl_7d": -999, "pnl_30d": -999, "wins_7d": 0, "losses_7d": 1},
                )
            )
            store.upsert_market_settlement(
                {
                    "market_slug": market,
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "winning_side": "Up",
                    "settled_at": "2026-05-25T07:40:00+00:00",
                    "completed": True,
                }
            )
            store.insert_wallet_activity_events(
                [
                    {
                        "tx_hash": "0xbuy-up",
                        "wallet": wallet,
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "outcome_index": 0,
                        "price": 0.5,
                        "size": 10,
                        "usdc": 5,
                        "observed_at": "2026-05-25T07:30:00+00:00",
                    },
                    {
                        "tx_hash": "0xbuy-down",
                        "wallet": wallet,
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 105,
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Down",
                        "outcome_index": 1,
                        "price": 0.2,
                        "size": 5,
                        "usdc": 1,
                        "observed_at": "2026-05-25T07:30:05+00:00",
                    },
                    {
                        "tx_hash": "0xmerge",
                        "wallet": wallet,
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 110,
                        "activity_type": "MERGE",
                        "outcome_index": 999,
                        "size": 5,
                        "usdc": 5,
                        "observed_at": "2026-05-25T07:35:00+00:00",
                    },
                ]
            )
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 25, 8, tzinfo=dt.timezone.utc))
            detail = wallet_detail(data_dir, wallet)

        row = status["candidates"]["watchlist"][0]
        self.assertAlmostEqual(row["metrics"]["local_observed_pnl_total"], 4.0)
        self.assertEqual(row["metrics"]["local_observed_pnl_source"], "watchlist_activity_ledger")
        self.assertAlmostEqual(detail["ledger_summary"]["realized_pnl"], 4.0)
        self.assertEqual(detail["settled_markets"][0]["pnl_source"], "watchlist_activity_ledger")

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

    def test_recent_events_enrich_trade_with_candidate_display_name(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0x1111111111111111111111111111111111111111"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.upsert_score(
                CandidateScore(
                    wallet=wallet,
                    status="active_candidate",
                    rank_score=10,
                    reasons=[],
                    metrics={
                        "wallet": wallet,
                        "profile_name": "alpha-label",
                        "pnl_7d": 1,
                        "pnl_30d": 1,
                        "wins_7d": 1,
                        "losses_7d": 0,
                    },
                )
            )
            store.close()
            now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
            writer = JsonlEventWriter(data_dir)
            writer.write(
                {
                    "event": "trade_observed",
                    "observed_at": now.isoformat(),
                    "wallet": wallet,
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

            status = build_dashboard_status(data_dir, now=now + dt.timedelta(seconds=1))

        self.assertEqual(status["events"]["recent"][0]["name"], "alpha-label")
        self.assertEqual(status["events"]["recent"][0]["wallet_short"], "0x1111...1111")

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

    def test_recent_events_include_watchlist_value_warning(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            writer = JsonlEventWriter(data_dir)
            now = dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc)
            writer.write(
                {
                    "event": "watchlist_activity_value_warning",
                    "observed_at": now.isoformat(),
                    "wallet": "0x1111111111111111111111111111111111111111",
                    "activity_type": "MERGE",
                    "market_slug": "btc-updown-5m-1",
                    "size": 25,
                    "usdc": 0,
                    "delta": 25,
                },
                now=now,
            )
            writer.close()

            status = build_dashboard_status(data_dir, now=now + dt.timedelta(seconds=1))

        event = status["events"]["recent"][0]
        self.assertEqual(event["event_label"], "Activity 金额异常")
        self.assertEqual(event["activity_type"], "MERGE")
        self.assertEqual(event["delta"], 25)

    def test_tail_raw_events_reads_only_recent_lines(self):
        from poly_monitor.dashboard.status import _tail_raw_events

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            old_dir = data_dir / "raw" / "2026-05-23"
            new_dir = data_dir / "raw" / "2026-05-24"
            old_dir.mkdir(parents=True)
            new_dir.mkdir(parents=True)
            (old_dir / "events.jsonl").write_text(
                "\n".join(json.dumps({"event": "old", "observed_at": f"2026-05-23T00:00:0{idx}+00:00"}) for idx in range(3)) + "\n",
                encoding="utf-8",
            )
            (new_dir / "events.jsonl").write_text(
                "\n".join(json.dumps({"event": "new", "observed_at": f"2026-05-24T00:00:0{idx}+00:00", "idx": idx}) for idx in range(5)) + "\n",
                encoding="utf-8",
            )

            events = _tail_raw_events(data_dir / "raw", max_lines=3)

        self.assertEqual([row["idx"] for row in events], [2, 3, 4])
        self.assertEqual({row["event"] for row in events}, {"new"})

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

    def test_current_market_can_come_from_sqlite_window_snapshot(self):
        from poly_monitor.dashboard.status import build_dashboard_status

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            store.upsert_market_window(
                symbol="BTC",
                market_slug="btc-updown-5m-1770000000",
                condition_id="0xcond",
                window_start="2026-02-02T02:40:00+00:00",
                window_end="2026-02-02T02:45:00+00:00",
            )
            base_trade = {
                "tx_hash": "0xwin",
                "wallet": "0x1111111111111111111111111111111111111111",
                "market_slug": "btc-updown-5m-1770000000",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": 1770000010,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }
            store.insert_trade(base_trade)
            store.close()

            status = build_dashboard_status(data_dir, now=dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc))

        self.assertEqual(status["markets"]["current"]["BTC"]["market_slug"], "btc-updown-5m-1770000000")
        self.assertEqual(status["markets"]["current"]["BTC"]["trade_count"], 1)

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
