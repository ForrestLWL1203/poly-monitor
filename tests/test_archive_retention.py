from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from poly_monitor.scoring import CandidateScore
from poly_monitor.storage import ObserverStore
from poly_monitor.observer import should_persist_score


class ArchiveRetentionTests(unittest.TestCase):
    def test_new_low_sample_archive_candidate_is_persisted_for_cooldown(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["trades_7d_below_threshold"],
            metrics={"wallet": "0xabc", "trades_7d": 3, "markets_24h": 1, "historical_trades": 3},
        )

        self.assertTrue(should_persist_score(score, previous_status=None))

    def test_previous_candidate_can_move_to_archive(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["inactive_for_archive"],
            metrics={"wallet": "0xabc", "trades_7d": 0, "markets_24h": 0, "historical_trades": 0},
        )

        self.assertTrue(should_persist_score(score, previous_status="dormant_candidate"))

    def test_new_high_sample_archive_candidate_is_persisted_for_cooldown(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["pnl_7d_not_positive"],
            metrics={"wallet": "0xabc", "trades_7d": 150, "markets_24h": 4, "historical_trades": 150},
        )

        self.assertTrue(should_persist_score(score, previous_status=None))

    def test_prune_archive_scores_keeps_recent_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            for idx in range(8):
                wallet = f"0x{idx:040x}"
                store.upsert_score(
                    CandidateScore(
                        wallet=wallet,
                        status="archive_candidate",
                        rank_score=float(idx),
                        reasons=["test"],
                        metrics={"wallet": wallet, "trades_7d": idx},
                    )
                )

            removed = store.prune_archive_scores(max_archive=3)
            rows = store.candidate_rows(limit=30)["archive_candidate"]
            store.close()

        self.assertEqual(removed, 5)
        self.assertEqual(len(rows), 3)

    def test_prune_archive_scores_preserves_cooldown_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            fresh = "0xfresh"
            old = "0xold"
            store.upsert_score(CandidateScore(fresh, "archive_candidate", 0, [], {"wallet": fresh}))
            store.upsert_score(CandidateScore(old, "archive_candidate", 0, [], {"wallet": old}))
            stale_updated_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
            store.conn.execute("UPDATE candidate_scores SET updated_at=? WHERE wallet=?", (stale_updated_at, old))
            store.conn.commit()

            removed = store.prune_archive_scores(max_archive=0, min_age_seconds=300)
            wallets = {row["wallet"] for row in store.candidate_rows(limit=30)["archive_candidate"]}
            store.close()

        self.assertEqual(removed, 1)
        self.assertEqual(wallets, {fresh})

    def test_prune_low_sample_archives_removes_route_through_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            rows = [
                ("0xlow", {"wallet": "0xlow", "trades_7d": 2, "markets_24h": 1, "historical_trades": 2}),
                ("0xbusy", {"wallet": "0xbusy", "trades_7d": 120, "markets_24h": 2, "historical_trades": 120}),
                ("0xwindow", {"wallet": "0xwindow", "trades_7d": 20, "markets_24h": 3, "historical_trades": 20}),
            ]
            for wallet, metrics in rows:
                store.upsert_score(
                    CandidateScore(
                        wallet=wallet,
                        status="archive_candidate",
                        rank_score=0.0,
                        reasons=["test"],
                        metrics=metrics,
                    )
                )

            removed = store.prune_low_sample_archives()
            archived = store.candidate_rows(limit=30)["archive_candidate"]
            store.close()

        wallets = {row["wallet"] for row in archived}
        self.assertEqual(removed, 1)
        self.assertEqual(wallets, {"0xbusy", "0xwindow"})

    def test_candidate_prunes_preserve_watchlist_scores(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            watched = "0xwatched"
            low = "0xlow"
            store.add_watchlist_wallet(watched)
            store.upsert_score(CandidateScore(watched, "archive_candidate", 0, ["test"], {"wallet": watched, "trades_7d": 1}))
            store.upsert_score(CandidateScore(low, "archive_candidate", 0, ["test"], {"wallet": low, "trades_7d": 1}))

            removed_low = store.prune_low_sample_archives()
            removed_cap = store.prune_archive_scores(max_archive=0)
            watched_status = store.candidate_status(watched)
            low_status = store.candidate_status(low)
            store.close()

        self.assertEqual(removed_low, 1)
        self.assertEqual(removed_cap, 0)
        self.assertEqual(watched_status, "archive_candidate")
        self.assertIsNone(low_status)

    def test_prune_low_sample_archives_preserves_cooldown_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            fresh = "0xfresh"
            old = "0xold"
            metrics = {"trades_7d": 1, "markets_24h": 1, "historical_trades": 1}
            store.upsert_score(CandidateScore(fresh, "archive_candidate", 0.0, ["test"], {"wallet": fresh, **metrics}))
            store.upsert_score(CandidateScore(old, "archive_candidate", 0.0, ["test"], {"wallet": old, **metrics}))
            stale_updated_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
            store.conn.execute("UPDATE candidate_scores SET updated_at=? WHERE wallet=?", (stale_updated_at, old))
            store.conn.commit()

            removed = store.prune_low_sample_archives(min_age_seconds=300)
            wallets = {row["wallet"] for row in store.candidate_rows(limit=30)["archive_candidate"]}
            store.close()

        self.assertEqual(removed, 1)
        self.assertEqual(wallets, {fresh})

    def test_prune_candidate_scores_caps_active_and_dormant_by_rank(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            for idx in range(35):
                for status in ("active_candidate", "dormant_candidate"):
                    wallet = f"0x{status[:2]}{idx:038d}"
                    store.upsert_score(
                        CandidateScore(
                            wallet=wallet,
                            status=status,
                            rank_score=float(idx),
                            reasons=[],
                            metrics={"wallet": wallet},
                        )
                    )

            removed_active = store.prune_candidate_scores("active_candidate", max_rows=30)
            removed_dormant = store.prune_candidate_scores("dormant_candidate", max_rows=30)
            rows = store.candidate_rows(limit=30)
            store.close()

        self.assertEqual(removed_active, 5)
        self.assertEqual(removed_dormant, 5)
        self.assertEqual(len(rows["active_candidate"]), 30)
        self.assertEqual(len(rows["dormant_candidate"]), 30)
        self.assertEqual(rows["active_candidate"][0]["rank_score"], 34.0)

    def test_candidate_rows_use_explicit_status_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.upsert_score(CandidateScore("0xarchive", "archive_candidate", 30, [], {"wallet": "0xarchive"}))
            store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 20, [], {"wallet": "0xdormant"}))
            store.upsert_score(CandidateScore("0xactive", "active_candidate", 10, [], {"wallet": "0xactive"}))
            flat = [
                row["status"]
                for rows in store.candidate_rows(limit=30).values()
                for row in rows
            ]
            store.close()

        self.assertEqual(flat, ["active_candidate", "dormant_candidate", "archive_candidate"])

    def test_cleanup_inactive_wallet_data_keeps_core_candidates_and_removes_stale_noise(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            active = "0xactive"
            dormant = "0xdormant"
            stale = "0xstale"
            fresh = "0xfresh"
            store.upsert_score(CandidateScore(active, "active_candidate", 10, [], {"wallet": active}))
            store.upsert_score(CandidateScore(dormant, "dormant_candidate", 5, [], {"wallet": dormant}))
            store.upsert_score(CandidateScore(stale, "archive_candidate", 0, ["test"], {"wallet": stale}))
            for wallet, tx, ts in [
                (active, "0xa", 100),
                (dormant, "0xd", 100),
                (stale, "0xold", 100),
                (fresh, "0xf", 990),
            ]:
                store.insert_trade(trade(wallet, tx, ts))

            result = store.cleanup_inactive_wallet_data(inactive_cutoff_ts=500)
            remaining_wallets = {
                str(row["wallet"])
                for row in store.conn.execute("SELECT DISTINCT wallet FROM trades").fetchall()
            }
            stale_score = store.candidate_status(stale)
            store.close()

        self.assertEqual(result["removed_wallets"], 1)
        self.assertEqual(result["removed_trades"], 1)
        self.assertEqual(result["removed_score_rows"], 1)
        self.assertEqual(remaining_wallets, {active, dormant, fresh})
        self.assertIsNone(stale_score)

    def test_cleanup_inactive_wallet_data_keeps_watchlist_wallets(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            watched = "0xwatched"
            stale = "0xstale"
            store.upsert_score(CandidateScore("0xanchor", "active_candidate", 1, [], {"wallet": "0xanchor"}))
            store.upsert_score(CandidateScore(watched, "archive_candidate", 0, ["test"], {"wallet": watched}))
            store.upsert_score(CandidateScore(stale, "archive_candidate", 0, ["test"], {"wallet": stale}))
            store.add_watchlist_wallet(watched)
            store.insert_trade(trade(watched, "0xw", 100))
            store.insert_trade(trade(stale, "0xs", 100))

            result = store.cleanup_inactive_wallet_data(inactive_cutoff_ts=500)
            remaining_wallets = {
                str(row["wallet"])
                for row in store.conn.execute("SELECT DISTINCT wallet FROM trades").fetchall()
            }
            watched_score = store.candidate_status(watched)
            stale_score = store.candidate_status(stale)
            store.close()

        self.assertEqual(result["removed_wallets"], 1)
        self.assertIn(watched, remaining_wallets)
        self.assertEqual(watched_score, "archive_candidate")
        self.assertIsNone(stale_score)

    def test_cleanup_does_not_make_observed_pnl_reset_on_settlement_refresh(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.4,
                "size": 10,
                "usdc": 4,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.upsert_score(CandidateScore("0xanchor", "active_candidate", 1, [], {"wallet": "0xanchor"}))
            store.insert_trade(trade("0xstrong", "0xold", 100))
            store.upsert_market_settlement(
                {
                    "market_slug": "btc-updown-5m-1",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "winning_side": "Up",
                    "settled_at": "2026-05-24T12:00:00+00:00",
                    "completed": True,
                }
            )
            store.cleanup_inactive_wallet_data(inactive_cutoff_ts=500)

            store.upsert_market_settlement(
                {
                    "market_slug": "btc-updown-5m-1",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "winning_side": "Up",
                    "settled_at": "2026-05-24T12:00:00+00:00",
                    "completed": True,
                }
            )
            metrics = store.wallet_observed_pnl_metrics(
                "0xstrong",
                now_ts=int(dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc).timestamp()),
            )
            store.close()

        self.assertAlmostEqual(metrics["pnl_total"], 6.0)
        self.assertEqual(metrics["settled_markets_total"], 1)

    def test_cleanup_inactive_wallet_data_caps_recent_non_candidate_wallets(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.upsert_score(CandidateScore("0xanchor", "active_candidate", 1, [], {"wallet": "0xanchor"}))
            for idx in range(5):
                wallet = f"0xnoise{idx}"
                store.insert_trade(trade(wallet, f"0x{idx}", 1000 + idx))

            result = store.cleanup_inactive_wallet_data(
                inactive_cutoff_ts=1,
                max_non_candidate_wallets=2,
            )
            remaining_wallets = [
                str(row["wallet"])
                for row in store.conn.execute("SELECT DISTINCT wallet FROM trades ORDER BY wallet").fetchall()
            ]
            store.close()

        self.assertEqual(result["removed_wallets"], 3)
        self.assertEqual(result["removed_trades"], 3)
        self.assertEqual(remaining_wallets, ["0xnoise3", "0xnoise4"])

    def test_cleanup_inactive_wallet_data_runs_when_score_table_empty(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            for idx in range(5):
                store.insert_trade(trade(f"0xnoise{idx}", f"0x{idx}", 1000 + idx))

            result = store.cleanup_inactive_wallet_data(
                inactive_cutoff_ts=2_000,
                max_non_candidate_wallets=2,
            )
            remaining = store.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
            store.close()

        self.assertEqual(result["removed_wallets"], 5)
        self.assertEqual(result["removed_trades"], 5)
        self.assertEqual(result["removed_score_rows"], 0)
        self.assertEqual(remaining, 0)

    def test_cleanup_inactive_wallet_data_runs_without_core_candidates(self):
        def trade(wallet: str, tx: str, ts: int) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": "btc-updown-5m-1",
                "condition_id": "0xcond",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.upsert_score(CandidateScore("0xarchive", "archive_candidate", 1, [], {"wallet": "0xarchive"}))
            for idx in range(5):
                store.insert_trade(trade(f"0xnoise{idx}", f"0x{idx}", 1000 + idx))

            result = store.cleanup_inactive_wallet_data(
                inactive_cutoff_ts=2_000,
                max_non_candidate_wallets=2,
            )
            remaining = store.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
            store.close()

        self.assertEqual(result["removed_wallets"], 5)
        self.assertEqual(result["removed_trades"], 5)
        self.assertEqual(result["removed_score_rows"], 1)
        self.assertEqual(remaining, 0)

    def test_storage_adds_query_indexes_and_market_last_exchange_ts(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.insert_trade(
                {
                    "tx_hash": "0x1",
                    "wallet": "0xwallet",
                    "market_slug": "btc-updown-5m-1",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 123,
                    "outcome": "Up",
                    "price": 0.5,
                    "size": 2,
                    "usdc": 1,
                }
            )
            indexes = {
                str(row["name"])
                for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
            }
            last_ts = store.market_last_exchange_ts("0xcond")
            store.close()

        self.assertIn("idx_trades_wallet_ts", indexes)
        self.assertIn("idx_trades_market_ts", indexes)
        self.assertIn("idx_trades_condition_ts", indexes)
        self.assertIn("idx_scores_status_rank", indexes)
        self.assertIn("idx_scores_status_updated", indexes)
        self.assertEqual(last_ts, 123)

    def test_trades_table_stores_trade_side(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            store.insert_trade(
                {
                    "tx_hash": "0xside",
                    "wallet": "0xabc",
                    "market_slug": "btc-updown-5m-1",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 123,
                    "outcome": "Up",
                    "side": "SELL",
                    "price": 0.5,
                    "size": 2,
                    "usdc": 1,
                }
            )
            row = store.conn.execute("SELECT side FROM trades WHERE tx_hash='0xside'").fetchone()
            store.close()

        self.assertEqual(row["side"], "SELL")

    def test_storage_applies_runtime_pragmas(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            cache_size = store.conn.execute("PRAGMA cache_size").fetchone()[0]
            wal_autocheckpoint = store.conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
            store.close()

        self.assertEqual(cache_size, -32000)
        self.assertEqual(wal_autocheckpoint, 2000)


if __name__ == "__main__":
    unittest.main()
