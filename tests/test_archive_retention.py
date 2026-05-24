from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from poly_monitor.scoring import CandidateScore
from poly_monitor.storage import ObserverStore
from poly_monitor.observer import should_persist_score


class ArchiveRetentionTests(unittest.TestCase):
    def test_new_low_sample_archive_candidate_is_not_persisted(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["trades_7d_below_threshold"],
            metrics={"wallet": "0xabc", "trades_7d": 3, "markets_24h": 1, "historical_trades": 3},
        )

        self.assertFalse(should_persist_score(score, previous_status=None))

    def test_previous_candidate_can_move_to_archive(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["inactive_for_archive"],
            metrics={"wallet": "0xabc", "trades_7d": 0, "markets_24h": 0, "historical_trades": 0},
        )

        self.assertTrue(should_persist_score(score, previous_status="dormant_candidate"))

    def test_new_high_sample_archive_candidate_is_not_persisted_without_prior_quality_state(self):
        score = CandidateScore(
            wallet="0xabc",
            status="archive_candidate",
            rank_score=0.0,
            reasons=["pnl_7d_not_positive"],
            metrics={"wallet": "0xabc", "trades_7d": 150, "markets_24h": 4, "historical_trades": 150},
        )

        self.assertFalse(should_persist_score(score, previous_status=None))

    def test_prune_archive_scores_keeps_seeds_and_recent_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            seed_wallet = "0xseed"
            store.add_seed(seed_wallet, "seed")
            for idx in range(8):
                wallet = seed_wallet if idx == 0 else f"0x{idx:040x}"
                store.upsert_score(
                    CandidateScore(
                        wallet=wallet,
                        status="archive_candidate",
                        rank_score=float(idx),
                        reasons=["test"],
                        metrics={"wallet": wallet, "trades_7d": idx},
                    )
                )

            removed = store.prune_archive_scores(max_archive=3, keep_wallets={seed_wallet})
            rows = store.candidate_rows(limit=30)["archive_candidate"]
            store.close()

        wallets = {row["wallet"] for row in rows}
        self.assertEqual(removed, 4)
        self.assertIn(seed_wallet, wallets)
        self.assertEqual(len(rows), 4)

    def test_prune_low_sample_archives_removes_route_through_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            seed_wallet = "0xseed"
            store.add_seed(seed_wallet, "seed")
            rows = [
                (seed_wallet, {"wallet": seed_wallet, "trades_7d": 1, "markets_24h": 1, "historical_trades": 1}),
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

            removed = store.prune_low_sample_archives(keep_wallets={seed_wallet})
            archived = store.candidate_rows(limit=30)["archive_candidate"]
            store.close()

        wallets = {row["wallet"] for row in archived}
        self.assertEqual(removed, 1)
        self.assertEqual(wallets, {seed_wallet, "0xbusy", "0xwindow"})

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
            seed = "0xseed"
            stale = "0xstale"
            fresh = "0xfresh"
            store.add_seed(seed, "seed")
            store.upsert_score(CandidateScore(active, "active_candidate", 10, [], {"wallet": active}))
            store.upsert_score(CandidateScore(dormant, "dormant_candidate", 5, [], {"wallet": dormant}))
            store.upsert_score(CandidateScore(stale, "archive_candidate", 0, ["test"], {"wallet": stale}))
            for wallet, tx, ts in [
                (active, "0xa", 100),
                (dormant, "0xd", 100),
                (seed, "0xs", 100),
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
        self.assertEqual(remaining_wallets, {active, dormant, seed, fresh})
        self.assertIsNone(stale_score)

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
        self.assertEqual(last_ts, 123)


if __name__ == "__main__":
    unittest.main()
