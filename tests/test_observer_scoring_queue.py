from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from poly_monitor.observer import CryptoWalletObserver, ObserverConfig
from poly_monitor.scoring import CandidateScore


class ObserverScoringQueueTests(unittest.TestCase):
    def test_score_batch_reserves_a_slot_for_discovery_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=2,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                for idx in range(3):
                    wallet = f"0xactive{idx}"
                    observer.store.upsert_score(CandidateScore(wallet, "active_candidate", 10 - idx, [], {"wallet": wallet}))
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xrecent",
                        "wallet": "0xrecent",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )

                first = observer._score_batch()
                second = observer._score_batch()
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(first, [("0xactive0", "active_candidate"), ("0xdormant", "dormant_candidate")])
        self.assertEqual(second, [("0xactive1", "active_candidate"), ("0xrecent", None)])

    def test_score_batch_prioritizes_watchlist_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    score_refresh_sec=0,
                    score_wallets_per_cycle=2,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                observer.store.add_watchlist_wallet("0xwatched")
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 10, [], {"wallet": "0xactive"}))
                observer.store.upsert_score(CandidateScore("0xwatched", "dormant_candidate", 1, [], {"wallet": "0xwatched"}))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xw",
                        "wallet": "0xwatched",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )

                batch = observer._score_batch()
                observer._refresh_candidate_caches()
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch[0], ("0xwatched", "dormant_candidate"))
        self.assertIn("0xwatched", observer._active_snapshot_wallets)

    def test_watchlist_does_not_starve_active_or_discovery_with_default_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    score_wallets_per_cycle=2,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                for idx in range(5):
                    observer.store.add_watchlist_wallet(f"0xwatch{idx}")
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 10, [], {"wallet": "0xactive"}))
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xrecent",
                        "wallet": "0xrecent",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )

                batches = [observer._score_batch() for _ in range(4)]
            finally:
                observer.writer.close()
                observer.store.close()

        flattened = [wallet for batch in batches for wallet, _status in batch]
        self.assertIn("0xactive", flattened)
        self.assertTrue({"0xdormant", "0xrecent"} & set(flattened))
        self.assertTrue(any(wallet.startswith("0xwatch") for wallet in flattened))
        self.assertTrue(all(sum(1 for wallet, _status in batch if wallet.startswith("0xwatch")) <= 1 for batch in batches))

    def test_watchlist_rotates_with_active_and_discovery_when_budget_is_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    score_wallets_per_cycle=1,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                observer.store.add_watchlist_wallet("0xwatch")
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 10, [], {"wallet": "0xactive"}))
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))

                batches = [observer._score_batch() for _ in range(6)]
            finally:
                observer.writer.close()
                observer.store.close()

        flattened = [wallet for batch in batches for wallet, _status in batch]
        self.assertIn("0xwatch", flattened)
        self.assertIn("0xactive", flattened)
        self.assertIn("0xdormant", flattened)

    def test_budget_one_watchlist_rotation_skips_empty_active_bucket(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    score_wallets_per_cycle=1,
                    max_active_candidates=0,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                observer.store.add_watchlist_wallet("0xwatch")
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))

                batches = [observer._score_batch() for _ in range(6)]
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertTrue(all(batch for batch in batches))
        flattened = [wallet for batch in batches for wallet, _status in batch]
        self.assertIn("0xwatch", flattened)
        self.assertIn("0xdormant", flattened)

    def test_score_batch_keeps_discovery_slot_when_active_pool_is_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=4,
                    max_active_candidates=15,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                for idx in range(15):
                    wallet = f"0xactive{idx}"
                    observer.store.upsert_score(CandidateScore(wallet, "active_candidate", 100 - idx, [], {"wallet": wallet}))
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xrecent",
                        "wallet": "0xrecent",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )

                batch = observer._score_batch()
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [
            ("0xactive0", "active_candidate"),
            ("0xactive1", "active_candidate"),
            ("0xdormant", "dormant_candidate"),
            ("0xrecent", None),
        ])

    def test_score_batch_discovers_high_activity_wallets_beyond_recent_limit(self):
        def trade(wallet: str, tx: str, ts: int, market: str) -> dict:
            return {
                "tx_hash": tx,
                "wallet": wallet,
                "market_slug": market,
                "condition_id": f"cond-{market}",
                "symbol": "BTC",
                "exchange_ts": ts,
                "outcome": "Up",
                "side": "BUY",
                "price": 0.5,
                "size": 2,
                "usdc": 1,
            }

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=1,
                    max_active_candidates=0,
                    score_wallet_pool_limit=3,
                )
            )
            try:
                strong = "0xstrong"
                for idx in range(6):
                    observer.store.insert_trade(trade(strong, f"0xs{idx}", 100 + idx, f"btc-updown-5m-strong-{idx}"))
                for idx in range(4):
                    observer.store.insert_trade(trade(f"0xrecent{idx}", f"0xr{idx}", 1_000 + idx, "btc-updown-5m-recent"))

                batch = observer._score_batch(now=dt.datetime.fromtimestamp(1_100, dt.timezone.utc))
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [(strong, None)])

    def test_score_batch_skips_fresh_dormant_and_archived_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=3,
                    max_active_candidates=0,
                    max_dormant_candidates=2,
                    dormant_metrics_ttl_sec=600,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))
                observer.store.upsert_score(CandidateScore("0xarchive", "archive_candidate", 0, [], {"wallet": "0xarchive"}))
                for wallet in ("0xdormant", "0xarchive", "0xfresh"):
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0x{wallet[-4:]}",
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "outcome": "Up",
                            "price": 0.5,
                            "size": 2,
                            "usdc": 1,
                        }
                    )

                batch = observer._score_batch()
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [("0xfresh", None)])

    def test_score_batch_reactivates_high_activity_archived_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=2,
                    max_active_candidates=0,
                    max_dormant_candidates=0,
                    score_wallet_pool_limit=30,
                    archive_revival_cooldown_sec=0,
                )
            )
            try:
                observer.store.upsert_score(CandidateScore("0xarchive", "archive_candidate", 0, [], {"wallet": "0xarchive"}))
                observer.store.upsert_score(CandidateScore("0xarchive2", "archive_candidate", 0, [], {"wallet": "0xarchive2"}))
                observer.store.upsert_score(CandidateScore("0xnoise", "archive_candidate", 0, [], {"wallet": "0xnoise"}))
                now = dt.datetime.now(dt.timezone.utc)
                now_ts = int(now.timestamp())
                for idx in range(20):
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xarchive{idx}",
                            "wallet": "0xarchive",
                            "market_slug": f"btc-updown-5m-{idx}",
                            "condition_id": f"0xcond{idx}",
                            "symbol": "BTC",
                            "exchange_ts": now_ts - idx,
                            "outcome": "Up",
                            "price": 0.5,
                            "size": 2,
                            "usdc": 1,
                        }
                    )
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xarchive2{idx}",
                            "wallet": "0xarchive2",
                            "market_slug": f"eth-updown-5m-{idx}",
                            "condition_id": f"0xethcond{idx}",
                            "symbol": "ETH",
                            "exchange_ts": now_ts - idx,
                            "outcome": "Up",
                            "price": 0.5,
                            "size": 2,
                            "usdc": 1,
                        }
                    )
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xnoise",
                        "wallet": "0xnoise",
                        "market_slug": "btc-updown-5m-noise",
                        "condition_id": "0xnoise",
                        "symbol": "BTC",
                        "exchange_ts": now_ts,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )

                batch = observer._score_batch(now=now)
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [("0xarchive", "archive_candidate"), ("0xarchive2", "archive_candidate")])

    def test_score_batch_uses_configured_archive_revival_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=2,
                    max_active_candidates=0,
                    max_dormant_candidates=0,
                    score_wallet_pool_limit=30,
                    archive_revival_cooldown_sec=0,
                    archive_revival_min_markets_24h=21,
                )
            )
            try:
                observer.store.upsert_score(CandidateScore("0xbelow", "archive_candidate", 0, [], {"wallet": "0xbelow"}))
                observer.store.upsert_score(CandidateScore("0xat", "archive_candidate", 0, [], {"wallet": "0xat"}))
                now = dt.datetime.now(dt.timezone.utc)
                now_ts = int(now.timestamp())
                for idx in range(21):
                    if idx < 20:
                        observer.store.insert_trade(
                            {
                                "tx_hash": f"0xbelow{idx}",
                                "wallet": "0xbelow",
                                "market_slug": f"btc-updown-5m-{idx}",
                                "condition_id": f"0xcond{idx}",
                                "symbol": "BTC",
                                "exchange_ts": now_ts - idx,
                                "outcome": "Up",
                                "price": 0.5,
                                "size": 2,
                                "usdc": 1,
                            }
                        )
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xat{idx}",
                            "wallet": "0xat",
                            "market_slug": f"eth-updown-5m-{idx}",
                            "condition_id": f"0xethcond{idx}",
                            "symbol": "ETH",
                            "exchange_ts": now_ts - idx,
                            "outcome": "Up",
                            "price": 0.5,
                            "size": 2,
                            "usdc": 1,
                        }
                    )

                batch = observer._score_batch(now=now)
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [("0xat", "archive_candidate")])

    def test_score_batch_with_budget_one_can_score_discovery_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=1,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    dormant_metrics_ttl_sec=0,
                    score_wallet_pool_limit=5,
                )
            )
            try:
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 10, [], {"wallet": "0xactive"}))
                observer.store.upsert_score(CandidateScore("0xdormant", "dormant_candidate", 1, [], {"wallet": "0xdormant"}))

                first = observer._score_batch()
                second = observer._score_batch()
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(first, [("0xdormant", "dormant_candidate")])
        self.assertEqual(second, [("0xactive", "active_candidate")])

    def test_score_batch_uses_one_time_source_for_archive_revival(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=1,
                    max_active_candidates=0,
                    max_dormant_candidates=0,
                    score_wallet_pool_limit=5,
                    archive_revival_cooldown_sec=300,
                )
            )
            try:
                wallet = "0xarchive"
                now = dt.datetime.fromtimestamp(2_000_000, dt.timezone.utc)
                stale_updated_at = (now - dt.timedelta(minutes=10)).isoformat()
                observer.store.upsert_score(CandidateScore(wallet, "archive_candidate", 0, [], {"wallet": wallet}))
                observer.store.conn.execute("UPDATE candidate_scores SET updated_at=? WHERE wallet=?", (stale_updated_at, wallet))
                observer.store.conn.commit()
                for idx in range(20):
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xarchive-time{idx}",
                            "wallet": wallet,
                            "market_slug": f"btc-updown-5m-{idx}",
                            "condition_id": f"0xtime{idx}",
                            "symbol": "BTC",
                            "exchange_ts": int(now.timestamp()) - idx,
                            "outcome": "Up",
                            "price": 0.5,
                            "size": 2,
                            "usdc": 1,
                        }
                    )

                batch = observer._score_batch(now=now)
            finally:
                observer.writer.close()
                observer.store.close()

        self.assertEqual(batch, [(wallet, "archive_candidate")])


if __name__ == "__main__":
    unittest.main()
