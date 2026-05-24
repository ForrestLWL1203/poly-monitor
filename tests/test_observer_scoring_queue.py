from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from poly_monitor.observer import CryptoWalletObserver, ObserverConfig
from poly_monitor.scoring import CandidateScore


class ObserverScoringQueueTests(unittest.TestCase):
    def test_score_batch_prioritizes_active_candidates_before_discovery_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=data_dir,
                    score_wallets_per_cycle=2,
                    max_active_candidates=3,
                    max_dormant_candidates=1,
                    score_wallet_pool_limit=5,
                ),
                seeds={},
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

        self.assertEqual(first, ["0xactive0", "0xactive1"])
        self.assertEqual(second, ["0xactive2", "0xactive0"])

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
                ),
                seeds={},
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

        self.assertEqual(batch, ["0xfresh"])


if __name__ == "__main__":
    unittest.main()
