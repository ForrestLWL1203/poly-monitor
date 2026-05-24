import datetime as dt
import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from poly_monitor.market import MarketWindow
from poly_monitor.observer import CryptoWalletObserver, ObserverConfig, context_snapshot
from poly_monitor.scoring import CandidateScore


class FakeStream:
    def get_book(self, token, max_age_sec=3.0):
        return [(0.49, 20.0)], [(0.51, 20.0)], 100

    def diagnostics(self, reset_counts=False):
        return {"messages": 1}


class FakeFeed:
    latest_price = 100_000.0

    def latest_age_sec(self):
        return 0.25

    def return_bps(self, lookback_sec):
        return lookback_sec


class ObserverContextTests(unittest.TestCase):
    def test_context_snapshot_includes_window_reference_prices(self):
        now = dt.datetime.now(dt.timezone.utc)
        window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xcond",
            question="Bitcoin Up or Down",
            up_token="up",
            down_token="down",
            start_time=now - dt.timedelta(seconds=30),
            end_time=now + dt.timedelta(seconds=270),
        )
        trade = {
            "wallet": "0xabc",
            "tx_hash": "0xtx",
            "outcome": "Up",
            "price": 0.51,
            "usdc": 25.5,
        }

        row = context_snapshot(
            trade=trade,
            window=window,
            stream=FakeStream(),
            feed=FakeFeed(),
            window_open_reference_price=99_900.0,
            window_close_reference_price=100_100.0,
        )

        self.assertEqual(row["window_open_reference_price"], 99_900.0)
        self.assertEqual(row["window_close_reference_price"], 100_100.0)

    def test_raw_trade_events_are_limited_to_followed_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            self.assertFalse(observer._should_write_raw_trade({"wallet": "0xinactive"}))
            self.assertFalse(observer._should_write_raw_trade({"wallet": "0xstranger"}))
            observer.store.close()
            observer.writer.close()

    def test_raw_trade_events_use_cached_active_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive"}))
            observer._refresh_candidate_caches()
            self.assertTrue(observer._should_write_raw_trade({"wallet": "0xactive"}))
            self.assertFalse(observer._should_write_raw_trade({"wallet": "0xstranger"}))
            observer.store.close()
            observer.writer.close()

    def test_settlement_retries_until_completed(self):
        now = dt.datetime.now(dt.timezone.utc)
        window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xcond",
            question="Bitcoin Up or Down",
            up_token="up",
            down_token="down",
            start_time=now - dt.timedelta(minutes=5),
            end_time=now - dt.timedelta(seconds=150),
        )
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), settlement_retry_sec=30.0))
            observer.pending_settlements[window.slug] = (window, now - dt.timedelta(seconds=1))
            with patch("poly_monitor.observer.fetch_crypto_price_api", return_value={"openPrice": 100, "closePrice": None, "completed": False, "cached": False}):
                asyncio.run(observer._write_pending_settlements())
            self.assertIn(window.slug, observer.pending_settlements)
            observer.store.close()
            observer.writer.close()

    def test_score_api_failure_does_not_overwrite_existing_candidate(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                with patch("poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("boom")):
                    await observer._refresh_scores_if_due()
                status = observer.store.candidate_status("0xactive")
                observer.store.close()
                observer.writer.close()
                return status

        self.assertEqual(asyncio.run(run_case()), "active_candidate")

    def test_reactivated_archive_wallet_is_scored_again(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        score_refresh_sec=0,
                        score_wallets_per_cycle=2,
                        max_active_candidates=0,
                        max_dormant_candidates=0,
                        score_wallet_pool_limit=30,
                        archive_revival_cooldown_sec=0,
                    )
                )
                now_ts = int(time.time())
                wallet = "0xarchive"
                observer.store.upsert_score(CandidateScore(wallet, "archive_candidate", 0.0, [], {"wallet": wallet}))
                for idx in range(20):
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xarchive{idx}",
                            "wallet": wallet,
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
                metrics = {
                    "wallet": wallet,
                    "trades_24h": 0,
                    "markets_24h": 0,
                    "trades_7d": 500,
                    "markets_7d": 90,
                    "trades_30d": 1500,
                    "markets_30d": 250,
                    "pnl_7d": 10,
                    "pnl_30d": 20,
                    "wins_7d": 30,
                    "losses_7d": 10,
                    "resolved_markets_7d": 40,
                    "top1_concentration": 0.1,
                    "top3_concentration": 0.2,
                    "longshot_profit_share": 0.1,
                    "last_active_age_hours": 0,
                    "historical_trades": 1500,
                    "historical_markets": 250,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=metrics) as fetch:
                    await observer._refresh_scores_if_due()
                    calls = fetch.call_count
                status = observer.store.candidate_status(wallet)
                observer.store.close()
                observer.writer.close()
                return status, calls

        self.assertEqual(asyncio.run(run_case()), ("active_candidate", 1))

    def test_archive_to_archive_refresh_uses_persistent_cooldown(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        score_refresh_sec=0,
                        score_wallets_per_cycle=1,
                        max_active_candidates=0,
                        max_dormant_candidates=0,
                        score_wallet_pool_limit=30,
                        archive_revival_cooldown_sec=300,
                    )
                )
                now_ts = int(time.time())
                wallet = "0xarchive"
                observer.store.upsert_score(CandidateScore(wallet, "archive_candidate", 0.0, [], {"wallet": wallet}))
                old_updated_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
                observer.store.conn.execute("UPDATE candidate_scores SET updated_at=? WHERE wallet=?", (old_updated_at, wallet))
                observer.store.conn.commit()
                for idx in range(20):
                    observer.store.insert_trade(
                        {
                            "tx_hash": f"0xarchivecooldown{idx}",
                            "wallet": wallet,
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
                metrics = {
                    "wallet": wallet,
                    "trades_24h": 0,
                    "markets_24h": 0,
                    "trades_7d": 0,
                    "markets_7d": 0,
                    "trades_30d": 0,
                    "markets_30d": 0,
                    "pnl_7d": 0,
                    "pnl_30d": 0,
                    "wins_7d": 0,
                    "losses_7d": 0,
                    "resolved_markets_7d": 0,
                    "top1_concentration": 1.0,
                    "top3_concentration": 1.0,
                    "longshot_profit_share": 0.0,
                    "last_active_age_hours": 0,
                    "historical_trades": 20,
                    "historical_markets": 20,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=metrics) as fetch:
                    await observer._refresh_scores_if_due()
                    observer._metrics_cache.clear()
                    await observer._refresh_scores_if_due()
                    calls = fetch.call_count
                rows = observer.store.candidate_rows()["archive_candidate"]
                observer.store.close()
                observer.writer.close()
                return rows[0]["updated_at"] > old_updated_at, calls

        self.assertEqual(asyncio.run(run_case()), (True, 1))

    def test_score_metrics_are_cached_within_ttl(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1, active_metrics_ttl_sec=60))
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                metrics = {
                    "wallet": "0xactive",
                    "trades_24h": 100,
                    "markets_24h": 30,
                    "trades_7d": 500,
                    "markets_7d": 90,
                    "trades_30d": 1500,
                    "markets_30d": 250,
                    "pnl_7d": 10,
                    "pnl_30d": 20,
                    "wins_7d": 30,
                    "losses_7d": 10,
                    "resolved_markets_7d": 40,
                    "top1_concentration": 0.1,
                    "top3_concentration": 0.2,
                    "longshot_profit_share": 0.1,
                    "last_active_age_hours": 0,
                    "historical_trades": 1500,
                    "historical_markets": 250,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=metrics) as fetch:
                    await observer._refresh_scores_if_due()
                    await observer._refresh_scores_if_due()
                    calls = fetch.call_count
                observer.store.close()
                observer.writer.close()
                return calls

        self.assertEqual(asyncio.run(run_case()), 1)

    def test_archive_metrics_cache_uses_dormant_ttl(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(data_dir=Path(tmp), active_metrics_ttl_sec=60, dormant_metrics_ttl_sec=600)
            )
            try:
                self.assertEqual(observer._metrics_cache_ttl("archive_candidate"), 600)
            finally:
                observer.store.close()
                observer.writer.close()

    def test_score_prune_runs_every_fifth_score_cycle(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1, active_metrics_ttl_sec=60))
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                metrics = {
                    "wallet": "0xactive",
                    "trades_24h": 100,
                    "markets_24h": 30,
                    "trades_7d": 500,
                    "markets_7d": 90,
                    "trades_30d": 1500,
                    "markets_30d": 250,
                    "pnl_7d": 10,
                    "pnl_30d": 20,
                    "wins_7d": 30,
                    "losses_7d": 10,
                    "resolved_markets_7d": 40,
                    "top1_concentration": 0.1,
                    "top3_concentration": 0.2,
                    "longshot_profit_share": 0.1,
                    "last_active_age_hours": 0,
                    "historical_trades": 1500,
                    "historical_markets": 250,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=metrics), patch.object(
                    observer.store, "prune_low_sample_archives", return_value=0
                ) as low_sample, patch.object(
                    observer.store, "prune_candidate_scores", return_value=0
                ) as candidate_prune, patch.object(
                    observer.store, "prune_archive_scores", return_value=0
                ) as archive_prune:
                    for _ in range(4):
                        await observer._refresh_scores_if_due()
                    before_fifth = (low_sample.call_count, candidate_prune.call_count, archive_prune.call_count)
                    await observer._refresh_scores_if_due()
                    after_fifth = (low_sample.call_count, candidate_prune.call_count, archive_prune.call_count)
                observer.store.close()
                observer.writer.close()
                return before_fifth, after_fifth

        self.assertEqual(asyncio.run(run_case()), ((0, 0, 0), (1, 2, 1)))

    def test_write_report_does_not_run_candidate_prunes(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            try:
                with patch.object(observer.store, "prune_low_sample_archives", return_value=0) as low_sample, patch.object(
                    observer.store, "prune_candidate_scores", return_value=0
                ) as candidate_prune, patch.object(
                    observer.store, "prune_archive_scores", return_value=0
                ) as archive_prune:
                    observer._write_report(force=True)

                self.assertEqual(low_sample.call_count, 0)
                self.assertEqual(candidate_prune.call_count, 0)
                self.assertEqual(archive_prune.call_count, 0)
            finally:
                observer.store.close()
                observer.writer.close()

    def test_poll_trades_skips_rows_older_than_market_last_seen_ts(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            window = MarketWindow(
                symbol="BTC",
                slug="btc-updown-5m-1",
                condition_id="0xcond",
                question="Bitcoin Up or Down",
                up_token="up",
                down_token="down",
                start_time=now,
                end_time=now + dt.timedelta(minutes=5),
            )
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
                observer.windows["BTC"] = window
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xexisting",
                        "wallet": "0xold",
                        "market_slug": window.slug,
                        "condition_id": window.condition_id,
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "outcome": "Up",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )
                raw = [
                    {"transactionHash": "0xolder", "proxyWallet": "0xolder", "conditionId": "0xcond", "slug": window.slug, "timestamp": 99, "outcome": "Up", "price": 0.5, "size": 2},
                    {"transactionHash": "0xnewer", "proxyWallet": "0xnewer", "conditionId": "0xcond", "slug": window.slug, "timestamp": 101, "outcome": "Down", "price": 0.4, "size": 3},
                ]
                observer.data_api.fetch_market_trades = AsyncMock(return_value=raw)
                await observer._poll_trades_once()
                count = observer.store.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
                observer.store.close()
                observer.writer.close()
                return count

        self.assertEqual(asyncio.run(run_case()), 2)

    def test_context_snapshot_cooldown_is_per_wallet_and_market(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), context_snapshot_cooldown_sec=5.0))
            try:
                trade = {"wallet": "0xabc", "market_slug": "btc-updown-5m-1"}
                self.assertTrue(observer._should_write_context_snapshot(trade))
                self.assertFalse(observer._should_write_context_snapshot(trade))
                self.assertTrue(observer._should_write_context_snapshot({"wallet": "0xabc", "market_slug": "btc-updown-5m-2"}))
            finally:
                observer.store.close()
                observer.writer.close()

    def test_due_wrappers_throttle_noop_tasks(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        window_refresh_sec=60.0,
                        open_price_refresh_sec=60.0,
                        settlement_check_sec=60.0,
                        poll_sec=60.0,
                    )
                )
                observer._last_window_refresh = 10**9
                observer._last_open_price_refresh = 10**9
                observer._last_settlement_check = 10**9
                observer._last_trade_poll = 10**9
                with patch.object(observer, "_refresh_windows", new=AsyncMock()) as windows, patch.object(
                    observer, "_refresh_window_open_prices", new=AsyncMock()
                ) as open_prices, patch.object(observer, "_write_pending_settlements", new=AsyncMock()) as settlements, patch.object(
                    observer, "_poll_trades_once", new=AsyncMock()
                ) as trades:
                    await observer._refresh_windows_if_due()
                    await observer._refresh_window_open_prices_if_due()
                    await observer._write_pending_settlements_if_due()
                    await observer._poll_trades_if_due()
                observer.store.close()
                observer.writer.close()
                return windows.await_count, open_prices.await_count, settlements.await_count, trades.await_count

        self.assertEqual(asyncio.run(run_case()), (0, 0, 0, 0))

    def test_cleanup_stale_data_prunes_old_context_snapshot_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), cleanup_interval_hours=0.001))
            now = time.monotonic()
            observer._last_data_cleanup = now - 10.0
            observer._last_context_snapshot = {
                ("0xold", "old-market"): now - 601,
                ("0xfresh", "fresh-market"): now - 599,
            }
            try:
                observer._cleanup_stale_data_if_due()
                self.assertEqual(set(observer._last_context_snapshot), {("0xfresh", "fresh-market")})
            finally:
                observer.store.close()
                observer.writer.close()

    def test_cleanup_stale_data_prunes_old_metrics_cache_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    cleanup_interval_hours=0.001,
                    active_metrics_ttl_sec=60,
                    dormant_metrics_ttl_sec=600,
                )
            )
            now = time.monotonic()
            observer._last_data_cleanup = now - 10.0
            observer._metrics_cache = {
                "0xold": type("_Entry", (), {"fetched_at": now - 6001, "metrics": {}})(),
                "0xfresh": type("_Entry", (), {"fetched_at": now - 5999, "metrics": {}})(),
            }
            try:
                observer._cleanup_stale_data_if_due()
                self.assertEqual(set(observer._metrics_cache), {"0xfresh"})
            finally:
                observer.store.close()
                observer.writer.close()


if __name__ == "__main__":
    unittest.main()
