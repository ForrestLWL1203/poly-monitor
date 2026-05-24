import datetime as dt
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
            observer = CryptoWalletObserver(
                ObserverConfig(data_dir=Path(tmp)),
                {"0xseed": "seed"},
            )
            self.assertTrue(observer._should_write_raw_trade({"wallet": "0xseed"}))
            self.assertFalse(observer._should_write_raw_trade({"wallet": "0xstranger"}))
            observer.store.close()
            observer.writer.close()

    def test_raw_trade_events_use_cached_active_wallets(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)), {})
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
            observer = CryptoWalletObserver(
                ObserverConfig(data_dir=Path(tmp), settlement_retry_sec=30.0),
                {},
            )
            observer.pending_settlements[window.slug] = (window, now - dt.timedelta(seconds=1))
            with patch("poly_monitor.observer.fetch_crypto_price_api", return_value={"openPrice": 100, "closePrice": None, "completed": False, "cached": False}):
                asyncio.run(observer._write_pending_settlements())
            self.assertIn(window.slug, observer.pending_settlements)
            observer.store.close()
            observer.writer.close()

    def test_score_api_failure_does_not_overwrite_existing_candidate(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1),
                    {},
                )
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                with patch("poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("boom")):
                    await observer._refresh_scores_if_due()
                status = observer.store.candidate_status("0xactive")
                observer.store.close()
                observer.writer.close()
                return status

        self.assertEqual(asyncio.run(run_case()), "active_candidate")

    def test_score_metrics_are_cached_within_ttl(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1, active_metrics_ttl_sec=60),
                    {},
                )
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
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)), {})
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
                with patch("poly_monitor.observer.fetch_market_trades", return_value=raw):
                    await observer._poll_trades_once()
                count = observer.store.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
                observer.store.close()
                observer.writer.close()
                return count

        self.assertEqual(asyncio.run(run_case()), 2)


if __name__ == "__main__":
    unittest.main()
