import datetime as dt
import asyncio
import json
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


class StaleFakeStream(FakeStream):
    def get_book(self, token, max_age_sec=3.0):
        return [], [], 10_000


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
            "exchange_ts": 1770000010,
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
        self.assertEqual(row["trade_exchange_ts"], 1770000010)

    def test_context_snapshot_marks_stale_books(self):
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
        row = context_snapshot(
            trade={"wallet": "0xabc", "tx_hash": "0xtx", "outcome": "Up", "price": 0.51, "usdc": 25.5},
            window=window,
            stream=StaleFakeStream(),
            feed=FakeFeed(),
            max_book_age_sec=3.0,
        )

        self.assertTrue(row["book_stale"])
        self.assertEqual(row["up"]["book_age_ms"], 10_000)

    def test_watchlist_trade_activity_writes_trade_context(self):
        async def run_case():
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
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), watchlist_activity_poll_sec=0))
                observer.windows["BTC"] = window
                observer.stream = FakeStream()
                observer.feeds["BTC"] = FakeFeed()
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xtrade",
                            "proxyWallet": "0xabc",
                            "timestamp": int(now.timestamp()),
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": window.slug,
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up",
                            "id": "fill-1",
                        },
                        {
                            "transactionHash": "0xmerge",
                            "proxyWallet": "0xabc",
                            "timestamp": int(now.timestamp()) + 1,
                            "conditionId": "0xcond",
                            "type": "MERGE",
                            "slug": window.slug,
                            "size": 10,
                            "usdcSize": 10,
                        },
                    ]
                )
                try:
                    await observer._poll_watchlist_activity_once()
                    contexts = observer.store.wallet_trade_contexts("0xabc")
                finally:
                    observer.store.close()
                    observer.writer.close()
                return contexts

        contexts = asyncio.run(run_case())
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["tx_hash"], "0xtrade")
        self.assertEqual(contexts[0]["fill_id"], "fill-1")
        self.assertFalse(contexts[0]["book_stale"])

    def test_historical_watchlist_activity_does_not_write_synthetic_context(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                data_dir = Path(tmp)
                observer = CryptoWalletObserver(ObserverConfig(data_dir=data_dir, watchlist_activity_poll_sec=0))
                observer.stream = FakeStream()
                observer.feeds["BTC"] = FakeFeed()
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xhistorical",
                            "proxyWallet": "0xabc",
                            "timestamp": 1770000010,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": "btc-updown-5m-1770000000",
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up-token",
                            "id": "historical-fill-1",
                        },
                    ]
                )
                try:
                    with patch("poly_monitor.observer.fetch_crypto_price_api", return_value=None):
                        await observer._poll_watchlist_activity_once()
                    contexts = observer.store.wallet_trade_contexts("0xabc")
                finally:
                    observer.store.close()
                    observer.writer.close()
                raw_events = []
                for path in sorted((data_dir / "raw").glob("*/events.jsonl")):
                    raw_events.extend(json.loads(line) for line in path.read_text().splitlines())
                return contexts, raw_events

        contexts, raw_events = asyncio.run(run_case())
        self.assertEqual(contexts, [])
        self.assertNotIn("context_snapshot", {row.get("event") for row in raw_events})
        self.assertIn("watchlist_activity_observed", {row.get("event") for row in raw_events})

    def test_watchlist_context_write_cap_limits_sqlite_without_raw_duplicate(self):
        async def run_case():
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
            with tempfile.TemporaryDirectory() as tmp:
                data_dir = Path(tmp)
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=data_dir,
                        watchlist_activity_poll_sec=0,
                        max_context_writes_per_cycle=1,
                    )
                )
                observer.windows["BTC"] = window
                observer.stream = FakeStream()
                observer.feeds["BTC"] = FakeFeed()
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": f"0xtrade{idx}",
                            "proxyWallet": "0xabc",
                            "timestamp": int(now.timestamp()) + idx,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": window.slug,
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up",
                            "id": f"fill-{idx}",
                        }
                        for idx in range(3)
                    ]
                )
                try:
                    await observer._poll_watchlist_activity_once()
                    contexts = observer.store.wallet_trade_contexts("0xabc")
                finally:
                    observer.store.close()
                    observer.writer.close()
                raw_events = []
                for path in sorted((data_dir / "raw").glob("*/events.jsonl")):
                    raw_events.extend(json.loads(line) for line in path.read_text().splitlines())
                return contexts, raw_events

        contexts, raw_events = asyncio.run(run_case())
        raw_contexts = [row for row in raw_events if row.get("event") == "context_snapshot"]
        self.assertEqual(len(contexts), 1)
        self.assertEqual(raw_contexts, [])

    def test_market_state_sampling_is_low_frequency_with_heartbeat(self):
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
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            observer.windows["BTC"] = window
            observer.stream = FakeStream()
            observer.feeds["BTC"] = FakeFeed()
            try:
                self.assertTrue(observer._sample_market_state_each_cycle(now=now))
                self.assertFalse(observer._sample_market_state_each_cycle(now=now + dt.timedelta(seconds=1)))
                self.assertFalse(observer._sample_market_state_each_cycle(now=now + dt.timedelta(seconds=5)))
                self.assertTrue(observer._sample_market_state_each_cycle(now=now + dt.timedelta(seconds=15)))
                rows = observer.store.market_state_samples(window.slug)
            finally:
                observer.store.close()
                observer.writer.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["sample_reason"], "initial")
        self.assertEqual(rows[1]["sample_reason"], "heartbeat")

    def test_watchlist_activity_registers_watched_market_window(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            window_start_ts = int(now.timestamp()) + 300
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_poll_sec=0,
                        watchlist_market_capture_after_end_sec=1800,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xtrade",
                            "proxyWallet": "0xabc",
                            "timestamp": window_start_ts + 10,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": f"btc-updown-5m-{window_start_ts}",
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up",
                            "id": "fill-1",
                        }
                    ]
                )
                try:
                    await observer._poll_watchlist_activity_once()
                    rows = observer.store.watched_market_windows()
                finally:
                    observer.store.close()
                    observer.writer.close()
                return rows

        rows = asyncio.run(run_case())
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["market_slug"].startswith("btc-updown-5m-"))
        self.assertEqual(rows[0]["source_wallet"], "0xabc")
        self.assertEqual(rows[0]["status"], "tracking")

    def test_watchlist_activity_does_not_register_historical_market_window(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            window_start_ts = int((now - dt.timedelta(hours=6)).timestamp())
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_poll_sec=0,
                        watchlist_market_capture_after_end_sec=1800,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xhistorical-trade",
                            "proxyWallet": "0xabc",
                            "timestamp": window_start_ts + 10,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": f"btc-updown-5m-{window_start_ts}",
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up",
                            "id": "fill-1",
                        }
                    ]
                )
                try:
                    await observer._poll_watchlist_activity_once()
                    rows = observer.store.watched_market_windows()
                finally:
                    observer.store.close()
                    observer.writer.close()
                return rows

        self.assertEqual(asyncio.run(run_case()), [])

    def test_watchlist_activity_recomputes_each_touched_market_once_per_poll(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            start_ts = int(now.timestamp()) - 30
            slug = f"btc-updown-5m-{start_ts}"
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), watchlist_activity_poll_sec=0))
                observer.store.add_watchlist_wallet("0xaaa")
                observer.store.add_watchlist_wallet("0xbbb")
                observer.store.upsert_market_settlement(
                    {
                        "market_slug": slug,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": now.isoformat(),
                        "completed": True,
                    }
                )

                async def fetch_activity(wallet, **_kwargs):
                    return [
                        {
                            "transactionHash": f"0x{wallet[-3:]}",
                            "proxyWallet": wallet,
                            "timestamp": start_ts + 10,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcomeIndex": 0,
                            "slug": slug,
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "asset": "up",
                            "id": f"fill-{wallet[-3:]}",
                        }
                    ]

                observer.data_api.fetch_user_activity = AsyncMock(side_effect=fetch_activity)
                try:
                    with patch.object(
                        observer.store,
                        "recompute_market_pnl_for_markets",
                        wraps=observer.store.recompute_market_pnl_for_markets,
                    ) as recompute:
                        await observer._poll_watchlist_activity_once()
                        call_args = recompute.call_args_list
                finally:
                    observer.store.close()
                    observer.writer.close()
                return slug, call_args

        slug, calls = asyncio.run(run_case())
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].args[0], {slug})

    def test_poll_trades_includes_watched_market_windows(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
                observer.store.upsert_watched_market_window(
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "0xwatched",
                        "symbol": "BTC",
                        "first_seen_at": now.isoformat(),
                        "window_start": dt.datetime.fromtimestamp(1770000000, dt.timezone.utc).isoformat(),
                        "window_end": dt.datetime.fromtimestamp(1770000300, dt.timezone.utc).isoformat(),
                        "tracking_reason": "watchlist_activity",
                        "source_wallet": "0xabc",
                        "capture_until": (now + dt.timedelta(minutes=20)).isoformat(),
                        "status": "tracking",
                    }
                )
                observer.data_api.fetch_market_trades = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xother",
                            "proxyWallet": "0xother",
                            "conditionId": "0xwatched",
                            "slug": "btc-updown-5m-1770000000",
                            "timestamp": 1770000011,
                            "side": "BUY",
                            "outcome": "Down",
                            "price": 0.4,
                            "size": 5,
                        }
                    ]
                )
                try:
                    await observer._poll_trades_once()
                    trades = observer.store.conn.execute("SELECT * FROM trades WHERE condition_id='0xwatched'").fetchall()
                finally:
                    observer.store.close()
                    observer.writer.close()
                return [dict(row) for row in trades], observer.data_api.fetch_market_trades.await_args_list

        trades, calls = asyncio.run(run_case())
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["wallet"], "0xother")
        self.assertEqual(calls[0].args[0], "0xwatched")

    def test_watched_market_state_uses_shorter_cadence(self):
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
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    market_state_sample_sec=5.0,
                    watched_market_state_sample_sec=1.0,
                    market_state_heartbeat_sec=15.0,
                )
            )
            observer.windows["BTC"] = window
            observer.stream = FakeStream()
            observer.feeds["BTC"] = FakeFeed()
            observer.store.upsert_watched_market_window(
                {
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": "BTC",
                    "first_seen_at": now.isoformat(),
                    "window_start": window.start_time.isoformat(),
                    "window_end": window.end_time.isoformat(),
                    "tracking_reason": "watchlist_activity",
                    "source_wallet": "0xabc",
                    "capture_until": (now + dt.timedelta(minutes=20)).isoformat(),
                    "status": "tracking",
                }
            )
            try:
                self.assertTrue(observer._sample_market_state_each_cycle(now=now))
                self.assertTrue(observer._sample_market_state_each_cycle(now=now + dt.timedelta(seconds=1)))
                rows = observer.store.market_state_samples(window.slug)
            finally:
                observer.store.close()
                observer.writer.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["sample_reason"], "watched_heartbeat")

    def test_market_state_sampling_caches_active_watched_markets_per_cycle(self):
        now = dt.datetime.now(dt.timezone.utc)
        windows = [
            MarketWindow(
                symbol="BTC",
                slug=f"btc-updown-5m-{1770000000 + idx * 300}",
                condition_id=f"0xcond{idx}",
                question="Bitcoin Up or Down",
                up_token=f"up{idx}",
                down_token=f"down{idx}",
                start_time=now - dt.timedelta(seconds=30),
                end_time=now + dt.timedelta(seconds=270),
            )
            for idx in range(2)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), watched_market_state_sample_sec=1.0))
            observer.stream = FakeStream()
            observer.feeds["BTC"] = FakeFeed()
            for idx, window in enumerate(windows):
                observer.windows[f"BTC{idx}"] = window
            observer.store.upsert_watched_market_window(
                {
                    "market_slug": windows[0].slug,
                    "condition_id": windows[0].condition_id,
                    "symbol": "BTC",
                    "first_seen_at": now.isoformat(),
                    "window_start": windows[0].start_time.isoformat(),
                    "window_end": windows[0].end_time.isoformat(),
                    "tracking_reason": "watchlist_activity",
                    "source_wallet": "0xabc",
                    "capture_until": (now + dt.timedelta(minutes=20)).isoformat(),
                    "status": "tracking",
                }
            )
            try:
                with patch.object(
                    observer.store,
                    "watched_market_windows",
                    wraps=observer.store.watched_market_windows,
                ) as watched:
                    observer._begin_cycle()
                    self.assertTrue(observer._sample_market_state_each_cycle(now=now))
                    calls = watched.call_count
            finally:
                observer.store.close()
                observer.writer.close()

        self.assertEqual(calls, 1)

    def test_next_sleep_delay_respects_watched_market_one_second_cadence(self):
        now = dt.datetime.now(dt.timezone.utc)
        window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xcond",
            question="Bitcoin Up or Down",
            up_token="up",
            down_token="down",
            start_time=now,
            end_time=now + dt.timedelta(minutes=5),
        )
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(
                    data_dir=Path(tmp),
                    poll_sec=2.0,
                    watched_market_state_sample_sec=1.0,
                    watched_market_state_heartbeat_sec=1.0,
                )
            )
            observer.store.upsert_watched_market_window(
                {
                    "market_slug": window.slug,
                    "condition_id": window.condition_id,
                    "symbol": "BTC",
                    "first_seen_at": now.isoformat(),
                    "window_start": window.start_time.isoformat(),
                    "window_end": window.end_time.isoformat(),
                    "tracking_reason": "watchlist_activity",
                    "source_wallet": "0xabc",
                    "capture_until": (now + dt.timedelta(minutes=20)).isoformat(),
                    "status": "tracking",
                }
            )
            try:
                with patch("poly_monitor.observer.time.monotonic", return_value=100.0):
                    observer._last_trade_poll = 100.0
                    observer._last_window_refresh = 100.0
                    observer._last_score_refresh = 100.0
                    observer._last_report_refresh = 100.0
                    observer._last_watchlist_activity_poll = 100.0
                    delay = observer._next_sleep_delay(100.0)
            finally:
                observer.store.close()
                observer.writer.close()

        self.assertLessEqual(delay, 1.0)

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

    def test_completed_settlement_is_persisted_for_local_ledger(self):
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

        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), settlement_retry_sec=30.0))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xtx",
                        "wallet": "0xabc",
                        "market_slug": window.slug,
                        "condition_id": window.condition_id,
                        "symbol": "BTC",
                        "exchange_ts": int(window.start_time.timestamp()) + 10,
                        "outcome": "Up",
                        "side": "BUY",
                        "price": 0.4,
                        "size": 10,
                        "usdc": 4,
                    }
                )
                observer.pending_settlements[window.slug] = (window, now - dt.timedelta(seconds=1))
                with patch("poly_monitor.observer.fetch_crypto_price_api", return_value={"openPrice": 100, "closePrice": 101, "completed": True, "cached": False}):
                    await observer._write_pending_settlements()
                rows = observer.store.wallet_market_pnl_rows("0xabc")
                status = window.slug in observer.pending_settlements
                observer.store.close()
                observer.writer.close()
                return rows, status

        rows, still_pending = asyncio.run(run_case())

        self.assertFalse(still_pending)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["realized_pnl"], 6.0)

    def test_watched_market_settlement_worker_checks_due_windows(self):
        async def run_case():
            now = dt.datetime.now(dt.timezone.utc)
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), settlement_retry_sec=30.0))
                observer.store.upsert_watched_market_window(
                    {
                        "market_slug": "btc-updown-5m-1770000000",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "first_seen_at": now.isoformat(),
                        "window_start": dt.datetime.fromtimestamp(1770000000, dt.timezone.utc).isoformat(),
                        "window_end": (now - dt.timedelta(minutes=5)).isoformat(),
                        "tracking_reason": "watchlist_trade",
                        "source_wallet": "0xabc",
                        "capture_until": (now + dt.timedelta(minutes=20)).isoformat(),
                        "status": "tracking",
                    }
                )
                with patch("poly_monitor.observer.fetch_crypto_price_api", return_value={"openPrice": 100, "closePrice": 101, "completed": True, "cached": False}):
                    await observer._write_pending_settlements()
                row = observer.store.conn.execute("SELECT * FROM market_settlements WHERE market_slug='btc-updown-5m-1770000000'").fetchone()
                observer.store.close()
                observer.writer.close()
                return dict(row) if row else None

        row = asyncio.run(run_case())
        self.assertIsNotNone(row)
        self.assertEqual(row["winning_side"], "Up")
        self.assertEqual(row["completed"], 1)

    def test_score_api_failure_does_not_overwrite_existing_candidate(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                with patch.object(observer.store, "wallet_trade_metrics", side_effect=RuntimeError("temporary metrics failure")):
                    await observer._refresh_scores_if_due()
                status = observer.store.candidate_status("0xactive")
                observer.store.close()
                observer.writer.close()
                return status

        self.assertEqual(asyncio.run(run_case()), "active_candidate")

    def test_scoring_does_not_persist_low_quality_new_archive_when_api_metrics_fail(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xtx",
                        "wallet": "0xrich",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": int(time.time()),
                        "outcome": "Up",
                        "side": "BUY",
                        "price": 0.5,
                        "size": 2000,
                        "usdc": 1000,
                    }
                )
                with patch("poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("api unavailable")):
                    await observer._refresh_scores_if_due()
                status = observer.store.candidate_status("0xrich")
                row = observer.store.conn.execute("SELECT metrics_json FROM candidate_scores WHERE wallet='0xrich'").fetchone()
                metrics = json.loads(row["metrics_json"]) if row else {}
                observer.store.close()
                observer.writer.close()
                return status, metrics

        status, metrics = asyncio.run(run_case())

        self.assertIsNone(status)
        self.assertEqual(metrics, {})

    def test_scoring_uses_historical_api_metrics_and_keeps_local_observed_pnl_separate(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                wallet = "0xrich"
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xtx",
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": int(time.time()),
                        "outcome": "Up",
                        "side": "BUY",
                        "price": 0.4,
                        "size": 10,
                        "usdc": 4,
                    }
                )
                observer.store.upsert_market_settlement(
                    {
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Down",
                        "completed": True,
                    }
                )
                api_metrics = {
                    "wallet": wallet,
                    "trades_24h": 600,
                    "markets_24h": 100,
                    "trades_7d": 1200,
                    "markets_7d": 200,
                    "trades_30d": 2400,
                    "markets_30d": 600,
                    "pnl_7d": 100,
                    "pnl_30d": 300,
                    "pnl_source": "crypto_closed_positions",
                    "profile_pnl_7d": 100,
                    "profile_pnl_30d": 300,
                    "wins_7d": 80,
                    "losses_7d": 20,
                    "top1_concentration": 0.05,
                    "top3_concentration": 0.15,
                    "longshot_profit_share": 0.1,
                    "longshot_profit_markets": 1,
                    "last_active_age_hours": 0.1,
                    "historical_trades": 2400,
                    "historical_markets": 600,
                    "historical_pnl": 300,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=api_metrics):
                    await observer._refresh_scores_if_due()
                row = observer.store.conn.execute("SELECT status, metrics_json FROM candidate_scores WHERE wallet=?", (wallet,)).fetchone()
                observer.store.close()
                observer.writer.close()
                return row["status"], json.loads(row["metrics_json"])

        status, metrics = asyncio.run(run_case())

        self.assertEqual(status, "active_candidate")
        self.assertEqual(metrics["pnl_source"], "crypto_closed_positions")
        self.assertEqual(metrics["pnl_7d"], 100)
        self.assertEqual(metrics["wins_7d"], 80)
        self.assertEqual(metrics["losses_7d"], 20)
        self.assertEqual(metrics["local_observed_pnl_7d"], -4.0)
        self.assertEqual(metrics["local_observed_settled_markets_7d"], 1)

    def test_scoring_keeps_wallet_with_local_ledger_after_trade_cleanup(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                wallet = "0xledger"
                observer.store.upsert_score(CandidateScore(wallet, "active_candidate", 1.0, [], {"wallet": wallet, "historical_trades": 10}))
                observer.store.conn.execute(
                    """
                    INSERT INTO wallet_market_pnl(
                        wallet, market_slug, condition_id, symbol, realized_pnl, buy_usdc,
                        sell_usdc, settled_value, net_shares_up, net_shares_down, trades,
                        winning_side, settled_at, incomplete
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0)
                    """,
                    (
                        wallet,
                        "btc-updown-5m-1",
                        "0xcond",
                        "BTC",
                        25.0,
                        50.0,
                        0.0,
                        75.0,
                        75.0,
                        0.0,
                        1,
                        "Up",
                        dt.datetime.now(dt.timezone.utc).isoformat(),
                    ),
                )
                observer.store.conn.commit()
                api_metrics = {
                    "wallet": wallet,
                    "trades_24h": 600,
                    "markets_24h": 100,
                    "trades_7d": 900,
                    "markets_7d": 100,
                    "trades_30d": 900,
                    "markets_30d": 100,
                    "pnl_7d": 100,
                    "pnl_30d": 100,
                    "pnl_source": "crypto_settled_positions",
                    "top1_concentration": 0.1,
                    "top3_concentration": 0.2,
                    "longshot_profit_share": 0.0,
                    "longshot_profit_markets": 0,
                    "last_active_age_hours": 0.1,
                    "historical_trades": 900,
                    "historical_markets": 100,
                    "historical_pnl": 100,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=api_metrics):
                    metrics = await observer._metrics_for_wallet(wallet, "active_candidate")
                exists = observer.store.conn.execute("SELECT 1 FROM candidate_scores WHERE wallet=?", (wallet,)).fetchone() is not None
                observer.store.close()
                observer.writer.close()
                return metrics, exists

        metrics, exists = asyncio.run(run_case())

        self.assertTrue(exists)
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics["local_observed_settled_markets_total"], 1)

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
                with patch("poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("api unavailable")):
                    await observer._refresh_scores_if_due()
                status = observer.store.candidate_status(wallet)
                observer.store.close()
                observer.writer.close()
                return status

        self.assertEqual(asyncio.run(run_case()), "archive_candidate")

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
                with patch("poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("api unavailable")):
                    await observer._refresh_scores_if_due()
                    observer._metrics_cache.clear()
                    await observer._refresh_scores_if_due()
                rows = observer.store.candidate_rows()["archive_candidate"]
                observer.store.close()
                observer.writer.close()
                return rows[0]["updated_at"] > old_updated_at

        self.assertEqual(asyncio.run(run_case()), True)

    def test_score_metrics_are_cached_within_ttl(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1, active_metrics_ttl_sec=60))
                observer.store.upsert_score(CandidateScore("0xactive", "active_candidate", 1.0, [], {"wallet": "0xactive", "pnl_7d": 10, "pnl_30d": 10, "wins_7d": 1, "losses_7d": 0}))
                metrics = {
                    "wallet": "0xactive",
                    "trades_24h": 100,
                    "markets_24h": 100,
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
                with patch.object(observer.store, "wallet_trade_metrics", return_value=metrics) as fetch, patch(
                    "poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("api unavailable")
                ):
                    await observer._refresh_scores_if_due()
                    await observer._refresh_scores_if_due()
                    calls = fetch.call_count
                observer.store.close()
                observer.writer.close()
                return calls

        self.assertEqual(asyncio.run(run_case()), 1)

    def test_score_refresh_reuses_local_observed_24h_metrics_without_extra_count_query(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                wallet = "0xactive"
                observer.store.insert_trade(
                    {
                        "tx_hash": "0xtx",
                        "wallet": wallet,
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": int(time.time()),
                        "outcome": "Up",
                        "side": "BUY",
                        "price": 0.5,
                        "size": 2,
                        "usdc": 1,
                    }
                )
                api_metrics = {
                    "wallet": wallet,
                    "trades_24h": 100,
                    "markets_24h": 0,
                    "trades_7d": 1000,
                    "markets_7d": 100,
                    "trades_30d": 2000,
                    "markets_30d": 200,
                    "pnl_7d": 100,
                    "pnl_30d": 200,
                    "pnl_source": "profile_profit",
                    "wins_7d": 20,
                    "losses_7d": 1,
                    "top1_concentration": 0.1,
                    "top3_concentration": 0.2,
                    "longshot_profit_share": 0.1,
                    "longshot_profit_markets": 1,
                    "last_active_age_hours": 0,
                    "historical_trades": 2000,
                    "historical_markets": 200,
                    "historical_pnl": 200,
                    "dual_side_rate": 0,
                    "late_bias_shift": 0,
                    "winner_add_rate": 0,
                }
                with patch("poly_monitor.observer.build_metrics_from_api", return_value=api_metrics), patch.object(
                    observer.store, "wallet_24h_counts", side_effect=AssertionError("extra 24h count query")
                ):
                    await observer._refresh_scores_if_due()
                row = observer.store.conn.execute("SELECT metrics_json FROM candidate_scores WHERE wallet=?", (wallet,)).fetchone()
                observer.store.close()
                observer.writer.close()
                return json.loads(row["metrics_json"])

        metrics = asyncio.run(run_case())

        self.assertEqual(metrics["markets_24h_source"], "local_observed")
        self.assertEqual(metrics["markets_24h"], 1)
        self.assertEqual(metrics["trades_24h"], 1)

    def test_existing_candidate_without_local_trades_is_deleted_and_skipped(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp), score_refresh_sec=0, score_wallets_per_cycle=1))
                wallet = "0xstale"
                observer.store.upsert_score(CandidateScore(wallet, "archive_candidate", 1.0, [], {"wallet": wallet, "historical_trades": 10}))
                observer._last_score_event_state[wallet] = ("archive_candidate", 1.0)
                metrics = await observer._metrics_for_wallet(wallet, "archive_candidate")
                status = observer.store.candidate_status(wallet)
                state = dict(observer._last_score_event_state)
                observer.store.close()
                observer.writer.close()
                return metrics, status, state

        metrics, status, state = asyncio.run(run_case())
        self.assertIsNone(metrics)
        self.assertIsNone(status)
        self.assertNotIn("0xstale", state)

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

    def test_watchlist_metrics_cache_uses_active_ttl_even_when_archived(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(
                ObserverConfig(data_dir=Path(tmp), active_metrics_ttl_sec=60, dormant_metrics_ttl_sec=600)
            )
            try:
                wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
                observer.store.add_watchlist_wallet(wallet)
                observer._refresh_candidate_caches()

                with patch.object(observer.store, "watchlist_wallets", side_effect=AssertionError("unexpected SQL")):
                    self.assertEqual(observer._metrics_cache_ttl("archive_candidate", wallet), 60)
                    self.assertEqual(observer._metrics_cache_ttl("archive_candidate", "0xnotwatched"), 600)
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
                    "markets_24h": 100,
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
                with patch.object(observer.store, "wallet_trade_metrics", return_value=metrics), patch(
                    "poly_monitor.observer.build_metrics_from_api", side_effect=RuntimeError("api unavailable")
                ), patch.object(
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

    def test_poll_trades_uses_low_page_count_for_candidate_discovery(self):
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
                observer = CryptoWalletObserver(
                    ObserverConfig(data_dir=Path(tmp), market_trade_pages=1, watched_market_trade_pages=3)
                )
                observer.windows["BTC"] = window
                observer.data_api.fetch_market_trades = AsyncMock(return_value=[])
                await observer._poll_trades_once()
                call = observer.data_api.fetch_market_trades.await_args
                observer.store.close()
                observer.writer.close()
                return call

        call = asyncio.run(run_case())
        self.assertEqual(call.args[0], "0xcond")
        self.assertEqual(call.kwargs["limit"], 100)
        self.assertEqual(call.kwargs["pages"], 1)

    def test_poll_trades_backs_off_after_rate_limit(self):
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
                observer = CryptoWalletObserver(
                    ObserverConfig(data_dir=Path(tmp), market_trade_rate_limit_backoff_sec=60)
                )
                observer.windows["BTC"] = window
                observer.data_api.fetch_market_trades = AsyncMock(side_effect=Exception("429 Too Many Requests"))
                await observer._poll_trades_once(now_monotonic=100.0)
                await observer._poll_trades_once(now_monotonic=120.0)
                calls = observer.data_api.fetch_market_trades.await_count
                observer.store.close()
                observer.writer.close()
                return calls

        self.assertEqual(asyncio.run(run_case()), 1)

    def test_watchlist_activity_poll_persists_trade_merge_and_redeem(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_poll_sec=0,
                        watchlist_activity_lookback_sec=600,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                raw = [
                    {
                        "transactionHash": "0xtrade",
                        "proxyWallet": "0xabc",
                        "timestamp": 100,
                        "conditionId": "0xcond",
                        "type": "TRADE",
                        "side": "BUY",
                        "outcome": "Down",
                        "outcomeIndex": 1,
                        "slug": "btc-updown-5m-1",
                        "price": 0.04,
                        "size": 25,
                        "usdcSize": 1,
                        "asset": "token-down",
                        "id": "activity-fill-1",
                    },
                    {
                        "transactionHash": "0xmerge",
                        "proxyWallet": "0xabc",
                        "timestamp": 110,
                        "conditionId": "0xcond",
                        "type": "MERGE",
                        "outcomeIndex": 999,
                        "slug": "btc-updown-5m-1",
                        "size": 25,
                        "usdcSize": 25,
                    },
                    {
                        "transactionHash": "0xredeem",
                        "proxyWallet": "0xabc",
                        "timestamp": 120,
                        "conditionId": "0xcond",
                        "type": "REDEEM",
                        "outcomeIndex": 999,
                        "slug": "btc-updown-5m-1",
                        "size": 5,
                        "usdcSize": 5,
                    },
                ]
                observer.data_api.fetch_user_activity = AsyncMock(return_value=raw)
                try:
                    await observer._poll_watchlist_activity_once()
                    rows = observer.store.wallet_activity_events("0xabc")
                    trade = observer.store.conn.execute("SELECT * FROM trades WHERE wallet='0xabc'").fetchone()
                finally:
                    observer.store.close()
                    observer.writer.close()
                return rows, dict(trade), observer.data_api.fetch_user_activity.await_args.kwargs

        rows, trade, kwargs = asyncio.run(run_case())
        self.assertEqual([row["activity_type"] for row in rows], ["TRADE", "MERGE", "REDEEM"])
        self.assertEqual(rows[0]["side"], "BUY")
        self.assertEqual(rows[1]["usdc"], 25)
        self.assertEqual(trade["fill_id"], "activity-fill-1")
        self.assertIn("start", kwargs)

    def test_watchlist_activity_poll_backfills_settlement_for_historical_market(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_poll_sec=0,
                        watchlist_activity_lookback_sec=600,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xtrade",
                            "proxyWallet": "0xabc",
                            "timestamp": 1770000010,
                            "conditionId": "0xcond",
                            "type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "outcomeIndex": 1,
                            "slug": "eth-updown-5m-1770000000",
                            "price": 0.6,
                            "size": 10,
                            "usdcSize": 6,
                            "asset": "token-down",
                            "id": "activity-fill-1",
                        },
                    ]
                )
                try:
                    with patch(
                        "poly_monitor.observer.fetch_crypto_price_api",
                        return_value={"openPrice": 100, "closePrice": 99, "completed": True, "cached": False},
                    ) as fetch_price:
                        await observer._poll_watchlist_activity_once()
                    pnl_rows = observer.store.watchlist_market_pnl_rows("0xabc")
                    settlement = observer.store.conn.execute(
                        "SELECT * FROM market_settlements WHERE market_slug='eth-updown-5m-1770000000'"
                    ).fetchone()
                finally:
                    observer.store.close()
                    observer.writer.close()
                return fetch_price.call_count, pnl_rows, dict(settlement)

        call_count, pnl_rows, settlement = asyncio.run(run_case())
        self.assertEqual(call_count, 1)
        self.assertEqual(settlement["winning_side"], "Down")
        self.assertEqual(pnl_rows, [])

    def test_watchlist_activity_poll_warns_on_cashflow_size_mismatch(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_poll_sec=0,
                        watchlist_activity_lookback_sec=600,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                observer.data_api.fetch_user_activity = AsyncMock(
                    return_value=[
                        {
                            "transactionHash": "0xmerge",
                            "proxyWallet": "0xabc",
                            "timestamp": 110,
                            "conditionId": "0xcond",
                            "type": "MERGE",
                            "outcomeIndex": 999,
                            "slug": "btc-updown-5m-1",
                            "size": 25,
                            "usdcSize": 0,
                        },
                    ]
                )
                try:
                    with patch.object(observer.writer, "write") as write:
                        await observer._poll_watchlist_activity_once()
                    events = [call.args[0] for call in write.call_args_list]
                    rows = observer.store.wallet_activity_events("0xabc")
                finally:
                    observer.store.close()
                    observer.writer.close()
                return events, rows

        events, rows = asyncio.run(run_case())
        warnings = [event for event in events if event["event"] == "watchlist_activity_value_warning"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["activity_type"], "MERGE")
        self.assertEqual(warnings[0]["size"], 25)
        self.assertEqual(warnings[0]["usdc"], 0)
        self.assertEqual(warnings[0]["delta"], 25)

    def test_watchlist_activity_poll_uses_last_seen_with_safety_window(self):
        async def run_case():
            with tempfile.TemporaryDirectory() as tmp:
                observer = CryptoWalletObserver(
                    ObserverConfig(
                        data_dir=Path(tmp),
                        watchlist_activity_lookback_sec=3600,
                        watchlist_activity_safety_window_sec=60,
                    )
                )
                observer.store.add_watchlist_wallet("0xabc")
                observer.store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xexisting",
                            "wallet": "0xabc",
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 1_000,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "observed_at": "2026-05-25T00:00:00+00:00",
                        }
                    ]
                )
                observer.data_api.fetch_user_activity = AsyncMock(return_value=[])
                with patch("poly_monitor.observer.dt.datetime") as fake_datetime:
                    fake_datetime.now.return_value = dt.datetime.fromtimestamp(1_500, dt.timezone.utc)
                    fake_datetime.fromtimestamp.side_effect = dt.datetime.fromtimestamp
                    try:
                        await observer._poll_watchlist_activity_once()
                    finally:
                        observer.store.close()
                        observer.writer.close()
                return observer.data_api.fetch_user_activity.await_args.kwargs

        kwargs = asyncio.run(run_case())
        self.assertEqual(kwargs["start"], 940)

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
            observer.store.upsert_score(CandidateScore("0xold", "active_candidate", 1.0, [], {"wallet": "0xold"}))
            observer.store.upsert_score(CandidateScore("0xfresh", "active_candidate", 1.0, [], {"wallet": "0xfresh"}))
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

    def test_cleanup_stale_data_prunes_wallet_caches_to_known_candidates(self):
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
            known = "0xknown"
            stale = "0xstale"
            observer.store.upsert_score(CandidateScore(known, "active_candidate", 1.0, [], {"wallet": known}))
            observer._last_score_event_state = {
                known: ("active_candidate", 1.0),
                stale: ("archive_candidate", 0.0),
            }
            observer._metrics_cache = {
                known: type("_Entry", (), {"fetched_at": now, "metrics": {}})(),
                stale: type("_Entry", (), {"fetched_at": now, "metrics": {}})(),
            }
            try:
                observer._cleanup_stale_data_if_due()
                self.assertEqual(set(observer._last_score_event_state), {known})
                self.assertEqual(set(observer._metrics_cache), {known})
            finally:
                observer.store.close()
                observer.writer.close()

    def test_score_raw_event_is_written_only_for_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            try:
                first = CandidateScore("0xabc", "archive_candidate", 1.0, [], {"wallet": "0xabc"})
                same = CandidateScore("0xabc", "archive_candidate", 1.0, [], {"wallet": "0xabc"})
                status_changed = CandidateScore("0xabc", "active_candidate", 1.0, [], {"wallet": "0xabc"})
                rank_changed = CandidateScore("0xabc", "active_candidate", 2.25, [], {"wallet": "0xabc"})

                self.assertTrue(observer._score_event_changed(first))
                observer._record_score_event(first)
                self.assertFalse(observer._score_event_changed(same))
                self.assertTrue(observer._score_event_changed(status_changed))
                observer._record_score_event(status_changed)
                self.assertTrue(observer._score_event_changed(rank_changed))
            finally:
                observer.store.close()
                observer.writer.close()

    def test_score_event_changed_is_pure_until_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            observer = CryptoWalletObserver(ObserverConfig(data_dir=Path(tmp)))
            try:
                first = CandidateScore("0xabc", "archive_candidate", 1.0, [], {"wallet": "0xabc"})
                same = CandidateScore("0xabc", "archive_candidate", 1.0, [], {"wallet": "0xabc"})

                self.assertTrue(observer._score_event_changed(first))
                self.assertTrue(observer._score_event_changed(first))
                self.assertEqual(observer._last_score_event_state, {})

                observer._record_score_event(first)
                self.assertFalse(observer._score_event_changed(same))
            finally:
                observer.store.close()
                observer.writer.close()

    def test_score_event_state_is_restored_from_candidate_scores_on_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = ObserverConfig(data_dir=Path(tmp))
            first = CryptoWalletObserver(config)
            first.store.upsert_score(CandidateScore("0xabc", "active_candidate", 2.0, [], {"wallet": "0xabc"}))
            first.store.close()
            first.writer.close()

            restarted = CryptoWalletObserver(config)
            try:
                same = CandidateScore("0xabc", "active_candidate", 2.0, [], {"wallet": "0xabc"})
                changed = CandidateScore("0xabc", "active_candidate", 3.25, [], {"wallet": "0xabc"})

                self.assertFalse(restarted._score_event_changed(same))
                self.assertTrue(restarted._score_event_changed(changed))
            finally:
                restarted.store.close()
                restarted.writer.close()


if __name__ == "__main__":
    unittest.main()
