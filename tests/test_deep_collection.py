import asyncio
import datetime as dt
import json
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from poly_monitor.deep_collection import (
    MultiWalletDeepCollector,
    MultiWalletDeepCollectorConfig,
    WalletDeepCollector,
    WalletDeepCollectorConfig,
    collector_status,
    ensure_multi_collector,
    l3_book_summary,
    process_cmdline,
    process_matches_wallet,
    read_deep_wallets,
    stop_collector,
    write_deep_wallets,
    write_status,
)
from poly_monitor.market import MarketWindow


class FakeFeed:
    latest_price = 100000.5

    def latest_age_sec(self):
        return 0.25

    def return_bps(self, _seconds):
        return 0.0


class FakeHub:
    def feed(self, _symbol):
        return FakeFeed()


class FakeStream:
    def get_book(self, token_id, *, max_age_sec=None):
        if token_id == "up":
            return [(0.51, 10), (0.5, 20), (0.49, 30), (0.48, 40)], [(0.52, 11), (0.53, 21), (0.54, 31), (0.55, 41)], 20
        return [(0.47, 12), (0.46, 22), (0.45, 32), (0.44, 42)], [(0.48, 13), (0.49, 23), (0.5, 33), (0.51, 43)], 30

    def diagnostics(self, *, reset_counts=False):
        return {"connected": True, "reset_counts": reset_counts}


class FakeAsyncClient:
    async def fetch_user_activity(self, *args, **kwargs):
        raise TimeoutError("activity timeout")

    async def close(self):
        pass


class FakeMultiWalletClient:
    def __init__(self):
        self.calls = []

    async def fetch_user_activity(self, wallet, *args, **kwargs):
        self.calls.append((wallet, kwargs))
        return [
            {
                "type": "TRADE",
                "timestamp": 1770000010,
                "slug": "btc-updown-5m-1770000000",
                "eventSlug": "btc-updown-5m-1770000000",
                "conditionId": "0xcond",
                "proxyWallet": wallet,
                "side": "BUY",
                "outcome": "Up",
                "outcomeIndex": 0,
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0x{wallet[-4:]}",
                "id": f"fill-{wallet[-4:]}",
            }
        ]

    async def close(self):
        pass


class DeepCollectionTests(unittest.TestCase):
    def test_l3_book_summary_limits_depth_to_three_levels(self):
        summary = l3_book_summary(
            bids=[(0.51, 10), (0.5, 20), (0.49, 30), (0.48, 40)],
            asks=[(0.52, 11), (0.53, 21), (0.54, 31), (0.55, 41)],
            book_age_ms=25,
        )

        self.assertEqual(summary["depth_levels"], 3)
        self.assertEqual(len(summary["bids"]), 3)
        self.assertEqual(len(summary["asks"]), 3)
        self.assertEqual(summary["bid"], 0.51)
        self.assertEqual(summary["ask"], 0.52)

    def test_collector_status_requires_matching_pid_and_fresh_heartbeat(self):
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_status(
                data_dir,
                wallet,
                {
                    "wallet": wallet,
                    "pid": 123,
                    "started_at": "2026-05-26T00:00:00+00:00",
                    "last_heartbeat_at": "2026-05-26T00:00:05+00:00",
                    "sample_sec": 1.0,
                    "book_depth_levels": 3,
                },
            )
            now = dt.datetime(2026, 5, 26, 0, 0, 10, tzinfo=dt.timezone.utc)
            with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet {wallet}") as cmdline:
                running = collector_status(data_dir, wallet, now=now)
            with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet 0x1111111111111111111111111111111111111111") as missing_cmdline:
                stopped = collector_status(data_dir, wallet, now=now)

        self.assertTrue(running["running"])
        self.assertEqual(running["state"], "running")
        cmdline.assert_called_once_with(123)
        self.assertFalse(stopped["running"])
        self.assertEqual(stopped["state"], "stopped")
        missing_cmdline.assert_called_once_with(123)

    def test_process_cmdline_prefers_procfs_without_forking_ps(self):
        proc_path = Mock()
        proc_path.exists.return_value = True
        proc_path.read_bytes.return_value = b"python\x00scripts/run_wallet_deep_collector.py\x00--wallet\x000xabc\x00"
        proc_root = MagicMock()
        proc_pid = MagicMock()
        proc_root.__truediv__.return_value = proc_pid
        proc_pid.__truediv__.return_value = proc_path

        with patch("poly_monitor.deep_collection.Path", return_value=proc_root), patch("poly_monitor.deep_collection.subprocess.check_output") as check:
            cmdline = process_cmdline(123)

        self.assertEqual(cmdline, "python scripts/run_wallet_deep_collector.py --wallet 0xabc")
        check.assert_not_called()

    def test_process_matches_wallet_requires_exact_wallet_argument(self):
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        longer = f"{wallet}9999"

        with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet {longer}"):
            self.assertFalse(process_matches_wallet(123, wallet))
        with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet {wallet}"):
            self.assertTrue(process_matches_wallet(123, wallet))

    def test_process_matches_wallet_accepts_multi_wallet_collector_args(self):
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        other = "0x1111111111111111111111111111111111111111"

        with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_multi_wallet_deep_collector.py --wallet {other} --wallet {wallet}"):
            self.assertTrue(process_matches_wallet(123, wallet))
        with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_multi_wallet_deep_collector.py --wallets {other},{wallet}"):
            self.assertTrue(process_matches_wallet(123, wallet))

    def test_stop_collector_kills_only_matching_wallet_process(self):
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_status(
                data_dir,
                wallet,
                {
                    "wallet": wallet,
                    "pid": 123,
                    "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "last_heartbeat_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                },
            )
            with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet {wallet}"), patch(
                "poly_monitor.deep_collection.os.kill"
            ) as kill:
                payload = stop_collector(data_dir, wallet)

        kill.assert_any_call(123, signal.SIGTERM)
        self.assertTrue(payload["ok"])

    def test_stop_collector_does_not_kill_multi_wallet_process(self):
        wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_status(
                data_dir,
                wallet,
                {
                    "wallet": wallet,
                    "pid": 123,
                    "collector_mode": "multi_wallet",
                    "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "last_heartbeat_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                },
            )
            with patch("poly_monitor.deep_collection.os.kill") as kill:
                payload = stop_collector(data_dir, wallet)

        kill.assert_not_called()
        self.assertTrue(payload["skipped_multi_wallet"])

    def test_ensure_multi_collector_merges_wallets_without_dropping_existing_list(self):
        existing = "0x1111111111111111111111111111111111111111"
        added = "0x2222222222222222222222222222222222222222"
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_deep_wallets(data_dir, [existing])
            with patch("poly_monitor.deep_collection.subprocess.Popen") as popen, patch(
                "poly_monitor.deep_collection.multi_collector_status",
                return_value={"running": True, "state": "running", "wallets": [existing], "wallet_count": 1},
            ):
                payload = ensure_multi_collector(data_dir, [added])
            wallets = read_deep_wallets(data_dir)

        popen.assert_not_called()
        self.assertTrue(payload["ok"])
        self.assertEqual(wallets, [existing, added])

    def test_sample_row_stores_l3_books_and_reference_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = WalletDeepCollectorConfig(wallet="0xabc", data_dir=Path(tmp))
            collector = WalletDeepCollector(config)
            try:
                collector.stream = FakeStream()
                collector.price_hub = FakeHub()
                window = MarketWindow(
                    symbol="BTC",
                    slug="btc-updown-5m-1770000000",
                    condition_id="0xcond",
                    question="",
                    up_token="up",
                    down_token="down",
                    start_time=dt.datetime.fromtimestamp(1770000000, tz=dt.timezone.utc),
                    end_time=dt.datetime.fromtimestamp(1770000300, tz=dt.timezone.utc),
                )
                row = collector._sample_row(window, now=dt.datetime.fromtimestamp(1770000010, tz=dt.timezone.utc))
            finally:
                collector.store.close()

        self.assertEqual(row["reference_price"], 100000.5)
        self.assertEqual(row["reference_price_age_sec"], 0.25)
        self.assertEqual(len(row["up_json"]["bids"]), 3)
        self.assertEqual(len(row["down_json"]["asks"]), 3)

    def test_refresh_windows_keeps_existing_windows_when_one_symbol_times_out(self):
        old_window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xold",
            question="",
            up_token="up",
            down_token="down",
            start_time=dt.datetime.fromtimestamp(1770000000, tz=dt.timezone.utc),
            end_time=dt.datetime.fromtimestamp(1770000300, tz=dt.timezone.utc),
        )
        new_window = MarketWindow(
            symbol="ETH",
            slug="eth-updown-5m-1770000000",
            condition_id="0xnew",
            question="",
            up_token="eth_up",
            down_token="eth_down",
            start_time=dt.datetime.fromtimestamp(1770000000, tz=dt.timezone.utc),
            end_time=dt.datetime.fromtimestamp(1770000300, tz=dt.timezone.utc),
        )

        def fake_find(series):
            if series.symbol == "BTC":
                raise TimeoutError("gamma timeout")
            return new_window

        with tempfile.TemporaryDirectory() as tmp:
            collector = WalletDeepCollector(WalletDeepCollectorConfig(wallet="0xabc", data_dir=Path(tmp)))
            collector.windows = {old_window.slug: old_window}
            collector.stream = MagicMock()
            collector.stream.switch_tokens = AsyncMock()
            try:
                with patch("poly_monitor.deep_collection.find_current_or_next_window", side_effect=fake_find):
                    asyncio.run(collector._refresh_windows(force=True))
            finally:
                collector.store.close()

        self.assertIn(old_window.slug, collector.windows)
        self.assertIn(new_window.slug, collector.windows)
        self.assertEqual(collector.stream.switch_tokens.call_count, 1)

    def test_activity_poll_timeout_is_recorded_without_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            collector = WalletDeepCollector(WalletDeepCollectorConfig(wallet="0xabc", data_dir=Path(tmp), activity_poll_sec=0.0))
            collector.data_api = FakeAsyncClient()
            try:
                asyncio.run(collector._poll_activity_if_due())
                status = json.loads((Path(tmp) / "state" / "deep_collectors" / "0xabc.json").read_text())
            finally:
                collector.store.close()

        self.assertEqual(status["status"], "running")
        self.assertEqual(status["last_error"]["stage"], "poll_activity")
        self.assertIn("activity timeout", status["last_error"]["message"])

    def test_multi_wallet_collector_shares_market_samples_and_separates_activity(self):
        window = MarketWindow(
            symbol="BTC",
            slug="btc-updown-5m-1770000000",
            condition_id="0xcond",
            question="",
            up_token="up",
            down_token="down",
            start_time=dt.datetime.fromtimestamp(1770000000, tz=dt.timezone.utc),
            end_time=dt.datetime.fromtimestamp(1770000300, tz=dt.timezone.utc),
        )
        wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        with tempfile.TemporaryDirectory() as tmp:
            collector = MultiWalletDeepCollector(
                MultiWalletDeepCollectorConfig(
                    wallets=(wallet_a, wallet_b),
                    data_dir=Path(tmp),
                    symbols=("BTC",),
                    sample_sec=0.0,
                    activity_poll_sec=0.0,
                )
            )
            collector.windows = {window.slug: window}
            collector.stream = FakeStream()
            collector.price_hub = FakeHub()
            collector.data_api = FakeMultiWalletClient()
            try:
                collector._sample_if_due()
                asyncio.run(collector._poll_activity_if_due())
                samples = collector.store.market_state_samples()
                activity_a = collector.store.wallet_activity_events(wallet_a)
                activity_b = collector.store.wallet_activity_events(wallet_b)
                contexts_a = collector.store.wallet_trade_contexts(wallet_a)
                contexts_b = collector.store.wallet_trade_contexts(wallet_b)
            finally:
                collector.store.close()

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["sample_reason"], "multi_wallet_deep_collector")
        self.assertEqual(len(activity_a), 1)
        self.assertEqual(len(activity_b), 1)
        self.assertEqual(len(contexts_a), 1)
        self.assertEqual(len(contexts_b), 1)


if __name__ == "__main__":
    unittest.main()
