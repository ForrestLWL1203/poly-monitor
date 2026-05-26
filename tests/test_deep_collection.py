import datetime as dt
import json
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from poly_monitor.deep_collection import (
    WalletDeepCollector,
    WalletDeepCollectorConfig,
    collector_status,
    l3_book_summary,
    process_cmdline,
    process_matches_wallet,
    start_collector,
    stop_collector,
    write_status,
)
from poly_monitor.market import MarketWindow


class FakeFeed:
    latest_price = 100000.5

    def latest_age_sec(self):
        return 0.25


class FakeHub:
    def feed(self, _symbol):
        return FakeFeed()


class FakeStream:
    def get_book(self, token_id, *, max_age_sec=None):
        if token_id == "up":
            return [(0.51, 10), (0.5, 20), (0.49, 30), (0.48, 40)], [(0.52, 11), (0.53, 21), (0.54, 31), (0.55, 41)], 20
        return [(0.47, 12), (0.46, 22), (0.45, 32), (0.44, 42)], [(0.48, 13), (0.49, 23), (0.5, 33), (0.51, 43)], 30


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

    def test_start_collector_is_idempotent_when_existing_process_is_healthy(self):
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
                "poly_monitor.deep_collection.subprocess.Popen"
            ) as popen:
                payload = start_collector(data_dir, wallet)

        self.assertTrue(payload["already_running"])
        self.assertEqual(popen.call_count, 0)

    def test_start_collector_rejects_when_max_active_collectors_reached(self):
        target = "0xabcdef1234567890abcdef1234567890abcdef12"
        active = "0x1111111111111111111111111111111111111111"
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_status(
                data_dir,
                active,
                {
                    "wallet": active,
                    "pid": 123,
                    "started_at": now,
                    "last_heartbeat_at": now,
                },
            )
            with patch("poly_monitor.deep_collection.process_cmdline", return_value=f"python scripts/run_wallet_deep_collector.py --wallet {active}"), patch("poly_monitor.deep_collection.subprocess.Popen") as popen:
                payload = start_collector(data_dir, target, max_active_collectors=1)

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "too_many_collectors")
        popen.assert_not_called()

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


if __name__ == "__main__":
    unittest.main()
