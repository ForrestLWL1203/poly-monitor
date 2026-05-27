from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from unittest import mock


class DashboardServerTests(unittest.TestCase):
    def test_server_requires_explicit_cookie_secret(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                create_server(DashboardConfig(data_dir=Path(tmp), password="secret"))

    def test_login_cookie_and_api_auth_flow(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server

        with tempfile.TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                with self.assertRaises(urllib.error.HTTPError) as unauth:
                    urllib.request.urlopen(f"{base}/api/status", timeout=3)
                self.assertEqual(unauth.exception.code, 401)

                bad_body = urllib.parse.urlencode({"username": "admin", "password": "wrong"}).encode()
                bad_req = urllib.request.Request(f"{base}/api/login", data=bad_body, method="POST")
                with self.assertRaises(urllib.error.HTTPError) as bad:
                    urllib.request.urlopen(bad_req, timeout=3)
                self.assertEqual(bad.exception.code, 401)

                body = urllib.parse.urlencode({"username": "admin", "password": "secret"}).encode()
                req = urllib.request.Request(f"{base}/api/login", data=body, method="POST")
                resp = urllib.request.urlopen(req, timeout=3)
                cookie = resp.headers["Set-Cookie"].split(";", 1)[0]
                self.assertIn("poly_monitor_session=", cookie)

                status_req = urllib.request.Request(f"{base}/api/status", headers={"Cookie": cookie})
                status = json.loads(urllib.request.urlopen(status_req, timeout=3).read().decode())
                self.assertTrue(status["health"]["ok"])

                wallet_req = urllib.request.Request(f"{base}/api/wallet?address=0xmissing", headers={"Cookie": cookie})
                with self.assertRaises(urllib.error.HTTPError) as missing:
                    urllib.request.urlopen(wallet_req, timeout=3)
                self.assertEqual(missing.exception.code, 404)
                payload = json.loads(missing.exception.read().decode())
                self.assertEqual(payload["error"], "wallet_not_found")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_session_tokens_expire_and_reject_wrong_secret(self):
        from poly_monitor.dashboard.server import make_session_token, verify_session_token

        token = make_session_token("admin", "secret", now=1000)
        self.assertEqual(verify_session_token(token, "secret", max_age_seconds=60, now=1010), "admin")
        self.assertIsNone(verify_session_token(token, "other-secret", max_age_seconds=60, now=1010))
        self.assertIsNone(verify_session_token(token, "secret", max_age_seconds=60, now=2000))

    def test_status_api_uses_short_ttl_cache(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token

        with tempfile.TemporaryDirectory() as tmp:
            calls = 0

            def fake_status(_data_dir: Path) -> dict:
                nonlocal calls
                calls += 1
                return {"health": {"ok": True}, "calls": calls}

            with mock.patch("poly_monitor.dashboard.server.build_dashboard_status", side_effect=fake_status):
                server = create_server(
                    DashboardConfig(
                        data_dir=Path(tmp),
                        host="127.0.0.1",
                        port=0,
                        username="admin",
                        password="secret",
                        cookie_secret="test-secret",
                        status_cache_ttl_seconds=5.0,
                    )
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    base = f"http://127.0.0.1:{server.server_address[1]}"
                    cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                    req1 = urllib.request.Request(f"{base}/api/status", headers={"Cookie": cookie})
                    req2 = urllib.request.Request(f"{base}/api/status", headers={"Cookie": cookie})
                    first = json.loads(urllib.request.urlopen(req1, timeout=3).read().decode())
                    second = json.loads(urllib.request.urlopen(req2, timeout=3).read().decode())
                    self.assertEqual(first["calls"], 1)
                    self.assertEqual(second["calls"], 1)
                    self.assertEqual(calls, 1)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)

    def test_status_cache_single_flights_concurrent_misses(self):
        from poly_monitor.dashboard import server as server_mod

        with tempfile.TemporaryDirectory() as tmp:
            calls = 0
            release = threading.Event()

            def fake_status(_data_dir: Path) -> dict:
                nonlocal calls
                calls += 1
                release.wait(timeout=3)
                return {"health": {"ok": True}, "calls": calls}

            server_mod._status_cache = None
            with mock.patch("poly_monitor.dashboard.server.build_dashboard_status", side_effect=fake_status):
                results = []
                threads = [
                    threading.Thread(target=lambda: results.append(server_mod._cached_status(Path(tmp), ttl=5.0)))
                    for _ in range(2)
                ]
                for thread in threads:
                    thread.start()
                time.sleep(0.05)
                release.set()
                for thread in threads:
                    thread.join(timeout=3)

        self.assertEqual(calls, 1)
        self.assertEqual([row["calls"] for row in results], [1, 1])

    def test_wallet_api_does_not_cache_detail_payloads(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token

        with tempfile.TemporaryDirectory() as tmp:
            calls = 0

            def fake_wallet(_data_dir: Path, address: str, *, trade_limit: int = 100) -> dict:
                nonlocal calls
                calls += 1
                return {"wallet": address, "calls": calls}

            with mock.patch("poly_monitor.dashboard.server.wallet_detail", side_effect=fake_wallet):
                server = create_server(
                    DashboardConfig(
                        data_dir=Path(tmp),
                        host="127.0.0.1",
                        port=0,
                        username="admin",
                        password="secret",
                        cookie_secret="test-secret",
                        status_cache_ttl_seconds=5.0,
                    )
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    base = f"http://127.0.0.1:{server.server_address[1]}"
                    cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                    req1 = urllib.request.Request(f"{base}/api/wallet?address=0xABC", headers={"Cookie": cookie})
                    req2 = urllib.request.Request(f"{base}/api/wallet?address=0xabc", headers={"Cookie": cookie})
                    first = json.loads(urllib.request.urlopen(req1, timeout=3).read().decode())
                    second = json.loads(urllib.request.urlopen(req2, timeout=3).read().decode())
                    self.assertEqual(first["calls"], 1)
                    self.assertEqual(second["calls"], 2)
                    self.assertEqual(calls, 2)
                finally:
                    server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_stop_observer_api_requires_auth_and_stops_processes(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch("poly_monitor.dashboard.server._stop_observer_processes", return_value=[1234]) as stop:
                server = create_server(
                    DashboardConfig(
                        data_dir=Path(tmp),
                        host="127.0.0.1",
                        port=0,
                        username="admin",
                        password="secret",
                        cookie_secret="test-secret",
                    )
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    base = f"http://127.0.0.1:{server.server_address[1]}"
                    unauth_req = urllib.request.Request(f"{base}/api/stop-observer", data=b"", method="POST")
                    with self.assertRaises(urllib.error.HTTPError) as unauth:
                        urllib.request.urlopen(unauth_req, timeout=3)
                    self.assertEqual(unauth.exception.code, 401)
                    self.assertEqual(stop.call_count, 0)

                    cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                    req = urllib.request.Request(
                        f"{base}/api/stop-observer",
                        data=b"",
                        headers={"Cookie": cookie},
                        method="POST",
                    )
                    payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                    self.assertEqual(payload, {"ok": True, "killed_pids": [1234], "count": 1})
                    self.assertEqual(stop.call_count, 1)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)

    def test_watchlist_api_requires_auth_and_mutates_store(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
                body = json.dumps({"wallet": wallet, "action": "add"}).encode()
                unauth_req = urllib.request.Request(
                    f"{base}/api/watchlist",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as unauth:
                    urllib.request.urlopen(unauth_req, timeout=3)
                self.assertEqual(unauth.exception.code, 401)

                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                req = urllib.request.Request(
                    f"{base}/api/watchlist",
                    data=body,
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                self.assertTrue(payload["watchlisted"])

                store = ObserverStore(data_dir / "state" / "observer.sqlite")
                try:
                    self.assertEqual(store.watchlist_wallets(), [wallet])
                finally:
                    store.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_watchlist_api_rejects_partial_wallet_addresses(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token

        with tempfile.TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                body = json.dumps({"wallet": "0x12345678", "action": "add"}).encode()
                req = urllib.request.Request(
                    f"{base}/api/watchlist",
                    data=body,
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as rejected:
                    urllib.request.urlopen(req, timeout=3)
                self.assertEqual(rejected.exception.code, 400)
                payload = json.loads(rejected.exception.read().decode())
                self.assertEqual(payload["error"], "invalid_wallet")
                self.assertIn("40 hex", payload["hint"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_watchlist_remove_api_purges_wallet_research_rows(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xactivity",
                            "fill_id": "fill-1",
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1",
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
                            "asset": "up",
                            "observed_at": "2026-05-25T00:00:00+00:00",
                        }
                    ]
                )
            finally:
                store.close()

            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                body = json.dumps({"wallet": wallet, "action": "remove"}).encode()
                req = urllib.request.Request(
                    f"{base}/api/watchlist",
                    data=body,
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                activity = store.wallet_activity_events(wallet)
                watchlist = store.watchlist_wallets()
            finally:
                store.close()

        self.assertFalse(payload["watchlisted"])
        self.assertEqual(payload["purge"]["removed_activity_events"], 1)
        self.assertEqual(activity, [])
        self.assertEqual(watchlist, [])

    def test_watchlist_get_api_returns_rows(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet, note="manual")
            finally:
                store.close()
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                unauth_req = urllib.request.Request(f"{base}/api/watchlist")
                with self.assertRaises(urllib.error.HTTPError) as unauth:
                    urllib.request.urlopen(unauth_req, timeout=3)
                self.assertEqual(unauth.exception.code, 401)

                req = urllib.request.Request(f"{base}/api/watchlist", headers={"Cookie": cookie})
                payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                self.assertEqual(payload["watchlist"][0]["wallet"], wallet)
                self.assertEqual(payload["watchlist"][0]["note"], "manual")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_wallet_export_api_requires_auth_and_watchlist_wallet(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            slug = "btc-updown-5m-1770000000"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xtrade",
                            "wallet": wallet,
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 1770000121,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.6,
                            "size": 10,
                            "usdc": 6,
                            "asset": "up",
                            "observed_at": "2026-05-26T02:22:00+00:00",
                        }
                    ]
                )
                store.insert_market_state_samples(
                    [
                        {
                            "market_slug": slug,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "sampled_ts": 1770000123,
                            "observed_at": "2026-05-26T02:22:03+00:00",
                            "window_remaining_sec": 177,
                            "reference_price": 100001,
                            "reference_price_age_sec": 0.4,
                            "up_json": {"bids": [[0.59, 10]], "asks": [[0.61, 4]]},
                            "down_json": {"bids": [[0.39, 4]], "asks": [[0.41, 8]]},
                            "book_stale": False,
                            "sample_reason": "deep_collector",
                        }
                    ]
                )
            finally:
                store.close()
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                body = json.dumps({"wallet": wallet}).encode()
                unauth_req = urllib.request.Request(
                    f"{base}/api/wallet-export",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as unauth:
                    urllib.request.urlopen(unauth_req, timeout=3)
                self.assertEqual(unauth.exception.code, 401)

                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                req = urllib.request.Request(
                    f"{base}/api/wallet-export",
                    data=body,
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                self.assertTrue(payload["ok"])
                self.assertTrue(Path(payload["zip_path"]).exists())

                missing_body = json.dumps({"wallet": "0x1111111111111111111111111111111111111111"}).encode()
                missing_req = urllib.request.Request(
                    f"{base}/api/wallet-export",
                    data=missing_body,
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as missing:
                    urllib.request.urlopen(missing_req, timeout=3)
                self.assertEqual(missing.exception.code, 404)
                self.assertEqual(json.loads(missing.exception.read().decode())["error"], "wallet_not_watchlisted")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_wallet_export_api_rejects_watchlist_wallet_without_deep_samples(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
            finally:
                store.close()
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                req = urllib.request.Request(
                    f"{base}/api/wallet-export",
                    data=json.dumps({"wallet": wallet}).encode(),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as rejected:
                    urllib.request.urlopen(req, timeout=3)
                self.assertEqual(rejected.exception.code, 409)
                self.assertEqual(json.loads(rejected.exception.read().decode())["error"], "no_deep_collection_data")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_deep_collection_api_requires_watchlist_and_starts_or_stops_wallet(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
            finally:
                store.close()
            with mock.patch("poly_monitor.dashboard.server.add_multi_collector_wallet", return_value={"ok": True, "state": "running", "pid": 123}) as start, mock.patch(
                "poly_monitor.dashboard.server.remove_multi_collector_wallet", return_value={"ok": True, "state": "stopped", "pid": 123}
            ) as stop:
                server = create_server(
                    DashboardConfig(
                        data_dir=data_dir,
                        host="127.0.0.1",
                        port=0,
                        username="admin",
                        password="secret",
                        cookie_secret="test-secret",
                    )
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    base = f"http://127.0.0.1:{server.server_address[1]}"
                    body = json.dumps({"wallet": wallet, "action": "start"}).encode()
                    unauth_req = urllib.request.Request(
                        f"{base}/api/deep-collection",
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError) as unauth:
                        urllib.request.urlopen(unauth_req, timeout=3)
                    self.assertEqual(unauth.exception.code, 401)

                    cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                    req = urllib.request.Request(
                        f"{base}/api/deep-collection",
                        data=body,
                        headers={"Cookie": cookie, "Content-Type": "application/json"},
                        method="POST",
                    )
                    payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                    self.assertEqual(payload["state"], "running")
                    self.assertEqual(start.call_count, 1)

                    stop_req = urllib.request.Request(
                        f"{base}/api/deep-collection",
                        data=json.dumps({"wallet": wallet, "action": "stop"}).encode(),
                        headers={"Cookie": cookie, "Content-Type": "application/json"},
                        method="POST",
                    )
                    stopped = json.loads(urllib.request.urlopen(stop_req, timeout=3).read().decode())
                    self.assertEqual(stopped["state"], "stopped")
                    self.assertEqual(stop.call_count, 1)

                    missing_req = urllib.request.Request(
                        f"{base}/api/deep-collection",
                        data=json.dumps({"wallet": "0x1111111111111111111111111111111111111111", "action": "start"}).encode(),
                        headers={"Cookie": cookie, "Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError) as missing:
                        urllib.request.urlopen(missing_req, timeout=3)
                    self.assertEqual(missing.exception.code, 404)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)

    def test_deep_collection_api_rejects_invalid_action(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
            finally:
                store.close()
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="secret",
                    cookie_secret="test-secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                req = urllib.request.Request(
                    f"{base}/api/deep-collection",
                    data=json.dumps({"wallet": wallet, "action": "bogus"}).encode(),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as rejected:
                    urllib.request.urlopen(req, timeout=3)
                self.assertEqual(rejected.exception.code, 400)
                self.assertEqual(json.loads(rejected.exception.read().decode())["error"], "invalid_action")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_watchlist_remove_stops_deep_collector_before_purge(self):
        from poly_monitor.dashboard.server import DashboardConfig, create_server, make_session_token
        from poly_monitor.storage import ObserverStore

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.add_watchlist_wallet(wallet)
            finally:
                store.close()
            with mock.patch("poly_monitor.dashboard.server.remove_multi_collector_wallet", return_value={"ok": True, "stopped": True}) as stop:
                server = create_server(
                    DashboardConfig(
                        data_dir=data_dir,
                        host="127.0.0.1",
                        port=0,
                        username="admin",
                        password="secret",
                        cookie_secret="test-secret",
                    )
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    base = f"http://127.0.0.1:{server.server_address[1]}"
                    cookie = f"poly_monitor_session={make_session_token('admin', 'test-secret')}"
                    req = urllib.request.Request(
                        f"{base}/api/watchlist",
                        data=json.dumps({"wallet": wallet, "action": "remove"}).encode(),
                        headers={"Cookie": cookie, "Content-Type": "application/json"},
                        method="POST",
                    )
                    payload = json.loads(urllib.request.urlopen(req, timeout=3).read().decode())
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)

        stop.assert_called_once_with(data_dir, wallet)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["collector"]["stopped"], True)


if __name__ == "__main__":
    unittest.main()
