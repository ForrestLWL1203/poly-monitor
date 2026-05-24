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


if __name__ == "__main__":
    unittest.main()
