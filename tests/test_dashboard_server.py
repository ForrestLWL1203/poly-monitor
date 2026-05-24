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


class DashboardServerTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
