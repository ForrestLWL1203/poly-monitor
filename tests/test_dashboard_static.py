from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DashboardStaticTests(unittest.TestCase):
    def test_wallet_detail_body_does_not_repeat_profile_header(self):
        html = (ROOT / "poly_monitor" / "dashboard" / "static" / "index.html").read_text()

        wallet_detail_template = html.split('$("walletDetail").innerHTML = `', 1)[1].split("`;", 1)[0]

        self.assertNotIn('title="${escapeHtml(detailName)}"', wallet_detail_template)
        self.assertNotIn('href="${detailProfileUrl}"', wallet_detail_template)

    def test_dashboard_does_not_display_local_observed_pnl(self):
        html = (ROOT / "poly_monitor" / "dashboard" / "static" / "index.html").read_text()

        self.assertNotIn("localObservedPnl", html)
        self.assertNotIn("本地已结算PnL", html)
        self.assertNotIn("本地观测跨度", html)
        self.assertNotIn("本地已结算窗口", html)
        self.assertNotIn("本地未结算窗口", html)

    def test_dashboard_health_displays_sqlite_size(self):
        html = (ROOT / "poly_monitor" / "dashboard" / "static" / "index.html").read_text()

        self.assertIn("SQLite 大小", html)
        self.assertIn("data.sqlite.total_bytes", html)

    def test_dashboard_window_count_caps_list_and_shows_symbol_split_in_detail(self):
        html = (ROOT / "poly_monitor" / "dashboard" / "static" / "index.html").read_text()

        self.assertIn("capped ? 288 : value", html)
        self.assertIn("BTC ${fmt(btc, 0)} / ETH ${fmt(eth, 0)}", html)


if __name__ == "__main__":
    unittest.main()
