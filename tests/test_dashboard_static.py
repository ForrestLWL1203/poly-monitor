from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DashboardStaticTests(unittest.TestCase):
    def test_wallet_detail_body_does_not_repeat_profile_header(self):
        html = (ROOT / "poly_monitor" / "dashboard" / "static" / "index.html").read_text()

        wallet_detail_template = html.split('$("walletDetail").innerHTML = `', 1)[1].split("`;", 1)[0]

        self.assertNotIn('title="${escapeHtml(detailName)}"', wallet_detail_template)
        self.assertNotIn('href="${detailProfileUrl}"', wallet_detail_template)


if __name__ == "__main__":
    unittest.main()
