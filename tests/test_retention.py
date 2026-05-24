import datetime as dt
import gzip
import tempfile
import unittest
from pathlib import Path

from poly_monitor.storage import cleanup_raw_retention


class RetentionTests(unittest.TestCase):
    def test_cleanup_raw_retention_removes_only_old_day_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            old = raw / "2026-05-10"
            keep = raw / "2026-05-18"
            bad_name = raw / "misc"
            for path in (old, keep, bad_name):
                path.mkdir(parents=True)
                (path / "events.jsonl").write_text("{}\n")

            cleanup_raw_retention(raw, now=dt.date(2026, 5, 24), retention_days=7)

            self.assertFalse(old.exists())
            self.assertTrue(keep.exists())
            self.assertTrue(bad_name.exists())

    def test_cleanup_raw_retention_gzips_kept_past_jsonl_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            yesterday = raw / "2026-05-23"
            today = raw / "2026-05-24"
            yesterday.mkdir(parents=True)
            today.mkdir(parents=True)
            (yesterday / "events.jsonl").write_text('{"event":"old"}\n', encoding="utf-8")
            (today / "events.jsonl").write_text('{"event":"today"}\n', encoding="utf-8")

            cleanup_raw_retention(raw, now=dt.date(2026, 5, 24), retention_days=7)

            self.assertFalse((yesterday / "events.jsonl").exists())
            self.assertTrue((today / "events.jsonl").exists())
            gz_path = yesterday / "events.jsonl.gz"
            self.assertTrue(gz_path.exists())
            with gzip.open(gz_path, "rt", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), '{"event":"old"}\n')


if __name__ == "__main__":
    unittest.main()
