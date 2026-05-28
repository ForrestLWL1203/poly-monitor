#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path


CORE_LOGS = ("decisions.jsonl", "executions.jsonl", "market_trades.jsonl", "ws_trades.jsonl")


def gzip_file(path: Path, *, remove_raw: bool = False) -> dict:
    gz_path = path.with_suffix(path.suffix + ".gz")
    tmp_path = path.with_suffix(path.suffix + ".gz.tmp")
    with path.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    tmp_path.replace(gz_path)
    raw_bytes = path.stat().st_size
    gz_bytes = gz_path.stat().st_size
    sha256 = hashlib.sha256()
    with gz_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256.update(chunk)
    if remove_raw:
        path.unlink()
    return {
        "path": str(path),
        "gzip_path": str(gz_path),
        "raw_bytes": raw_bytes,
        "gzip_bytes": gz_bytes,
        "gzip_sha256": sha256.hexdigest(),
        "removed_raw": remove_raw,
    }


def archive_run(run_dir: Path, *, remove_raw: bool = False) -> dict:
    run_dir = run_dir.resolve()
    results = []
    for name in CORE_LOGS:
        path = run_dir / name
        if path.exists():
            results.append(gzip_file(path, remove_raw=remove_raw))
    manifest = {
        "run_dir": str(run_dir),
        "core_logs": results,
        "summary_path": str(run_dir / "summary.json") if (run_dir / "summary.json").exists() else None,
        "stderr_path": str(run_dir / "stderr.log") if (run_dir / "stderr.log").exists() else None,
    }
    (run_dir / "archive_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Compress live paper JSONL logs for transfer or archival.")
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--remove-raw", action="store_true", help="Delete raw JSONL files after writing .gz copies.")
    args = parser.parse_args()
    print(json.dumps(archive_run(args.run_dir, remove_raw=args.remove_raw), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
