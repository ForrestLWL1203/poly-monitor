#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.wallet_research import build_wallet_research_report, render_markdown_report


def default_report_path(data_dir: Path, wallet: str) -> Path:
    return data_dir / "reports" / f"wallet_research_{wallet.lower()}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a read-only behavior distillation report for one wallet.")
    parser.add_argument("--wallet", required=True, help="Proxy wallet address to research.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--api-backfill", choices=("auto", "never", "always"), default="auto")
    parser.add_argument("--min-local-trades", type=int, default=100)
    parser.add_argument("--min-local-markets", type=int, default=20)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--markdown", action="store_true", help="Also write a Markdown summary next to the JSON report.")
    args = parser.parse_args()

    report = build_wallet_research_report(
        args.wallet,
        data_dir=args.data_dir,
        days=args.days,
        api_backfill=args.api_backfill,
        min_local_trades=args.min_local_trades,
        min_local_markets=args.min_local_markets,
    )
    out = args.out or default_report_path(args.data_dir, args.wallet)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        out.with_suffix(".md").write_text(render_markdown_report(report), encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
