#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.deep_wallet_analysis import analyze_deep_wallet_export, render_markdown_report


def default_out_path(zip_path: Path) -> Path:
    return zip_path.with_suffix("").with_suffix(".analysis.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze a complete-window deep wallet export zip.")
    parser.add_argument("--zip", required=True, type=Path, help="Path to bundle.complete-windows.zip")
    parser.add_argument("--out", type=Path, help="JSON report path. Defaults next to the zip.")
    parser.add_argument("--markdown", action="store_true", help="Also write a Markdown report next to the JSON report.")
    args = parser.parse_args()

    report = analyze_deep_wallet_export(args.zip)
    out = args.out or default_out_path(args.zip)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        out.with_suffix(".md").write_text(render_markdown_report(report), encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
