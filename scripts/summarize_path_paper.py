#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def summarize(path: Path) -> dict:
    rows = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    executions = [row for row in rows if row.get("record_type") == "execution"]
    settlements = [row for row in rows if row.get("record_type") == "settlement" or row.get("execution", {}).get("status") == "paper_settled"]
    pnls = [float(row.get("execution", {}).get("detail", {}).get("realized_pnl") or 0.0) for row in settlements]
    wins = sum(1 for pnl in pnls if pnl > 0)
    losses = sum(1 for pnl in pnls if pnl < 0)
    return {
        "path": str(path),
        "execution_records": len(executions),
        "settled_records": len(settlements),
        "open_records": max(0, len(executions) - len(settlements)),
        "total_pnl": round(sum(pnls), 6),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / (wins + losses), 6) if wins + losses else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize wallet path paper executions JSONL.")
    parser.add_argument("--file", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.file), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
