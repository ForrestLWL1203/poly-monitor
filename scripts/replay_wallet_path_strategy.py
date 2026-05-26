#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.path_strategy import PathStrategyConfig, SettlementPaperExecutionAdapter, load_deep_export_for_path_strategy, replay_path_strategy


def _parse_checkpoints(value: str) -> tuple[int, ...]:
    checkpoints = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not checkpoints:
        raise argparse.ArgumentTypeError("at least one checkpoint is required")
    return checkpoints


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay the wallet path-shadow strategy against a deep export zip.")
    parser.add_argument("--zip", required=True, type=Path, help="Path to bundle.complete-windows.zip")
    parser.add_argument("--wallet", required=True, help="Target wallet address")
    parser.add_argument("--out", type=Path, help="Output JSON path")
    parser.add_argument("--notional", type=float, default=25.0, help="Paper notional per intent")
    parser.add_argument("--bias-threshold", type=float, default=25.0, help="Minimum wallet net Up-Down flow to trigger")
    parser.add_argument("--max-price", type=float, default=0.95, help="Maximum acceptable simulated buy price")
    parser.add_argument("--checkpoints", type=_parse_checkpoints, default=(120, 180, 240), help="Comma-separated elapsed-second checkpoints")
    args = parser.parse_args()

    loaded = load_deep_export_for_path_strategy(args.zip)
    adapter = SettlementPaperExecutionAdapter(loaded.winning_sides)
    result = replay_path_strategy(
        loaded.activity_rows,
        loaded.market_state_samples,
        PathStrategyConfig(
            wallet=args.wallet,
            checkpoints=args.checkpoints,
            notional_usdc=args.notional,
            first_bias_min_usdc=args.bias_threshold,
            max_price=args.max_price,
        ),
        adapter=adapter,
    )
    payload = result.to_dict()
    settled = [item for item in payload["executions"] if item["status"] == "paper_settled"]
    total_pnl = sum(float(item["detail"].get("realized_pnl") or 0.0) for item in settled)
    wins = sum(1 for item in settled if float(item["detail"].get("realized_pnl") or 0.0) > 0)
    losses = sum(1 for item in settled if float(item["detail"].get("realized_pnl") or 0.0) < 0)
    payload["summary"] = {
        "intents": len(payload["intents"]),
        "paper_settled": len(settled),
        "paper_total_pnl": round(total_pnl, 6),
        "paper_wins": wins,
        "paper_losses": losses,
        "paper_win_rate": round(wins / (wins + losses), 6) if wins + losses else None,
    }
    payload["config"] = {
        "wallet": args.wallet.lower(),
        "notional_usdc": args.notional,
        "bias_threshold": args.bias_threshold,
        "max_price": args.max_price,
        "checkpoints": list(args.checkpoints),
        "source_zip": str(args.zip),
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(str(args.out))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
