from __future__ import annotations

from poly_monitor.path_strategy import PathStrategyConfig, WalletPathStrategy
from poly_monitor.strategy_runtime import StrategyHistory, StrategySnapshot, TradeIntent


class X32PairCostInventoryStrategy(WalletPathStrategy):
    strategy_name = "x32_pair_cost_inventory_v0"
    one_trade_per_market = False
    terminal_stop_sec = 240

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        if snapshot.elapsed_sec >= self.terminal_stop_sec:
            return None
        intent = super().evaluate(snapshot, history)
        if intent is None:
            return None
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=intent.wallet,
            market_slug=intent.market_slug,
            sampled_ts=intent.sampled_ts,
            checkpoint_sec=intent.checkpoint_sec,
            intent=intent.intent,
            outcome=intent.outcome,
            notional_usdc=intent.notional_usdc,
            max_price=intent.max_price,
            expected_price=intent.expected_price,
            symbol=intent.symbol,
            reason="x32_pair_cost_inventory",
            features={
                **intent.features,
                "strategy_profile": "x32_pair_cost_inventory",
                "terminal_stop_sec": self.terminal_stop_sec,
            },
        )


def x32_default_config(wallet: str, **overrides) -> PathStrategyConfig:
    defaults = {
        "wallet": wallet,
        "checkpoints": (1,),
        "notional_usdc": 5.0,
        "max_price": 0.95,
        "target_pair_shares_per_side": 100.0,
        "max_pair_cost": 0.995,
        "max_unpaired_price": 0.60,
        "early_inventory_imbalance_ratio": 0.30,
        "mid_inventory_imbalance_ratio": 0.12,
        "late_inventory_imbalance_ratio": 0.06,
        "final_inventory_imbalance_ratio": 0.03,
        "rebalance_start_sec": 180,
        "min_order_usdc": 1.0,
        "execution_style": "maker",
        "one_trade_per_market": False,
    }
    defaults.update({key: value for key, value in overrides.items() if value is not None})
    return PathStrategyConfig(**defaults)
