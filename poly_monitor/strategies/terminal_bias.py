from __future__ import annotations

from poly_monitor.path_strategy import D950MarketPathStrategy as _LegacyD950MarketPathStrategy


class D950MarketPathStrategy(_LegacyD950MarketPathStrategy):
    strategy_name = "d950_terminal_bias_v0"
    one_trade_per_market = True
