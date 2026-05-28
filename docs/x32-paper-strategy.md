# 0x32 Paper Strategy Rulebook

This document describes `x32_pair_cost_inventory_v0`, the current paper strategy
inspired by 0x32 / KimchiJuSaeYo behavior.

The goal is not to follow 0x32 trades and not to copy 0x32 position size. The
goal is to reproduce the core maker idea using our own capital budget:

```text
Build balanced Up + Down inventory only when the quoted pair cost is below 1.
```

## Strategy Identity

- Strategy name: `x32_pair_cost_inventory_v0`
- Runtime mode: paper/backtest only in this project.
- Execution style: maker quote simulation.
- Target markets: crypto 5-minute Up/Down windows.
- Live paper convention: one run tracks one symbol/current market. Run BTC and
  ETH as separate processes so `summary.json`, pending state, and replay logs
  stay symbol-local and easy to audit.
- Source file: `poly_monitor/strategies/pair_cost_inventory.py`

This strategy consumes only normalized `StrategySnapshot` and `StrategyHistory`
objects. It must not read target-wallet activity, dashboard state, SQLite wallet
tables, wallet exports, account config, private keys, or live account balances.

## What We Learned From 0x32

The observed 0x32 behavior appears maker-like:

- It buys both Up and Down outcomes.
- It tends to build paired inventory rather than directional one-shot exposure.
- It uses many small fills, commonly around 5 or 10 shares.
- Its useful edge appears to come from quoting/market-making around the
  combined Up + Down cost, not from simply predicting direction or copying
  another trader.

The observed 0x32 wallet sometimes ends with `avg_up + avg_down > 1`. We do not
treat that as the intended alpha. In this strategy, above-1 pair cost is an
execution drift or market-complexity failure mode to control, not a desired
entry rule.

## Capital And Sizing

Position size is based on our configured paper budget, not on 0x32's wallet
size.

Defaults:

```text
target_pair_notional_usdc = 55.0
notional_usdc = 5.0
min_order_usdc = 1.0
```

`target_pair_notional_usdc` is the intended per-market paired-inventory budget.
The strategy converts it into a per-side share target from the current maker
pair cost:

```text
target_pair_shares_per_side = target_pair_notional_usdc / maker_pair_cost
```

Example:

```text
maker_pair_cost = Up bid + Down bid = 0.99
target_pair_notional_usdc = 55
target_pair_shares_per_side ~= 55.56
```

This is a scaling rule for our account. It is not trying to match 0x32's
absolute share count.

## Entry Condition

The active entry condition is:

```text
maker_pair_cost = best_up_bid + best_down_bid
maker_pair_cost <= max_pair_cost
```

Default:

```text
max_pair_cost = 0.995
```

If `maker_pair_cost` is missing, zero, or above `max_pair_cost`, the strategy
does not open or add inventory for that snapshot.

This deliberately rejects `Up + Down >= 1` as an active entry. A final realized
pair average above 1 may still appear in replay due to fills, stale quotes,
partial execution, queue assumptions, or later rebalancing, but the strategy is
not supposed to seek those entries.

## Quote Price

Default execution style is maker. The strategy quotes at the best bid of the
selected outcome:

```text
quote_price = outcome best bid
```

When one side has less filled inventory than the other, the strategy prioritizes
the lower-inventory side. It still uses a maker bid quote; it does not
intentionally cross to the ask just to force a rebalance.

Feature flags in emitted intents:

- `book_fill.source = maker_quote_at_best_bid`
- `book_fill.source = maker_rebalance_quote`

Both are maker-style quotes. The second label means the quote was selected to
reduce inventory imbalance.

## Inventory Accounting

For `x32_pair_cost_inventory_v0`, there are two inventory views:

- Filled inventory is based on filled/emitted paper intents only. This is the
  source of truth for average price, pair cost, settlement, and PnL.
- Working inventory is filled inventory plus pending maker quotes. This is used
  only for sizing, deficit selection, and imbalance checks.

Reason:

- A resting maker quote may never fill, so it cannot be counted as realized
  inventory or PnL.
- Ignoring pending quotes for sizing causes the strategy to keep submitting the
  same side every second until the open-order limit is hit.
- Working inventory lets the strategy reserve capacity for orders already in the
  book while still reporting real inventory from fills only.

The emitted diagnostics therefore distinguish `current_*_shares` (filled only)
from `working_*_shares` (filled plus pending).

## Candidate Selection

For each snapshot, the strategy evaluates Up and Down as candidates.

It skips an outcome if:

- the book is stale;
- either side's spread is wider than `max_quote_spread`, when configured;
- either side's book age is above `max_quote_book_age_ms`, when configured;
- either side's bid-side depth is below `min_quote_bid_depth_usdc`, when
  configured;
- elapsed time is at or past `terminal_stop_sec`;
- ask data is invalid;
- ask is above `max_price`;
- maker pair cost is above `max_pair_cost`;
- quote price is invalid or above `max_price`;
- the order would be below `min_order_usdc`;
- projected pair average would exceed `max_pair_cost` after both sides exist;
- the candidate would worsen an already-too-large inventory imbalance.

Among valid candidates, it prefers:

1. the candidate that improves inventory balance most;
2. the cheaper quote;
3. the larger remaining deficit to the budget-derived target.

## Order Clip

The order clip is small and share-based to resemble the observed maker cadence,
while still being constrained by our budget-derived target:

```text
if quote_price > 0.50 or remaining deficit < 10 shares:
    clip_shares = 5
else:
    clip_shares = 10

order_shares = min(clip_shares, remaining deficit)
order_notional = order_shares * quote_price
```

This does not mean the strategy has a fixed 5 or 10 USDC order size. It uses 5
or 10 shares, so the USDC amount varies with quote price.

## Time And Stop Rules

Defaults:

```text
checkpoints = (1,)
terminal_stop_sec = 300
```

The strategy can evaluate from the beginning of the 5-minute window. It does not
open new inventory once the window reaches the terminal stop.

The current implementation no longer scales target size upward by elapsed time.
Time still matters indirectly through available quotes, fills, pending-order
expiry in replay, and the final stop.

## Imbalance Controls

The strategy attempts to keep Up and Down inventory balanced, but it allows
temporary imbalance because maker fills are naturally uneven.

Defaults:

```text
early_inventory_imbalance_ratio = 1.00
mid_inventory_imbalance_ratio = 0.60
late_inventory_imbalance_ratio = 0.30
final_inventory_imbalance_ratio = 0.12
rebalance_start_sec = 240
```

With these defaults, early applies before 60 seconds, mid from 60 to 179
seconds, late from 180 to 239 seconds, and final from 240 seconds onward.

The emitted features include:

- `current_up_shares`
- `current_down_shares`
- `working_up_shares`
- `working_down_shares`
- `current_imbalance_ratio`
- `projected_imbalance_ratio`
- `deficit_side`
- `projected_pair_avg`

Reviewers should read these as risk-control diagnostics, not as a target to
copy from 0x32.

`projected_pair_avg` is `None` while the projected inventory is still one-sided.
It becomes numeric only after both Up and Down have filled inventory. A `None`
value is not a failure of the pair-cost check; it means the pair is not complete
yet.

## Important Defaults

Current default config for `x32_pair_cost_inventory_v0`:

```text
checkpoints = (1,)
notional_usdc = 5.0
target_pair_notional_usdc = 55.0
target_pair_shares_per_side = None
max_pair_cost = 0.995
max_unpaired_price = 0.70
max_price = 0.95
min_order_usdc = 1.0
max_quote_spread = 0.02
max_quote_book_age_ms = 50.0
min_quote_bid_depth_usdc = 20.0
execution_style = maker
one_trade_per_market = False
rebalance_start_sec = 240
terminal_stop_sec = 300
```

`target_pair_shares_per_side = None` is intentional. The x32 paper strategy is
not configured from a fixed wallet-share target.

## Backtest Interpretation

Deep-export backtests are useful for behavior shape checks, but they are not a
complete maker PnL model.

Known limitations:

- Queue position is approximated.
- Maker order cancellation/reposting is simplified.
- Live paper maker fills are triggered by CLOB WebSocket trade events. Data API
  market trades are retained only as delayed audit evidence, because they can
  arrive after short TTL maker quotes have already expired.
- Partial fills use configurable replay assumptions.
- The replay may show final pair average above 1 even when active quote entries
  all had `maker_pair_cost <= max_pair_cost`.
- Current PnL should not be compared directly with Polymarket official wallet
  PnL.

## Execution Diagnostics

Use `scripts/analyze_deep_wallet_export.py` to compare observed wallet trade
prices with captured books:

```bash
python3 scripts/analyze_deep_wallet_export.py \
  --zip data/exports/0x32e4abe3e97aaf3c95c315ba3ffbe7b2a313beed/20260528-015727-complete-only/bundle.zip \
  --out data/exports/0x32e4abe3e97aaf3c95c315ba3ffbe7b2a313beed/20260528-015727-complete-only/x32.deep-analysis.json \
  --markdown
```

The `execution_analysis` section classifies each trade against:

- the captured trade context snapshot;
- the exact same-second deep sample;
- the nearest deep sample within 2 seconds;
- the last sample before the trade within 2 seconds;
- the first sample after the trade within 2 seconds.

Classifications are `bid_match`, `ask_match`, `inside_spread`, `below_bid`,
`above_ask`, `both_tick`, `missing_book`, `outside_book`, or `no_sample`.

Current 0x32 complete-window diagnostics show that simple ask-taking is not the
dominant explanation: nearest-sample ask match is about 15%, and bid match is
about 11.5%. This supports treating 0x32 as maker-like, but it does not prove
exact queue position or maker/taker status because the available deep samples
are second-level snapshots rather than pre-trade CLOB frames.

The path analysis now also records final per-window `up_avg_price`,
`down_avg_price`, `paired_shares`, and `final_pair_cost`. These are diagnostics
for execution drift and inventory risk, not active targets.

## Current Calibration

The latest calibration keeps the strategy maker-like but rejects obviously poor
quote states:

```text
max_quote_spread = 0.02
max_quote_book_age_ms = 50.0
min_quote_bid_depth_usdc = 20.0
```

These values are intentionally conservative. In the 0x32 deep sample, spread was
usually one tick and did not strongly separate profitable from losing windows.
Book freshness and bid-side depth were more useful as hygiene filters: they
remove stale or thin books without trying to predict direction.

The current best research replay is not the default size. On the 0x32 complete
bundle, `target_pair_notional_usdc=25` and `max_pair_cost=0.99` produced much
cleaner execution drift than the larger default budget:

```text
paper_total_pnl = -127.638699
paper_win_rate = 0.418158
filled pair markets = 198
final pair cost p50 / p75 / p90 = 0.955549 / 0.980000 / 0.996290
pair markets < 1 = 178
pair markets >= 1 = 20
```

This is still not evidence of live edge. It is evidence that smaller budget and
stricter pair-cost entry better preserve the intended `Up + Down cost < 1`
shape under the current maker replay model.

Useful review checks:

```text
active quote maker_pair_cost should be <= max_pair_cost
active quotes above pair cost 1 should be zero
target_pair_notional_usdc should drive scale
target_pair_shares_per_side should be derived, not copied from 0x32
pending quotes should not count as filled inventory or PnL
pending quotes should count as working inventory for sizing
projected_pair_avg may be None while inventory is one-sided
final pair_avg above 1 is a drift/risk metric, not intended alpha
```

## Current Review Questions

- Is `max_pair_cost = 0.995` too strict or should it leave more margin under 1?
- Should the strategy use a per-symbol budget rather than one global
  `target_pair_notional_usdc`?
- Should quote cadence use a dollar clip instead of a 5/10 share clip?
- Should the replay maker-fill model be calibrated before judging paper PnL?
- Should the strategy stop earlier than 300 seconds to avoid late fill drift?
