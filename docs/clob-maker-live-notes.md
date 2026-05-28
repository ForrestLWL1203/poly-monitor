# CLOB Maker Live Notes

Live execution is not implemented in this project yet. These notes capture CLOB
maker-order details that matter if live maker paper/live support is added later.

## User WebSocket Is Required For Live State

For live execution, do not use REST balance or position polling as the primary
fill signal. It is too slow for a 5-minute two-leg inventory strategy.

The live runner should start an authenticated CLOB user WebSocket at process
startup, at the same time as the market WebSocket. On every window rollover it
should subscribe to the current market `condition_id` and keep the previous
window subscribed briefly enough to drain late status messages.

The local order/fill ledger should be driven by user-channel events:

- `trade` events with `MATCHED` update optimistic filled inventory immediately.
- `order` `UPDATE` events and `size_matched` reconcile partial fills.
- `MINED` / `CONFIRMED` update finality state.
- `FAILED` rolls back or flags optimistic fills that did not settle.
- REST `getOrder`, `getOpenOrders`, `getTrades`, balances, and positions are
  fallback reconciliation only, not the main strategy control loop.

This applies to both maker and taker orders. Public market WS can show that the
market traded, but only user WS can prove how much of our own order filled.

## Fixed-Share Maker Buys

For Polymarket CLOB limit orders, fixed share count is controlled by the order
`size` field, not by a USDC notional.

Use limit order flow:

```text
createOrder / createAndPostOrder
side = BUY
price = target bid price
size = target shares, such as 5 or 10
orderType = GTC or GTD
postOnly = true
```

This means:

- BUY `size` is the maximum number of outcome shares to buy.
- `price` is the limit price.
- GTC/GTD orders rest on the book.
- `postOnly=true` rejects the order if it would cross and execute as a taker.
- FOK/FAK market-order flow is not suitable for thin pair-cost maker strategies.

## Target Inventory Accounting

The CLOB will not guarantee that a maker order fills completely. A fixed-share
inventory strategy must track order state:

```text
target_shares
- matched_shares
- open_unmatched_shares
= next_order_size
```

Use order/trade state such as:

- `original_size`
- `size_matched`
- `status`
- `price`
- `associate_trades`

Cancel or replace stale open orders before posting more size for the same target
inventory bucket.

## 0x32-Style Strategy Implication

For thin two-sided pair strategies, the executable edge should be measured in
maker terms:

```text
up_bid + down_bid < 1
```

not taker terms:

```text
up_ask + down_ask < 1
```

The live design should therefore prefer post-only maker GTC/GTD orders, fixed
share targets, short order TTL/replacement, and fill-adjusted pair-cost tracking.
