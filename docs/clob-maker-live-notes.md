# CLOB Maker Live Notes

Live execution is not implemented in this project yet. These notes capture CLOB
maker-order details that matter if live maker paper/live support is added later.

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
