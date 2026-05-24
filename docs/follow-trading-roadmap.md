# Follow Trading Roadmap

This project should not jump from wallet scoring directly into live copy
trading. The next phase is to prove that a selected wallet's edge remains
copyable after observation delay, slippage, missed fills, and risk controls.

## Core Constraint

Polymarket does not provide a public wallet-specific WebSocket for other
traders' orders or fills.

The public market WebSocket can stream order books, price changes, and last
trade prices for subscribed markets, but it does not identify every trade by
target wallet. The authenticated user WebSocket is for our own orders and
trades. For third-party target wallets, wallet attribution must be detected
through Data API polling, especially `GET /trades?user=<wallet>`.

The practical architecture is therefore a hybrid:

- Use market WebSocket for the fastest local book, best bid/ask, depth, and
  trade-price context.
- Use REST polling for a small set of target wallets to detect their public
  trades.
- Join detected target trades to the local book snapshot at detection time.

## Phase 1: Target Wallet Watcher

Input: a small curated list of candidate wallets, not the whole leaderboard.

For each target wallet:

- Poll recent trades with a short interval, initially 0.5-2 seconds.
- Deduplicate trades using stable fields such as transaction hash, asset,
  timestamp, side, price, and size.
- Record exchange timestamp, local detection timestamp, and detection latency.
- Keep the polling set small enough that rate limits and network jitter do not
  dominate the signal.

This watcher should emit normalized target-trade events, but it should not place
orders.

## Phase 2: Real-Time Book Join

Maintain market WebSocket subscriptions for active crypto 5-minute markets.

For each target-trade event, attach:

- Current best bid and ask.
- Top-of-book freshness.
- Executable book depth for 5, 25, and 100 USDC targets.
- Whether a simulated follow could fill immediately.
- Estimated follow price, worst price, and slippage versus leader price.

This is where we decide whether a wallet is actually copyable, not merely
profitable.

## Phase 3: Shadow Copy Simulation

Before paper or live trading, replay each target trade under fixed delay
assumptions:

- 1 second.
- 2 seconds.
- 5 seconds.
- 10 seconds.

For every simulated follow:

- Use the book state available at the delayed observation time.
- Skip trades when the book is stale.
- Skip trades when price moved beyond the allowed slippage.
- Cap notional per wallet and per market.
- Record filled/not filled, simulated fill price, slippage, and final settlement
  PnL.

The main metric is:

```text
copyable_edge = simulated_follow_pnl - delay_cost - slippage - missed_fill_cost
```

A wallet with high raw PnL but negative shadow-copy PnL is not useful for
following.

## Phase 4: Paper Follow

Only after shadow-copy results are positive, add a paper follower.

The paper follower should generate the exact order decision we would have made,
but without submitting live orders. Each event must record:

- Follow or skip decision.
- Skip reason.
- Intended side, token, price, and size.
- Book freshness.
- Slippage from leader trade price.
- Per-wallet and per-market exposure after the hypothetical order.
- Final settlement result.

This phase validates operational behavior and risk controls.

## Phase 5: Live Follow

Live follow remains out of scope until explicitly approved.

If later enabled, it must require:

- Explicit live mode flag.
- Explicit risk acknowledgement flag.
- Small default notional.
- Per-day loss cap.
- Per-wallet max exposure.
- Per-market max exposure.
- Max follow price.
- Max slippage from leader price.
- Minimum book freshness.
- Minimum fillable depth.
- No chasing after large immediate price jumps.
- Dashboard emergency stop.
- Full audit log for every submitted, skipped, partially filled, cancelled, or
  failed order.

## Wallets Most Worth Following

The best follow targets are not necessarily the highest-PnL wallets.

Prefer wallets that:

- Trade at medium or low frequency.
- Have strong win rate and positive PnL across many completed markets.
- Are not dependent on one or two outsized wins.
- Enter when the book still has copyable depth.
- Remain profitable after 2-5 seconds of simulated delay.
- Have high 5/25/100 USDC fill-ok rates.

Downgrade wallets that:

- Trade hundreds or thousands of times per day.
- Require sub-second reaction.
- Mostly act as market makers.
- Generate edge from fills that cannot be copied by taking after them.
- Show high raw PnL but poor shadow-copy PnL.

High-frequency wallets may still be useful as research signals, but they should
not be treated as primary live follow candidates until shadow-copy results prove
otherwise.

