# AGENTS.md - Polymarket Crypto 5m Trader Monitor

## Project Intent

This is a fresh workspace for researching a new Polymarket crypto 5-minute
strategy based on active trader monitoring and copyability analysis.

The working thesis is:

- Directly predicting BTC/crypto direction from our own signals has been too
  unstable.
- Some highly active Polymarket crypto 5-minute traders may have repeatable
  edge from speed, market microstructure, execution quality, or better short
  horizon judgment.
- The first goal is not to blindly copy high-PnL wallets. The first goal is to
  identify wallets whose trades remain profitable after realistic detection
  delay, slippage, failed fills, and risk controls.

This project should start as research, monitoring, and replay. Do not begin
with live auto-copy trading.

## Clean-Room Strategy Rule

This workspace may reuse infrastructure knowledge from:

- `/Users/forrestliao/workspace/new-poly`
- `/Users/forrestliao/workspace/poly-bot`

Allowed to reuse:

- Polymarket Gamma API market discovery.
- Polymarket Data API activity, trades, position, and leaderboard access.
- Polymarket CLOB API authentication and order-book mechanics.
- CLOB WebSocket market feed parsing and local book-cache handling.
- Safe retry, reconnect, freshness, tick-size, and balance-query practices.

Do not reuse unless explicitly requested:

- Old BTC direction-prediction strategy logic.
- Old entry/exit windows, thresholds, confidence formulas, or stop-loss rules.
- Old backtest claims, parameter grids, strategy names, or VPS presets.
- The `new-poly` active `poly_source.py` strategy as trading logic.

This project starts from a new strategic premise: trader-following signal
research, not self-generated direction forecasting.

## Current Research Direction

Monitor Polymarket crypto 5-minute markets in real time, especially active
front-row traders in BTC/ETH/SOL/XRP short-window markets.

For each active trader address, evaluate whether the address appears to have
repeatable, copyable edge:

- High enough sample count.
- Positive realized PnL.
- Stable profitability across time, not one-off lottery wins.
- Reasonable win rate and expected value.
- Low concentration of PnL in one or two lucky markets.
- Entry prices that are still copyable after detection delay.
- Trade behavior that is not just market making, hedging, wash-like activity,
  or inventory movement that cannot be followed.

The core metric is not raw wallet PnL.

The core metric is:

```text
copyable_edge = simulated_follow_pnl - delay_cost - slippage - missed_fill_cost
```

Only wallets with positive copyable edge under realistic assumptions should be
considered candidates for later paper trading or live copy experiments.

## Initial Product Shape

Build this in phases.

### Phase 1: Data Collection

Collect market and trader activity for crypto 5-minute windows.

Minimum data to capture:

- Market slug, condition ID, token IDs, start/end time, resolution result.
- Public trades for the market when available from Polymarket Data API.
- Trader/proxy wallet address.
- Side/outcome, price, size, timestamp, trade type, and transaction/order IDs
  when available.
- Local observation time, so detection latency can be measured.
- Best bid/ask and executable book depth near observed trade time.
- Final settlement outcome.

No live orders in this phase.

### Phase 2: Offline Wallet Scoring

Score addresses using completed markets only.

Preferred metrics:

- Number of crypto 5-minute trades.
- Number of distinct markets traded.
- Realized PnL.
- Win rate by trade and by market.
- Average EV per dollar risked.
- Median PnL, not only mean PnL.
- Maximum drawdown.
- PnL concentration ratio, such as top 1 and top 3 markets as percentage of
  total PnL.
- Odds bucket distribution, to separate steady edge from rare long-shot wins.
- Time-in-window distribution, such as early, middle, late, and terminal trades.
- Side switching, repeated averaging down, and chase behavior.

Reject or downgrade addresses whose PnL is mostly explained by:

- One or two outsized bets.
- Extreme odds lottery tickets.
- Illiquid marks or stale unrealized PnL.
- Obvious long-only market direction exposure rather than repeatable short-window
  timing.
- Trades that cannot be copied at comparable prices.

### Phase 3: Shadow Copy Simulation

For each candidate address, simulate following each observed trade after a fixed
delay.

Required delay scenarios:

- 1 second.
- 2 seconds.
- 5 seconds.
- 10 seconds.

For each simulated follow, use the book state available at the delayed
observation time. Record:

- Whether a fill would have been possible.
- Expected fill price and size.
- Slippage versus leader trade price.
- Whether price moved too far and should have been skipped.
- Settlement PnL.
- PnL after realistic fees/cost assumptions if applicable.

This phase determines copyability. A wallet with excellent raw PnL but negative
shadow-copy PnL is not useful.

### Phase 4: Paper Follow

Only after shadow-copy results are positive, run a paper follower.

Paper follower rules should include:

- Per-wallet max exposure.
- Per-market max exposure.
- Max follow price.
- Max slippage from leader price.
- Minimum book freshness.
- Minimum fillable depth.
- No chasing after large immediate price jumps.
- Cooldown after losses or abnormal fills.
- Full audit log for every followed or skipped signal.

### Phase 5: Live Follow

Live follow is out of scope until explicitly approved.

If later enabled, it must require:

- Explicit `--mode live` style flag.
- Explicit risk acknowledgement flag.
- Small notional defaults.
- Per-day loss cap.
- Per-address disable switch.
- Emergency global kill switch.
- Post-trade reconciliation.

## Important Research Questions

Answer these before live execution:

- Can we reliably observe relevant public trades fast enough?
- Does the observed address represent the real trader, or just one wallet in a
  multi-wallet strategy?
- Are the best addresses takers with copyable direction edge, or makers whose
  edge cannot be copied by taking after them?
- Does a wallet's edge persist out-of-sample?
- Does the edge survive after realistic delay and slippage?
- Does the edge disappear once we avoid chasing after price moves?
- Are profits concentrated in a few extreme markets?
- Are addresses profitable across multiple crypto symbols or only one regime?
- Do candidate addresses degrade after being identified?

## Data Source Notes

Likely useful Polymarket systems:

| API | Base URL | Use |
|---|---|---|
| Gamma | `https://gamma-api.polymarket.com` | Market/event metadata and crypto 5m slug discovery |
| Data | `https://data-api.polymarket.com` | Trades, activity, positions, holders, leaderboards |
| CLOB | `https://clob.polymarket.com` | Order books, trading, balances, tick sizes |
| CLOB WS | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Real-time book and trade-related market events |

Prefer public/non-authenticated data for research collection whenever possible.
Do not read or print private keys or Polymarket account config unless the user
explicitly asks for authenticated trading work.

## Security Rules

Do not commit, print, or log:

- Private keys.
- API keys.
- Polymarket account config.
- Proxy wallet secrets.
- VPS passwords.
- Full signed orders.

If secrets are needed later, keep them outside tracked files and document only
the path and required permissions, not the contents.

## Engineering Rules

- Keep this workspace independent from `new-poly` strategy code.
- Prefer small research scripts with clear inputs and outputs.
- Store raw observations separately from derived scores.
- Make replay deterministic: a scoring result should be reproducible from saved
  raw data.
- Keep collector code free of strategy decisions where possible.
- Keep wallet scoring separate from shadow-copy simulation.
- Use JSONL for append-only tick/trade observation logs.
- Use CSV or Parquet only for derived analysis if useful.
- Include enough timestamps to distinguish exchange event time, API/server time,
  and local observation time.
- Make all assumptions explicit in output files and reports.

## First Concrete Milestone

Create a read-only crypto 5-minute market and trader activity collector that can:

1. Discover current/recent crypto 5-minute markets.
2. Fetch or observe public trades for those markets.
3. Persist normalized trade rows with trader address, side, price, size, market,
   and timestamps.
4. Join completed markets to settlement outcomes.
5. Produce a first offline wallet leaderboard with sample count, realized PnL,
   win rate, PnL concentration, and basic odds buckets.

The first milestone should not submit orders and should not require private
account credentials.
