# poly-monitor

Read-only Polymarket BTC/ETH 5-minute wallet observer.

The first version monitors public BTC/ETH 5m market trades, records compact
trade-triggered context snapshots, and maintains a strict candidate wallet
leaderboard plus a manual watchlist for wallets that should remain under close
observation. It does not authenticate, submit orders, or read account secrets.

## Quick Start

```bash
python3 -m pip install -r requirements.txt
python3 scripts/run_crypto_wallet_observer.py \
  --symbols BTC,ETH \
  --data-dir data \
  --poll-sec 2
```

Short smoke test:

```bash
python3 scripts/run_crypto_wallet_observer.py \
  --symbols BTC,ETH \
  --seconds 60 \
  --data-dir /tmp/poly-monitor-smoke
```

## Independent Strategy Paper/Backtest

The strategy runtime is separate from the observer. It discovers live crypto 5m
windows, subscribes to CLOB market WS depth, samples Polymarket Chainlink
reference prices, and feeds normalized snapshots into a pluggable strategy.

## Project Structure

- `poly_monitor/observer.py` and `scripts/run_crypto_wallet_observer.py`: read-only wallet and market monitoring.
- `poly_monitor/strategy_live.py`, `poly_monitor/strategy_runner.py`, `poly_monitor/strategy_backtest.py`, and `poly_monitor/strategy_runtime.py`: shared live-paper/backtest runtime, snapshot types, execution adapters, and replay plumbing.
- `poly_monitor/strategies/`: pluggable paper strategies. Strategies consume `StrategySnapshot` plus `StrategyHistory` and return `TradeIntent`; they must not read observer SQLite, dashboard state, wallet exports, or account secrets directly.
- `scripts/run_strategy_paper.py`: live paper runner for any registered strategy.
- `scripts/run_strategy_backtest.py`: deep-export backtest runner for any registered strategy.
- `scripts/run_dashboard.py`: read-only dashboard.

## Paper Strategies

Registered strategies are named by address tag and behavior so multiple paper
experiments do not blur together:

- `x32_pair_cost_inventory_v0`: 0x32 / KimchiJuSaeYo-style maker quote policy.
  It is not a follow-trade strategy. It emits small maker bid quotes from our
  own per-market notional budget, targets balanced Up+Down inventory only when
  maker pair cost is at or below `max_pair_cost` (default `0.995`), and treats
  above-1 pair cost as execution drift to control rather than an intended entry
  condition. It also filters obviously poor maker quote states by spread,
  book age, and bid-side depth. See
  `docs/x32-paper-strategy.md` for the full paper-strategy rulebook and review
  notes.
- `d950_terminal_bias_v0`: renamed D950 terminal/path bias strategy. The legacy
  `d950_path_v0` name remains accepted as an alias.
- `wallet_path_v0` / `wallet_path`: generic pair-cost inventory scaffold.
- `parity_terminal_bias_v0`: parity inventory with terminal bias overlay.

```bash
python3 scripts/run_strategy_paper.py \
  --strategy x32_pair_cost_inventory_v0 \
  --mode paper \
  --symbols BTC \
  --data-dir data \
  --run-id x32-paper-live \
  --target-pair-notional 25 \
  --max-pair-cost 0.99
```

Live paper writes analysis logs under
`data/paper_live/<strategy>/<run-id>/`: `decisions.jsonl`,
`executions.jsonl`, `ws_trades.jsonl`, `market_trades.jsonl`, `state.json`, and
`summary.json`. The JSONL files are the rebuildable truth; `state.json` and
`summary.json` are derived snapshots. For transfer or archival, compress the
core logs first:

```bash
python3 scripts/archive_paper_live_run.py data/paper_live/<strategy>/<run-id>
```

It is read-only paper execution: maker orders are simulated with TTL/pending
state and CLOB WebSocket trade events, not submitted to Polymarket. Data API
market trades are retained as an audit trail and are not used as the live fill
trigger.
The default maker quote TTL is short (`5s`). Each tick reconciles pending
quotes before submitting new ones: stale prices are cancelled as
`quote_moved`, balanced inventory cancels as `balance_reconciled`, and surplus
side quotes cancel as `side_no_longer_needed`.
TTL can be swept as a fixed value with `--maker-order-ttl-sec`, or by phase with
`--maker-early-ttl-sec`, `--maker-mid-ttl-sec`, `--maker-late-ttl-sec`, and
`--maker-final-ttl-sec` for offline replay comparisons.
Maker fills use a conservative queue model by default: `--maker-queue-position-ratio 1.0`
assumes our new quote sits behind the visible size at that bid level, and later
WS trade volume must consume that queue before paper fills start.
The independent live paper runner intentionally runs one symbol/current market
at a time; start separate runs for BTC and ETH when comparing symbols.

Deep wallet export backtests use the same strategy interface:

```bash
python3 scripts/run_strategy_backtest.py \
  --strategy x32_pair_cost_inventory_v0 \
  --zip data/exports/<wallet>/<timestamp>/bundle.zip \
  --out data/backtests/x32_pair_cost_inventory_v0.json
```

`wallet_path_v0` is an independent strategy distilled from the observed wallet's
behavior. It does not read target-wallet activity at runtime; it builds scaled
two-sided Up/Down inventory from normalized market snapshots. The core rule is
window-level pair-cost inventory: keep Up/Down shares close to balanced and only
continue buying when the projected cumulative `avg_up + avg_down` stays under
`--max-pair-cost`. Use `--target-pair-shares` to scale from the learned wallet's
per-window share inventory, or `--target-pair-notional` for a notional-derived
target. Use `--notional`, `--max-unpaired-price`,
`--max-inventory-imbalance-ratio`, and `--min-order-usdc` to choose order cadence
and fill limits. The default `--execution-style maker` evaluates quotes at the
current best bid; `--execution-style taker` is available only as a conservative
shape check against ask-side fills.

`--mode live` is intentionally wired to a rejecting adapter in this project for
now. It records that live execution is not implemented and does not read account
secrets or submit orders.

## Outputs

- `data/raw/YYYY-MM-DD/events.jsonl`: compact raw events, 2-day rolling retention by default.
- `data/state/observer.sqlite`: dedupe state, trades, candidate scores,
  watchlist wallets, watchlist activity events, compact trade contexts, low-rate
  market-state samples, and local PnL ledger for non-watchlist trade rows.
- `data/archive/YYYY-MM-DD/*.jsonl.gz`: cold strategy-research rows exported
  from SQLite after hot retention windows expire.
- `data/reports/latest_candidates.json`: active, dormant, and archive candidate snapshots.

## Dashboard

Run the read-only dashboard separately from the observer process:

```bash
export POLY_MONITOR_DASH_PASSWORD='change-me'
export POLY_MONITOR_DASH_COOKIE_SECRET='change-me-too'
python3 scripts/run_dashboard.py \
  --data-dir data \
  --host 127.0.0.1 \
  --port 8787
```

Open `http://127.0.0.1:8787` locally, or access it from a VPS through an SSH
tunnel. The dashboard reads `observer.sqlite`, `latest_candidates.json`, and
recent compact raw JSONL. It can stop the local observer process and can add or
remove wallets from the local watchlist, but it does not submit orders.

The wallet table starts with a star column:

- `☆`: add the wallet to the watchlist.
- `★`: remove the wallet from the watchlist.

Watchlisted wallets move out of the active/dormant tabs and appear in the
front `重点关注` tab. Removing a wallet from the watchlist restores its normal
candidate placement based on the latest stored score.

Useful authenticated JSON endpoints:

- `GET /api/status`: full dashboard status.
- `GET /api/watchlist`: lightweight watchlist rows.
- `POST /api/watchlist` with `{"wallet":"0x...","action":"add"}` or
  `{"wallet":"0x...","action":"remove"}`. Wallet addresses must be full
  `0x` + 40 hex-character Ethereum addresses.

Optional environment variables:

- `POLY_MONITOR_DASH_USER`: login username, default `admin`.
- `POLY_MONITOR_DASH_COOKIE_SECRET`: required cookie signing secret.

## Candidate Thresholds

Active candidates require high recent BTC/ETH 5m activity and stable positive PnL.
Scoring is hybrid so a fresh deployment is not forced to wait for a full local
observation window:

- 7d trades >= 500.
- 24h BTC/ETH 5m windows >= 100, so previously active wallets that only trade
  around 20 windows today do not remain active candidates.
- 30d trades >= 800.
- 7d and 30d PnL positive.
- top1 profit concentration <= 25%, top3 <= 50%.
- low-odds longshot profit is not rejected if it repeats across multiple markets;
  one-off longshot concentration is still downgraded.
- last activity within 48 hours.

Historically strong but currently inactive wallets are no longer retained as
`dormant_candidate`. Wallets that do not qualify as active candidates, including
wallets below the 550 rank-score floor, are purged from local research tables
unless they are manually watchlisted. The dashboard focuses on a small elite
pool: 15 active candidates by default. Non-core wallet trade rows are cleaned
periodically and capped to the most recent 100 wallets.

The candidate score table is also capped to 100 wallets total by default.
Active candidates keep their configured top slots first; archive
only receives the remaining capacity. New archive rows are persisted only when
they have a minimum activity sample or positive reliable profile/leaderboard PnL,
so route-through wallets do not crowd out stronger candidates.

Watchlisted wallets are treated as manually protected research targets:

- They are not shown in the active or dormant tabs while watchlisted.
- They are protected from inactive-wallet cleanup and candidate-score pruning.
- They remain eligible for trade snapshots and score refreshes even if their
  current stored status is dormant or archived.
- Their metrics cache uses the active refresh TTL, so archived watchlist wallets
  are refreshed more often than ordinary archived wallets.
- The observer also polls each watchlisted wallet's Polymarket Activity feed
  on the configured watchlist activity interval and stores BTC/ETH/SOL/XRP 5m
  `TRADE`, `SPLIT`, `MERGE`, and `REDEEM` rows in `wallet_activity_events`. This
  is separate from the market `/trades` collector and is the data needed to
  reconstruct complete set locking, merges, and final redemptions. After the
  initial lookback, each poll starts from the wallet's latest saved activity
  timestamp minus the configured safety window instead of refetching the full
  lookback.
- Watchlist activity rows are kept as raw behavior detail for later manual
  strategy reconstruction. The dashboard does not convert them into local
  observed PnL, win/loss, or settlement-quality metrics.
- Each watchlist `TRADE` attempts to store one compact `wallet_trade_contexts`
  row with the current window timing, reference price movement, Up/Down top-book
  summary, depth totals, and small-notional fill estimates. Stale books are
  marked instead of blocking activity collection.
- Current market state is sampled at low frequency into `market_state_samples`:
  by default every 5 seconds when changed, every 2 seconds in the final 60
  seconds, and at least every 15 seconds as a heartbeat. Full order-book depth is
  not stored.
- `SPLIT`, `MERGE`, and `REDEEM` assume Polymarket's activity `usdcSize` matches
  `size`. If a future API response breaks that invariant, the observer writes a
  `watchlist_activity_value_warning` raw event and the dashboard surfaces it in
  recent events without stopping collection.
- Activity rows, trade contexts, and market-state samples are retained hot for
  2 days by default. Export deep-collection bundles before that window expires
  when a wallet needs longer offline analysis.
- Strategy archive runs every hour by default, writes JSONL gzip files plus an
  `archive_manifest` row, then deletes exported SQLite rows in batches and asks
  SQLite for incremental vacuum.
- When a watchlisted wallet emits a BTC/ETH/SOL/XRP 5m `TRADE`, `SPLIT`,
  `MERGE`, or `REDEEM`, the observer registers that `market_slug` in
  `watched_market_windows` and keeps collecting market-level trades plus denser
  book/reference samples through the window and the delayed settlement/redeem
  period. This is forward collection only; missing historical book state is not
  reconstructed later.
- The dashboard wallet detail view can export a watchlisted wallet into
  `data/exports/<wallet>/<timestamp>/bundle.zip`. The bundle is organized by
  full window slug and includes wallet activity, wallet trades, market trades,
  compact trade contexts, market-state samples, settlement data, and a
  `manifest.json` coverage report. If the observer did not capture a window
  early enough, or settlement/book samples are missing, the manifest marks the
  window with `insufficient_market_capture=true` instead of pretending it is
  complete.
- New SQLite databases enable incremental auto-vacuum and activity cleanup asks
  SQLite to reclaim a small number of free pages after deletions. Existing
  production databases that were created before this setting still require a
  manual maintenance-window `VACUUM` if physical file size must be rebuilt.

- Historical BTC/ETH 5m activity is used for candidate discovery and ranking.
  Polymarket profile portfolio-PnL curves are used as the primary historical
  PnL because they match the rolling 1D/1W/1M Profit/Loss widget on the public
  profile page.
- Leaderboard profit, BTC/ETH 5m settled-position `cashPnl`, and
  closed-position estimates are kept as reference diagnostics only. The
  positions endpoint can understate wallets by retaining unresolved/redeemable
  losing rows while missing redeemed winners, while closed-position rows can
  overstate winners.
- Dashboard historical PnL columns keep the Polymarket public profile/official
  web-page style PnL. Watchlist rows should be interpreted as high-detail
  behavior capture, not local profitability scoring.
- Very high frequency is capped and penalized in ranking because it is harder to
  follow under realistic network and execution delay.
- A wallet can become an active candidate soon after it is first observed if the
  historical API metrics already satisfy the activity and profitability gates.
  It still needs local follow/replay evidence before any paper or live copy
  experiment.

## Sweden VPS Notes

Sweden is the default VPS when a task says "VPS":

```text
host: 70.34.207.45
user: root
poly-monitor repo: /opt/poly-monitor/repo
poly-monitor data: /opt/poly-monitor/data
poly-monitor logs: /opt/poly-monitor/logs
new-poly legacy repo: /opt/new-poly/repo
new-poly legacy venv: /opt/new-poly/venv
new-poly shared config: /opt/new-poly/shared/polymarket_config.json
```

Access is password-based. Use the ignored local password file with `SSHPASS`;
never echo the password and never print `/opt/new-poly/shared/polymarket_config.json`.

```bash
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'uname -a'
```

Useful read-only checks:

```bash
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'pgrep -af "run_crypto_wallet_observer|run_dashboard|run_poly_source_bot|collect_poly_source_data" || true'

SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'ls -ld /opt/poly-monitor /opt/new-poly 2>/dev/null || true'
```
