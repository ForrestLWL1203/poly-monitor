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

## Outputs

- `data/raw/YYYY-MM-DD/events.jsonl`: compact raw events, 3-day rolling retention by default.
- `data/state/observer.sqlite`: dedupe state, trades, candidate scores,
  watchlist wallets, watchlist activity events, and local PnL ledger.
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

Historically strong but inactive wallets become `dormant_candidate`; long
inactive wallets are not retained by default. The dashboard focuses on a small
elite pool: 15 active candidates and 10 dormant candidates by default. Non-core
wallet trade rows are cleaned periodically and capped to the most recent 100
wallets.

Watchlisted wallets are treated as manually protected research targets:

- They are not shown in the active or dormant tabs while watchlisted.
- They are protected from inactive-wallet cleanup and candidate-score pruning.
- They remain eligible for trade snapshots and score refreshes even if their
  current stored status is dormant or archived.
- Their metrics cache uses the active refresh TTL, so archived watchlist wallets
  are refreshed more often than ordinary archived wallets.
- The observer also polls each watchlisted wallet's Polymarket Activity feed
  every 30 seconds by default and stores BTC/ETH/SOL/XRP 5m `TRADE`, `SPLIT`,
  `MERGE`, and `REDEEM` rows in `wallet_activity_events`. This is separate from
  the market `/trades` collector and is the data needed to reconstruct complete
  set locking, merges, and final redemptions. After the initial lookback, each
  poll starts from the wallet's latest saved activity timestamp minus a 60
  second safety window instead of refetching the full lookback.
- Watchlisted wallets use a more precise local activity ledger when those rows
  are available. `watchlist_market_pnl` applies `TRADE`, `SPLIT`, `MERGE`, and
  `REDEEM` cashflows per wallet/market, then the dashboard prefers that result
  for watchlist local observed PnL. Markets observed before the wallet was added
  to the watchlist are still included from the legacy local ledger unless the
  same wallet/market already has a watchlist activity-ledger row.
- `SPLIT`, `MERGE`, and `REDEEM` assume Polymarket's activity `usdcSize` matches
  `size`. If a future API response breaks that invariant, the observer writes a
  `watchlist_activity_value_warning` raw event and the dashboard surfaces it in
  recent events without stopping collection.
- When a wallet/market has watchlist `SPLIT`, `MERGE`, or `REDEEM` rows, the
  legacy `wallet_market_pnl` row is removed and future settlement recomputes skip
  it. The activity ledger is the source of truth for that wallet/market because
  the legacy trade table cannot represent those cashflows.
- Activity rows are retained for 30 days while the wallet remains watchlisted
  and 7 days after it is no longer watchlisted; derived `watchlist_market_pnl`
  rows are removed when their underlying activity rows age out.
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
- The observer also records a separate local-observed Ledger PnL from trades and
  settlements collected after this script started. This local PnL is cumulative
  across the saved local ledger, not just the current active-candidate window.
- Dashboard historical PnL columns are ranking inputs; the local-observed PnL
  column is the stricter live-monitoring evidence that should drive later
  copyability decisions.
- Dashboard win/loss counts use locally observed settled markets. Polymarket
  closed-position rows are not used for win/loss because that endpoint can be
  biased toward winning outcome positions and does not match profile daily PnL.
- Once a wallet has enough local settled markets, local observed PnL and
  win/loss quality gate active status. Locally losing wallets can drop out of
  active even if they trade frequently, and they can return after their local
  ledger turns positive again.
- Very high frequency is capped and penalized in ranking because it is harder to
  follow under realistic network and execution delay. Moderate-frequency
  wallets with stable local profit can qualify for active with a lower 24h
  market-count threshold.
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
