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
- `data/state/observer.sqlite`: dedupe state, trades, candidate scores, watchlist wallets, and local PnL ledger.
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

- Historical BTC/ETH 5m activity and BTC/ETH 5m closed-position estimates are
  used for candidate discovery and ranking.
- Whole-profile leaderboard profit is kept as reference context only; it is not
  used as scoring PnL because it may come from unrelated markets.
- The observer also records a separate local-observed Ledger PnL from trades and
  settlements collected after this script started. This local PnL is cumulative
  across the saved local ledger, not just the current active-candidate window.
- Dashboard historical PnL columns are ranking inputs; the local-observed PnL
  column is the stricter live-monitoring evidence that should drive later
  copyability decisions.
- Dashboard win/loss counts use locally observed settled markets. Polymarket
  closed-position rows are not used for win/loss because that endpoint can be
  biased toward winning outcome positions and does not match profile daily PnL.
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
