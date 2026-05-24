# poly-monitor

Read-only Polymarket BTC/ETH 5-minute wallet observer.

The first version monitors public BTC/ETH 5m market trades, records compact
trade-triggered context snapshots, and maintains a strict candidate wallet
leaderboard. It does not authenticate, submit orders, or read account secrets.

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
- `data/state/observer.sqlite`: dedupe state, seeds, trades, candidate scores.
- `data/reports/latest_candidates.json`: active and dormant candidate lists.

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
tunnel. The dashboard only reads `observer.sqlite`, `latest_candidates.json`,
and recent compact raw JSONL. It does not start/stop the collector, expose raw
downloads, or submit orders.

Optional environment variables:

- `POLY_MONITOR_DASH_USER`: login username, default `admin`.
- `POLY_MONITOR_DASH_COOKIE_SECRET`: required cookie signing secret.

## Optional Seeds

The observer defaults to no pinned seed wallets. Add temporary manual watch
addresses only when needed:

```bash
python3 scripts/run_crypto_wallet_observer.py \
  --seed-wallet label=0xwallet,other=0xwallet
```

## Candidate Thresholds

Active candidates require high recent BTC/ETH 5m activity and stable positive PnL:

- 7d trades >= 500 and 7d markets >= 5.
- 30d trades >= 800 and 30d markets >= 5.
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

Scoring PnL uses only BTC/ETH 5m closed-position estimates. Whole-profile
Polymarket leaderboard PnL is fetched only as display/diagnostic context and is
not used to decide whether a wallet has crypto 5m edge.

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
