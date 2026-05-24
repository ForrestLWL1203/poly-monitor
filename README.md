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

- `data/raw/YYYY-MM-DD/events.jsonl`: compact raw events, 7-day rolling retention.
- `data/state/observer.sqlite`: dedupe state, seeds, trades, candidate scores.
- `data/reports/latest_candidates.json`: active/dormant/archive candidate lists.

## Dashboard

Run the read-only dashboard separately from the observer process:

```bash
export POLY_MONITOR_DASH_PASSWORD='change-me'
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
- `POLY_MONITOR_DASH_COOKIE_SECRET`: cookie signing secret; defaults to the
  dashboard password when unset.

## Default Seeds

- `username123123`: `0xd950a1a89f3e61a7a9efc85a46e440ce58c15e86`
- `bonereaper`: `0xeebde7a0e019a63e6b476eb425505b7b3e6eba30`
- `pbot-6`: `0x21d0a97aac03917e752857a551bbe5103a00e8d7`

Override seeds with:

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
inactive wallets become `archive_candidate`. Archive storage is capped by
`--max-archive-candidates` and low-signal wallets are not persisted just because
they appeared in one live trade.
