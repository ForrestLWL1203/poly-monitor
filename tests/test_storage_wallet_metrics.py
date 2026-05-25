import datetime as dt
import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from poly_monitor.storage import ObserverStore, WatchlistStore


def trade_row(
    wallet: str,
    market_slug: str,
    exchange_ts: int,
    *,
    tx_hash: str,
    outcome: str = "Up",
    side: str = "BUY",
    price: float = 0.5,
    size: float = 10,
) -> dict:
    return {
        "tx_hash": tx_hash,
        "fill_id": "",
        "wallet": wallet.lower(),
        "market_slug": market_slug,
        "condition_id": f"cond-{market_slug}",
        "symbol": "BTC",
        "exchange_ts": exchange_ts,
        "outcome": outcome,
        "side": side,
        "price": price,
        "size": size,
        "usdc": round(price * size, 6),
    }


class StorageWalletMetricsTests(unittest.TestCase):
    def test_wallet_activity_events_store_trade_merge_and_redeem_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                rows = [
                    {
                        "tx_hash": "0xtrade",
                        "wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 100,
                        "activity_type": "TRADE",
                        "side": "BUY",
                        "outcome": "Up",
                        "outcome_index": 0,
                        "price": 0.52,
                        "size": 10,
                        "usdc": 5.2,
                        "asset": "token-up",
                        "observed_at": "2026-05-25T00:00:00+00:00",
                    },
                    {
                        "tx_hash": "0xmerge",
                        "wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 110,
                        "activity_type": "MERGE",
                        "side": "",
                        "outcome": "",
                        "outcome_index": 999,
                        "price": 0.0,
                        "size": 10,
                        "usdc": 10,
                        "asset": "",
                        "observed_at": "2026-05-25T00:00:01+00:00",
                    },
                    {
                        "tx_hash": "0xredeem",
                        "wallet": "0xabc",
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "exchange_ts": 120,
                        "activity_type": "REDEEM",
                        "side": "",
                        "outcome": "",
                        "outcome_index": 999,
                        "price": 0.0,
                        "size": 5,
                        "usdc": 5,
                        "asset": "",
                        "observed_at": "2026-05-25T00:00:02+00:00",
                    },
                ]

                inserted = store.insert_wallet_activity_events(rows + [dict(rows[0])])
                saved = store.wallet_activity_events("0xABC")
            finally:
                store.close()

        self.assertEqual(len(inserted), 3)
        self.assertEqual([row["activity_type"] for row in saved], ["TRADE", "MERGE", "REDEEM"])
        self.assertEqual(saved[1]["usdc"], 10)
        self.assertEqual(saved[2]["outcome_index"], 999)

    def test_wallet_activity_events_use_compact_fill_id_primary_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                base = {
                    "tx_hash": "0xtrade",
                    "wallet": "0xabc",
                    "market_slug": "btc-updown-5m-1",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 100,
                    "activity_type": "TRADE",
                    "side": "BUY",
                    "outcome": "Up",
                    "outcome_index": 0,
                    "price": 0.52,
                    "size": 10,
                    "usdc": 5.2,
                    "asset": "token-up",
                    "fill_id": "fill-1",
                    "observed_at": "2026-05-25T00:00:00+00:00",
                }
                inserted = store.insert_wallet_activity_events(
                    [
                        base,
                        {**base, "price": 0.53, "size": 11, "usdc": 5.83},
                        {**base, "fill_id": "fill-2", "price": 0.53, "size": 11, "usdc": 5.83},
                    ]
                )
                pk = {
                    str(row["name"]): int(row["pk"] or 0)
                    for row in store.conn.execute("PRAGMA table_info(wallet_activity_events)").fetchall()
                }
                saved = store.wallet_activity_events("0xabc")
            finally:
                store.close()

        self.assertEqual((pk["tx_hash"], pk["fill_id"], pk["activity_type"]), (1, 2, 3))
        self.assertEqual(len(inserted), 2)
        self.assertEqual([row["fill_id"] for row in saved], ["fill-1", "fill-2"])

    def test_wallet_trade_contexts_store_compact_context_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                context = {
                    "wallet": "0xabc",
                    "tx_hash": "0xtrade",
                    "fill_id": "fill-1",
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "exchange_ts": 1770000010,
                    "observed_at": "2026-05-25T00:00:00+00:00",
                    "context_json": {"event": "context_snapshot", "up": {"bid": 0.49}, "down": {"ask": 0.51}},
                    "book_stale": True,
                }

                inserted = store.insert_wallet_trade_contexts([context, dict(context)])
                rows = store.wallet_trade_contexts("0xABC")
            finally:
                store.close()

        self.assertEqual(len(inserted), 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["wallet"], "0xabc")
        self.assertEqual(rows[0]["fill_id"], "fill-1")
        self.assertEqual(rows[0]["book_stale"], 1)
        self.assertEqual(json.loads(rows[0]["context_json"])["up"]["bid"], 0.49)

    def test_market_state_samples_store_summary_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                sample = {
                    "market_slug": "btc-updown-5m-1770000000",
                    "condition_id": "0xcond",
                    "symbol": "BTC",
                    "sampled_ts": 1770000015,
                    "observed_at": "2026-05-25T00:00:15+00:00",
                    "window_remaining_sec": 120.0,
                    "reference_price": 100000.0,
                    "reference_price_age_sec": 0.25,
                    "up_json": {"bid": 0.49, "ask": 0.51},
                    "down_json": {"bid": 0.48, "ask": 0.52},
                    "book_stale": False,
                    "sample_reason": "heartbeat",
                }

                inserted = store.insert_market_state_samples([sample, dict(sample)])
                rows = store.market_state_samples("btc-updown-5m-1770000000")
            finally:
                store.close()

        self.assertEqual(len(inserted), 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sample_reason"], "heartbeat")
        self.assertEqual(json.loads(rows[0]["up_json"])["ask"], 0.51)

    def test_market_state_samples_store_stale_empty_books_as_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                inserted = store.insert_market_state_samples(
                    [
                        {
                            "market_slug": "btc-updown-5m-1770000000",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "sampled_ts": 1770000010,
                            "observed_at": "2026-05-25T00:00:00+00:00",
                            "up_json": {},
                            "down_json": {},
                            "book_stale": True,
                            "sample_reason": "heartbeat",
                        }
                    ]
                )
                rows = store.market_state_samples("btc-updown-5m-1770000000")
            finally:
                store.close()

        self.assertEqual(len(inserted), 1)
        self.assertIsNone(rows[0]["up_json"])
        self.assertIsNone(rows[0]["down_json"])
        self.assertEqual(rows[0]["book_stale"], 1)

    def test_wallet_activity_profiles_are_deduplicated_from_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xtrade",
                            "wallet": "0xabc",
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.52,
                            "size": 10,
                            "usdc": 5.2,
                            "asset": "token-up",
                            "name": "Joe Trader",
                            "pseudonym": "joe",
                            "observed_at": "2026-05-25T00:00:00+00:00",
                        }
                    ]
                )
                activity_columns = {
                    str(row["name"])
                    for row in store.conn.execute("PRAGMA table_info(wallet_activity_events)").fetchall()
                }
                profile = store.conn.execute("SELECT * FROM wallet_profiles WHERE wallet='0xabc'").fetchone()
                activity = store.wallet_activity_events("0xabc")[0]
            finally:
                store.close()

        self.assertNotIn("name", activity_columns)
        self.assertNotIn("pseudonym", activity_columns)
        self.assertEqual(profile["name"], "Joe Trader")
        self.assertEqual(profile["pseudonym"], "joe")
        self.assertNotIn("name", activity)

    def test_wallet_activity_profile_migration_preserves_latest_legacy_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            conn = sqlite3.connect(path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE wallet_activity_events (
                        tx_hash TEXT NOT NULL,
                        wallet TEXT NOT NULL,
                        market_slug TEXT NOT NULL,
                        condition_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        exchange_ts INTEGER NOT NULL,
                        activity_type TEXT NOT NULL,
                        side TEXT NOT NULL DEFAULT '',
                        outcome TEXT NOT NULL DEFAULT '',
                        outcome_index INTEGER NOT NULL DEFAULT -1,
                        price REAL NOT NULL DEFAULT 0,
                        size REAL NOT NULL DEFAULT 0,
                        usdc REAL NOT NULL DEFAULT 0,
                        asset TEXT NOT NULL DEFAULT '',
                        name TEXT NOT NULL DEFAULT '',
                        pseudonym TEXT NOT NULL DEFAULT '',
                        raw_json TEXT NOT NULL DEFAULT '{}',
                        observed_at TEXT NOT NULL,
                        PRIMARY KEY(tx_hash, wallet, condition_id, activity_type, outcome_index, asset, price, size)
                    );
                    INSERT INTO wallet_activity_events(
                        tx_hash,wallet,market_slug,condition_id,symbol,exchange_ts,activity_type,
                        side,outcome,outcome_index,price,size,usdc,asset,name,pseudonym,raw_json,observed_at
                    ) VALUES
                    ('0xold','0xabc','btc-updown-5m-1','0xcond','BTC',100,'TRADE','BUY','Up',0,0.5,10,5,'up','Old Name','old','{}','2026-05-25T00:00:00+00:00'),
                    ('0xnew','0xabc','btc-updown-5m-2','0xcond2','BTC',200,'TRADE','BUY','Up',0,0.5,10,5,'up','New Name','new','{}','2026-05-25T00:01:00+00:00');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = ObserverStore(path)
            try:
                profile = store.conn.execute("SELECT * FROM wallet_profiles WHERE wallet='0xabc'").fetchone()
                activity_columns = {
                    str(row["name"])
                    for row in store.conn.execute("PRAGMA table_info(wallet_activity_events)").fetchall()
                }
            finally:
                store.close()

        self.assertEqual(profile["name"], "New Name")
        self.assertEqual(profile["pseudonym"], "new")
        self.assertNotIn("name", activity_columns)
        self.assertNotIn("raw_json", activity_columns)

    def test_wallet_activity_schema_migration_adds_fill_id_and_compact_pk(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            conn = sqlite3.connect(path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE wallet_activity_events (
                        tx_hash TEXT NOT NULL,
                        wallet TEXT NOT NULL,
                        market_slug TEXT NOT NULL,
                        condition_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        exchange_ts INTEGER NOT NULL,
                        activity_type TEXT NOT NULL,
                        side TEXT NOT NULL DEFAULT '',
                        outcome TEXT NOT NULL DEFAULT '',
                        outcome_index INTEGER NOT NULL DEFAULT -1,
                        price REAL NOT NULL DEFAULT 0,
                        size REAL NOT NULL DEFAULT 0,
                        usdc REAL NOT NULL DEFAULT 0,
                        asset TEXT NOT NULL DEFAULT '',
                        observed_at TEXT NOT NULL,
                        PRIMARY KEY(tx_hash, wallet, condition_id, activity_type, outcome_index, asset, price, size)
                    );
                    INSERT INTO wallet_activity_events(
                        tx_hash,wallet,market_slug,condition_id,symbol,exchange_ts,activity_type,
                        side,outcome,outcome_index,price,size,usdc,asset,observed_at
                    ) VALUES
                    ('0xtrade','0xabc','btc-updown-5m-1','0xcond','BTC',100,'TRADE','BUY','Up',0,0.5,10,5,'up','2026-05-25T00:00:00+00:00'),
                    ('0xtrade','0xabc','btc-updown-5m-1','0xcond','BTC',101,'TRADE','BUY','Down',1,0.4,12,4.8,'down','2026-05-25T00:00:01+00:00');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = ObserverStore(path)
            try:
                pk = {
                    str(row["name"]): int(row["pk"] or 0)
                    for row in store.conn.execute("PRAGMA table_info(wallet_activity_events)").fetchall()
                }
                rows = store.wallet_activity_events("0xabc")
            finally:
                store.close()

        self.assertEqual((pk["tx_hash"], pk["fill_id"], pk["activity_type"]), (1, 2, 3))
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(str(row["fill_id"]).startswith("legacy:") for row in rows))

    def test_archive_strategy_rows_exports_gzip_and_deletes_old_rows_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = ObserverStore(data_dir / "state" / "observer.sqlite")
            try:
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xold",
                            "wallet": "0xabc",
                            "market_slug": "btc-updown-5m-old",
                            "condition_id": "0xoldcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "observed_at": "1970-01-02T00:00:00+00:00",
                        },
                    ]
                )
                store.insert_wallet_trade_contexts(
                    [
                        {
                            "wallet": "0xabc",
                            "tx_hash": "0xold",
                            "fill_id": "",
                            "market_slug": "btc-updown-5m-old",
                            "condition_id": "0xoldcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "observed_at": "1970-01-02T00:00:00+00:00",
                            "context_json": {"event": "context_snapshot"},
                            "book_stale": False,
                        }
                    ]
                )
                result = store.archive_strategy_rows(
                    data_dir / "archive",
                    activity_cutoff_ts=200,
                    context_cutoff_ts=200,
                    sample_cutoff_ts=0,
                    delete_batch_size=10000,
                )
                second = store.archive_strategy_rows(
                    data_dir / "archive",
                    activity_cutoff_ts=200,
                    context_cutoff_ts=200,
                    sample_cutoff_ts=0,
                    delete_batch_size=10000,
                )
                remaining_activity = store.wallet_activity_events("0xabc")
                remaining_contexts = store.wallet_trade_contexts("0xabc")
                manifest = store.archive_manifest_rows()
            finally:
                store.close()

            activity_archive = data_dir / "archive" / "1970-01-01" / "wallet_activity_events.jsonl.gz"
            context_archive = data_dir / "archive" / "1970-01-01" / "wallet_trade_contexts.jsonl.gz"
            with gzip.open(activity_archive, "rt", encoding="utf-8") as handle:
                activity_rows = [json.loads(line) for line in handle]
            with gzip.open(context_archive, "rt", encoding="utf-8") as handle:
                context_rows = [json.loads(line) for line in handle]

        self.assertEqual(result["wallet_activity_events"], 1)
        self.assertEqual(result["wallet_trade_contexts"], 1)
        self.assertEqual(second["wallet_activity_events"], 0)
        self.assertEqual(second["wallet_trade_contexts"], 0)
        self.assertEqual(remaining_activity, [])
        self.assertEqual(remaining_contexts, [])
        self.assertEqual(activity_rows[0]["tx_hash"], "0xold")
        self.assertEqual(context_rows[0]["tx_hash"], "0xold")
        self.assertEqual({row["data_type"] for row in manifest}, {"wallet_activity_events", "wallet_trade_contexts"})

    def test_watchlist_merge_activity_creates_wallet_market_pnl_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
                market = "btc-updown-5m-1779694200"
                store.add_watchlist_wallet(wallet)
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-down",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "outcome_index": 1,
                            "price": 0.19243,
                            "size": 247.134302,
                            "usdc": 47.557265,
                            "observed_at": "2026-05-25T07:30:00+00:00",
                        },
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 101,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.621,
                            "size": 168,
                            "usdc": 104.32742,
                            "observed_at": "2026-05-25T07:30:01+00:00",
                        },
                        {
                            "tx_hash": "0xmerge-1",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 110,
                            "activity_type": "MERGE",
                            "outcome_index": 999,
                            "size": 163,
                            "usdc": 163,
                            "observed_at": "2026-05-25T07:34:00+00:00",
                        },
                        {
                            "tx_hash": "0xredeem",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 120,
                            "activity_type": "REDEEM",
                            "outcome_index": 999,
                            "size": 5,
                            "usdc": 5,
                            "observed_at": "2026-05-25T07:40:00+00:00",
                        },
                    ]
                )

                rows = store.wallet_market_pnl_rows(wallet)
                metrics = store.wallet_trade_metrics(wallet, now_ts=1779699600)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pnl_source"], "activity_ledger")
        self.assertEqual(rows[0]["has_merge_or_split"], 1)
        self.assertEqual(rows[0]["incomplete"], 0)
        self.assertAlmostEqual(rows[0]["realized_pnl"], 16.115315)
        self.assertAlmostEqual(rows[0]["activity_realized_pnl"], 16.115315)
        self.assertAlmostEqual(rows[0]["activity_merge_usdc"], 163.0)
        self.assertAlmostEqual(rows[0]["activity_redeem_usdc"], 5.0)
        self.assertEqual(metrics["pnl_source"], "local_observed_ledger")
        self.assertAlmostEqual(metrics["pnl_total"], 16.115315)
        self.assertEqual(metrics["settled_markets_total"], 1)
        self.assertEqual(metrics["activity_ledger_markets_total"], 1)
        self.assertEqual(metrics["merge_or_split_markets_total"], 1)

    def test_watchlist_activity_without_settlement_does_not_infer_pnl_winner(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xd950a1a89f3e61a7a9efc85a46e440ce58c15e86"
                market = "eth-updown-5m-1779706800"
                store.add_watchlist_wallet(wallet)
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-down",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "ETH",
                            "exchange_ts": 1779706811,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "outcome_index": 1,
                            "price": 0.8626,
                            "size": 420.855256,
                            "usdc": 363.044535,
                            "observed_at": "2026-05-25T11:00:11+00:00",
                        },
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "ETH",
                            "exchange_ts": 1779706812,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.1161,
                            "size": 456.651382,
                            "usdc": 53.003266,
                            "observed_at": "2026-05-25T11:00:12+00:00",
                        },
                        {
                            "tx_hash": "0xredeem",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "ETH",
                            "exchange_ts": 1779707137,
                            "activity_type": "REDEEM",
                            "outcome_index": 999,
                            "size": 420.855256,
                            "usdc": 420.855256,
                            "observed_at": "2026-05-25T11:05:37+00:00",
                        },
                    ]
                )

                rows = store.wallet_market_pnl_rows(wallet)
                metrics = store.wallet_trade_metrics(wallet, now_ts=1779708000)
            finally:
                store.close()

        self.assertEqual(rows, [])
        self.assertEqual(metrics.get("pnl_source"), "local_observed_ledger")
        self.assertAlmostEqual(metrics["pnl_total"], 0.0)
        self.assertEqual(metrics["settled_markets_total"], 0)

    def test_activity_ledger_partial_redeem_keeps_unredeemed_winner_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xpartial"
                market = "btc-updown-5m-partial-redeem"
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.4,
                            "size": 100,
                            "usdc": 40,
                            "observed_at": "2026-05-25T07:30:00+00:00",
                        },
                        {
                            "tx_hash": "0xredeem-partial",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 120,
                            "activity_type": "REDEEM",
                            "outcome_index": 999,
                            "size": 30,
                            "usdc": 30,
                            "observed_at": "2026-05-25T07:40:00+00:00",
                        },
                    ]
                )

                row = store.wallet_market_pnl_rows(wallet)[0]
            finally:
                store.close()

        self.assertEqual(row["pnl_source"], "activity_ledger")
        self.assertAlmostEqual(row["realized_pnl"], 60.0)
        self.assertAlmostEqual(row["settled_value"], 100.0)
        self.assertAlmostEqual(row["activity_redeem_usdc"], 30.0)
        self.assertAlmostEqual(row["activity_net_shares_up"], 70.0)
        self.assertAlmostEqual(row["activity_net_shares_down"], 0.0)

    def test_activity_ledger_marks_bad_cashflow_value_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xbadvalue"
                market = "btc-updown-5m-bad-cashflow"
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.4,
                            "size": 100,
                            "usdc": 40,
                            "observed_at": "2026-05-25T07:30:00+00:00",
                        },
                        {
                            "tx_hash": "0xbad-merge",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 120,
                            "activity_type": "MERGE",
                            "outcome_index": 999,
                            "size": 100,
                            "usdc": 0,
                            "observed_at": "2026-05-25T07:35:00+00:00",
                        },
                    ]
                )

                row = store.wallet_market_pnl_rows(wallet)[0]
            finally:
                store.close()

        self.assertEqual(row["pnl_source"], "activity_ledger")
        self.assertEqual(row["incomplete"], 1)
        self.assertAlmostEqual(row["activity_merge_usdc"], 0.0)
        self.assertAlmostEqual(row["activity_cash_flow"], -40.0)
        self.assertAlmostEqual(row["activity_net_shares_up"], 100.0)
        self.assertAlmostEqual(row["activity_net_shares_down"], 0.0)

    def test_store_startup_recomputes_settled_activity_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            wallet = "0xd950a1a89f3e61a7a9efc85a46e440ce58c15e86"
            market = "eth-updown-5m-1779706800"
            store = ObserverStore(path)
            try:
                for tx_hash, exchange_ts, activity_type, side, outcome, outcome_index, size, usdc in [
                    ("0xbuy-down", 1779706811, "TRADE", "BUY", "Down", 1, 420.855256, 363.044535),
                    ("0xbuy-up", 1779706812, "TRADE", "BUY", "Up", 0, 456.651382, 53.003266),
                    ("0xredeem", 1779707137, "REDEEM", "", "", 999, 420.855256, 420.855256),
                ]:
                    store.conn.execute(
                        """
                        INSERT INTO wallet_activity_events(
                            tx_hash,wallet,market_slug,condition_id,symbol,exchange_ts,activity_type,
                            side,outcome,outcome_index,price,size,usdc,asset,observed_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            tx_hash,
                            wallet,
                            market,
                            "0xcond",
                            "ETH",
                            exchange_ts,
                            activity_type,
                            side,
                            outcome,
                            outcome_index,
                            0.0,
                            size,
                            usdc,
                            "",
                            "2026-05-25T11:05:37+00:00",
                        ),
                    )
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "ETH",
                        "winning_side": "Down",
                        "settled_at": "2026-05-25T11:05:00+00:00",
                        "completed": True,
                    }
                )
                store.conn.commit()
            finally:
                store.close()

            store = ObserverStore(path)
            try:
                rows = store.wallet_market_pnl_rows(wallet)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pnl_source"], "activity_ledger")
        self.assertAlmostEqual(rows[0]["realized_pnl"], 4.807455)
        self.assertEqual(rows[0]["incomplete"], 0)

    def test_store_startup_does_not_recompute_activity_pnl_after_migration_version_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            store = ObserverStore(path)
            try:
                wallet = "0xstartupskip"
                market = "btc-updown-5m-startup-skip"
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "observed_at": "2026-05-25T07:30:00+00:00",
                        }
                    ]
                )
            finally:
                store.close()

            with patch.object(ObserverStore, "_recompute_market_pnl", side_effect=AssertionError("unexpected startup recompute")):
                store = ObserverStore(path)
                store.close()

    def test_watchlist_activity_settlement_creates_pnl_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xd950a1a89f3e61a7a9efc85a46e440ce58c15e86"
                market = "eth-updown-5m-1779706800"
                store.add_watchlist_wallet(wallet)
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": "0xcond",
                        "symbol": "ETH",
                        "winning_side": "Down",
                        "settled_at": "2026-05-25T11:05:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-down",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "ETH",
                            "exchange_ts": 1779706811,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Down",
                            "outcome_index": 1,
                            "price": 0.8626,
                            "size": 420.855256,
                            "usdc": 363.044535,
                            "observed_at": "2026-05-25T11:00:11+00:00",
                        },
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": "0xcond",
                            "symbol": "ETH",
                            "exchange_ts": 1779706812,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.1161,
                            "size": 456.651382,
                            "usdc": 53.003266,
                            "observed_at": "2026-05-25T11:00:12+00:00",
                        },
                    ]
                )

                rows = store.wallet_market_pnl_rows(wallet)
                metrics = store.wallet_trade_metrics(wallet, now_ts=1779708000)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pnl_source"], "activity_ledger")
        self.assertEqual(rows[0]["has_merge_or_split"], 0)
        self.assertAlmostEqual(rows[0]["realized_pnl"], 4.807455)
        self.assertEqual(metrics["pnl_source"], "local_observed_ledger")
        self.assertEqual(metrics["wins_7d"], 1)

    def test_watchlist_metrics_do_not_merge_activity_pnl_with_legacy_markets(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = int(dt.datetime(2026, 5, 25, 12, 0, tzinfo=dt.timezone.utc).timestamp())
                wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
                old_market = "btc-updown-5m-before-watchlist"
                new_market = "btc-updown-5m-after-watchlist"
                store.insert_trade(trade_row(wallet, old_market, now_ts - 500, tx_hash="tx-old", outcome="Up", price=0.4, size=10))
                store.upsert_market_settlement(
                    {
                        "market_slug": old_market,
                        "condition_id": f"cond-{old_market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T10:00:00+00:00",
                        "completed": True,
                    }
                )
                store.add_watchlist_wallet(wallet)
                store.upsert_market_settlement(
                    {
                        "market_slug": new_market,
                        "condition_id": f"cond-{new_market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T11:00:00+00:00",
                        "completed": True,
                    }
                )
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xbuy-up",
                            "wallet": wallet,
                            "market_slug": new_market,
                            "condition_id": f"cond-{new_market}",
                            "symbol": "BTC",
                            "exchange_ts": now_ts - 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "observed_at": "2026-05-25T11:00:00+00:00",
                        },
                    ]
                )

                metrics = store.wallet_trade_metrics(wallet, now_ts=now_ts)
            finally:
                store.close()

        self.assertEqual(metrics["pnl_source"], "local_observed_ledger")
        self.assertAlmostEqual(metrics["pnl_total"], 11.0)
        self.assertAlmostEqual(metrics["pnl_7d"], 11.0)
        self.assertEqual(metrics["settled_markets_total"], 2)
        self.assertEqual(metrics["wins_7d"], 2)
        self.assertEqual(metrics["activity_ledger_markets_total"], 1)

    def test_watchlist_cashflow_without_activity_trades_marks_traditional_pnl_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            store = ObserverStore(path)
            try:
                wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
                market = "btc-updown-5m-split"
                store.add_watchlist_wallet(wallet)
                store.insert_trades(
                    [
                        {
                            **trade_row(wallet, market, 100, tx_hash="tx-up", outcome="Up", price=0.0, size=10),
                            "usdc": 0.0,
                        },
                        {
                            **trade_row(wallet, market, 101, tx_hash="tx-down", outcome="Down", price=0.0, size=10),
                            "usdc": 0.0,
                        },
                    ]
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": f"cond-{market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                self.assertAlmostEqual(store.wallet_market_pnl_rows(wallet)[0]["realized_pnl"], 10.0)

                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xsplit",
                            "wallet": wallet,
                            "market_slug": market,
                            "condition_id": f"cond-{market}",
                            "symbol": "BTC",
                            "exchange_ts": 99,
                            "activity_type": "SPLIT",
                            "outcome_index": 999,
                            "size": 10,
                            "usdc": 10,
                            "observed_at": "2026-05-25T07:30:00+00:00",
                        },
                    ]
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": f"cond-{market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                traditional_rows = store.wallet_market_pnl_rows(wallet)
                watchlist_rows = store.watchlist_market_pnl_rows(wallet)
            finally:
                store.close()

        self.assertEqual(len(traditional_rows), 1)
        self.assertAlmostEqual(traditional_rows[0]["realized_pnl"], 10.0)
        self.assertEqual(traditional_rows[0]["pnl_source"], "trade_ledger")
        self.assertEqual(traditional_rows[0]["has_merge_or_split"], 0)
        self.assertEqual(traditional_rows[0]["incomplete"], 1)
        self.assertEqual(watchlist_rows, [])

    def test_store_startup_keeps_traditional_pnl_when_activity_is_only_raw_detail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
            market = "btc-updown-5m-split"
            store = ObserverStore(path)
            try:
                store.insert_trade(
                    {
                        **trade_row(wallet, market, 100, tx_hash="tx-up", outcome="Up", price=0.0, size=10),
                        "usdc": 0.0,
                    }
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": f"cond-{market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-25T07:40:00+00:00",
                        "completed": True,
                    }
                )
                store.conn.execute(
                    """
                    INSERT INTO wallet_activity_events(
                        tx_hash,wallet,market_slug,condition_id,symbol,exchange_ts,activity_type,
                        side,outcome,outcome_index,price,size,usdc,asset,observed_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "0xsplit",
                        wallet,
                        market,
                        f"cond-{market}",
                        "BTC",
                        99,
                        "SPLIT",
                        "",
                        "",
                        999,
                        0.0,
                        10.0,
                        10.0,
                        "",
                        "2026-05-25T07:30:00+00:00",
                    ),
                )
                store.conn.commit()
                self.assertEqual(len(store.wallet_market_pnl_rows(wallet)), 1)
            finally:
                store.close()

            store = ObserverStore(path)
            try:
                rows = store.wallet_market_pnl_rows(wallet)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["realized_pnl"], 10.0)

    def test_watchlist_store_initializes_only_watchlist_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            store = WatchlistStore(path)
            try:
                wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
                store.add_wallet(wallet, note="manual")
                rows = store.rows()
                tables = {
                    str(row["name"])
                    for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                journal_mode = store.conn.execute("PRAGMA journal_mode").fetchone()[0]
                synchronous = store.conn.execute("PRAGMA synchronous").fetchone()[0]
            finally:
                store.close()

        self.assertEqual(rows[0]["wallet"], wallet)
        self.assertEqual(rows[0]["note"], "manual")
        self.assertEqual(tables, {"watchlist_wallets"})
        self.assertEqual(journal_mode, "wal")
        self.assertEqual(synchronous, 1)

    def test_watchlist_store_empty_note_does_not_clear_existing_note(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "observer.sqlite"
            store = WatchlistStore(path)
            try:
                wallet = "0xabcdef1234567890abcdef1234567890abcdef12"
                store.add_wallet(wallet, note="manual")
                first = store.rows()[0]
                store.add_wallet(wallet)
                second = store.rows()[0]
            finally:
                store.close()

        self.assertEqual(second["note"], "manual")
        self.assertEqual(second["created_at"], first["created_at"])
        self.assertNotEqual(second["updated_at"], "")

    def test_remove_watchlist_wallet_and_purge_deletes_research_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                wallet = "0xabc0000000000000000000000000000000000000"
                other = "0xdef0000000000000000000000000000000000000"
                store.add_watchlist_wallet(wallet)
                store.add_watchlist_wallet(other)
                store.insert_wallet_activity_events(
                    [
                        {
                            "tx_hash": "0xactivity",
                            "fill_id": "fill-1",
                            "wallet": wallet,
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "asset": "up",
                            "name": "Remove Me",
                            "observed_at": "2026-05-25T00:00:00+00:00",
                        },
                        {
                            "tx_hash": "0xother",
                            "fill_id": "fill-2",
                            "wallet": other,
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 101,
                            "activity_type": "TRADE",
                            "side": "BUY",
                            "outcome": "Up",
                            "outcome_index": 0,
                            "price": 0.5,
                            "size": 10,
                            "usdc": 5,
                            "asset": "up",
                            "observed_at": "2026-05-25T00:00:01+00:00",
                        },
                    ]
                )
                store.insert_wallet_trade_contexts(
                    [
                        {
                            "wallet": wallet,
                            "tx_hash": "0xactivity",
                            "fill_id": "fill-1",
                            "market_slug": "btc-updown-5m-1",
                            "condition_id": "0xcond",
                            "symbol": "BTC",
                            "exchange_ts": 100,
                            "observed_at": "2026-05-25T00:00:00+00:00",
                            "context_json": {"event": "context_snapshot"},
                            "book_stale": False,
                        }
                    ]
                )
                store.conn.execute(
                    """
                    INSERT INTO wallet_market_pnl(
                        wallet, market_slug, condition_id, symbol, realized_pnl, buy_usdc, sell_usdc,
                        settled_value, net_shares_up, net_shares_down, trades, winning_side, settled_at,
                        incomplete
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (wallet, "btc-updown-5m-1", "0xcond", "BTC", 1, 0, 0, 0, 0, 0, 1, "Up", "2026-05-25T00:00:00+00:00", 0),
                )
                store.upsert_watched_market_window(
                    {
                        "market_slug": "btc-updown-5m-1",
                        "condition_id": "0xcond",
                        "symbol": "BTC",
                        "first_seen_at": "2026-05-25T00:00:00+00:00",
                        "window_start": "2026-05-25T00:00:00+00:00",
                        "window_end": "2026-05-25T00:05:00+00:00",
                        "tracking_reason": "watchlist_activity",
                        "source_wallet": wallet,
                        "capture_until": "2026-05-25T00:35:00+00:00",
                        "status": "tracking",
                    }
                )

                result = store.remove_watchlist_wallet_and_purge(wallet)
                remaining_wallets = store.watchlist_wallets()
                removed_activity = store.wallet_activity_events(wallet)
                other_activity = store.wallet_activity_events(other)
                removed_contexts = store.wallet_trade_contexts(wallet)
                removed_pnl = store.wallet_market_pnl_rows(wallet)
                profile = store.conn.execute("SELECT 1 FROM wallet_profiles WHERE wallet=?", (wallet,)).fetchone()
                watched = store.watched_market_windows()
            finally:
                store.close()

        self.assertEqual(result["removed_watchlist_rows"], 1)
        self.assertEqual(result["removed_activity_events"], 1)
        self.assertEqual(result["removed_trade_contexts"], 1)
        self.assertEqual(result["removed_wallet_market_pnl"], 1)
        self.assertEqual(result["removed_wallet_profiles"], 1)
        self.assertEqual(result["removed_watched_market_windows"], 1)
        self.assertEqual(remaining_wallets, [other])
        self.assertEqual(removed_activity, [])
        self.assertEqual(len(other_activity), 1)
        self.assertEqual(removed_contexts, [])
        self.assertEqual(removed_pnl, [])
        self.assertIsNone(profile)
        self.assertEqual(watched, [])

    def test_wallet_trade_metrics_counts_time_windows_without_loading_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-updown-5m-1999500", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "eth-updown-5m-1999500", now_ts - 200, tx_hash="tx-2"),
                        trade_row("0xabc", "eth-updown-5m-1800000", now_ts - 2 * 86400, tx_hash="tx-3"),
                        trade_row("0xabc", "sol-1", now_ts - 20 * 86400, tx_hash="tx-4"),
                        trade_row("0xabc", "xrp-1", now_ts - 40 * 86400, tx_hash="tx-5"),
                        trade_row("0xdef", "btc-1", now_ts - 50, tx_hash="tx-6"),
                    ]
                )

                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)

                self.assertEqual(metrics["wallet"], "0xabc")
                self.assertEqual(metrics["trades_24h"], 2)
                self.assertEqual(metrics["markets_24h"], 2)
                self.assertEqual(metrics["btc_markets_24h"], 1)
                self.assertEqual(metrics["eth_markets_24h"], 1)
                self.assertEqual(metrics["trades_7d"], 3)
                self.assertEqual(metrics["markets_7d"], 3)
                self.assertEqual(metrics["trades_30d"], 4)
                self.assertEqual(metrics["markets_30d"], 4)
                self.assertEqual(metrics["max_trades_per_market_24h"], 1)
                self.assertEqual(metrics["max_trades_per_market_7d"], 1)
                self.assertEqual(metrics["max_trades_per_market_30d"], 1)
                self.assertEqual(metrics["historical_trades"], 5)
                self.assertEqual(metrics["historical_markets"], 5)
                self.assertAlmostEqual(metrics["last_active_age_hours"], round(100 / 3600, 3))
            finally:
                store.close()

    def test_wallet_trade_metrics_exposes_extreme_single_window_trade_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                rows = [
                    trade_row("0xabc", "btc-updown-5m-1999800", now_ts - idx, tx_hash=f"tx-{idx}")
                    for idx in range(603)
                ]
                rows.append(trade_row("0xabc", "eth-updown-5m-1999800", now_ts - 10, tx_hash="eth-1"))
                store.insert_trades(rows)

                metrics = store.wallet_trade_metrics("0xabc", now_ts=now_ts)

                self.assertEqual(metrics["trades_24h"], 604)
                self.assertEqual(metrics["max_trades_per_market_24h"], 603)
                self.assertEqual(metrics["max_trades_per_market_7d"], 603)
                self.assertEqual(metrics["max_trades_per_market_30d"], 603)
            finally:
                store.close()

    def test_wallet_trade_metrics_exposes_terminal_thin_edge_share(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-updown-5m-1000", 1245, tx_hash="tx-thin-1", side="BUY", price=0.99),
                        trade_row("0xabc", "eth-updown-5m-1300", 1541, tx_hash="tx-thin-2", side="BUY", price=0.98),
                        trade_row("0xabc", "btc-updown-5m-1600", 1640, tx_hash="tx-early", side="BUY", price=0.99),
                        trade_row("0xabc", "btc-updown-5m-1900", 2145, tx_hash="tx-sell", side="SELL", price=0.99),
                    ]
                )

                metrics = store.wallet_trade_metrics("0xabc", now_ts=2200)

                self.assertEqual(metrics["terminal_near_certain_trades_30d"], 2)
                self.assertEqual(metrics["terminal_near_certain_trade_share_30d"], 0.5)
            finally:
                store.close()

    def test_wallet_trade_metrics_uses_one_max_trades_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-updown-5m-hot", now_ts - 100 - idx, tx_hash=f"tx-hot-{idx}")
                        for idx in range(3)
                    ]
                    + [
                        trade_row("0xabc", "btc-updown-5m-week", now_ts - 2 * 86400 - idx, tx_hash=f"tx-week-{idx}")
                        for idx in range(5)
                    ]
                    + [
                        trade_row("0xabc", "btc-updown-5m-month", now_ts - 10 * 86400 - idx, tx_hash=f"tx-month-{idx}")
                        for idx in range(7)
                    ]
                )
                statements: list[str] = []
                store.conn.set_trace_callback(statements.append)
                metrics = store.wallet_trade_metrics("0xabc", now_ts=now_ts)
            finally:
                store.conn.set_trace_callback(None)
                store.close()

        grouped_max_queries = [sql for sql in statements if "n_24h" in sql and "GROUP BY market_slug" in sql]
        self.assertEqual(len(grouped_max_queries), 1)
        self.assertEqual(metrics["max_trades_per_market_24h"], 3)
        self.assertEqual(metrics["max_trades_per_market_7d"], 5)
        self.assertEqual(metrics["max_trades_per_market_30d"], 7)

    def test_wallet_24h_counts_returns_observer_override_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-1", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "btc-updown-5m-1999500", now_ts - 150, tx_hash="tx-btc"),
                        trade_row("0xabc", "eth-updown-5m-1999500", now_ts - 200, tx_hash="tx-eth"),
                        trade_row("0xabc", "btc-3", now_ts - 2 * 86400, tx_hash="tx-3"),
                    ]
                )

                self.assertEqual(
                    store.wallet_24h_counts("0xABC", now_ts=now_ts),
                    {"trades_24h": 3, "markets_24h": 3, "btc_markets_24h": 1, "eth_markets_24h": 1},
                )
            finally:
                store.close()

    def test_market_settlement_recomputes_wallet_market_pnl_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                market = "btc-updown-5m-2000000"
                condition = f"cond-{market}"
                store.insert_trades(
                    [
                        trade_row("0xabc", market, 100, tx_hash="tx-1", outcome="Up", side="BUY", price=0.40, size=10),
                        trade_row("0xabc", market, 110, tx_hash="tx-2", outcome="Up", side="SELL", price=0.70, size=4),
                        trade_row("0xabc", market, 120, tx_hash="tx-3", outcome="Down", side="BUY", price=0.30, size=5),
                        trade_row("0xdef", market, 130, tx_hash="tx-4", outcome="Down", side="SELL", price=0.20, size=3),
                    ]
                )

                changed = store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": condition,
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settlement_open_price": 100.0,
                        "settlement_close_price": 101.0,
                        "settled_at": "2026-05-24T12:00:00+00:00",
                        "completed": True,
                    }
                )
                rows = store.wallet_market_pnl_rows()
            finally:
                store.close()

        self.assertTrue(changed)
        by_wallet = {row["wallet"]: row for row in rows}
        self.assertAlmostEqual(by_wallet["0xabc"]["realized_pnl"], 3.3)
        self.assertAlmostEqual(by_wallet["0xabc"]["buy_usdc"], 5.5)
        self.assertAlmostEqual(by_wallet["0xabc"]["sell_usdc"], 2.8)
        self.assertAlmostEqual(by_wallet["0xabc"]["settled_value"], 6.0)
        self.assertAlmostEqual(by_wallet["0xabc"]["net_shares_up"], 6.0)
        self.assertAlmostEqual(by_wallet["0xabc"]["net_shares_down"], 5.0)
        self.assertEqual(by_wallet["0xabc"]["trades"], 3)
        self.assertAlmostEqual(by_wallet["0xdef"]["realized_pnl"], 0.6)
        self.assertEqual(by_wallet["0xdef"]["incomplete"], 1)

    def test_wallet_metrics_exclude_incomplete_ledger_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                market = "btc-updown-5m-short"
                store.insert_trade(
                    trade_row(
                        "0xabc",
                        market,
                        now_ts - 100,
                        tx_hash="tx-short",
                        outcome="Up",
                        side="SELL",
                        price=0.70,
                        size=100,
                    )
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": f"cond-{market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-24T12:00:00+00:00",
                        "completed": True,
                    }
                )

                rows = store.wallet_market_pnl_rows("0xabc")
                metrics = store.wallet_trade_metrics("0xabc", now_ts=now_ts)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["incomplete"], 1)
        self.assertAlmostEqual(rows[0]["realized_pnl"], -30.0)
        self.assertAlmostEqual(metrics["pnl_7d"], 0.0)
        self.assertAlmostEqual(metrics["pnl_30d"], 0.0)
        self.assertEqual(metrics["wins_7d"], 0)
        self.assertEqual(metrics["losses_7d"], 0)
        self.assertEqual(metrics["settled_markets_7d"], 0)
        self.assertEqual(metrics["incomplete_settled_markets_7d"], 1)

    def test_wallet_observed_metrics_use_settled_local_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                win_market = "btc-updown-5m-win"
                loss_market = "btc-updown-5m-loss"
                store.insert_trade(trade_row("0xabc", win_market, now_ts - 100, tx_hash="tx-win", outcome="Up", side="BUY", price=0.40, size=10))
                store.insert_trade(trade_row("0xabc", loss_market, now_ts - 200, tx_hash="tx-loss", outcome="Up", side="BUY", price=0.60, size=10))
                store.upsert_market_settlement(
                    {
                        "market_slug": win_market,
                        "condition_id": f"cond-{win_market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-24T12:00:00+00:00",
                        "completed": True,
                    }
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": loss_market,
                        "condition_id": f"cond-{loss_market}",
                        "symbol": "BTC",
                        "winning_side": "Down",
                        "settled_at": "2026-05-24T12:01:00+00:00",
                        "completed": True,
                    }
                )

                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)
            finally:
                store.close()

        self.assertAlmostEqual(metrics["pnl_7d"], 0.0)
        self.assertAlmostEqual(metrics["pnl_30d"], 0.0)
        self.assertEqual(metrics["wins_7d"], 1)
        self.assertEqual(metrics["losses_7d"], 1)
        self.assertEqual(metrics["settled_markets_7d"], 2)
        self.assertAlmostEqual(metrics["top1_concentration"], 1.0)
        self.assertEqual(metrics["pnl_source"], "local_observed_ledger")

    def test_settlement_z_timestamp_is_normalized_for_ledger_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = int(dt.datetime(2026, 5, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
                market = "btc-updown-5m-zulu"
                store.insert_trade(trade_row("0xabc", market, now_ts - 100, tx_hash="tx-z", outcome="Up", side="BUY", price=0.40, size=10))
                store.upsert_market_settlement(
                    {
                        "market_slug": market,
                        "condition_id": f"cond-{market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-05-24T12:00:00Z",
                        "completed": True,
                    }
                )

                row = store.wallet_market_pnl_rows("0xabc")[0]
                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)
            finally:
                store.close()

        self.assertEqual(row["settled_at"], "2026-05-24T12:00:00+00:00")
        self.assertEqual(metrics["settled_markets_7d"], 1)

    def test_wallet_observed_metrics_include_cumulative_settled_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = int(dt.datetime(2026, 5, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp())
                old_market = "btc-updown-5m-old"
                recent_market = "btc-updown-5m-recent"
                store.insert_trade(trade_row("0xabc", old_market, now_ts - 40 * 86400, tx_hash="tx-old", outcome="Up", side="BUY", price=0.40, size=10))
                store.insert_trade(trade_row("0xabc", recent_market, now_ts - 100, tx_hash="tx-recent", outcome="Up", side="BUY", price=0.60, size=10))
                store.upsert_market_settlement(
                    {
                        "market_slug": old_market,
                        "condition_id": f"cond-{old_market}",
                        "symbol": "BTC",
                        "winning_side": "Up",
                        "settled_at": "2026-04-14T12:00:00+00:00",
                        "completed": True,
                    }
                )
                store.upsert_market_settlement(
                    {
                        "market_slug": recent_market,
                        "condition_id": f"cond-{recent_market}",
                        "symbol": "BTC",
                        "winning_side": "Down",
                        "settled_at": "2026-05-24T12:00:00+00:00",
                        "completed": True,
                    }
                )

                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)
            finally:
                store.close()

        self.assertAlmostEqual(metrics["pnl_30d"], -6.0)
        self.assertAlmostEqual(metrics["pnl_total"], 0.0)
        self.assertAlmostEqual(metrics["historical_pnl"], 0.0)
        self.assertEqual(metrics["settled_markets_total"], 2)

    def test_wallet_observed_metrics_use_one_pnl_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = int(dt.datetime(2026, 5, 24, 12, tzinfo=dt.timezone.utc).timestamp())
                for idx, pnl in enumerate((10.0, 5.0, -2.0, 1.0), start=1):
                    store.conn.execute(
                        """
                        INSERT INTO wallet_market_pnl(
                            wallet, market_slug, condition_id, symbol, realized_pnl, buy_usdc, sell_usdc,
                            settled_value, net_shares_up, net_shares_down, trades, winning_side, settled_at,
                            incomplete, pnl_source, has_merge_or_split
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            "0xabc",
                            f"btc-updown-5m-{idx}",
                            f"0xcond{idx}",
                            "BTC",
                            pnl,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1,
                            "Up",
                            dt.datetime.fromtimestamp(now_ts - idx * 3600, dt.timezone.utc).isoformat(),
                            0,
                            "activity_ledger" if idx == 1 else "trade_ledger",
                            1 if idx == 2 else 0,
                        ),
                    )
                store.conn.execute(
                    """
                    INSERT INTO wallet_market_pnl(
                        wallet, market_slug, condition_id, symbol, realized_pnl, buy_usdc, sell_usdc,
                        settled_value, net_shares_up, net_shares_down, trades, winning_side, settled_at,
                        incomplete, pnl_source, has_merge_or_split
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "0xabc",
                        "btc-updown-5m-bad",
                        "0xbad",
                        "BTC",
                        99.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1,
                        "Up",
                        dt.datetime.fromtimestamp(now_ts - 1800, dt.timezone.utc).isoformat(),
                        1,
                        "activity_ledger",
                        0,
                    ),
                )
                store.conn.commit()
                statements: list[str] = []
                store.conn.set_trace_callback(statements.append)
                metrics = store.wallet_observed_pnl_metrics("0xabc", now_ts=now_ts)
            finally:
                store.conn.set_trace_callback(None)
                store.close()

        pnl_queries = [sql for sql in statements if "FROM wallet_market_pnl" in sql]
        self.assertEqual(len(pnl_queries), 1)
        self.assertAlmostEqual(metrics["pnl_30d"], 14.0)
        self.assertEqual(metrics["wins_7d"], 3)
        self.assertEqual(metrics["losses_7d"], 1)
        self.assertEqual(metrics["incomplete_settled_markets_7d"], 1)
        self.assertEqual(metrics["activity_ledger_markets_7d"], 1)
        self.assertEqual(metrics["merge_or_split_markets_7d"], 1)
        self.assertAlmostEqual(metrics["top1_concentration"], round(10.0 / 16.0, 6))
        self.assertAlmostEqual(metrics["top3_concentration"], 1.0)


if __name__ == "__main__":
    unittest.main()
