import datetime as dt
import tempfile
import unittest
from pathlib import Path

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
                        "raw_json": '{"type":"TRADE"}',
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
                        "raw_json": '{"type":"MERGE"}',
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
                        "raw_json": '{"type":"REDEEM"}',
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

    def test_watchlist_activity_pnl_counts_merge_and_redeem_cashflows(self):
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

                rows = store.watchlist_market_pnl_rows(wallet)
                metrics = store.wallet_trade_metrics(wallet, now_ts=1779699600)
            finally:
                store.close()

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertAlmostEqual(row["realized_pnl"], 16.115315)
        self.assertAlmostEqual(row["merge_usdc"], 163.0)
        self.assertAlmostEqual(row["redeem_usdc"], 5.0)
        self.assertEqual(row["activity_events"], 4)
        self.assertTrue(row["has_merge"])
        self.assertTrue(row["has_redeem"])
        self.assertFalse(row["incomplete"])
        self.assertAlmostEqual(metrics["pnl_total"], 16.115315)
        self.assertEqual(metrics["pnl_source"], "watchlist_activity_ledger")

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

    def test_wallet_trade_metrics_counts_time_windows_without_loading_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-1", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "btc-1", now_ts - 200, tx_hash="tx-2"),
                        trade_row("0xabc", "eth-1", now_ts - 2 * 86400, tx_hash="tx-3"),
                        trade_row("0xabc", "sol-1", now_ts - 20 * 86400, tx_hash="tx-4"),
                        trade_row("0xabc", "xrp-1", now_ts - 40 * 86400, tx_hash="tx-5"),
                        trade_row("0xdef", "btc-1", now_ts - 50, tx_hash="tx-6"),
                    ]
                )

                metrics = store.wallet_trade_metrics("0xABC", now_ts=now_ts)

                self.assertEqual(metrics["wallet"], "0xabc")
                self.assertEqual(metrics["trades_24h"], 2)
                self.assertEqual(metrics["markets_24h"], 1)
                self.assertEqual(metrics["trades_7d"], 3)
                self.assertEqual(metrics["markets_7d"], 2)
                self.assertEqual(metrics["trades_30d"], 4)
                self.assertEqual(metrics["markets_30d"], 3)
                self.assertEqual(metrics["historical_trades"], 5)
                self.assertEqual(metrics["historical_markets"], 4)
                self.assertAlmostEqual(metrics["last_active_age_hours"], round(100 / 3600, 3))
            finally:
                store.close()

    def test_wallet_24h_counts_returns_observer_override_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ObserverStore(Path(tmp) / "observer.sqlite")
            try:
                now_ts = 2_000_000
                store.insert_trades(
                    [
                        trade_row("0xabc", "btc-1", now_ts - 100, tx_hash="tx-1"),
                        trade_row("0xabc", "btc-2", now_ts - 200, tx_hash="tx-2"),
                        trade_row("0xabc", "btc-3", now_ts - 2 * 86400, tx_hash="tx-3"),
                    ]
                )

                self.assertEqual(store.wallet_24h_counts("0xABC", now_ts=now_ts), {"trades_24h": 2, "markets_24h": 2})
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


if __name__ == "__main__":
    unittest.main()
