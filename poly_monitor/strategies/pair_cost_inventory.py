from __future__ import annotations

from poly_monitor.path_strategy import (
    PathStrategyConfig,
    WalletPathStrategy,
    _avg_price,
    _checkpoint_for_elapsed,
    _dynamic_imbalance_limit,
    _imbalance_ratio,
    _maker_quote_at_price,
    _safe_float,
)
from poly_monitor.strategy_runtime import EvaluationTrace, StrategyHistory, StrategySnapshot, TradeIntent


class X32PairCostInventoryStrategy(WalletPathStrategy):
    strategy_name = "x32_pair_cost_inventory_v0"
    one_trade_per_market = False
    terminal_stop_sec = 300

    def _target_pair_shares(self, maker_pair_cost: float | None) -> float:
        if maker_pair_cost is None or maker_pair_cost <= 0:
            return 0.0
        # X32 sizing deliberately ignores target_pair_shares_per_side; scale
        # comes from our configured per-market notional budget.
        return float(self.config.target_pair_notional_usdc) / maker_pair_cost

    def _filled_inventory(self, history: StrategyHistory, market_slug: str, outcome: str) -> tuple[float, float]:
        shares = 0.0
        cost = 0.0
        # In the paper/maker replay contract, emitted intents are confirmed fills.
        # Resting pending quotes live in history.pending_intents and are excluded.
        for intent in history.emitted_intents:
            if intent.market_slug != market_slug or intent.outcome != outcome or intent.intent != "BUY":
                continue
            if intent.expected_price > 0:
                shares += intent.notional_usdc / intent.expected_price
                cost += intent.notional_usdc
        return shares, cost

    def _pending_inventory(self, history: StrategyHistory, market_slug: str, outcome: str) -> tuple[float, float]:
        shares = 0.0
        cost = 0.0
        for intent in history.pending_intents:
            if intent.market_slug != market_slug or intent.outcome != outcome or intent.intent != "BUY":
                continue
            if intent.expected_price > 0:
                shares += intent.notional_usdc / intent.expected_price
                cost += intent.notional_usdc
        return shares, cost

    def _inventory_sets(self, history: StrategyHistory, market_slug: str) -> tuple[dict[str, tuple[float, float]], dict[str, tuple[float, float]]]:
        filled = {outcome: self._filled_inventory(history, market_slug, outcome) for outcome in ("Up", "Down")}
        pending = {outcome: self._pending_inventory(history, market_slug, outcome) for outcome in ("Up", "Down")}
        working = {
            outcome: (filled[outcome][0] + pending[outcome][0], filled[outcome][1] + pending[outcome][1])
            for outcome in ("Up", "Down")
        }
        return filled, working

    def _book_quality_ok(self, book) -> bool:
        return self._book_quality_reason(book) is None

    def _book_quality_reason(self, book) -> str | None:
        if self.config.max_quote_spread is not None:
            spread = _safe_float(book.spread, -1.0)
            if spread < 0 or spread > float(self.config.max_quote_spread):
                return "wide_spread"
        if self.config.max_quote_book_age_ms is not None:
            age = _safe_float(book.book_age_ms, -1.0)
            if age < 0 or age > float(self.config.max_quote_book_age_ms):
                return "stale_quote_book"
        if self.config.min_quote_bid_depth_usdc is not None:
            depth = _safe_float(book.bid_depth_usdc, -1.0)
            if depth < float(self.config.min_quote_bid_depth_usdc):
                return "shallow_bid_depth"
        return None

    def evaluate_with_trace(self, snapshot: StrategySnapshot, history: StrategyHistory) -> EvaluationTrace:
        base_features = self._trace_base_features(snapshot, history)
        intent = self.evaluate(snapshot, history)
        if intent is not None:
            return EvaluationTrace(decision="intent", intent=intent, features={**base_features, **intent.features})
        return EvaluationTrace(decision="skip", skip_reason=self._skip_reason(snapshot, history), features=base_features)

    def _trace_base_features(self, snapshot: StrategySnapshot, history: StrategyHistory) -> dict:
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        maker_pair_cost = round(up_bid + down_bid, 6) if up_bid > 0 and down_bid > 0 else None
        filled_inventory, working_inventory = self._inventory_sets(history, snapshot.market_slug)
        up_avg = _avg_price(filled_inventory["Up"][1], filled_inventory["Up"][0])
        down_avg = _avg_price(filled_inventory["Down"][1], filled_inventory["Down"][0])
        working_up_avg = _avg_price(working_inventory["Up"][1], working_inventory["Up"][0])
        working_down_avg = _avg_price(working_inventory["Down"][1], working_inventory["Down"][0])
        return {
            "top_pair_cost": round(up_ask + down_ask, 6) if up_ask > 0 and down_ask > 0 else None,
            "maker_pair_cost": maker_pair_cost,
            "max_pair_cost": float(self.config.max_pair_cost),
            "quote_quality": self._quote_quality(snapshot),
            "inventory": {
                "up_shares": round(filled_inventory["Up"][0], 6),
                "up_cost": round(filled_inventory["Up"][1], 6),
                "up_avg": round(up_avg, 6) if up_avg is not None else None,
                "down_shares": round(filled_inventory["Down"][0], 6),
                "down_cost": round(filled_inventory["Down"][1], 6),
                "down_avg": round(down_avg, 6) if down_avg is not None else None,
                "pair_avg": round(up_avg + down_avg, 6) if up_avg is not None and down_avg is not None else None,
                "working_up_shares": round(working_inventory["Up"][0], 6),
                "working_down_shares": round(working_inventory["Down"][0], 6),
                "working_pair_avg": round(working_up_avg + working_down_avg, 6) if working_up_avg is not None and working_down_avg is not None else None,
            },
            "pending_order_count": len([intent for intent in history.pending_intents if intent.market_slug == snapshot.market_slug]),
        }

    def _quote_quality(self, snapshot: StrategySnapshot) -> dict:
        return {
            "max_quote_spread": self.config.max_quote_spread,
            "max_quote_book_age_ms": self.config.max_quote_book_age_ms,
            "min_quote_bid_depth_usdc": self.config.min_quote_bid_depth_usdc,
            "up_reason": self._book_quality_reason(snapshot.up),
            "down_reason": self._book_quality_reason(snapshot.down),
            "pass": self._book_quality_ok(snapshot.up) and self._book_quality_ok(snapshot.down),
        }

    def _skip_reason(self, snapshot: StrategySnapshot, history: StrategyHistory) -> str:
        if snapshot.book_stale:
            return "book_stale"
        if snapshot.elapsed_sec >= self.terminal_stop_sec:
            return "terminal_stop"
        if _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints) is None:
            return "checkpoint_not_ready"
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        if up_ask <= 0 or down_ask <= 0 or up_ask > self.config.max_price or down_ask > self.config.max_price:
            return "invalid_or_expensive_ask"
        maker_pair_cost = round(up_bid + down_bid, 6) if up_bid > 0 and down_bid > 0 else None
        if maker_pair_cost is None:
            return "pair_cost_missing"
        if maker_pair_cost > float(self.config.max_pair_cost):
            return "pair_cost_above_max"
        quality_reason = self._book_quality_reason(snapshot.up) or self._book_quality_reason(snapshot.down)
        if quality_reason:
            return quality_reason
        return self._candidate_skip_reason(snapshot, history, maker_pair_cost)

    def _candidate_skip_reason(self, snapshot: StrategySnapshot, history: StrategyHistory, maker_pair_cost: float) -> str:
        target_pair_shares = self._target_pair_shares(maker_pair_cost)
        _filled_inventory, current_inventory = self._inventory_sets(history, snapshot.market_slug)
        current_shares = {outcome: current_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_cost = {outcome: current_inventory[outcome][1] for outcome in ("Up", "Down")}
        current_imbalance = _imbalance_ratio(current_shares["Up"], current_shares["Down"])
        imbalance_limit = _dynamic_imbalance_limit(self.config, snapshot.elapsed_sec)
        deficit_side = "Up" if current_shares["Up"] < current_shares["Down"] else "Down" if current_shares["Down"] < current_shares["Up"] else None
        last_reason = "no_candidate"

        for outcome in ("Up", "Down"):
            book = snapshot.book_for_outcome(outcome)
            ask = _safe_float(book.ask)
            bid = _safe_float(book.bid)
            if ask <= 0 or ask > self.config.max_price:
                last_reason = "invalid_or_expensive_ask"
                continue
            quote_source = "maker_quote_at_best_bid"
            quote_price = bid if self.config.execution_style == "maker" else ask
            if self.config.execution_style == "maker" and outcome == deficit_side:
                quote_source = "maker_rebalance_quote"
            if quote_price <= 0 or quote_price > self.config.max_price:
                last_reason = "invalid_maker_quote"
                continue
            deficit_shares = target_pair_shares - current_shares[outcome]
            if deficit_shares <= 1e-9:
                last_reason = "deficit_satisfied"
                continue
            clip_shares = 5.0 if quote_price > 0.50 or deficit_shares < 10.0 else 10.0
            order_shares = min(clip_shares, deficit_shares)
            order_notional = round(order_shares * quote_price, 6)
            if order_notional + 1e-9 < float(self.config.min_order_usdc):
                last_reason = "below_min_order"
                continue
            fill, expected_price = _maker_quote_at_price(order_notional, quote_price, source=quote_source)
            if fill is None or expected_price <= 0 or expected_price > self.config.max_price:
                last_reason = "invalid_maker_quote"
                continue

            projected_shares = dict(current_shares)
            projected_cost = dict(current_cost)
            projected_shares[outcome] += order_shares
            projected_cost[outcome] += order_notional
            projected_up_avg = _avg_price(projected_cost["Up"], projected_shares["Up"])
            projected_down_avg = _avg_price(projected_cost["Down"], projected_shares["Down"])
            projected_pair_avg = None
            if projected_up_avg is not None and projected_down_avg is not None:
                projected_pair_avg = projected_up_avg + projected_down_avg
                if projected_pair_avg > float(self.config.max_pair_cost):
                    last_reason = "projected_pair_above_cap"
                    continue
            elif expected_price > float(self.config.max_unpaired_price):
                last_reason = "unpaired_price_above_cap"
                continue
            projected_imbalance = _imbalance_ratio(projected_shares["Up"], projected_shares["Down"])
            if projected_pair_avg is not None and projected_imbalance > imbalance_limit and projected_imbalance >= current_imbalance:
                last_reason = "imbalance_limit"
                continue
            return "candidate_available"

        return last_reason

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        if snapshot.book_stale or snapshot.elapsed_sec >= self.terminal_stop_sec:
            return None
        checkpoint = _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints)
        if checkpoint is None:
            return None
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        if up_ask <= 0 or down_ask <= 0 or up_ask > self.config.max_price or down_ask > self.config.max_price:
            return None
        maker_pair_cost = round(up_bid + down_bid, 6) if up_bid > 0 and down_bid > 0 else None
        if maker_pair_cost is None or maker_pair_cost > float(self.config.max_pair_cost):
            return None
        if not self._book_quality_ok(snapshot.up) or not self._book_quality_ok(snapshot.down):
            return None
        target_pair_shares = self._target_pair_shares(maker_pair_cost)

        filled_inventory, working_inventory = self._inventory_sets(history, snapshot.market_slug)
        filled_shares = {outcome: filled_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_shares = {outcome: working_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_cost = {outcome: working_inventory[outcome][1] for outcome in ("Up", "Down")}
        filled_cost = {outcome: filled_inventory[outcome][1] for outcome in ("Up", "Down")}
        current_up_avg = _avg_price(filled_cost["Up"], filled_shares["Up"])
        current_down_avg = _avg_price(filled_cost["Down"], filled_shares["Down"])
        current_pair_avg = current_up_avg + current_down_avg if current_up_avg is not None and current_down_avg is not None else None
        current_imbalance = _imbalance_ratio(current_shares["Up"], current_shares["Down"])
        imbalance_limit = _dynamic_imbalance_limit(self.config, snapshot.elapsed_sec)
        deficit_side = "Up" if current_shares["Up"] < current_shares["Down"] else "Down" if current_shares["Down"] < current_shares["Up"] else None

        candidates = []
        for outcome in ("Up", "Down"):
            book = snapshot.book_for_outcome(outcome)
            ask = _safe_float(book.ask)
            bid = _safe_float(book.bid)
            if ask <= 0 or ask > self.config.max_price:
                continue
            quote_source = "maker_quote_at_best_bid"
            quote_price = bid if self.config.execution_style == "maker" else ask
            if self.config.execution_style == "maker" and outcome == deficit_side:
                quote_source = "maker_rebalance_quote"
            if quote_price <= 0 or quote_price > self.config.max_price:
                continue
            deficit_shares = target_pair_shares - current_shares[outcome]
            if deficit_shares <= 1e-9:
                continue
            clip_shares = 5.0 if quote_price > 0.50 or deficit_shares < 10.0 else 10.0
            order_shares = min(clip_shares, deficit_shares)
            order_notional = round(order_shares * quote_price, 6)
            if order_notional + 1e-9 < float(self.config.min_order_usdc):
                continue
            fill, expected_price = _maker_quote_at_price(order_notional, quote_price, source=quote_source)
            if fill is None or expected_price <= 0 or expected_price > self.config.max_price:
                continue

            projected_shares = dict(current_shares)
            projected_cost = dict(current_cost)
            projected_shares[outcome] += order_shares
            projected_cost[outcome] += order_notional
            projected_up_avg = _avg_price(projected_cost["Up"], projected_shares["Up"])
            projected_down_avg = _avg_price(projected_cost["Down"], projected_shares["Down"])
            projected_pair_avg = None
            if projected_up_avg is not None and projected_down_avg is not None:
                projected_pair_avg = projected_up_avg + projected_down_avg
                if projected_pair_avg > float(self.config.max_pair_cost):
                    continue
            elif expected_price > float(self.config.max_unpaired_price):
                continue
            projected_imbalance = _imbalance_ratio(projected_shares["Up"], projected_shares["Down"])
            if projected_pair_avg is not None and projected_imbalance > imbalance_limit and projected_imbalance >= current_imbalance:
                continue
            candidates.append(
                (
                    current_imbalance - projected_imbalance,
                    -expected_price,
                    deficit_shares,
                    outcome,
                    fill,
                    expected_price,
                    order_notional,
                    order_shares,
                    clip_shares,
                    projected_pair_avg,
                    projected_imbalance,
                    1 if outcome == "Up" else 0,
                )
            )
        if not candidates:
            return None
        _imbalance_improvement, _cheapness, deficit_shares, outcome, fill, expected_price, order_notional, order_shares, clip_shares, projected_pair_avg, projected_imbalance, _outcome_tiebreaker = max(
            candidates,
            key=lambda item: (item[0], item[1], item[2], item[11]),
        )
        return TradeIntent(
            strategy_name=self.strategy_name,
            wallet=self.config.wallet.lower(),
            market_slug=snapshot.market_slug,
            sampled_ts=snapshot.sampled_ts,
            checkpoint_sec=checkpoint,
            intent="BUY",
            outcome=outcome,
            notional_usdc=round(float(order_notional), 6),
            max_price=float(self.config.max_price),
            expected_price=round(expected_price, 6),
            symbol=snapshot.symbol,
            reason="x32_pair_cost_inventory",
            features={
                "elapsed_sec": snapshot.elapsed_sec,
                "top_pair_cost": round(up_ask + down_ask, 6),
                "maker_pair_cost": maker_pair_cost,
                "execution_style": self.config.execution_style,
                "target_pair_notional_usdc": float(self.config.target_pair_notional_usdc),
                "max_pair_cost": float(self.config.max_pair_cost),
                "max_unpaired_price": float(self.config.max_unpaired_price),
                "max_quote_spread": self.config.max_quote_spread,
                "max_quote_book_age_ms": self.config.max_quote_book_age_ms,
                "min_quote_bid_depth_usdc": self.config.min_quote_bid_depth_usdc,
                "dynamic_inventory_imbalance_limit": round(imbalance_limit, 6),
                "deficit_side": deficit_side,
                "current_pair_avg": round(current_pair_avg, 6) if current_pair_avg is not None else None,
                "projected_pair_avg": round(projected_pair_avg, 6) if projected_pair_avg is not None else None,
                "current_imbalance_ratio": round(current_imbalance, 6),
                "projected_imbalance_ratio": round(projected_imbalance, 6),
                "sizing_mode": "fixed_share_clip",
                "order_shares": round(order_shares, 6),
                "clip_shares": clip_shares,
                "target_pair_shares_per_side": target_pair_shares,
                "current_up_shares": round(filled_shares["Up"], 6),
                "current_down_shares": round(filled_shares["Down"], 6),
                "working_up_shares": round(current_shares["Up"], 6),
                "working_down_shares": round(current_shares["Down"], 6),
                "deficit_shares": round(deficit_shares, 6),
                "book_fill": fill,
                "quote_level_size_shares": _safe_float(snapshot.book_for_outcome(outcome).bid_size),
                "strategy_profile": "x32_pair_cost_inventory",
                "terminal_stop_sec": self.terminal_stop_sec,
            },
        )


def x32_default_config(wallet: str, **overrides) -> PathStrategyConfig:
    defaults = {
        "wallet": wallet,
        "checkpoints": (1,),
        "notional_usdc": 5.0,
        "max_price": 0.95,
        "target_pair_notional_usdc": 55.0,
        "target_pair_shares_per_side": None,
        "max_pair_cost": 0.995,
        "max_unpaired_price": 0.70,
        "early_inventory_imbalance_ratio": 1.00,
        "mid_inventory_imbalance_ratio": 0.60,
        "late_inventory_imbalance_ratio": 0.30,
        "final_inventory_imbalance_ratio": 0.12,
        "rebalance_start_sec": 240,
        "min_order_usdc": 1.0,
        "max_quote_spread": 0.02,
        "max_quote_book_age_ms": 50.0,
        "min_quote_bid_depth_usdc": 20.0,
        "execution_style": "maker",
        "one_trade_per_market": False,
    }
    defaults.update({key: value for key, value in overrides.items() if value is not None})
    return PathStrategyConfig(**defaults)
