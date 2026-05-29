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


_DUAL_TRACE_FEATURE_KEYS = {
    "elapsed_sec",
    "top_pair_cost",
    "maker_pair_cost",
    "execution_style",
    "target_pair_notional_usdc",
    "max_pair_cost",
    "max_unpaired_price",
    "max_quote_spread",
    "max_quote_book_age_ms",
    "min_quote_bid_depth_usdc",
    "dynamic_inventory_imbalance_limit",
    "effective_deficit_side",
    "current_pair_avg",
    "projected_pair_avg",
    "working_deficit_side",
    "missing_filled_side",
    "current_pair_avg_basis",
    "projected_pair_avg_basis",
    "current_imbalance_ratio",
    "projected_imbalance_ratio",
    "sizing_mode",
    "target_pair_shares_per_side",
    "current_up_shares",
    "current_down_shares",
    "working_up_shares",
    "working_down_shares",
    "dual_build_abs_bid_diff",
    "dual_build_max_abs_bid_diff",
    "strategy_profile",
    "terminal_stop_sec",
}


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

    def _pending_outcomes(self, history: StrategyHistory, market_slug: str) -> set[str]:
        return {
            intent.outcome
            for intent in history.pending_intents
            if intent.market_slug == market_slug and intent.intent == "BUY"
        }

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

    def _x32_clip_shares(self, *, elapsed_sec: int, deficit_shares: float) -> float:
        if elapsed_sec >= self.terminal_stop_sec - 30:
            return 5.0
        if deficit_shares < 10.0:
            return 5.0
        return 10.0

    def _x32_quote(self, *, snapshot: StrategySnapshot, outcome: str, deficit_side: str | None, force_rebalance: bool = False) -> tuple[float, str]:
        book = snapshot.book_for_outcome(outcome)
        bid = _safe_float(book.bid)
        ask = _safe_float(book.ask)
        quote_price = bid if self.config.execution_style == "maker" else ask
        quote_source = "maker_quote_at_best_bid"
        if self.config.execution_style == "maker" and outcome == deficit_side:
            quote_source = "maker_rebalance_quote"
            if force_rebalance or snapshot.elapsed_sec >= int(self.config.rebalance_start_sec):
                quote_price = min(
                    ask,
                    float(self.config.max_price),
                    quote_price + float(self.config.tick_size) * max(0, int(self.config.maker_rebalance_ticks)),
                )
        return quote_price, quote_source

    def evaluate_with_trace(self, snapshot: StrategySnapshot, history: StrategyHistory) -> EvaluationTrace:
        return self.evaluate_many_with_trace(snapshot, history)

    def evaluate_many_with_trace(self, snapshot: StrategySnapshot, history: StrategyHistory) -> EvaluationTrace:
        base_features = self._trace_base_features(snapshot, history)
        intents = self.evaluate_many(snapshot, history)
        if intents:
            merged_features = dict(base_features)
            if len(intents) == 1:
                merged_features.update(intents[0].features)
            else:
                merged_features.update({key: value for key, value in intents[0].features.items() if key in _DUAL_TRACE_FEATURE_KEYS})
            return EvaluationTrace(decision="intent", intent=intents[0], intents=tuple(intents), features=merged_features)
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
            "dual_build_abs_bid_diff": self._dual_build_gap(snapshot),
            "dual_build_max_abs_bid_diff": self.config.dual_build_max_abs_bid_diff,
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

    def _dual_build_gap(self, snapshot: StrategySnapshot) -> float | None:
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        if up_bid <= 0 or down_bid <= 0:
            return None
        return round(abs(up_bid - down_bid), 6)

    def _candidate_skip_reason(self, snapshot: StrategySnapshot, history: StrategyHistory, maker_pair_cost: float) -> str:
        target_pair_shares = self._target_pair_shares(maker_pair_cost)
        _filled_inventory, current_inventory = self._inventory_sets(history, snapshot.market_slug)
        pending_outcomes = self._pending_outcomes(history, snapshot.market_slug)
        filled_shares = {outcome: _filled_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_shares = {outcome: current_inventory[outcome][0] for outcome in ("Up", "Down")}
        current_cost = {outcome: current_inventory[outcome][1] for outcome in ("Up", "Down")}
        current_imbalance = _imbalance_ratio(current_shares["Up"], current_shares["Down"])
        imbalance_limit = _dynamic_imbalance_limit(self.config, snapshot.elapsed_sec)
        deficit_side = "Up" if current_shares["Up"] < current_shares["Down"] else "Down" if current_shares["Down"] < current_shares["Up"] else None
        missing_filled_side = self._missing_filled_side(filled_shares)
        dual_gate = self._dual_build_gate(
            snapshot=snapshot,
            target_pair_shares=target_pair_shares,
            current_shares=current_shares,
            current_imbalance=current_imbalance,
            deficit_side=deficit_side,
            missing_filled_side=missing_filled_side,
        )
        if dual_gate["blocked_by_gap"]:
            return "dual_build_gap_above_cap"
        last_reason = "no_candidate"

        for outcome in ("Up", "Down"):
            if outcome in pending_outcomes:
                last_reason = "pending_outcome_blocked"
                continue
            if missing_filled_side is not None and outcome != missing_filled_side:
                if last_reason != "pending_outcome_blocked":
                    last_reason = "awaiting_opposite_side_fill"
                continue
            book = snapshot.book_for_outcome(outcome)
            ask = _safe_float(book.ask)
            if ask <= 0 or ask > self.config.max_price:
                last_reason = "invalid_or_expensive_ask"
                continue
            effective_deficit_side = missing_filled_side or deficit_side
            quote_price, quote_source = self._x32_quote(
                snapshot=snapshot,
                outcome=outcome,
                deficit_side=effective_deficit_side,
                force_rebalance=missing_filled_side == outcome,
            )
            if quote_price <= 0 or quote_price > self.config.max_price:
                last_reason = "invalid_maker_quote"
                continue
            deficit_shares = target_pair_shares - current_shares[outcome]
            if deficit_shares <= 1e-9:
                last_reason = "deficit_satisfied"
                continue
            clip_shares = self._x32_clip_shares(elapsed_sec=snapshot.elapsed_sec, deficit_shares=deficit_shares)
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
            if (
                missing_filled_side is None
                and projected_pair_avg is not None
                and projected_imbalance > imbalance_limit
                and projected_imbalance >= current_imbalance
            ):
                last_reason = "imbalance_limit"
                continue
            return "candidate_available"

        return last_reason

    def _dual_build_gate(
        self,
        *,
        snapshot: StrategySnapshot,
        target_pair_shares: float,
        current_shares: dict[str, float],
        current_imbalance: float,
        deficit_side: str | None,
        missing_filled_side: str | None = None,
    ) -> dict[str, bool | float | None]:
        abs_bid_diff = self._dual_build_gap(snapshot)
        max_dual_gap = getattr(self.config, "dual_build_max_abs_bid_diff", None)
        in_build_phase = snapshot.elapsed_sec < int(getattr(self.config, "build_phase_until_sec", self.config.rebalance_start_sec))
        both_need_inventory = current_shares["Up"] < target_pair_shares and current_shares["Down"] < target_pair_shares
        # This guard intentionally avoids starting a fresh two-leg batch after
        # working inventory has already drifted; equal-size batches then wait
        # until single-leg rebalancing restores the shape.
        near_flat_working = deficit_side is None or current_imbalance <= float(self.config.early_inventory_imbalance_ratio)
        gap_configured = max_dual_gap is not None
        blocked_by_gap = bool(
            in_build_phase
            and both_need_inventory
            and near_flat_working
            and gap_configured
            and abs_bid_diff is not None
            and abs_bid_diff > float(max_dual_gap)
        )
        eligible = bool(
            in_build_phase
            and both_need_inventory
            and near_flat_working
            and missing_filled_side is None
            and gap_configured
            and abs_bid_diff is not None
            and abs_bid_diff <= float(max_dual_gap)
        )
        return {
            "abs_bid_diff": abs_bid_diff,
            "max_dual_gap": max_dual_gap,
            "in_build_phase": in_build_phase,
            "both_need_inventory": both_need_inventory,
            "near_flat_working": near_flat_working,
            "blocked_by_gap": blocked_by_gap,
            "eligible": eligible,
        }

    def evaluate_many(self, snapshot: StrategySnapshot, history: StrategyHistory) -> list[TradeIntent]:
        if snapshot.book_stale or snapshot.elapsed_sec >= self.terminal_stop_sec:
            return []
        checkpoint = _checkpoint_for_elapsed(snapshot.elapsed_sec, self.config.checkpoints)
        if checkpoint is None:
            return []
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        up_bid = _safe_float(snapshot.up.bid)
        down_bid = _safe_float(snapshot.down.bid)
        if up_ask <= 0 or down_ask <= 0 or up_ask > self.config.max_price or down_ask > self.config.max_price:
            return []
        maker_pair_cost = round(up_bid + down_bid, 6) if up_bid > 0 and down_bid > 0 else None
        if maker_pair_cost is None or maker_pair_cost > float(self.config.max_pair_cost):
            return []
        if not self._book_quality_ok(snapshot.up) or not self._book_quality_ok(snapshot.down):
            return []
        target_pair_shares = self._target_pair_shares(maker_pair_cost)
        pending_outcomes = self._pending_outcomes(history, snapshot.market_slug)

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
        missing_filled_side = self._missing_filled_side(filled_shares)
        dual_gate = self._dual_build_gate(
            snapshot=snapshot,
            target_pair_shares=target_pair_shares,
            current_shares=current_shares,
            current_imbalance=current_imbalance,
            deficit_side=deficit_side,
            missing_filled_side=missing_filled_side,
        )
        abs_bid_diff = dual_gate["abs_bid_diff"]
        max_dual_gap = dual_gate["max_dual_gap"]

        if dual_gate["eligible"] and abs_bid_diff is not None and not pending_outcomes:
            dual_intents = self._dual_build_intents(
                snapshot=snapshot,
                checkpoint=checkpoint,
                maker_pair_cost=maker_pair_cost,
                target_pair_shares=target_pair_shares,
                filled_shares=filled_shares,
                filled_cost=filled_cost,
                current_shares=current_shares,
                current_cost=current_cost,
                current_pair_avg=current_pair_avg,
                current_imbalance=current_imbalance,
                imbalance_limit=imbalance_limit,
                abs_bid_diff=abs_bid_diff,
            )
            if dual_intents:
                return dual_intents
        if dual_gate["blocked_by_gap"]:
            if deficit_side is None:
                return []

        candidates = []
        for outcome in ("Up", "Down"):
            if outcome in pending_outcomes:
                continue
            if missing_filled_side is not None and outcome != missing_filled_side:
                continue
            book = snapshot.book_for_outcome(outcome)
            ask = _safe_float(book.ask)
            if ask <= 0 or ask > self.config.max_price:
                continue
            effective_deficit_side = missing_filled_side or deficit_side
            quote_price, quote_source = self._x32_quote(
                snapshot=snapshot,
                outcome=outcome,
                deficit_side=effective_deficit_side,
                force_rebalance=missing_filled_side == outcome,
            )
            if quote_price <= 0 or quote_price > self.config.max_price:
                continue
            deficit_shares = target_pair_shares - current_shares[outcome]
            if deficit_shares <= 1e-9:
                continue
            clip_shares = self._x32_clip_shares(elapsed_sec=snapshot.elapsed_sec, deficit_shares=deficit_shares)
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
            if (
                missing_filled_side is None
                and projected_pair_avg is not None
                and projected_imbalance > imbalance_limit
                and projected_imbalance >= current_imbalance
            ):
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
            return []
        _imbalance_improvement, _cheapness, deficit_shares, outcome, fill, expected_price, order_notional, order_shares, clip_shares, projected_pair_avg, projected_imbalance, _outcome_tiebreaker = max(
            candidates,
            key=lambda item: (item[0], item[1], item[2], item[11]),
        )
        return [TradeIntent(
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
                "effective_deficit_side": effective_deficit_side,
                "working_deficit_side": deficit_side,
                "missing_filled_side": missing_filled_side,
                "current_pair_avg": round(current_pair_avg, 6) if current_pair_avg is not None else None,
                "projected_pair_avg": round(projected_pair_avg, 6) if projected_pair_avg is not None else None,
                "current_pair_avg_basis": "filled_inventory",
                "projected_pair_avg_basis": "working_inventory_plus_order",
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
                "quote_source": fill.get("source"),
                "dual_build_abs_bid_diff": abs_bid_diff,
                "dual_build_max_abs_bid_diff": max_dual_gap,
                "quote_level_size_shares": _safe_float(snapshot.book_for_outcome(outcome).bid_size),
                "strategy_profile": "x32_pair_cost_inventory",
                "terminal_stop_sec": self.terminal_stop_sec,
            },
        )]

    def _dual_build_intents(
        self,
        *,
        snapshot: StrategySnapshot,
        checkpoint: int,
        maker_pair_cost: float,
        target_pair_shares: float,
        filled_shares: dict[str, float],
        filled_cost: dict[str, float],
        current_shares: dict[str, float],
        current_cost: dict[str, float],
        current_pair_avg: float | None,
        current_imbalance: float,
        imbalance_limit: float,
        abs_bid_diff: float,
    ) -> list[TradeIntent]:
        quotes: dict[str, tuple[float, dict, float, float, float]] = {}
        deficits = {outcome: target_pair_shares - current_shares[outcome] for outcome in ("Up", "Down")}
        batch_shares = min(
            self._x32_clip_shares(elapsed_sec=snapshot.elapsed_sec, deficit_shares=deficits["Up"]),
            self._x32_clip_shares(elapsed_sec=snapshot.elapsed_sec, deficit_shares=deficits["Down"]),
            deficits["Up"],
            deficits["Down"],
        )
        if batch_shares <= 1e-9:
            return []
        projected_shares = dict(current_shares)
        projected_cost = dict(current_cost)
        for outcome in ("Up", "Down"):
            quote_price, quote_source = self._x32_quote(snapshot=snapshot, outcome=outcome, deficit_side=None)
            if quote_price <= 0 or quote_price > float(self.config.max_price):
                return []
            order_notional = round(batch_shares * quote_price, 6)
            if order_notional + 1e-9 < float(self.config.min_order_usdc):
                return []
            fill, expected_price = _maker_quote_at_price(order_notional, quote_price, source=quote_source)
            if fill is None or expected_price <= 0 or expected_price > float(self.config.max_price):
                return []
            quotes[outcome] = (expected_price, fill, order_notional, batch_shares, quote_price)
            projected_shares[outcome] += batch_shares
            projected_cost[outcome] += order_notional
        projected_up_avg = _avg_price(projected_cost["Up"], projected_shares["Up"])
        projected_down_avg = _avg_price(projected_cost["Down"], projected_shares["Down"])
        if projected_up_avg is None or projected_down_avg is None:
            return []
        projected_pair_avg = projected_up_avg + projected_down_avg
        if projected_pair_avg > float(self.config.max_pair_cost):
            return []
        projected_imbalance = _imbalance_ratio(projected_shares["Up"], projected_shares["Down"])
        if projected_imbalance > imbalance_limit and projected_imbalance >= current_imbalance:
            return []

        intents: list[TradeIntent] = []
        up_ask = _safe_float(snapshot.up.ask)
        down_ask = _safe_float(snapshot.down.ask)
        for outcome in ("Up", "Down"):
            expected_price, fill, order_notional, order_shares, _quote_price = quotes[outcome]
            intents.append(
                TradeIntent(
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
                        "deficit_side": None,
                        "effective_deficit_side": None,
                        "working_deficit_side": None,
                        "missing_filled_side": None,
                        "current_pair_avg": round(current_pair_avg, 6) if current_pair_avg is not None else None,
                        "projected_pair_avg": round(projected_pair_avg, 6),
                        "current_pair_avg_basis": "filled_inventory",
                        "projected_pair_avg_basis": "working_inventory_plus_batch",
                        "current_imbalance_ratio": round(current_imbalance, 6),
                        "projected_imbalance_ratio": round(projected_imbalance, 6),
                        "sizing_mode": "dual_build_equal_clip",
                        "order_shares": round(order_shares, 6),
                        "clip_shares": order_shares,
                        "target_pair_shares_per_side": target_pair_shares,
                        "current_up_shares": round(filled_shares["Up"], 6),
                        "current_down_shares": round(filled_shares["Down"], 6),
                        "working_up_shares": round(current_shares["Up"], 6),
                        "working_down_shares": round(current_shares["Down"], 6),
                        "deficit_shares": round(deficits[outcome], 6),
                        "book_fill": fill,
                        "quote_source": fill.get("source"),
                        "dual_build_abs_bid_diff": abs_bid_diff,
                        "dual_build_max_abs_bid_diff": self.config.dual_build_max_abs_bid_diff,
                        "quote_level_size_shares": _safe_float(snapshot.book_for_outcome(outcome).bid_size),
                        "strategy_profile": "x32_pair_cost_inventory",
                        "terminal_stop_sec": self.terminal_stop_sec,
                    },
                )
            )
        return intents

    def evaluate(self, snapshot: StrategySnapshot, history: StrategyHistory) -> TradeIntent | None:
        intents = self.evaluate_many(snapshot, history)
        return intents[0] if intents else None

    def _missing_filled_side(self, filled_shares: dict[str, float]) -> str | None:
        has_up = filled_shares["Up"] > 1e-9
        has_down = filled_shares["Down"] > 1e-9
        if has_up and not has_down:
            return "Down"
        if has_down and not has_up:
            return "Up"
        return None


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
        "early_inventory_imbalance_ratio": 0.30,
        "mid_inventory_imbalance_ratio": 0.12,
        "late_inventory_imbalance_ratio": 0.06,
        "final_inventory_imbalance_ratio": 0.05,
        "rebalance_start_sec": 240,
        "min_order_usdc": 1.0,
        "max_quote_spread": 0.02,
        "max_quote_book_age_ms": 50.0,
        "min_quote_bid_depth_usdc": 20.0,
        "dual_build_max_abs_bid_diff": 0.60,
        "build_phase_until_sec": 240,
        "execution_style": "maker",
        "one_trade_per_market": False,
    }
    defaults.update({key: value for key, value in overrides.items() if value is not None and value != "__use_default__"})
    if "dual_build_max_abs_bid_diff" in overrides and overrides["dual_build_max_abs_bid_diff"] != "__use_default__":
        defaults["dual_build_max_abs_bid_diff"] = overrides["dual_build_max_abs_bid_diff"]
    return PathStrategyConfig(**defaults)
