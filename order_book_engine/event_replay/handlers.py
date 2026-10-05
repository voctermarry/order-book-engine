"""Business handlers for every committed event kind.

Each handler is a plain function ``(session, ctx, payload) -> _Applied``:
it receives the live session (for cross-security reads), the commit context
(owning the target security's state, the envelope fields and the book
capture) and the normalized payload, and describes *only* the business
outcome. Id occupancy, sequence advancement, book echo and response
assembly all happen in the shared commit path; validation and dispatch live
in the registry. Adding a new event kind means adding one handler here and
one registry entry — never touching the orchestrator.
"""

from __future__ import annotations

from ..engine import (
    ADD,
    BREAKS_FOUND,
    DUPLICATE_ORDER_ID,
    FIELD_MISMATCH,
    ICEBERG,
    IOC,
    LIMIT,
    INVALID_SCHEMA,
    MISSING_ACTUAL,
    MISSING_EXPECTED,
    RECONCILED,
    REPLACE,
    REPORTED,
    SELL,
    UNKNOWN_ACCOUNT,
)
from .constants import (
    ACCEPTED,
    ALGORITHM_POV,
    ALGORITHM_VWAP,
    DUPLICATE_EXECUTION_PLAN,
    EXECUTION_PLAN_CLOSED,
    MARK_PRICE_MISMATCH,
    PLAN_ACTIVE,
    PLAN_CANCELLED,
    PLAN_COMPLETED,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
    VWAP_SLICE,
    VWAP_START,
    TWAP_CANCEL,
    VWAP_CANCEL,
    POV_CANCEL,
)
from .plan import ExecutionPlan
from .state import _Applied, _CommitContext, _SymbolState


# -- baseline ADD/CANCEL/REPLACE ----------------------------------------------


def apply_baseline(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Run the single-security baseline engine for ADD/CANCEL/REPLACE.

    The shared commit/response path expresses the outcome; the well-formed
    event occupies its id however the business result ends, exactly like the
    baseline's id-occupancy rule.
    """
    state = ctx.state
    _eid, engine_result, reason, trades, _stp, _analysis, _position = (
        state.engine.handle_object_position(payload)
    )
    if reason is None:
        return _Applied(ACCEPTED, result=engine_result, trades=trades)
    # Baseline business rejection codes (UNKNOWN_ORDER, DUPLICATE_ORDER_ID,
    # ...) are preserved verbatim. The replay log commit occupying the id and
    # sequence already happened; the per-symbol engine journal must name
    # exactly the same baseline events for an exported snapshot to restore.
    # Almost every business rejection occupies its engine event id through the
    # regular engine path (the id is spent before the business rule runs). The
    # single pre-commit exception is the REPLACE rule that forbids
    # display_quantity on a plain resting target: the baseline engine rejects
    # it before spending its own journal id, so the replay layer occupies that
    # journal id here, exactly as it does for a pre-matching price-limit
    # rejection. The replay-only id/sequence commit is unaffected either way.
    occupy_engine_id = (
        ctx.event_type == REPLACE
        and reason == INVALID_SCHEMA
        and "display_quantity" in payload
        and state.engine.replace_target_kind(payload["order_id"]) == "plain"
    )
    return _Applied(REJECTED, code=reason, trades=trades,
                    occupy_engine_id=occupy_engine_id)


# -- active per-security price limits ------------------------------------------


def price_limit_rejection(
    payload: dict[str, object],
    event_type: str,
    state: _SymbolState,
) -> str | None:
    """Decide a price-limit breach against the active interval.

    Only the price of a LIMIT/ICEBERG ADD, of a REPLACE and of a LIMIT
    TWAP/VWAP/POV plan is tested against the security's currently active
    closed interval (the static config interval as replaced by accepted
    PRICE_LIMIT_UPDATE events); MARKET orders and MARKET plans are
    exempt. Identifier clashes the baseline reaches first keep precedence
    and are reported as ``None`` here: a reused order id, a replace
    against a non-resting target, a duplicate plan id or a clash over a
    derived child id.
    """
    bounds = state.price_limits
    if bounds is None:
        return None
    lower, upper = bounds

    def out_of_bounds(price: int) -> bool:
        return price < lower or price > upper

    if event_type == ADD:
        order_type = payload["order_type"]
        if order_type not in (LIMIT, ICEBERG):
            return None
        order_id: str = payload["order_id"]
        if state.engine.has_order_id(order_id):
            # The engine would reject DUPLICATE_ORDER_ID first.
            return None
        return PRICE_LIMIT_EXCEEDED if out_of_bounds(payload["price"]) else None

    if event_type == REPLACE:
        order_id = payload["order_id"]
        target_kind = state.engine.replace_target_kind(order_id)
        if target_kind is None:
            # A missing or finished target yields UNKNOWN_ORDER first.
            return None
        if target_kind == "plain" and "display_quantity" in payload:
            # The engine rejects this pre-commit with INVALID_SCHEMA; it
            # must keep precedence and consume neither id nor sequence.
            return None
        return PRICE_LIMIT_EXCEEDED if out_of_bounds(payload["price"]) else None

    if event_type in (TWAP_START, VWAP_START, POV_START):
        if payload["order_type"] != LIMIT:
            # MARKET plans are exempt, exactly like MARKET orders.
            return None
        plan_id: str = payload["plan_id"]
        if plan_id in state.plans:
            # Mirror _register_plan: the plan id clash wins.
            return None
        if event_type == TWAP_START:
            child_count: int = payload["slice_count"]
        elif event_type == VWAP_START:
            child_count = len(payload["volume_weights"])
        else:
            # POV reserves one derived id per unit of its total.
            child_count = payload["total_quantity"]
        for index in range(child_count):
            child_id = f"{plan_id}#{index + 1}"
            if state.engine.has_order_id(child_id) or child_id in state.plan_index:
                # A derived id clash rejects DUPLICATE_ORDER_ID first.
                return None
        return PRICE_LIMIT_EXCEEDED if out_of_bounds(payload["price"]) else None

    return None


# -- parent-order plan handling -------------------------------------------------


def apply_plan(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Apply a structurally valid, in-sequence parent-order command.

    Only the business outcome is described; the shared commit path
    occupies the well-formed event id and advances the symbol sequence,
    including business rejections — the baseline rule that a valid event
    occupies its id whatever the business result.
    """
    event_type = ctx.event_type
    state = ctx.state
    if event_type == TWAP_START:
        result_out = _twap_start(payload, state)
    elif event_type == VWAP_START:
        result_out = _vwap_start(payload, state)
    elif event_type == POV_START:
        result_out = _pov_start(payload, state)
    elif event_type == POV_VOLUME:
        result_out = _plan_volume(payload, state)
    elif event_type in (TWAP_SLICE, VWAP_SLICE):
        result_out = _plan_slice(payload, state)
    elif event_type in (TWAP_CANCEL, VWAP_CANCEL, POV_CANCEL):
        result_out = _plan_cancel(payload, state)
    else:
        result_out = _plan_report(payload, state)

    status, code, plan, slice_info = result_out
    if status != ACCEPTED:
        # A business rejection moves no book; the shared path echoes the
        # untouched book and (from the pre-dispatch level capture) an
        # empty change set.
        return _Applied(REJECTED, code=code)

    if slice_info is None:
        # START, CANCEL and REPORT never move the book.
        trades: list[dict[str, object]] = []
        child_result: str | None = None
        slice_number = child_order_id = None
        target_weight = scheduled_quantity = None
        release_number = None
        pov_volume = False
    else:
        trades = slice_info["trades"]
        child_result = slice_info["engine_result"]
        slice_number = slice_info.get("slice_number")
        child_order_id = slice_info["child_order_id"]
        target_weight = slice_info.get("target_weight")
        scheduled_quantity = slice_info.get("scheduled_quantity")
        release_number = slice_info.get("release_number")
        pov_volume = slice_info.get("pov_volume", False)

    execution_plan = plan.summary(
        slice_number=slice_number,
        child_order_id=child_order_id,
        target_weight=target_weight,
        scheduled_quantity=scheduled_quantity,
        release_number=release_number,
        pov_volume=pov_volume,
    ) if plan is not None else None
    # The released child's engine result keeps its historical place after
    # the book fields; the plan summary always follows it.
    return _Applied(
        ACCEPTED, result=child_result, trades=trades,
        result_after_book=True,
        analysis_key="execution_plan", analysis=execution_plan,
    )


def _twap_start(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Create the TWAP plan; no matching occurs."""
    plan = ExecutionPlan(
        plan_id=payload["plan_id"],
        side=payload["side"],
        order_type=payload["order_type"],
        total_quantity=payload["total_quantity"],
        slice_count=payload["slice_count"],
        benchmark_price=payload["benchmark_price"],
        price=payload.get("price") if payload["order_type"] == LIMIT else None,
        account_id=payload.get("account_id"),
    )
    return _register_plan(state, plan)


def _vwap_start(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Create the VWAP plan; no matching occurs."""
    weights: list[int] = payload["volume_weights"]
    plan = ExecutionPlan(
        plan_id=payload["plan_id"],
        side=payload["side"],
        order_type=payload["order_type"],
        total_quantity=payload["total_quantity"],
        slice_count=len(weights),
        benchmark_price=payload["benchmark_price"],
        price=payload.get("price") if payload["order_type"] == LIMIT else None,
        account_id=payload.get("account_id"),
        algorithm=ALGORITHM_VWAP,
        volume_weights=weights,
    )
    return _register_plan(state, plan)


def _pov_start(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Create the POV plan; no matching occurs."""
    plan = ExecutionPlan(
        plan_id=payload["plan_id"],
        side=payload["side"],
        order_type=payload["order_type"],
        total_quantity=payload["total_quantity"],
        slice_count=0,
        benchmark_price=payload["benchmark_price"],
        price=payload.get("price") if payload["order_type"] == LIMIT else None,
        account_id=payload.get("account_id"),
        algorithm=ALGORITHM_POV,
        participation_bps=payload["participation_bps"],
    )
    return _register_plan(state, plan)


def _plan_volume(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, dict[str, object] | None]:
    """Feed a market volume increment and release one POV child order.

    The cumulative target release is
    ``min(total_quantity, floor(market_volume * participation_bps / 10000))``;
    the event releases the difference between the target and the quantity
    already released, if positive. A zero difference still consumes the
    increment (and the event id/sequence) but submits no child order.
    """
    plan_id: str = payload["plan_id"]
    plan = state.plans.get(plan_id)
    if plan is None:
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    if plan.algorithm != ALGORITHM_POV:
        # A fixed-schedule TWAP/VWAP plan cannot be driven by market
        # volume: no POV plan with this id exists, so this is reported
        # like an unknown plan and moves nothing.
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    if plan.status != PLAN_ACTIVE:
        return REJECTED, EXECUTION_PLAN_CLOSED, None, None

    # A LIMIT plan started inside the band is re-checked against the
    # current active interval before every release, exactly like a TWAP/
    # VWAP slice. The check precedes any state change: on a breach neither
    # the cumulative market volume nor the release state moves, and the
    # reserved derived ids stay reserved.
    if plan.order_type == LIMIT and state.price_limits is not None:
        lower, upper = state.price_limits
        if plan.price < lower or plan.price > upper:
            return REJECTED, PRICE_LIMIT_EXCEEDED, None, None

    increment: int = payload["market_volume_increment"]
    plan.market_volume += increment

    target = min(
        plan.total_quantity,
        plan.market_volume * plan.participation_bps // 10000,
    )
    release_quantity = target - plan.released_quantity
    release_number = plan.released + 1

    if release_quantity == 0:
        # The event succeeds and the market volume is retained, but no
        # child order is submitted and no trade id is spent.
        return ACCEPTED, None, plan, {
            "pov_volume": True,
            "release_number": None,
            "child_order_id": None,
            "engine_result": None,
            "trades": [],
            "release_quantity": 0,
        }

    child_id = plan.child_order_id(release_number)
    child: dict[str, object] = {
        "event_id": child_id,
        "type": ADD,
        "order_id": child_id,
        "side": plan.side,
        "order_type": plan.order_type,
        "quantity": release_quantity,
        "time_in_force": IOC,
    }
    if plan.order_type == LIMIT:
        child["price"] = plan.price
    if plan.account_id is not None:
        child["account_id"] = plan.account_id

    # The child id was reserved at plan start; release it immediately
    # before the baseline ADD spends it through the regular path.
    state.engine.release_order_id(child_id)
    _eid, engine_result, reason, trades, _stp = state.engine.handle_object(child)
    # A correctly synthesized child can only fail on a programming error;
    # fail loudly rather than corrupt the plan counters.
    if reason is not None:  # pragma: no cover - defensive
        raise RuntimeError(f"plan release child order rejected: {reason}")

    traded_quantity = sum(trade["quantity"] for trade in trades)
    plan.released += 1
    plan.released_quantity += release_quantity
    # The released child's original quantity joins the schedule list, in
    # release order; snapshot validation reads it back per child.
    plan.slice_quantities.append(release_quantity)
    plan.filled_quantity += traded_quantity
    plan.notional += sum(trade["price"] * trade["quantity"] for trade in trades)
    if plan.released_quantity == plan.total_quantity:
        plan.status = PLAN_COMPLETED

    return ACCEPTED, None, plan, {
        "pov_volume": True,
        "release_number": release_number,
        "child_order_id": child_id,
        "engine_result": engine_result,
        "trades": trades,
        "release_quantity": release_quantity,
    }


def _register_plan(
    state: _SymbolState, plan: ExecutionPlan
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Register a new plan and reserve its derived ids.

    TWAP and VWAP plans share the per-symbol ``plan_id`` namespace, so a
    duplicate id rejects whatever algorithm started the existing plan.
    Every derived id is reserved up front; a clash with an existing order
    or another plan's derived id rejects the start before anything is
    registered, so the event leaves no plan or reservation behind.
    """
    plan_id = plan.plan_id
    if plan_id in state.plans:
        return REJECTED, DUPLICATE_EXECUTION_PLAN, None, None
    for child_id in plan.child_ids():
        if state.engine.has_order_id(child_id) or child_id in state.plan_index:
            return REJECTED, DUPLICATE_ORDER_ID, None, None
    for child_id in plan.child_ids():
        state.engine.reserve_order_id(child_id)
        state.plan_index[child_id] = plan_id
    state.plans[plan_id] = plan
    return ACCEPTED, None, plan, None


def _plan_slice(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, dict[str, object] | None]:
    """Release the next slice as an IOC child order against current book."""
    plan_id: str = payload["plan_id"]
    plan = state.plans.get(plan_id)
    if plan is None:
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    if plan.algorithm == ALGORITHM_POV:
        # A fixed-schedule slice command cannot drive a volume-driven
        # plan: the id names an existing plan but none of this command's
        # kind, so it is reported like an unknown plan and moves nothing.
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    if plan.status != PLAN_ACTIVE:
        return REJECTED, EXECUTION_PLAN_CLOSED, None, None

    # A LIMIT plan started inside the band is re-checked against the
    # *current* active interval before every release: an intraday
    # PRICE_LIMIT_UPDATE may have moved the band since the start. A
    # breach rejects the SLICE without advancing the slice number or any
    # cumulative counter, and the reserved derived ids stay reserved.
    if plan.order_type == LIMIT and state.price_limits is not None:
        lower, upper = state.price_limits
        if plan.price < lower or plan.price > upper:
            return REJECTED, PRICE_LIMIT_EXCEEDED, None, None

    slice_number = plan.released + 1
    child_id = plan.child_order_id(slice_number)
    quantity = plan.slice_quantities[plan.released]

    child: dict[str, object] = {
        "event_id": child_id,
        "type": ADD,
        "order_id": child_id,
        "side": plan.side,
        "order_type": plan.order_type,
        "quantity": quantity,
        "time_in_force": IOC,
    }
    if plan.order_type == LIMIT:
        child["price"] = plan.price
    if plan.account_id is not None:
        child["account_id"] = plan.account_id

    # The child id was reserved at plan start; release it immediately
    # before the baseline ADD spends it through the regular path.
    state.engine.release_order_id(child_id)
    _eid, engine_result, reason, trades, _stp = state.engine.handle_object(child)
    # A correctly synthesized child can only fail on a programming error;
    # fail loudly rather than corrupt the plan counters.
    if reason is not None:  # pragma: no cover - defensive
        raise RuntimeError(f"plan slice child order rejected: {reason}")

    traded_quantity = sum(trade["quantity"] for trade in trades)
    plan.released += 1
    plan.released_quantity += quantity
    plan.filled_quantity += traded_quantity
    plan.notional += sum(trade["price"] * trade["quantity"] for trade in trades)
    if plan.released == plan.slice_count:
        plan.status = PLAN_COMPLETED

    slice_info = {
        "slice_number": slice_number,
        "child_order_id": child_id,
        "engine_result": engine_result,
        "trades": trades,
    }
    if plan.algorithm == ALGORITHM_VWAP:
        # A successful VWAP slice also reports its scheduled bucket.
        slice_info["target_weight"] = plan.volume_weights[slice_number - 1]
        slice_info["scheduled_quantity"] = quantity
    return ACCEPTED, None, plan, slice_info


def _plan_cancel(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Count the unreleased quantity as cancelled and close the plan.

    Purely a plan-state change: no resting order is touched, no trade id
    or historical trade changes.
    """
    plan_id: str = payload["plan_id"]
    plan = state.plans.get(plan_id)
    if plan is None:
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    if plan.status != PLAN_ACTIVE:
        return REJECTED, EXECUTION_PLAN_CLOSED, None, None

    unreleased = plan.total_quantity - plan.released_quantity
    plan.cancelled_quantity += unreleased
    plan.status = PLAN_CANCELLED
    return ACCEPTED, None, plan, None


def _plan_report(
    payload: dict[str, object], state: _SymbolState
) -> tuple[str, str | None, ExecutionPlan | None, None]:
    """Read-only cumulative plan summary; closed plans stay queryable."""
    plan_id: str = payload["plan_id"]
    plan = state.plans.get(plan_id)
    if plan is None:
        return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
    return ACCEPTED, None, plan, None


# -- intraday price-limit adjustment --------------------------------------------


def apply_price_limit_update(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Replace the security's active price-limit interval wholesale.

    Purely a replay-layer state change: no order, trade, plan or book is
    touched, resting orders outside the new interval keep their queue
    priority and may still become makers, and no trade id is spent. The
    shared commit path occupies the event id and advances the sequence;
    the id lives solely in the replay log, exactly like a parent-order
    command id. Re-applying the current bounds is a successful update,
    not a conflict.
    """
    state = ctx.state
    lower_price: int = payload["lower_price"]
    upper_price: int = payload["upper_price"]
    state.price_limits = (lower_price, upper_price)
    return _Applied(
        ACCEPTED, result=PRICE_LIMIT_UPDATED,
        analysis_key="active_price_limits",
        analysis={"lower_price": lower_price, "upper_price": upper_price},
    )


# -- per-order execution report --------------------------------------------------


def apply_execution_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only cumulative execution query for one order.

    The query only reads this security's order records and trade journal:
    it never matches, never replenishes an iceberg slice, never releases
    a plan slice and never moves an order, a queue, the trade log, an
    account set, a plan or the next trade id. Every accepted order id of
    this security is queryable — resting, filled or cancelled, including
    iceberg orders, orders replaced under the same id and released
    TWAP/VWAP/POV child orders; parent plan ids and not-yet-released
    derived ids are not orders, and an id accepted on another security
    never resolves here. The shared commit path occupies the query id and
    advances the symbol sequence, including the ``UNKNOWN_ORDER``
    business rejection; the id lives solely in the replay log and never
    enters the engine journal. The analysis itself is produced by the
    baseline engine's own report routine, so its content matches the
    single-security entry point exactly.
    """
    state = ctx.state
    _eid, result, reason, _trades, _stp, analysis = (
        state.engine._execution_report(payload["event_id"], payload)
    )
    if reason is not None:
        # Business rejection (UNKNOWN_ORDER): the book is untouched.
        return _Applied(REJECTED, code=reason)
    return _Applied(
        ACCEPTED, result=result,
        analysis_key="execution_analysis", analysis=analysis,
    )


# -- per-symbol book-impact what-if report ---------------------------------------


def apply_impact_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only what-if market-order impact query for one book.

    The analysis is produced by the baseline engine's own impact routine,
    so its content matches the single-security JSON Lines entry point
    exactly: the walk follows price-time priority at maker prices, uses
    only an iceberg's current visible slice and requeues a replenished
    slice at its level tail, reports the precise fraction figures and
    never mutates anything. The simulated order is anonymous, so
    self-trade prevention never applies, and the active price-limit
    interval never blocks it. A first-seen symbol answers against its
    empty book successfully. The shared commit path occupies the query
    id and advances the symbol sequence; the id lives solely in the
    replay log, exactly like an EXECUTION_REPORT id, and never enters
    the engine journal.
    """
    state = ctx.state
    _eid, result, _reason, _trades, analysis = state.engine._impact_report(
        payload["event_id"], payload
    )
    return _Applied(
        ACCEPTED, result=result,
        analysis_key="impact_analysis", analysis=analysis,
    )


# -- cross-security portfolio report ----------------------------------------------


def _account_symbols(session: object, account_id: str) -> set[str]:
    """The securities on which an accepted event named ``account_id``.

    An account is known on a security once either an accepted ADD carried
    it or an accepted TWAP/VWAP/POV plan carried it; released slices
    inherit the plan's account and therefore already show up in the
    engine's account set, while a started-but-never-released plan is
    picked up directly. This mirrors the baseline rule that an accepted
    ADD makes an account known whatever later happens to the order.
    """
    symbols: set[str] = set()
    for sym, symbol_state in session._symbols.items():
        if symbol_state.engine.knows_account(account_id):
            symbols.add(sym)
            continue
        for plan in symbol_state.plans.values():
            if plan.account_id == account_id:
                symbols.add(sym)
                break
    return symbols


def apply_portfolio_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only cross-security portfolio query.

    The query only reads trades produced by earlier accepted events: it
    never matches, never releases a plan slice and never moves a book, a
    trade id, a plan or an account set. The shared commit path occupies
    the query id and advances the envelope symbol's sequence, including
    the ``UNKNOWN_ACCOUNT`` and ``MARK_PRICE_MISMATCH`` business
    rejections; the id lives solely in the replay log, exactly like a
    parent-order command id.
    """
    account_id: str = payload["account_id"]
    mark_prices: dict[str, object] = payload["mark_prices"]

    known_symbols = _account_symbols(session, account_id)
    if not known_symbols:
        return _Applied(REJECTED, code=UNKNOWN_ACCOUNT)
    # The mark map must name exactly the securities the account ever
    # appeared on: one missing or one extra is a business rejection, not a
    # schema error, and still consumes the id and the sequence.
    if set(mark_prices) != known_symbols:
        return _Applied(REJECTED, code=MARK_PRICE_MISMATCH)

    positions: list[dict[str, object]] = []
    total_buy_notional = 0
    total_sell_notional = 0
    total_turnover = 0
    total_market_value = 0
    total_exposure = 0
    total_pnl = 0
    for sym in sorted(known_symbols):
        mark_price = mark_prices[sym]
        engine = session._symbols[sym].engine
        buy_quantity, sell_quantity, buy_notional, sell_notional = (
            engine.account_aggregates(account_id)
        )
        net_position = buy_quantity - sell_quantity
        cash_balance = sell_notional - buy_notional
        turnover = buy_notional + sell_notional
        market_value = net_position * mark_price
        exposure = abs(net_position) * mark_price
        pnl = cash_balance + market_value
        positions.append({
            "symbol": sym,
            "mark_price": mark_price,
            "buy_quantity": buy_quantity,
            "sell_quantity": sell_quantity,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "net_position": net_position,
            "cash_balance": cash_balance,
            "buy_vwap": (
                {"numerator": buy_notional, "denominator": buy_quantity}
                if buy_quantity else None
            ),
            "sell_vwap": (
                {"numerator": sell_notional, "denominator": sell_quantity}
                if sell_quantity else None
            ),
            "turnover_notional": turnover,
            "position_market_value": market_value,
            "risk_exposure": exposure,
            "mark_to_market_pnl": pnl,
        })
        total_buy_notional += buy_notional
        total_sell_notional += sell_notional
        total_turnover += turnover
        total_market_value += market_value
        total_exposure += exposure
        total_pnl += pnl

    analysis: dict[str, object] = {
        "account_id": account_id,
        "positions": positions,
        "totals": {
            "buy_notional": total_buy_notional,
            "sell_notional": total_sell_notional,
            "cash_balance": total_sell_notional - total_buy_notional,
            "turnover_notional": total_turnover,
            "position_market_value": total_market_value,
            "risk_exposure": total_exposure,
            "mark_to_market_pnl": total_pnl,
        },
    }
    return _Applied(
        ACCEPTED, result=REPORTED,
        analysis_key="portfolio_analysis", analysis=analysis,
    )


# -- cross-security portfolio stress report ---------------------------------------


def apply_portfolio_stress_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only cross-security portfolio stress query.

    The query re-prices one account's existing positions under each
    caller-supplied scenario: it only reads trades produced by earlier
    accepted events, so it never matches, never releases a plan slice and
    never moves a book, a plan, an account set, the trade log, a trade id
    or a price-limit interval. The shared commit path occupies the query
    id and advances the envelope symbol's sequence, including the
    ``UNKNOWN_ACCOUNT`` and ``MARK_PRICE_MISMATCH`` business rejections;
    the id lives solely in the replay log, exactly like a
    PORTFOLIO_REPORT id. Account attribution follows the portfolio
    report's rule exactly: maker, taker, REPLACE-inherited, iceberg
    replenishment and released TWAP/VWAP/POV child-order fills all count
    under their order's account. Every figure is a plain integer.
    """
    account_id: str = payload["account_id"]
    mark_prices: dict[str, int] = payload["mark_prices"]
    scenarios: list[dict[str, object]] = payload["scenarios"]

    known_symbols = _account_symbols(session, account_id)
    if not known_symbols:
        return _Applied(REJECTED, code=UNKNOWN_ACCOUNT)
    # Every price object — the baseline marks and each scenario's prices —
    # must name exactly the securities the account ever appeared on: one
    # missing or one extra is a business rejection, not a schema error,
    # and still consumes the id and the sequence.
    if set(mark_prices) != known_symbols:
        return _Applied(REJECTED, code=MARK_PRICE_MISMATCH)
    for scenario in scenarios:
        if set(scenario["prices"]) != known_symbols:
            return _Applied(REJECTED, code=MARK_PRICE_MISMATCH)

    ordered_symbols = sorted(known_symbols)
    net_positions: dict[str, int] = {}
    total_cash = 0
    baseline_mark_to_market_pnl = 0
    baseline_risk_exposure = 0
    for sym in ordered_symbols:
        engine = session._symbols[sym].engine
        buy_quantity, sell_quantity, buy_notional, sell_notional = (
            engine.account_aggregates(account_id)
        )
        net_position = buy_quantity - sell_quantity
        cash_balance = sell_notional - buy_notional
        net_positions[sym] = net_position
        total_cash += cash_balance
        baseline_mark_to_market_pnl += cash_balance + net_position * mark_prices[sym]
        baseline_risk_exposure += abs(net_position) * mark_prices[sym]

    scenario_reports: list[dict[str, object]] = []
    worst_scenario: str | None = None
    worst_total_pnl_change: int | None = None
    for scenario in scenarios:
        name: str = scenario["name"]
        prices: dict[str, int] = scenario["prices"]
        positions: list[dict[str, object]] = []
        stressed_mark_to_market_pnl = total_cash
        risk_exposure = 0
        total_pnl_change = 0
        for sym in ordered_symbols:
            net_position = net_positions[sym]
            shocked_price = prices[sym]
            pnl_change = net_position * (shocked_price - mark_prices[sym])
            positions.append({
                "symbol": sym,
                "shocked_price": shocked_price,
                "net_position": net_position,
                "pnl_change": pnl_change,
            })
            stressed_mark_to_market_pnl += net_position * shocked_price
            risk_exposure += abs(net_position) * shocked_price
            total_pnl_change += pnl_change
        scenario_reports.append({
            "name": name,
            "positions": positions,
            "stressed_mark_to_market_pnl": stressed_mark_to_market_pnl,
            "risk_exposure": risk_exposure,
            "total_pnl_change": total_pnl_change,
        })
        # The worst scenario carries the smallest total pnl change; ties
        # break towards the lexicographically smallest scenario name.
        if (
            worst_total_pnl_change is None
            or (total_pnl_change, name) < (worst_total_pnl_change, worst_scenario)
        ):
            worst_total_pnl_change = total_pnl_change
            worst_scenario = name

    analysis: dict[str, object] = {
        "account_id": account_id,
        "baseline_mark_to_market_pnl": baseline_mark_to_market_pnl,
        "baseline_risk_exposure": baseline_risk_exposure,
        "scenarios": scenario_reports,
        "worst_scenario": worst_scenario,
    }
    return _Applied(
        ACCEPTED, result=REPORTED,
        analysis_key="portfolio_stress_analysis", analysis=analysis,
    )


# -- whole-session reconciliation --------------------------------------------------


def _actual_session_trades(
    session: object,
) -> dict[tuple[str, int], dict[str, object]]:
    """Every historical trade of every security, keyed by (symbol, id).

    Trade ids are per symbol, so the composite key is unique across the
    session. The public record shape mirrors the external trade items:
    the internal ``event_id`` tag the engine journal carries is not part
    of it. Securities are visited in sorted order; within one security the
    journal already runs in trade_id order.
    """
    actual: dict[tuple[str, int], dict[str, object]] = {}
    for sym in sorted(session._symbols):
        for trade in session._symbols[sym].engine.trade_history():
            key = (sym, trade["trade_id"])
            actual[key] = {
                "symbol": sym,
                "trade_id": trade["trade_id"],
                "maker_order_id": trade["maker_order_id"],
                "taker_order_id": trade["taker_order_id"],
                "price": trade["price"],
                "quantity": trade["quantity"],
            }
    return actual


def _actual_session_accounts(
    session: object,
) -> dict[tuple[str, str], dict[str, object]]:
    """The actual book of every account on every security.

    An account is known on a security once either an accepted ADD carried
    it or an accepted TWAP/VWAP/POV plan carried it, including accounts
    whose orders or plans never traded (they reconcile at zero) — the same
    knownness rule the cross-security portfolio query uses. Positions and
    cash come from the engine's own per-account journal aggregation
    (replacements keep the account and iceberg slices keep the maker id):
    net position is bought quantity minus sold quantity, cash is sell
    notional minus buy notional.
    """
    actual: dict[tuple[str, str], dict[str, object]] = {}
    for sym in sorted(session._symbols):
        symbol_state = session._symbols[sym]
        account_ids: set[str] = set(symbol_state.engine.known_accounts())
        for plan in symbol_state.plans.values():
            if plan.account_id is not None:
                account_ids.add(plan.account_id)
        for account_id in sorted(account_ids):
            buy_quantity, sell_quantity, buy_notional, sell_notional = (
                symbol_state.engine.account_aggregates(account_id)
            )
            actual[(sym, account_id)] = {
                "symbol": sym,
                "account_id": account_id,
                "net_position": buy_quantity - sell_quantity,
                "cash_balance": sell_notional - buy_notional,
            }
    return actual


def _session_breaks(
    expected: dict[tuple[object, ...], dict[str, object]],
    actual: dict[tuple[object, ...], dict[str, object]],
) -> list[dict[str, object]]:
    """Full outer comparison of composite-keyed session record sets.

    Breaks are emitted in ascending composite-key order, so trade breaks
    sort by (symbol, trade_id) and account breaks by (symbol,
    account_id); the identifier is the key itself and the missing side is
    reported as ``None``.
    """
    breaks: list[dict[str, object]] = []
    for identifier_tuple in sorted(expected.keys() | actual.keys()):
        exp = expected.get(identifier_tuple)
        act = actual.get(identifier_tuple)
        if exp is None:
            reason = MISSING_EXPECTED
        elif act is None:
            reason = MISSING_ACTUAL
        elif exp != act:
            reason = FIELD_MISMATCH
        else:
            continue
        breaks.append(
            {
                "identifier": list(identifier_tuple),
                "expected": exp,
                "actual": act,
                "reason": reason,
            }
        )
    return breaks


def apply_session_reconciliation(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only whole-session reconciliation query.

    The query compares the caller's external trade and account records
    against the full history of every security in the session at once: it
    never matches, never releases a plan slice and never moves an order, a
    plan, an account set, a book, the trade journal, a trade id counter or
    a price-limit interval. The shared commit path occupies the query id
    and advances the envelope symbol's sequence; the id lives solely in
    the replay log, exactly like a PORTFOLIO_REPORT id. A perfect match
    reports ``RECONCILED``; any difference reports ``BREAKS_FOUND`` with
    the two sorted break arrays.
    """
    expected_trades: dict[tuple[str, int], dict[str, object]] = {}
    for item in payload["expected_trades"]:
        expected_trades[(item["symbol"], item["trade_id"])] = {
            "symbol": item["symbol"],
            "trade_id": item["trade_id"],
            "maker_order_id": item["maker_order_id"],
            "taker_order_id": item["taker_order_id"],
            "price": item["price"],
            "quantity": item["quantity"],
        }
    expected_accounts: dict[tuple[str, str], dict[str, object]] = {}
    for item in payload["expected_accounts"]:
        expected_accounts[(item["symbol"], item["account_id"])] = {
            "symbol": item["symbol"],
            "account_id": item["account_id"],
            "net_position": item["net_position"],
            "cash_balance": item["cash_balance"],
        }

    actual_trades = _actual_session_trades(session)
    actual_accounts = _actual_session_accounts(session)
    trade_breaks = _session_breaks(expected_trades, actual_trades)
    account_breaks = _session_breaks(expected_accounts, actual_accounts)
    result = RECONCILED if not trade_breaks and not account_breaks else BREAKS_FOUND
    return _Applied(
        ACCEPTED, result=result,
        analysis_key="reconciliation",
        analysis={"trade_breaks": trade_breaks, "account_breaks": account_breaks},
    )


# -- per-plan implementation-shortfall report ---------------------------------------


def _plan_tca_analysis(plan: ExecutionPlan, mark_price: int) -> dict[str, object]:
    """Build the read-only ``plan_tca_analysis`` object for one plan.

    The identifiers, algorithm, side, lifecycle status, benchmark price,
    total quantity, executed notional and the exact-fraction ``vwap`` are
    the same values the plan's own cumulative report carries; ``vwap`` is
    ``None`` until the plan has a fill. Every derived figure uses plain
    integers and the existing fraction only — no float, no rounding:

    * ``opportunity_quantity`` is the total minus the filled quantity;
    * ``execution_slippage_notional`` is
      ``executed_notional - benchmark_price * filled_quantity`` for a buy
      and its negation for a sell;
    * ``opportunity_cost_notional`` is
      ``(mark_price - benchmark_price) * opportunity_quantity`` for a buy
      and its negation for a sell;
    * ``implementation_shortfall_notional`` is the sum of the two, so a
      negative value means improvement.
    """
    total_quantity = plan.total_quantity
    filled_quantity = plan.filled_quantity
    notional = plan.notional
    vwap = (
        {"numerator": notional, "denominator": filled_quantity}
        if filled_quantity
        else None
    )
    opportunity_quantity = total_quantity - filled_quantity
    execution_slippage = notional - plan.benchmark_price * filled_quantity
    opportunity_cost = (
        (mark_price - plan.benchmark_price) * opportunity_quantity
    )
    if plan.side == SELL:
        # Mirror the buy formulas: negative always means improvement.
        execution_slippage = -execution_slippage
        opportunity_cost = -opportunity_cost
    return {
        "plan_id": plan.plan_id,
        "algorithm": plan.algorithm,
        "side": plan.side,
        "status": plan.status,
        "benchmark_price": plan.benchmark_price,
        "total_quantity": total_quantity,
        "executed_notional": notional,
        "vwap": vwap,
        "mark_price": mark_price,
        "opportunity_quantity": opportunity_quantity,
        "execution_slippage_notional": execution_slippage,
        "opportunity_cost_notional": opportunity_cost,
        "implementation_shortfall_notional": (
            execution_slippage + opportunity_cost
        ),
    }


def apply_plan_tca_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only implementation-shortfall query for one plan.

    The query only reads this security's plan state: it never matches,
    never releases a slice, never feeds POV market volume and never moves
    an order, a queue, the trade log, an account set, a plan, the active
    price-limit interval or the next trade id. Every TWAP/VWAP/POV plan
    accepted under the envelope symbol is queryable — ACTIVE, COMPLETED
    and CANCELLED alike; a plan id that is unknown under this symbol,
    including one that only exists on another security, is an
    ``UNKNOWN_EXECUTION_PLAN`` business rejection. The shared commit path
    occupies the query id and advances the symbol sequence, including
    that rejection; the id lives solely in the replay log, exactly like a
    SESSION_RECONCILIATION id.
    """
    state = ctx.state
    plan_id: str = payload["plan_id"]
    mark_price: int = payload["mark_price"]
    plan = state.plans.get(plan_id)
    if plan is None:
        return _Applied(REJECTED, code=UNKNOWN_EXECUTION_PLAN)
    return _Applied(
        ACCEPTED, result=REPORTED,
        analysis_key="plan_tca_analysis",
        analysis=_plan_tca_analysis(plan, mark_price),
    )


# -- read-only current-book depth summary ------------------------------------------


def _liquidity_levels(
    queued_levels: list[dict[str, object]], depth: int
) -> tuple[list[dict[str, object]], int]:
    """Project at most ``depth`` already-ordered queue levels for the report.

    Only public queue quantities are aggregated: an iceberg's hidden reserve
    never counts and its current visible fragment counts as exactly one
    order, exactly as the queue projection presents it. Returns the capped
    level list and the cumulative visible quantity across it.
    """
    levels: list[dict[str, object]] = []
    cumulative = 0
    for level in queued_levels[:depth]:
        visible_quantity: int = level["visible_quantity"]
        cumulative += visible_quantity
        levels.append({
            "price": level["price"],
            "visible_quantity": visible_quantity,
            "cumulative_visible_quantity": cumulative,
            "order_count": len(level["orders"]),
        })
    return levels, cumulative


def apply_book_liquidity_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only depth-summary query for the current book.

    The query only reads this security's resting queues: it never matches,
    never replenishes an iceberg slice and never moves an order, a queue, a
    plan, an account set, the trade journal, the next trade id or the
    active price-limit interval. Only public queue quantities enter the
    statistics — an iceberg's reserve stays hidden and its current visible
    fragment counts as one resting order. A first-seen symbol answers
    against its empty book successfully. The shared commit path occupies
    the query id and advances the envelope symbol's sequence; the id lives
    solely in the replay log, exactly like an IMPACT_REPORT id, and never
    enters the engine journal.
    """
    depth: int = payload["depth"]
    engine = ctx.state.engine
    bid_queues, ask_queues = engine.book_queue_view()
    bid_levels, bid_quantity = _liquidity_levels(bid_queues, depth)
    ask_levels, ask_quantity = _liquidity_levels(ask_queues, depth)

    best_bid: int | None = bid_queues[0]["price"] if bid_queues else None
    best_ask: int | None = ask_queues[0]["price"] if ask_queues else None
    if best_bid is None or best_ask is None:
        # With one side empty there is no spread and no midpoint; the side
        # that does exist still keeps its best price and capped levels.
        spread = None
        midpoint = None
    else:
        spread = best_ask - best_bid
        midpoint = {"numerator": best_ask + best_bid, "denominator": 2}

    imbalance_denominator = bid_quantity + ask_quantity
    if imbalance_denominator == 0:
        imbalance = None
    else:
        # The unreduced exact fraction over the returned levels' public
        # quantities: bids minus asks over their sum.
        imbalance = {
            "numerator": bid_quantity - ask_quantity,
            "denominator": imbalance_denominator,
        }

    analysis: dict[str, object] = {
        "depth": depth,
        "bid_levels": bid_levels,
        "ask_levels": ask_levels,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "imbalance": imbalance,
    }
    return _Applied(
        ACCEPTED, result=REPORTED,
        analysis_key="liquidity_analysis", analysis=analysis,
    )
