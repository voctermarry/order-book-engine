"""Point-in-time reconstruction of one security's book at a past sequence.

The multi-symbol replay layer only retains the *current* book; this module
rebuilds the book and the active price-limit interval exactly as they were
immediately after a given per-symbol sequence was committed, by replaying the
security's committed event log from an empty session onto a fresh baseline
:class:`~order_book_engine.engine.Engine` together with the replay-layer plan
state and active price-limit interval.

The reconstruction is deterministic and read-only with respect to the live
session: it never mutates the replayer's state. The replay itself reproduces
the historical matching (and therefore the historical trades and trade ids),
but nothing it does is ever observed as a new match — no plan is released for
the caller and no trade id beyond the ones the committed events already spent
leaves the reconstruction. Rejected events, read-only reports and
parent-order commands occupy sequences but never change book state; the replay
reproduces that exactly.

The import of :mod:`order_book_engine.event_replay` is performed lazily inside
:func:`reconstruct_prefix` to avoid an import cycle (the replay layer imports
this module at its top level).
"""

from __future__ import annotations

import json

from .engine import ADD, ICEBERG, IOC, LIMIT, REPLACE, Engine

#: Parent-order command type names, spelled literally so this module needs no
#: top-level import of the replay layer.
_TWAP_START = "TWAP_START"
_VWAP_START = "VWAP_START"
_POV_START = "POV_START"
_TWAP_SLICE = "TWAP_SLICE"
_VWAP_SLICE = "VWAP_SLICE"
_POV_VOLUME = "POV_VOLUME"
_PLAN_CANCEL_TYPES = frozenset({"TWAP_CANCEL", "VWAP_CANCEL", "POV_CANCEL"})
_PLAN_REPORT_TYPES = frozenset({"TWAP_REPORT", "VWAP_REPORT", "POV_REPORT"})
_PLAN_ACTIVE = "ACTIVE"
_PLAN_COMPLETED = "COMPLETED"
_PLAN_CANCELLED = "CANCELLED"
_PRICE_LIMIT_UPDATE = "PRICE_LIMIT_UPDATE"


def reconstruct_prefix(
    events: list[tuple[str, dict[str, object]]],
    initial_price_limits: tuple[int, int] | None,
) -> tuple[Engine, tuple[int, int] | None]:
    """Replay one security's committed events up to and including a prefix.

    Parameters
    ----------
    events:
        ``(event_id, payload)`` pairs in the security's committed sequence
        order, covering sequences ``1 .. target_sequence`` contiguously. The
        payloads are exactly the canonicalized payloads stored in the replayer's
        event log (baseline events and replay-only events alike).
    initial_price_limits:
        The session's static configured interval for this security (seed value
        of the active interval), or ``None`` when the security is unlimited.

    Returns
    -------
    (engine, active_price_limits):
        A fresh baseline engine holding the book as it was after the prefix's
        final committed event, and the active price-limit interval (a
        ``(lower, upper)`` tuple or ``None``) in force at that point.

    Business outcomes are reproduced from the replayed state alone rather than
    read from the live session: a committed event was rejected iff the same
    rule rejects it here (a duplicate plan id, a derived-id clash, a
    price-limit breach, an unknown/closed plan, ...). Because the live layer is
    deterministic, the two verdicts always agree.
    """
    # Lazy import: breaks the cycle with the replay layer.
    from .event_replay import ExecutionPlan, _REPLAY_ONLY_TYPES

    engine = Engine()
    active_price_limits: tuple[int, int] | None = initial_price_limits
    # plan_id -> ExecutionPlan, in start order; holds only accepted plans.
    plans: dict[str, ExecutionPlan] = {}
    # Every derived id an accepted plan ever reserved (released or not), so a
    # committed external event whose *event id* collided with one can be
    # recognized: the live layer rejected those (DUPLICATE_EVENT_ID) before any
    # dispatch, so they must not reach the fresh engine either.
    derived_ids: set[str] = set()

    def plan_price_out_of_bounds(plan: ExecutionPlan) -> bool:
        if plan.order_type != LIMIT or active_price_limits is None:
            return False
        lower, upper = active_price_limits
        return plan.price < lower or plan.price > upper

    def try_start_plan(payload: dict[str, object], algorithm: str) -> bool:
        """Reproduce the live layer's START acceptance decision.

        Returns ``True`` and registers the plan (reserving every derived id)
        when the committed START was accepted, ``False`` when it was a business
        rejection. The precedence mirrors the live layer: an existing plan id
        or a clash over a derived id wins over the active price-limit check.
        """
        plan_id = payload["plan_id"]
        kwargs: dict[str, object] = {}
        if algorithm == "VWAP":
            kwargs["algorithm"] = "VWAP"
            kwargs["volume_weights"] = payload["volume_weights"]
            slice_count = len(payload["volume_weights"])
        elif algorithm == "POV":
            kwargs["algorithm"] = "POV"
            kwargs["participation_bps"] = payload["participation_bps"]
            slice_count = 0
        else:
            slice_count = payload["slice_count"]
        plan = ExecutionPlan(
            plan_id=plan_id,
            side=payload["side"],
            order_type=payload["order_type"],
            total_quantity=payload["total_quantity"],
            slice_count=slice_count,
            benchmark_price=payload["benchmark_price"],
            price=payload.get("price") if payload["order_type"] == LIMIT else None,
            account_id=payload.get("account_id"),
            **kwargs,
        )
        # An existing plan id rejects before everything else.
        if plan_id in plans:
            return False
        # So does a derived id already spent by an engine order or claimed by
        # another plan.
        if any(
            engine.has_order_id(child_id) or child_id in derived_ids
            for child_id in plan.child_ids()
        ):
            return False
        # A LIMIT plan outside the active band is rejected last and creates no
        # plan and no reservation; MARKET plans are exempt.
        if payload["order_type"] == LIMIT and price_out_of_bounds(payload["price"]):
            return False
        plans[plan_id] = plan
        for child_id in plan.child_ids():
            engine.reserve_order_id(child_id)
            derived_ids.add(child_id)
        return True

    def price_out_of_bounds(price: int) -> bool:
        if active_price_limits is None:
            return False
        lower, upper = active_price_limits
        return price < lower or price > upper

    def release_child(plan: ExecutionPlan, quantity: int) -> None:
        """Submit one plan slice / positive POV release as the live layer does.

        Committed releases were accepted (a historical slice re-checked inside
        the band then in force and its child id was reserved), so the
        synthesized child cannot reject.
        """
        release_number = plan.released + 1
        child_id = plan.child_order_id(release_number)
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
        engine.release_order_id(child_id)
        _eid, _result, reason, trades, _stp = engine.handle_object(child)
        if reason is not None:  # pragma: no cover - defensive, cannot happen
            raise RuntimeError(f"reconstructed plan slice rejected: {reason}")
        traded_quantity = sum(trade["quantity"] for trade in trades)
        plan.released += 1
        plan.released_quantity += quantity
        if plan.algorithm == "POV":
            # Only POV grows its schedule list at run time, one original
            # quantity per positive release; TWAP/VWAP slices were fixed at
            # plan start.
            plan.slice_quantities.append(quantity)
        plan.filled_quantity += traded_quantity
        plan.notional += sum(trade["price"] * trade["quantity"] for trade in trades)
        if plan.algorithm == "POV":
            if plan.released_quantity == plan.total_quantity:
                plan.status = _PLAN_COMPLETED
        elif plan.released == plan.slice_count:
            plan.status = _PLAN_COMPLETED

    for event_id, payload in events:
        event_type = payload["type"]

        # The live layer rejects every well-formed event whose id is a
        # reserved/released derived child id (DUPLICATE_EVENT_ID) before any
        # type-specific dispatch, so such a committed event moved nothing at
        # all — not even the plan named in its payload.
        if event_id in derived_ids:
            continue

        if event_type in (_TWAP_START, _VWAP_START, _POV_START):
            # A START that the live layer business-rejected (a duplicate plan
            # id, a derived-id clash or a price-limit breach) created no plan;
            # the same rules decide it here from the replayed state.
            try_start_plan(payload, {
                _TWAP_START: "TWAP",
                _VWAP_START: "VWAP",
                _POV_START: "POV",
            }[event_type])
            continue

        if event_type in (_TWAP_SLICE, _VWAP_SLICE):
            plan = plans.get(payload["plan_id"])
            # A slice names an unknown plan, a POV plan, a closed plan or a
            # LIMIT plan whose price is outside the band then in force: the
            # committed command was a business rejection and moved nothing.
            if (
                plan is not None
                and plan.algorithm != "POV"
                and plan.status == _PLAN_ACTIVE
                and not plan_price_out_of_bounds(plan)
            ):
                quantity = plan.slice_quantities[plan.released]
                release_child(plan, quantity)
            continue

        if event_type == _POV_VOLUME:
            plan = plans.get(payload["plan_id"])
            # Only a matching ACTIVE in-band POV plan was driven; every other
            # committed volume command rejected without retaining the
            # increment.
            if (
                plan is not None
                and plan.algorithm == "POV"
                and plan.status == _PLAN_ACTIVE
                and not plan_price_out_of_bounds(plan)
            ):
                plan.market_volume += payload["market_volume_increment"]
                target = min(
                    plan.total_quantity,
                    plan.market_volume * plan.participation_bps // 10000,
                )
                release_quantity = target - plan.released_quantity
                if release_quantity > 0:
                    release_child(plan, release_quantity)
            continue

        if event_type in _PLAN_CANCEL_TYPES:
            plan = plans.get(payload["plan_id"])
            if plan is not None and plan.status == _PLAN_ACTIVE:
                # A committed cancel counted the unreleased quantity once and
                # closed the plan; a cancel against an unknown/closed plan was
                # a business rejection and moved nothing.
                unreleased = plan.total_quantity - plan.released_quantity
                plan.cancelled_quantity += unreleased
                plan.status = _PLAN_CANCELLED
            continue

        if event_type in _PLAN_REPORT_TYPES:
            # Reports never move state (and stay valid for closed plans).
            continue

        if event_type == _PRICE_LIMIT_UPDATE:
            active_price_limits = (
                payload["lower_price"], payload["upper_price"]
            )
            continue

        if event_type in _REPLAY_ONLY_TYPES:
            # Every other replay-only event (EXECUTION_REPORT, IMPACT_REPORT,
            # PORTFOLIO_REPORT, SESSION_RECONCILIATION, PLAN_TCA_REPORT and
            # BOOK_RECONSTRUCTION_REPORT itself) is read-only and moves
            # nothing.
            continue

        # Baseline ADD / CANCEL / REPLACE. The replay layer enforces the active
        # price-limit interval before the engine; a price breach is a committed
        # business rejection that only occupies the event id, so the event must
        # not reach the fresh engine. Identifier clashes the live layer
        # reaches first are left for the engine to reproduce (the committed log
        # contains them with their original rejection outcome).
        if event_type == ADD and payload.get("order_type") in (LIMIT, ICEBERG):
            # The live layer reports DUPLICATE_ORDER_ID before the price
            # breach, so a priced ADD is price-rejected only when its order id
            # is still free; otherwise the engine reproduces the clash.
            if (
                not engine.has_order_id(payload["order_id"])
                and price_out_of_bounds(payload["price"])
            ):
                engine.occupy_event_id(event_id)
                continue
        if event_type == REPLACE and price_out_of_bounds(payload["price"]):
            # The live layer only blocks on price after confirming the target
            # is a resting one (UNKNOWN_ORDER keeps precedence, and an illegal
            # display_quantity replace was a schema error that never reached
            # the log). replace_target_kind returns None exactly then.
            if engine.replace_target_kind(payload["order_id"]) is not None:
                engine.occupy_event_id(event_id)
                continue

        engine.handle_object_position(payload)

    return engine, active_price_limits


def payloads_from_event_log(
    seen: dict[str, str],
) -> list[tuple[str, dict[str, object]]]:
    """Turn a symbol's ordered event log into ``(event_id, payload)`` pairs.

    The per-symbol ``seen`` log preserves first-seen (committed) order, which
    is the sequence order; malformed stored content cannot occur because only
    canonicalized payloads are ever written.
    """
    ordered: list[tuple[str, dict[str, object]]] = []
    for event_id, content in seen.items():
        payload = json.loads(content)
        ordered.append((event_id, payload))
    return ordered
