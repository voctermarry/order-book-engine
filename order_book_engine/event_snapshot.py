"""Snapshot export and restoration for a replay session.

A snapshot fully preserves (per security): price-time queue priority, order
remainders, the iceberg current slice and replenishment state, the last
sequence, cumulative trades, the next trade id counter, the active
price-limit interval, the event logs, the reserved derived order ids and the
plans. It carries the format version, the matching configuration (plus its
digest) and a SHA-256 digest over the normalized whole document.

Restoration verifies version, configuration and digest before touching any
state, and validates every component into locals before any of it is
adopted: any failure raises :class:`SnapshotError` and leaves no partially
recovered session behind.
"""

from __future__ import annotations

import copy
import json
from collections import deque

from . import __version__
from .engine import (
    BUY,
    LIMIT,
    MARKET,
    SELL,
    Engine,
    _is_int,
    _is_non_empty_str,
    _is_positive_int,
)
from .event_registry import _REPLAY_ONLY_TYPES
from .event_replayer import EventReplayer
from .event_types import (
    ALGORITHM_POV,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
    CONFIG_MISMATCH,
    FORMAT_VERSION,
    PLAN_ACTIVE,
    PLAN_CANCELLED,
    PLAN_COMPLETED,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
)
from .event_validation import DEFAULT_CONFIG, _validate_config
from .execution_plans import ExecutionPlan, _allocate_slices
from .replay_json import _digest
from .replay_state import _SymbolState


# ---------------------------------------------------------------------------
# Snapshot restoration errors
# ---------------------------------------------------------------------------


class SnapshotError(ValueError):
    """A snapshot could not be restored.

    Raising (rather than mutating and returning) guarantees that no partial
    recovery state is ever observable: the target replayer stays untouched.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _engine_to_json(state: _SymbolState) -> dict[str, object]:
    raw = state.engine.dump_state()
    return {
        "last_sequence": state.last_sequence,
        # The security's active price-limit interval travels with the
        # snapshot so a resumed run enforces exactly the band a continuous
        # run would; ``None`` means the security is unlimited.
        "price_limits": (
            {"lower": state.price_limits[0], "upper": state.price_limits[1]}
            if state.price_limits is not None
            else None
        ),
        "event_log": [
            {"event_id": event_id, "content": content}
            for event_id, content in state.seen.items()
        ],
        "plans": [state.plans[plan_id].to_json() for plan_id in sorted(state.plans)],
        "engine": {
            "event_ids": sorted(raw["event_ids"]),
            "order_ids": sorted(raw["order_ids"]),
            "reserved_order_ids": sorted(raw["reserved_order_ids"]),
            "orders": {
                order_id: raw["orders"][order_id]
                for order_id in sorted(raw["orders"])
            },
            "bids": [
                {"price": price, "order_ids": list(queue)}
                for price, queue in sorted(raw["bids"].items())
            ],
            "asks": [
                {"price": price, "order_ids": list(queue)}
                for price, queue in sorted(raw["asks"].items())
            ],
            "bid_totals": {str(price): qty for price, qty in sorted(raw["bid_totals"].items())},
            "ask_totals": {str(price): qty for price, qty in sorted(raw["ask_totals"].items())},
            "next_trade_id": raw["next_trade_id"],
            "accounts": sorted(raw["accounts"]),
            "trade_log": raw["trade_log"],
        },
    }


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SnapshotError(SNAPSHOT_CORRUPT, message)


def _is_str_set_list(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _parse_plan_entry(entry: dict[str, object]) -> "ExecutionPlan":
    """Validate one serialized plan record and rebuild the :class:`ExecutionPlan`."""

    def pos_int(name: str) -> int:
        value = entry.get(name)
        _require(
            isinstance(value, int) and not isinstance(value, bool) and value > 0,
            f"plan {entry.get('plan_id')!r} {name} must be a positive integer",
        )
        return value

    plan_id = entry.get("plan_id")
    _require(_is_non_empty_str(plan_id), "plan plan_id must be a non-empty string")
    side = entry.get("side")
    _require(side in (BUY, SELL), f"plan {plan_id} has a bad side")
    order_type = entry.get("order_type")
    _require(order_type in (LIMIT, MARKET), f"plan {plan_id} has a bad order type")
    benchmark_price = pos_int("benchmark_price")
    price = entry.get("price")
    if order_type == LIMIT:
        _require(
            isinstance(price, int) and not isinstance(price, bool) and price > 0,
            f"plan {plan_id} LIMIT price must be a positive integer",
        )
    else:
        _require(price is None, f"plan {plan_id} MARKET must not carry a price")
    account_id = entry.get("account_id")
    _require(
        account_id is None or _is_non_empty_str(account_id),
        f"plan {plan_id} account_id is malformed",
    )
    raw_slices = entry.get("slice_quantities")
    _require(isinstance(raw_slices, list), f"plan {plan_id} slice_quantities must be a list")
    slice_quantities: list[int] = []
    for value in raw_slices:
        _require(
            isinstance(value, int) and not isinstance(value, bool) and value > 0,
            f"plan {plan_id} slice quantities must be positive integers",
        )
        slice_quantities.append(value)
    released = entry.get("released")
    released_quantity = entry.get("released_quantity")
    filled_quantity = entry.get("filled_quantity")
    cancelled_quantity = entry.get("cancelled_quantity")
    notional = entry.get("notional")
    for name, value in (
        ("released", released),
        ("released_quantity", released_quantity),
        ("filled_quantity", filled_quantity),
        ("cancelled_quantity", cancelled_quantity),
        ("notional", notional),
    ):
        _require(_non_negative_int(value), f"plan {plan_id} {name} must be a non-negative integer")
    status = entry.get("status")
    _require(
        status in (PLAN_ACTIVE, PLAN_COMPLETED, PLAN_CANCELLED),
        f"plan {plan_id} has a bad status",
    )
    algorithm = entry.get("algorithm", ALGORITHM_TWAP)
    volume_weights: list[int] | None = None
    participation_bps: int | None = None
    market_volume: int | None = None
    plan_total: int | None = None

    slice_count = len(slice_quantities)
    _require(0 <= released <= slice_count, f"plan {plan_id} released slice index out of range")

    if algorithm == ALGORITHM_VWAP:
        # TWAP/VWAP plans always carry at least one scheduled slice.
        _require(slice_count > 0, f"plan {plan_id} slice_quantities must be a non-empty list")
        total_quantity = sum(slice_quantities)
        raw_weights = entry.get("volume_weights")
        _require(
            isinstance(raw_weights, list) and len(raw_weights) == slice_count,
            f"plan {plan_id} volume_weights must be a list matching its slices",
        )
        volume_weights = []
        for value in raw_weights:
            _require(
                isinstance(value, int) and not isinstance(value, bool) and value > 0,
                f"plan {plan_id} volume weights must be positive integers",
            )
            volume_weights.append(value)
        _require(
            slice_quantities == _allocate_slices(total_quantity, volume_weights),
            f"plan {plan_id} slice quantities disagree with its volume weights",
        )
        _require(
            released_quantity == sum(slice_quantities[:released]),
            f"plan {plan_id} released_quantity does not match its slices",
        )
    elif algorithm == ALGORITHM_POV:
        # POV carries an explicit total and rate; its slice list holds exactly
        # one original quantity per positive release and may be empty before
        # the first one.
        plan_total = pos_int("total_quantity")
        participation_bps = entry.get("participation_bps")
        _require(
            _is_int(participation_bps) and 1 <= participation_bps <= 10000,
            f"plan {plan_id} participation_bps must be an integer from 1 to 10000",
        )
        market_volume = entry.get("market_volume")
        _require(
            _non_negative_int(market_volume),
            f"plan {plan_id} market_volume must be a non-negative integer",
        )
        _require(
            released == slice_count,
            f"plan {plan_id} POV release count disagrees with its released slices",
        )
        _require(
            released_quantity == sum(slice_quantities),
            f"plan {plan_id} released_quantity does not match its releases",
        )
        _require(
            released_quantity <= plan_total,
            f"plan {plan_id} released quantity exceeds its total",
        )
        # Every accepted POV_VOLUME event leaves the released quantity exactly
        # at the participation target for the cumulative market volume (capped
        # at the total), whatever lifecycle status followed.
        target = min(
            plan_total, market_volume * participation_bps // 10000
        )
        _require(
            released_quantity == target,
            f"plan {plan_id} released quantity disagrees with market volume",
        )
        total_quantity = plan_total
    else:
        _require(
            algorithm == ALGORITHM_TWAP,
            f"plan {plan_id} has an unknown algorithm",
        )
        _require(slice_count > 0, f"plan {plan_id} slice_quantities must be a non-empty list")
        total_quantity = sum(slice_quantities)
        _require(
            released_quantity == sum(slice_quantities[:released]),
            f"plan {plan_id} released_quantity does not match its slices",
        )

    _require(
        filled_quantity <= released_quantity,
        f"plan {plan_id} filled quantity exceeds released quantity",
    )

    if algorithm == ALGORITHM_POV:
        if status == PLAN_ACTIVE:
            _require(
                released_quantity < total_quantity,
                f"plan {plan_id} is ACTIVE after its total was released",
            )
            _require(cancelled_quantity == 0, f"plan {plan_id} is ACTIVE but carries cancellations")
        elif status == PLAN_COMPLETED:
            _require(
                released_quantity == total_quantity,
                f"plan {plan_id} is COMPLETED before its total was released",
            )
            _require(cancelled_quantity == 0, f"plan {plan_id} is COMPLETED but carries cancellations")
        else:
            _require(
                released_quantity < total_quantity,
                f"plan {plan_id} is CANCELLED after its total was released",
            )
            _require(
                cancelled_quantity == total_quantity - released_quantity,
                f"plan {plan_id} cancelled quantity does not cover the unreleased remainder",
            )
    elif status == PLAN_ACTIVE:
        _require(released < slice_count, f"plan {plan_id} is ACTIVE with no slices left")
        _require(cancelled_quantity == 0, f"plan {plan_id} is ACTIVE but carries cancellations")
    elif status == PLAN_COMPLETED:
        _require(released == slice_count, f"plan {plan_id} is COMPLETED before all slices")
        _require(cancelled_quantity == 0, f"plan {plan_id} is COMPLETED but carries cancellations")
    else:
        _require(released < slice_count, f"plan {plan_id} is CANCELLED after all slices")
        _require(
            cancelled_quantity == total_quantity - released_quantity,
            f"plan {plan_id} cancelled quantity does not cover the unreleased remainder",
        )

    plan = ExecutionPlan.__new__(ExecutionPlan)
    plan.plan_id = plan_id
    plan.side = side
    plan.order_type = order_type
    plan.benchmark_price = benchmark_price
    plan.price = price
    plan.account_id = account_id
    plan.algorithm = algorithm
    plan.volume_weights = volume_weights
    plan.slice_quantities = slice_quantities
    plan.released = released
    plan.released_quantity = released_quantity
    plan.filled_quantity = filled_quantity
    plan.cancelled_quantity = cancelled_quantity
    plan.notional = notional
    plan.status = status
    plan.participation_bps = participation_bps
    plan.market_volume = market_volume
    plan.plan_total = plan_total
    return plan


_ORDER_RECORD_KEYS = frozenset(
    {"side", "price", "remaining", "status", "account_id",
     "display_quantity", "visible"}
)
_TRADE_KEYS = frozenset(
    {"trade_id", "maker_order_id", "taker_order_id", "price", "quantity", "event_id"}
)
_SYMBOL_STATE_KEYS = frozenset(
    {"last_sequence", "price_limits", "event_log", "plans", "engine"}
)
#: Snapshots written before the intraday price-limit adjustment existed
#: carry no per-symbol ``price_limits``; on restore the active interval is
#: seeded from the static configuration instead.
_SYMBOL_STATE_KEYS_LEGACY = _SYMBOL_STATE_KEYS - {"price_limits"}
_ENGINE_KEYS = frozenset(
    {"event_ids", "order_ids", "reserved_order_ids", "orders", "bids", "asks",
     "bid_totals", "ask_totals", "next_trade_id", "accounts", "trade_log"}
)
_PLAN_KEYS = frozenset(
    {"plan_id", "side", "order_type", "benchmark_price", "price", "account_id",
     "slice_quantities", "released", "released_quantity", "filled_quantity",
     "cancelled_quantity", "notional", "status"}
)
_VWAP_PLAN_KEYS = _PLAN_KEYS | {"algorithm", "volume_weights"}
_POV_PLAN_KEYS = _PLAN_KEYS | {
    "algorithm", "total_quantity", "participation_bps", "market_volume"
}
_ENVELOPE_KEYS_SNAPSHOT = frozenset(
    {"format_version", "engine_version", "config", "config_digest",
     "content", "content_digest"}
)
_CONTENT_KEYS = frozenset({"symbols", "events"})


def _engine_from_json(
    data: dict[str, object],
    config_price_limits: tuple[int, int] | None,
) -> _SymbolState:
    _require(isinstance(data, dict), "symbol state must be an object")
    keys = set(data)
    _require(
        keys == _SYMBOL_STATE_KEYS or keys == _SYMBOL_STATE_KEYS_LEGACY,
        "symbol state has unknown fields",
    )
    if "price_limits" in keys:
        raw_limits = data.get("price_limits")
        if raw_limits is None:
            price_limits: tuple[int, int] | None = None
        else:
            _require(
                isinstance(raw_limits, dict) and set(raw_limits) == {"lower", "upper"},
                "price_limits must be null or an object with exactly 'lower' and 'upper'",
            )
            lower = raw_limits["lower"]
            upper = raw_limits["upper"]
            _require(
                _is_positive_int(lower) and _is_positive_int(upper),
                "price_limits bounds must be positive integers",
            )
            _require(lower <= upper, "price_limits require lower <= upper")
            price_limits = (lower, upper)
    else:
        # A legacy snapshot predates intraday adjustments: the security had
        # only its static configured interval, so seed from the config.
        price_limits = config_price_limits
    last_sequence = data.get("last_sequence")
    _require(
        isinstance(last_sequence, int) and not isinstance(last_sequence, bool) and last_sequence >= 0,
        "last_sequence must be a non-negative integer",
    )
    event_log = data.get("event_log")
    _require(isinstance(event_log, list), "event_log must be a list")
    seen: dict[str, str] = {}
    for entry in event_log:
        _require(isinstance(entry, dict), "event_log entry must be an object")
        _require(set(entry) == {"event_id", "content"},
                 "event_log entry has unknown fields")
        event_id = entry.get("event_id")
        content = entry.get("content")
        _require(isinstance(event_id, str) and isinstance(content, str),
                 "event_log entry fields have wrong types")
        _require(event_id not in seen, "duplicate event id in symbol event log")
        seen[event_id] = content

    # Partition the accepted events into baseline engine events and
    # replay-only events: the engine journal only knows the former plus the
    # child orders synthesized for released slices, while every TWAP/VWAP/POV
    # command id, every read-only EXECUTION_REPORT / IMPACT_REPORT id and
    # every PORTFOLIO_REPORT / PORTFOLIO_STRESS_REPORT id lives solely in the
    # replay log.
    baseline_event_ids: set[str] = set()
    replay_only_event_ids: set[str] = set()
    for event_id, content in seen.items():
        try:
            stored_type = json.loads(content).get("type")
        except (ValueError, AttributeError):
            _require(False, f"event log content for {event_id} is not a JSON object")
            stored_type = None
        if stored_type in _REPLAY_ONLY_TYPES:
            replay_only_event_ids.add(event_id)
        else:
            baseline_event_ids.add(event_id)

    plans_raw = data.get("plans")
    _require(isinstance(plans_raw, list), "plans must be a list")
    plans: dict[str, ExecutionPlan] = {}
    plan_index: dict[str, str] = {}
    released_child_ids: set[str] = set()
    reserved_child_ids: set[str] = set()
    for entry in plans_raw:
        _require(isinstance(entry, dict), "plan entry must be an object")
        _require(
            set(entry) == _PLAN_KEYS
            or set(entry) == _VWAP_PLAN_KEYS
            or set(entry) == _POV_PLAN_KEYS,
            "plan entry has unknown fields",
        )
        plan = _parse_plan_entry(entry)
        plan_id = plan.plan_id
        _require(plan_id not in plans, f"duplicate execution plan id {plan_id}")
        for index in range(plan.child_count):
            child_id = plan.child_order_id(index + 1)
            _require(
                child_id not in plan_index,
                f"derived order id {child_id} is claimed by two plans",
            )
            plan_index[child_id] = plan_id
            if index < plan.released:
                released_child_ids.add(child_id)
            else:
                reserved_child_ids.add(child_id)
        plans[plan_id] = plan

    engine_data = data.get("engine")
    _require(isinstance(engine_data, dict), "engine state must be an object")
    _require(set(engine_data) == _ENGINE_KEYS, "engine state has unknown fields")

    event_ids = engine_data.get("event_ids")
    order_ids = engine_data.get("order_ids")
    reserved_raw = engine_data.get("reserved_order_ids")
    accounts = engine_data.get("accounts")
    _require(_is_str_set_list(event_ids), "engine.event_ids must be a list of strings")
    _require(_is_str_set_list(order_ids), "engine.order_ids must be a list of strings")
    _require(
        _is_str_set_list(reserved_raw),
        "engine.reserved_order_ids must be a list of strings",
    )
    _require(_is_str_set_list(accounts), "engine.accounts must be a list of strings")
    _require(
        set(reserved_raw) == reserved_child_ids,
        "reserved order ids do not match the plans' unreleased slices",
    )
    _require(
        set(reserved_raw).isdisjoint(order_ids),
        "an order id is both reserved and spent",
    )

    orders = engine_data.get("orders")
    _require(isinstance(orders, dict), "engine.orders must be an object")
    for order_id, record in orders.items():
        _require(isinstance(order_id, str) and isinstance(record, dict),
                 "engine.orders entries are malformed")
        _require(set(record) <= _ORDER_RECORD_KEYS,
                 f"order {order_id} has unknown fields")

    def parse_levels(raw_levels: object, raw_totals: object, side_name: str):
        _require(isinstance(raw_levels, list), f"{side_name} levels must be a list")
        levels: list[tuple[int, list[str]]] = []
        seen_prices: set[int] = set()
        for level in raw_levels:
            _require(isinstance(level, dict), f"{side_name} level must be an object")
            _require(set(level) == {"price", "order_ids"},
                     f"{side_name} level has unknown fields")
            price = level.get("price")
            ids = level.get("order_ids")
            _require(
                isinstance(price, int) and not isinstance(price, bool) and price > 0,
                f"{side_name} level price must be a positive integer",
            )
            _require(_is_str_set_list(ids), f"{side_name} level order_ids must be strings")
            _require(price not in seen_prices, f"duplicate {side_name} price level")
            seen_prices.add(price)
            levels.append((price, ids))
        _require(isinstance(raw_totals, dict), f"{side_name} totals must be an object")
        totals: dict[int, int] = {}
        for price_text, qty in raw_totals.items():
            _require(isinstance(price_text, str), f"{side_name} total price key must be a string")
            try:
                price = int(price_text)
            except ValueError:
                _require(False, f"{side_name} total price key is not numeric")
            _require(str(price) == price_text and price > 0,
                     f"{side_name} total price key is not normalized")
            _require(isinstance(qty, int) and not isinstance(qty, bool) and qty > 0,
                     f"{side_name} total quantity must be a positive integer")
            totals[price] = qty
        _require(set(totals) == seen_prices,
                 f"{side_name} levels and aggregates disagree")
        return levels, totals

    bid_levels, bid_totals = parse_levels(
        engine_data.get("bids"), engine_data.get("bid_totals"), "bid"
    )
    ask_levels, ask_totals = parse_levels(
        engine_data.get("asks"), engine_data.get("ask_totals"), "ask"
    )

    bids = {price: deque(ids) for price, ids in bid_levels}
    asks = {price: deque(ids) for price, ids in ask_levels}

    valid_status = {"FILLED", "RESTING", "CANCELLED"}
    for order_id, record in orders.items():
        side = record.get("side")
        status = record.get("status")
        remaining = record.get("remaining")
        price = record.get("price")
        _require(side in ("BUY", "SELL"), f"order {order_id} has a bad side")
        _require(status in valid_status, f"order {order_id} has a bad status")
        _require(
            isinstance(remaining, int) and not isinstance(remaining, bool) and remaining >= 0,
            f"order {order_id} remaining must be a non-negative integer",
        )
        _require(
            price is None or (isinstance(price, int) and not isinstance(price, bool) and price > 0),
            f"order {order_id} price is malformed",
        )
        if "account_id" in record:
            _require(isinstance(record["account_id"], str) and record["account_id"] != "",
                     f"order {order_id} account_id is malformed")
        if "display_quantity" in record or "visible" in record:
            # Iceberg records keep their peak size after they finish; a fully
            # filled record additionally carries a zero current slice, while a
            # cancelled one keeps the slice it had when removed.
            display_quantity = record.get("display_quantity")
            visible = record.get("visible")
            _require(
                isinstance(display_quantity, int) and not isinstance(display_quantity, bool)
                and display_quantity > 0,
                f"iceberg order {order_id} display_quantity is malformed",
            )
            _require(
                isinstance(visible, int) and not isinstance(visible, bool) and visible >= 0,
                f"iceberg order {order_id} visible slice is malformed",
            )
            if status == "RESTING":
                _require(visible > 0, f"resting iceberg {order_id} has no visible slice")
                _require(remaining > 0, f"resting iceberg {order_id} has no remainder")
                # The current slice may itself be partially consumed; it is only
                # reset to min(peak, remaining) once fully exhausted.
                _require(
                    visible <= min(display_quantity, remaining),
                    f"resting iceberg {order_id} slice exceeds peak or remainder",
                )
        else:
            _require(
                status != "RESTING" or remaining > 0,
                f"resting order {order_id} must have a positive remainder",
            )

    def check_level_aggregates(levels, totals, side_name):
        queued: set[str] = set()
        for price, ids in levels:
            level_visible = 0
            for order_id in ids:
                _require(order_id not in queued, f"order {order_id} queued twice")
                queued.add(order_id)
                record = orders.get(order_id)
                _require(record is not None, f"{side_name} queue references unknown order")
                level_visible += record.get("visible", record["remaining"])
            _require(level_visible == totals[price],
                     f"{side_name} aggregate at {price} does not match the queue")
        return queued

    bid_queued = check_level_aggregates(bid_levels, bid_totals, "bid")
    ask_queued = check_level_aggregates(ask_levels, ask_totals, "ask")
    _require(bid_queued.isdisjoint(ask_queued), "an order rests on both sides")

    # Every resting order is queued, and every finished order is not.
    resting = {oid for oid, rec in orders.items() if rec["status"] == "RESTING"}
    _require(resting == bid_queued | ask_queued,
             "resting orders and queued orders disagree")
    for order_id in bid_queued:
        _require(orders[order_id]["side"] == "BUY", f"{order_id} rests on the wrong side")
    for order_id in ask_queued:
        _require(orders[order_id]["side"] == "SELL", f"{order_id} rests on the wrong side")

    # The id sets mirror exactly what the baseline engine would hold. The
    # engine only knows baseline events and synthesized child orders;
    # TWAP/VWAP/POV command ids live solely in the replay layer's event log.
    _require(set(order_ids) == set(orders), "order_ids and order records disagree")
    _require(
        set(event_ids) == (baseline_event_ids - set(plan_index)) | released_child_ids,
        "engine event ids do not match the baseline events and released slices",
    )
    _require(
        released_child_ids <= set(order_ids),
        "a released plan slice is missing an engine order record",
    )
    _require(
        released_child_ids.isdisjoint(reserved_child_ids),
        "a plan slice is both released and reserved",
    )
    _require(last_sequence == len(seen),
             "last_sequence does not match the contiguous accepted event count")

    for record in orders.values():
        account_id = record.get("account_id")
        if account_id is not None:
            _require(account_id in accounts, f"account {account_id} missing from accounts")

    next_trade_id = engine_data.get("next_trade_id")
    _require(
        isinstance(next_trade_id, int) and not isinstance(next_trade_id, bool) and next_trade_id >= 1,
        "next_trade_id must be a positive integer",
    )

    trade_log = engine_data.get("trade_log")
    _require(isinstance(trade_log, list), "trade_log must be a list")
    expected_trade_id = 1
    for trade in trade_log:
        _require(isinstance(trade, dict), "trade log entry must be an object")
        _require(set(trade) == _TRADE_KEYS, "trade log entry has unknown fields")
        trade_id = trade.get("trade_id")
        _require(
            isinstance(trade_id, int) and not isinstance(trade_id, bool) and trade_id == expected_trade_id,
            "trade log ids must be consecutive from 1",
        )
        expected_trade_id += 1
        for key in ("maker_order_id", "taker_order_id", "event_id"):
            _require(isinstance(trade.get(key), str), f"trade log {key} must be a string")
        for key in ("price", "quantity"):
            value = trade.get(key)
            _require(
                isinstance(value, int) and not isinstance(value, bool) and value > 0,
                f"trade log {key} must be a positive integer",
            )
    _require(next_trade_id == expected_trade_id,
             "next_trade_id does not match the cumulative trade log")

    # Historical trades may only reference orders and events that the engine
    # still considers accepted.
    for trade in trade_log:
        maker_id = trade["maker_order_id"]
        taker_id = trade["taker_order_id"]
        trade_event_id = trade["event_id"]
        _require(maker_id in orders, f"trade references unknown maker {maker_id}")
        _require(taker_id in orders, f"trade references unknown taker {taker_id}")
        _require(trade_event_id in event_ids,
                 f"trade references unknown event {trade_event_id}")
        maker = orders[maker_id]
        taker = orders[taker_id]
        _require(maker["side"] != taker["side"], "a trade cannot join two same-side orders")

    # Cross-validate every plan against the engine journal and order records:
    # released child ids must denote IOC child orders with the plan's side and
    # account, the child's original quantity must equal its slice quantity, and
    # the cumulative filled quantity / notional must match the trades exactly.
    child_fill_qty: dict[str, int] = {}
    child_notional: dict[str, int] = {}
    for trade in trade_log:
        taker_id = trade["taker_order_id"]
        if taker_id in plan_index:
            child_fill_qty[taker_id] = child_fill_qty.get(taker_id, 0) + trade["quantity"]
            child_notional[taker_id] = (
                child_notional.get(taker_id, 0) + trade["price"] * trade["quantity"]
            )
    for plan in plans.values():
        plan_filled = 0
        plan_notional = 0
        for index in range(plan.slice_count):
            child_id = plan.child_order_id(index + 1)
            if index >= plan.released:
                _require(
                    child_id not in orders,
                    f"plan {plan.plan_id} unreleased slice already has an order record",
                )
                continue
            record = orders[child_id]
            _require(record["side"] == plan.side,
                     f"child {child_id} trades on the wrong side")
            _require(record["status"] != "RESTING",
                     f"IOC child {child_id} must not rest in the book")
            _require(
                record["price"] == plan.price,
                f"child {child_id} price disagrees with its plan",
            )
            _require(
                record.get("account_id") == plan.account_id,
                f"child {child_id} account disagrees with its plan",
            )
            filled = child_fill_qty.get(child_id, 0)
            slice_quantity = plan.slice_quantities[index]
            _require(
                filled + record["remaining"] == slice_quantity,
                f"child {child_id} quantity does not match its plan slice",
            )
            plan_filled += filled
            plan_notional += child_notional.get(child_id, 0)
        _require(plan_filled == plan.filled_quantity,
                 f"plan {plan.plan_id} filled quantity disagrees with the trade log")
        _require(plan_notional == plan.notional,
                 f"plan {plan.plan_id} notional disagrees with the trade log")

    # Every derived id is either a released child order or an engine
    # reservation; no plan id may be missing on either side, and external
    # orders may never squat on a derived id.
    _require(
        set(plan_index) == released_child_ids | reserved_child_ids,
        "plan derived ids disagree with engine order state",
    )
    _require(
        released_child_ids.isdisjoint(reserved_child_ids),
        "a plan slice is both released and reserved",
    )

    # Deep copy so later caller mutation of the snapshot document can never
    # reach the live engine.
    engine = Engine(
        _state={
            "event_ids": set(event_ids),
            "order_ids": set(order_ids),
            "reserved_order_ids": set(reserved_raw),
            "orders": copy.deepcopy(orders),
            "bids": bids,
            "asks": asks,
            "bid_totals": bid_totals,
            "ask_totals": ask_totals,
            "next_trade_id": next_trade_id,
            "accounts": set(accounts),
            "trade_log": copy.deepcopy(trade_log),
        }
    )
    state = _SymbolState(engine)
    state.last_sequence = last_sequence
    state.seen = seen
    state.plans = plans
    state.plan_index = plan_index
    state.price_limits = price_limits
    return state


def export_snapshot(replayer: EventReplayer) -> dict[str, object]:
    """Export a resumable, JSON-compatible snapshot of a replay session.

    The snapshot fully preserves (per security): price-time queue priority,
    order remainders, the iceberg current slice and replenishment state, the
    last sequence, cumulative trades, the next trade id counter and the
    active price-limit interval. It also carries the format version, the
    matching configuration (plus its digest) and a SHA-256 digest over the
    normalized whole document.
    """
    symbols = [
        {"symbol": symbol, "state": _engine_to_json(replayer._symbols[symbol])}
        for symbol in sorted(replayer._symbols)
    ]
    content = {
        "symbols": symbols,
        "events": [
            {"event_id": event_id, "symbol": symbol, "content": content_str}
            for event_id, (symbol, content_str) in sorted(replayer._events.items())
        ],
    }
    envelope: dict[str, object] = {
        "format_version": FORMAT_VERSION,
        "engine_version": __version__,
        "config": replayer.config,
        "config_digest": replayer.config_digest,
        "content": content,
    }
    envelope["content_digest"] = _digest(
        {key: value for key, value in envelope.items() if key != "content_digest"}
    )
    return envelope


def restore_replayer(
    snapshot: dict[str, object],
    config: dict[str, object] | None = None,
) -> EventReplayer:
    """Rebuild an :class:`EventReplayer` from :func:`export_snapshot` output.

    Version, configuration digest and content SHA-256 are verified before any
    state is adopted. Any failure raises :class:`SnapshotError` and leaves no
    partially recovered session behind.
    """
    if not isinstance(snapshot, dict):
        raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot must be a JSON object")
    if not set(snapshot) <= _ENVELOPE_KEYS_SNAPSHOT:
        raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot document has unknown fields")

    # A malformed caller configuration is a request error and is rejected
    # before any snapshot content (or the configuration comparison) is
    # consulted, so it never masquerades as a CONFIG_MISMATCH.
    _validate_config(dict(DEFAULT_CONFIG if config is None else config))

    version = snapshot.get("format_version")
    if version != FORMAT_VERSION:
        raise SnapshotError(
            SNAPSHOT_VERSION_UNSUPPORTED,
            f"unsupported snapshot format version: {version!r}",
        )
    engine_version = snapshot.get("engine_version")
    if not isinstance(engine_version, str) or engine_version == "":
        raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot engine_version is malformed")

    expected_config = DEFAULT_CONFIG if config is None else config
    snapshot_config = snapshot.get("config")
    if snapshot_config != expected_config:
        raise SnapshotError(CONFIG_MISMATCH, "snapshot configuration does not match")
    if snapshot.get("config_digest") != _digest(expected_config):
        raise SnapshotError(CONFIG_MISMATCH, "snapshot configuration digest does not match")

    stored_digest = snapshot.get("content_digest")
    actual_digest = _digest(
        {key: value for key, value in snapshot.items() if key != "content_digest"}
    )
    if not isinstance(stored_digest, str) or stored_digest != actual_digest:
        raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot content digest mismatch")

    content = snapshot.get("content")
    if (
        not isinstance(content, dict)
        or set(content) != _CONTENT_KEYS
        or not isinstance(content.get("symbols"), list)
        or not isinstance(content.get("events"), list)
    ):
        raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot content is malformed")

    # Build the whole session in locals first; only commit it to the returned
    # object after every component parsed successfully.
    replayer = EventReplayer(expected_config)
    symbols: dict[str, _SymbolState] = {}
    events: dict[str, tuple[str, str]] = {}
    for entry in content["symbols"]:
        if not (
            isinstance(entry, dict)
            and set(entry) == {"symbol", "state"}
            and isinstance(entry.get("symbol"), str)
            and isinstance(entry.get("state"), dict)
        ):
            raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot symbol entry is malformed")
        symbol = entry["symbol"]
        if symbol in symbols:
            raise SnapshotError(SNAPSHOT_CORRUPT, f"duplicate symbol in snapshot: {symbol}")
        try:
            symbols[symbol] = _engine_from_json(
                entry["state"], replayer.price_limits.get(symbol)
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise SnapshotError(SNAPSHOT_CORRUPT, f"snapshot state for {symbol} is malformed") from exc
    for entry in content.get("events", []):
        if not isinstance(entry, dict) or set(entry) != {"event_id", "symbol", "content"}:
            raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot event log is malformed")
        event_id = entry.get("event_id")
        symbol = entry.get("symbol")
        text = entry.get("content")
        if not isinstance(event_id, str) or not isinstance(symbol, str) or not isinstance(text, str):
            raise SnapshotError(SNAPSHOT_CORRUPT, "snapshot event log entry is malformed")
        if symbol not in symbols:
            raise SnapshotError(SNAPSHOT_CORRUPT, f"event {event_id} references unknown symbol")
        if event_id in events:
            raise SnapshotError(SNAPSHOT_CORRUPT, f"duplicate event id in snapshot: {event_id}")
        events[event_id] = (symbol, text)

    # The global event log and the per-symbol event logs must describe exactly
    # the same accepted events, with identical normalized content.
    logged_per_symbol: dict[str, int] = {}
    for symbol, text in events.values():
        logged_per_symbol[symbol] = logged_per_symbol.get(symbol, 0) + 1
    for symbol, symbol_state in symbols.items():
        for event_id, text in symbol_state.seen.items():
            if events.get(event_id) != (symbol, text):
                raise SnapshotError(
                    SNAPSHOT_CORRUPT,
                    f"event {event_id} disagrees between global and symbol log",
                )
        if logged_per_symbol.get(symbol, 0) != len(symbol_state.seen):
            raise SnapshotError(
                SNAPSHOT_CORRUPT,
                f"event log counts disagree for symbol {symbol}",
            )

    replayer._symbols = symbols
    replayer._events = events
    return replayer
