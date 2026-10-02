"""Deterministic multi-symbol ordered event replay with resumable snapshots.

This module adds a new public entry point on top of the baseline single-book
:class:`~order_book_engine.engine.Engine`; it never changes the baseline
matching rules, priorities, rejection semantics or trade record shapes:

* A single public call (:func:`replay_events`) accepts an ordered stream of
  order events for one or several securities. Every security keeps its own
  sequence counter and its own book; events are applied strictly in input
  order, so identical timestamps never reorder anything. The stream covers
  the baseline ADD/CANCEL/REPLACE behaviours, resumable TWAP/VWAP parent
  orders (TWAP_START/TWAP_SLICE/TWAP_CANCEL/TWAP_REPORT and
  VWAP_START/VWAP_SLICE/VWAP_CANCEL/VWAP_REPORT), the read-only
  cross-security PORTFOLIO_REPORT query and the intraday PRICE_LIMIT_UPDATE
  adjustment, which replaces one security's active price-limit interval
  (seeded from the static ``price_limits`` configuration) for all
  subsequently submitted limit prices; plans never read a wall clock and
  are advanced solely by their SLICE events. TWAP slices divide
  the total evenly; VWAP slices follow a caller-supplied volume-weight
  curve.
* Each event is committed individually: a later failure never rolls back an
  earlier success, and a failed event leaves no order, trade, counter or book
  change behind.
* Results are plain JSON-compatible dictionaries serialized from canonical
  content (sorted keys, compact separators). Equal inputs always produce
  byte-identical output.
* A snapshot can be exported after any successful event and later passed back
  as the starting point. Snapshots carry a format version, a matching
  configuration summary and a SHA-256 digest over their normalized content.
  Resumption verifies version, configuration and digest before touching any
  state, and continued replay produces trade ids, trade ordering and final
  results identical to an uninterrupted one-shot run. Plan progress, reserved
  derived order ids and cumulative plan analytics are part of the snapshot.

The module never opens files: events and snapshots are received from, and
returned to, the caller.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import deque

from . import __version__
from .engine import (
    ADD,
    BUY,
    CANCEL,
    DUPLICATE_EVENT_ID,
    DUPLICATE_ORDER_ID,
    ICEBERG,
    IOC,
    LIMIT,
    MARKET,
    REPLACE,
    REPORTED,
    SELL,
    Engine,
    UNKNOWN_ACCOUNT,
    _is_non_empty_str,
    _is_positive_int,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FORMAT_VERSION = "event-replay/2"

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"
DUPLICATE = "DUPLICATE"

# New validation / ordering rejection codes.
INVALID_EVENT = "INVALID_EVENT"
SEQUENCE_GAP = "SEQUENCE_GAP"
OUT_OF_ORDER = "OUT_OF_ORDER"
EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"

# TWAP parent-order rejection codes.
UNKNOWN_EXECUTION_PLAN = "UNKNOWN_EXECUTION_PLAN"
EXECUTION_PLAN_CLOSED = "EXECUTION_PLAN_CLOSED"
DUPLICATE_EXECUTION_PLAN = "DUPLICATE_EXECUTION_PLAN"

# Static per-security price-limit rejection code.
PRICE_LIMIT_EXCEEDED = "PRICE_LIMIT_EXCEEDED"

# Intraday price-limit adjustment event and its success result. The command
# replaces the security's active interval wholesale; it is replay-only and
# never reaches the baseline engine or the JSON Lines entry point.
PRICE_LIMIT_UPDATE = "PRICE_LIMIT_UPDATE"
PRICE_LIMIT_UPDATED = "PRICE_LIMIT_UPDATED"

# Cross-security portfolio report event and its business rejection codes.
PORTFOLIO_REPORT = "PORTFOLIO_REPORT"
MARK_PRICE_MISMATCH = "MARK_PRICE_MISMATCH"

# TWAP event types.
TWAP_START = "TWAP_START"
TWAP_SLICE = "TWAP_SLICE"
TWAP_CANCEL = "TWAP_CANCEL"
TWAP_REPORT = "TWAP_REPORT"

# VWAP event types.
VWAP_START = "VWAP_START"
VWAP_SLICE = "VWAP_SLICE"
VWAP_CANCEL = "VWAP_CANCEL"
VWAP_REPORT = "VWAP_REPORT"

# Plan algorithm labels: TWAP plans keep the historical summary shape, VWAP
# plans additionally report their algorithm and per-slice schedule.
ALGORITHM_TWAP = "TWAP"
ALGORITHM_VWAP = "VWAP"

# TWAP plan lifecycle statuses.
PLAN_ACTIVE = "ACTIVE"
PLAN_COMPLETED = "COMPLETED"
PLAN_CANCELLED = "CANCELLED"

# Snapshot restoration failure codes.
SNAPSHOT_CORRUPT = "SNAPSHOT_CORRUPT"
SNAPSHOT_VERSION_UNSUPPORTED = "SNAPSHOT_VERSION_UNSUPPORTED"
CONFIG_MISMATCH = "CONFIG_MISMATCH"

#: Event types this replay layer accepts. The TWAP/VWAP parent-order commands
#: join the baseline mutating behaviours; the baseline single-security
#: read-only reports stay exclusive to the JSON Lines entry point, while the
#: cross-security PORTFOLIO_REPORT query and the intraday PRICE_LIMIT_UPDATE
#: adjustment are exclusive to this layer.
SUPPORTED_TYPES = frozenset(
    {ADD, CANCEL, REPLACE,
     TWAP_START, TWAP_SLICE, TWAP_CANCEL, TWAP_REPORT,
     VWAP_START, VWAP_SLICE, VWAP_CANCEL, VWAP_REPORT,
     PORTFOLIO_REPORT, PRICE_LIMIT_UPDATE}
)

_ENVELOPE_KEYS = frozenset({"event_id", "symbol", "sequence", "timestamp"})
_BASELINE_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity", "account_id"}
)
_PORTFOLIO_REPORT_KEYS = frozenset(
    {"event_id", "type", "account_id", "mark_prices"}
)
_PRICE_LIMIT_UPDATE_KEYS = frozenset(
    {"event_id", "type", "lower_price", "upper_price"}
)
_TWAP_START_KEYS = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity", "slice_count",
     "order_type", "benchmark_price", "price", "account_id"}
)
_TWAP_START_REQUIRED = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity", "slice_count",
     "order_type", "benchmark_price"}
)
_TWAP_PLAN_REF_KEYS = frozenset({"event_id", "type", "plan_id"})
_TWAP_TYPES = frozenset({TWAP_START, TWAP_SLICE, TWAP_CANCEL, TWAP_REPORT})
_VWAP_START_KEYS = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity", "volume_weights",
     "order_type", "benchmark_price", "price", "account_id"}
)
_VWAP_START_REQUIRED = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity", "volume_weights",
     "order_type", "benchmark_price"}
)
_VWAP_TYPES = frozenset({VWAP_START, VWAP_SLICE, VWAP_CANCEL, VWAP_REPORT})
#: Every parent-order command type, across algorithms.
_PLAN_TYPES = _TWAP_TYPES | _VWAP_TYPES
#: Event types whose ids live solely in the replay log: parent-order commands
#: never touch the engine journal, the cross-security portfolio query is
#: read-only and matched by no engine, and a price-limit adjustment only
#: rewrites replay-layer state. Baseline ADD/CANCEL/REPLACE ids occupy the
#: per-symbol engine journal instead.
_REPLAY_ONLY_TYPES = _PLAN_TYPES | frozenset({PORTFOLIO_REPORT, PRICE_LIMIT_UPDATE})


def _allocate_slices(total_quantity: int, weights: list[int]) -> list[int]:
    """Split ``total_quantity`` across buckets proportionally to ``weights``.

    Every bucket receives one unit up front; the remaining units are
    distributed in proportion to the weights by integer quotient, and the
    units left over by that truncation go one each to the buckets with the
    largest division remainder, earlier buckets first on ties. The result
    always sums to exactly ``total_quantity``.
    """
    count = len(weights)
    remainder = total_quantity - count
    total_weight = sum(weights)
    quantities: list[int] = []
    residuals: list[int] = []
    for weight in weights:
        quotient, residual = divmod(remainder * weight, total_weight)
        quantities.append(1 + quotient)
        residuals.append(residual)
    leftover = remainder - (sum(quantities) - count)
    by_residual = sorted(range(count), key=lambda i: (-residuals[i], i))
    for index in by_residual[:leftover]:
        quantities[index] += 1
    return quantities


def _twap_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a TWAP command payload.

    Mirrors the baseline contract: any field problem (missing or extra field,
    wrong type, a non-positive integer where a positive one is required, an
    empty identifier, a LIMIT without a positive price or a MARKET carrying
    one, ...) is an ``INVALID_EVENT`` and consumes neither the event id nor the
    sequence.
    """
    event_type = payload["type"]
    if event_type == TWAP_START:
        keys = set(payload)
        if not keys >= _TWAP_START_REQUIRED or not keys <= _TWAP_START_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if payload.get("side") not in (BUY, SELL):
            return INVALID_EVENT
        order_type = payload.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_EVENT
        total_quantity = payload.get("total_quantity")
        slice_count = payload.get("slice_count")
        benchmark_price = payload.get("benchmark_price")
        if not _is_positive_int(total_quantity):
            return INVALID_EVENT
        if not _is_positive_int(slice_count):
            return INVALID_EVENT
        if total_quantity < slice_count:
            return INVALID_EVENT
        if not _is_positive_int(benchmark_price):
            return INVALID_EVENT
        if order_type == LIMIT:
            if not _is_positive_int(payload.get("price")):
                return INVALID_EVENT
        elif "price" in payload and payload["price"] is not None:
            # MARKET plans must not carry a non-null price; omission or an
            # explicit null is accepted.
            return INVALID_EVENT
        if "account_id" in payload and not _is_non_empty_str(payload.get("account_id")):
            return INVALID_EVENT
        return None
    # TWAP_SLICE / TWAP_CANCEL / TWAP_REPORT are pure plan references.
    if set(payload) != _TWAP_PLAN_REF_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    return None


def _vwap_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a VWAP command payload.

    Follows the TWAP contract exactly: any field problem (missing or extra
    field, wrong type, a non-positive integer where a positive one is
    required, an empty identifier, an empty or non-positive weight, a LIMIT
    without a positive price or a MARKET carrying one, ...) is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    event_type = payload["type"]
    if event_type == VWAP_START:
        keys = set(payload)
        if not keys >= _VWAP_START_REQUIRED or not keys <= _VWAP_START_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if payload.get("side") not in (BUY, SELL):
            return INVALID_EVENT
        order_type = payload.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_EVENT
        total_quantity = payload.get("total_quantity")
        volume_weights = payload.get("volume_weights")
        benchmark_price = payload.get("benchmark_price")
        if not _is_positive_int(total_quantity):
            return INVALID_EVENT
        if not (
            isinstance(volume_weights, list)
            and len(volume_weights) > 0
            and all(_is_positive_int(weight) for weight in volume_weights)
        ):
            return INVALID_EVENT
        # Every bucket receives one unit up front, so the total must cover
        # the bucket count.
        if total_quantity < len(volume_weights):
            return INVALID_EVENT
        if not _is_positive_int(benchmark_price):
            return INVALID_EVENT
        if order_type == LIMIT:
            if not _is_positive_int(payload.get("price")):
                return INVALID_EVENT
        elif "price" in payload and payload["price"] is not None:
            # MARKET plans must not carry a non-null price; omission or an
            # explicit null is accepted.
            return INVALID_EVENT
        if "account_id" in payload and not _is_non_empty_str(payload.get("account_id")):
            return INVALID_EVENT
        return None
    # VWAP_SLICE / VWAP_CANCEL / VWAP_REPORT are pure plan references.
    if set(payload) != _TWAP_PLAN_REF_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    return None


def _portfolio_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PORTFOLIO_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``account_id`` and
    ``mark_prices``; identifiers are non-empty strings and ``mark_prices`` is
    an object mapping non-empty symbol strings to positive integers (booleans
    do not count). The map may be empty structurally: for a known account it
    can never match that account's securities, so the empty map is classified
    as ``MARK_PRICE_MISMATCH`` (which consumes the id and the sequence) rather
    than as a schema error. Whether the account is known and whether the key
    set actually covers its securities are therefore business checks, not
    schema checks: an INVALID_EVENT consumes neither the event id nor the
    sequence, while the business rejections do.
    """
    if set(payload) != _PORTFOLIO_REPORT_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("account_id")):
        return INVALID_EVENT
    mark_prices = payload.get("mark_prices")
    if not isinstance(mark_prices, dict):
        return INVALID_EVENT
    for symbol, mark_price in mark_prices.items():
        if not _is_non_empty_str(symbol):
            return INVALID_EVENT
        if not _is_positive_int(mark_price):
            return INVALID_EVENT
    return None


def _price_limit_update_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PRICE_LIMIT_UPDATE command payload.

    The command carries exactly ``event_id``, ``type``, ``lower_price`` and
    ``upper_price``; both bounds are positive integers (booleans do not
    count) with ``lower_price <= upper_price``. Any field problem is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    if set(payload) != _PRICE_LIMIT_UPDATE_KEYS:
        return INVALID_EVENT
    lower_price = payload.get("lower_price")
    upper_price = payload.get("upper_price")
    if not _is_positive_int(lower_price) or not _is_positive_int(upper_price):
        return INVALID_EVENT
    if lower_price > upper_price:
        return INVALID_EVENT
    return None


def _valid_timestamp(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    return isinstance(value, str) and value != ""

DEFAULT_CONFIG: dict[str, object] = {
    "matching_engine": "order-book-engine",
    "engine_version": __version__,
    "price_time_priority": True,
    "trade_id_scheme": "per_symbol_monotonic_from_1",
    "iceberg_replenishment": "tail_of_price_level",
    "self_trade_prevention": "same_account_only",
}


def _price_limit_error(price_limits: object) -> str | None:
    """Validate the optional static ``price_limits`` configuration block.

    The block maps each configured security name (a non-empty string) to an
    object holding exactly ``lower`` and ``upper``; both bounds are positive
    integers (booleans do not count) with ``lower <= upper``. An empty block
    (or an absent one) leaves every security at the baseline behaviour.
    Returns an error message when the block is malformed, otherwise ``None``.
    """
    if not isinstance(price_limits, dict):
        return "price_limits must be an object mapping symbols to bounds"
    for symbol, bounds in price_limits.items():
        if not _is_non_empty_str(symbol):
            return "price_limits keys must be non-empty symbol strings"
        if not isinstance(bounds, dict) or set(bounds) != {"lower", "upper"}:
            return (
                f"price_limits for {symbol!r} must be an object with exactly "
                "'lower' and 'upper'"
            )
        lower = bounds["lower"]
        upper = bounds["upper"]
        if not _is_positive_int(lower) or not _is_positive_int(upper):
            return f"price_limits for {symbol!r} must use positive integer bounds"
        if lower > upper:
            return f"price_limits for {symbol!r} require lower <= upper"
    return None


def _validate_config(config: dict[str, object]) -> None:
    """Raise ``ValueError`` before any event or snapshot is touched."""
    if "price_limits" in config:
        message = _price_limit_error(config["price_limits"])
        if message is not None:
            raise ValueError(message)


# ---------------------------------------------------------------------------
# Deterministic serialization
# ---------------------------------------------------------------------------


def canonical_json(value: object) -> bytes:
    """Serialize JSON content deterministically.

    Object keys are sorted, separators are compact and no ASCII escaping is
    applied. Integers stay as exact JSON integers; only JSON-native content is
    accepted (the module never produces floats or other lossy types).
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


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


# ---------------------------------------------------------------------------
# Replayer
# ---------------------------------------------------------------------------


class ExecutionPlan:
    """Mutable state of one resumable TWAP or VWAP parent order on one security.

    All cumulative analytics (filled quantity, notional, cancelled quantity)
    are maintained incrementally, so a snapshot carries exactly the state a
    continued or resumed slice needs. Slice quantities are fixed at creation:
    a TWAP plan divides the total evenly across ``slice_count`` slices and
    spreads the division remainder as one extra unit over the earliest
    slices, while a VWAP plan allocates one unit per bucket up front and
    distributes the rest proportionally to its volume weights (see
    :func:`_allocate_slices`).
    """

    __slots__ = (
        "plan_id", "side", "order_type", "benchmark_price", "price",
        "account_id", "algorithm", "volume_weights", "slice_quantities",
        "released", "released_quantity", "filled_quantity",
        "cancelled_quantity", "notional", "status",
    )

    def __init__(
        self,
        plan_id: str,
        side: str,
        order_type: str,
        total_quantity: int,
        slice_count: int,
        benchmark_price: int,
        price: int | None,
        account_id: str | None,
        *,
        algorithm: str = ALGORITHM_TWAP,
        volume_weights: list[int] | None = None,
    ) -> None:
        self.plan_id = plan_id
        self.side = side
        self.order_type = order_type
        self.benchmark_price = benchmark_price
        self.price = price
        self.account_id = account_id
        self.algorithm = algorithm
        self.volume_weights = list(volume_weights) if volume_weights is not None else None
        if algorithm == ALGORITHM_VWAP:
            self.slice_quantities: list[int] = _allocate_slices(
                total_quantity, self.volume_weights
            )
        else:
            base, extra = divmod(total_quantity, slice_count)
            self.slice_quantities = [
                base + (1 if index < extra else 0)
                for index in range(slice_count)
            ]
        # Number of slices already released; also the index of the next one.
        self.released = 0
        self.released_quantity = 0
        self.filled_quantity = 0
        self.cancelled_quantity = 0
        self.notional = 0
        self.status = PLAN_ACTIVE

    @property
    def slice_count(self) -> int:
        return len(self.slice_quantities)

    @property
    def total_quantity(self) -> int:
        return sum(self.slice_quantities)

    @property
    def remaining_slices(self) -> int:
        if self.status != PLAN_ACTIVE:
            return 0
        return self.slice_count - self.released

    def child_order_id(self, slice_number: int) -> str:
        """The deterministic derived order id of slice ``slice_number``.

        Slice numbers are 1-based and joined with ``#``; the scheme is fixed,
        so the same input stream always derives the same identifiers.
        """
        return f"{self.plan_id}#{slice_number}"

    def child_ids(self) -> list[str]:
        return [self.child_order_id(i + 1) for i in range(self.slice_count)]

    def summary(
        self,
        *,
        slice_number: int | None = None,
        child_order_id: str | None = None,
        target_weight: int | None = None,
        scheduled_quantity: int | None = None,
    ) -> dict[str, object]:
        """Build the ``execution_plan`` object echoed by plan responses."""
        vwap = (
            {"numerator": self.notional, "denominator": self.filled_quantity}
            if self.filled_quantity
            else None
        )
        slippage = self.notional - self.benchmark_price * self.filled_quantity
        if self.side == SELL:
            # Mirror the buy formula: negative always means improvement.
            slippage = -slippage
        plan: dict[str, object] = {
            "status": self.status,
            "released_quantity": self.released_quantity,
            "filled_quantity": self.filled_quantity,
            "cancelled_quantity": self.cancelled_quantity,
            "remaining_slices": self.remaining_slices,
            "executed_notional": self.notional,
            "vwap": vwap,
            "slippage_notional": slippage,
        }
        if self.algorithm == ALGORITHM_VWAP:
            plan["algorithm"] = ALGORITHM_VWAP
        if slice_number is not None:
            plan["slice_number"] = slice_number
        if child_order_id is not None:
            plan["child_order_id"] = child_order_id
        if target_weight is not None:
            plan["target_weight"] = target_weight
        if scheduled_quantity is not None:
            plan["scheduled_quantity"] = scheduled_quantity
        return plan

    def to_json(self) -> dict[str, object]:
        """Serialize the complete plan state for a snapshot."""
        data: dict[str, object] = {
            "plan_id": self.plan_id,
            "side": self.side,
            "order_type": self.order_type,
            "benchmark_price": self.benchmark_price,
            "price": self.price,
            "account_id": self.account_id,
            "slice_quantities": list(self.slice_quantities),
            "released": self.released,
            "released_quantity": self.released_quantity,
            "filled_quantity": self.filled_quantity,
            "cancelled_quantity": self.cancelled_quantity,
            "notional": self.notional,
            "status": self.status,
        }
        if self.algorithm == ALGORITHM_VWAP:
            # TWAP records keep their historical shape; VWAP records add the
            # algorithm label and the schedule the allocation derives from.
            data["algorithm"] = ALGORITHM_VWAP
            data["volume_weights"] = list(self.volume_weights)
        return data


class _SymbolState:
    __slots__ = ("engine", "last_sequence", "seen", "plans", "plan_index",
                 "price_limits")

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine or Engine()
        self.last_sequence = 0
        # eventId -> canonical payload content, in first-seen input order.
        self.seen: dict[str, str] = {}
        # plan_id -> ExecutionPlan, in start order. Closed plans stay queryable.
        self.plans: dict[str, ExecutionPlan] = {}
        # child order id -> plan_id, for every derived id of every accepted
        # plan, including slices not yet released.
        self.plan_index: dict[str, str] = {}
        # The security's active price-limit interval (closed), or None when
        # unlimited. Seeded from the static config block and replaced
        # wholesale by every accepted PRICE_LIMIT_UPDATE.
        self.price_limits: tuple[int, int] | None = None


class EventReplayer:
    """Stateful, resumable multi-symbol replay session.

    One session binds together the per-symbol books, per-symbol sequence
    counters, the global idempotency log and the matching configuration.
    Instances are normally obtained from :func:`replay_events` or restored via
    :func:`restore_replayer`; they are not thread safe on purpose.
    """

    def __init__(self, config: dict[str, object] | None = None) -> None:
        if config is not None and not isinstance(config, dict):
            raise TypeError("config must be a JSON object or None")
        self.config: dict[str, object] = dict(DEFAULT_CONFIG if config is None else config)
        _validate_config(self.config)
        # Static per-security price bounds (closed interval), parsed from the
        # validated config. They seed each security's *active* interval (and
        # a legacy snapshot's restored one); accepted PRICE_LIMIT_UPDATE
        # events then replace the active interval per security. Securities
        # without an entry start unlimited.
        raw_limits = self.config.get("price_limits") or {}
        self.price_limits: dict[str, tuple[int, int]] = {
            symbol: (bounds["lower"], bounds["upper"])
            for symbol, bounds in raw_limits.items()
        }
        self.config_digest = _digest(self.config)
        self._symbols: dict[str, _SymbolState] = {}
        # Global index of every accepted eventId: eventId -> (symbol, content).
        self._events: dict[str, tuple[str, str]] = {}

    # -- book helpers -------------------------------------------------------

    def book(self, symbol: str) -> tuple[list[dict[str, int]], list[dict[str, int]]]:
        """The final ``(bids, asks)`` aggregates for one security."""
        state = self._symbols.get(symbol)
        if state is None:
            return [], []
        return state.engine.snapshot()

    # -- main entry ---------------------------------------------------------

    def submit(self, events: list[dict[str, object]]) -> list[dict[str, object]]:
        """Apply an ordered list of event objects, one result per event."""
        return [self._submit_one(event) for event in events]

    def _book_changes(
        self,
        engine: Engine,
        before_bids: dict[int, int],
        before_asks: dict[int, int],
    ) -> dict[str, list[dict[str, int]]]:
        bid_totals, ask_totals = engine.level_totals()
        bid_changes: list[dict[str, int]] = []
        for price in sorted(set(before_bids) | set(bid_totals), reverse=True):
            new_qty = bid_totals.get(price, 0)
            if before_bids.get(price, 0) != new_qty:
                bid_changes.append({"price": price, "quantity": new_qty})
        ask_changes: list[dict[str, int]] = []
        for price in sorted(set(before_asks) | set(ask_totals)):
            new_qty = ask_totals.get(price, 0)
            if before_asks.get(price, 0) != new_qty:
                ask_changes.append({"price": price, "quantity": new_qty})
        return {"bids": bid_changes, "asks": ask_changes}

    @staticmethod
    def _invalid(
        event: object,
        code: str = INVALID_EVENT,
        *,
        expected_sequence: int | None = None,
        state: _SymbolState | None = None,
    ) -> dict[str, object]:
        event_id = symbol = None
        sequence = None
        if isinstance(event, dict):
            eid = event.get("event_id")
            if isinstance(eid, str) and eid != "":
                event_id = eid
            sym = event.get("symbol")
            if isinstance(sym, str) and sym != "":
                symbol = sym
            seq = event.get("sequence")
            if isinstance(seq, int) and not isinstance(seq, bool):
                sequence = seq
        result: dict[str, object] = {
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "status": REJECTED,
            "rejection_code": code,
        }
        if expected_sequence is not None:
            result["expected_sequence"] = expected_sequence
        result["trades"] = []
        result["book_changes"] = {"bids": [], "asks": []}
        if state is None:
            # The event references an unknown symbol, so there is no book to
            # echo; it also must not create the symbol on its own.
            result["bids"] = []
            result["asks"] = []
        else:
            # A pre-dispatch failure against a known symbol echoes the
            # untouched book, exactly like a baseline rejection does.
            result["bids"], result["asks"] = state.engine.snapshot()
        return result

    def _submit_one(self, event: object) -> dict[str, object]:
        # ---- envelope validation (nothing is consumed on failure) --------
        if not isinstance(event, dict):
            return self._invalid(event)
        event_id = event.get("event_id")
        symbol = event.get("symbol")
        sequence = event.get("sequence")
        timestamp = event.get("timestamp")
        valid_envelope_ids = (
            isinstance(event_id, str)
            and event_id != ""
            and isinstance(symbol, str)
            and symbol != ""
            and isinstance(sequence, int)
            and not isinstance(sequence, bool)
            and sequence > 0
        )
        if not valid_envelope_ids:
            # The symbol cannot be trusted yet, so no book is echoed and no
            # symbol state is created.
            return self._invalid(event)

        def reject_envelope(code: str = INVALID_EVENT) -> dict[str, object]:
            # A structurally invalid event for a recognizable symbol echoes the
            # untouched book but never mutates or registers anything.
            return self._invalid(event, code, state=self._symbols.get(symbol))

        if "timestamp" in event and not _valid_timestamp(timestamp):
            return reject_envelope()

        # The baseline payload is either nested under "event" or carried
        # inline alongside the envelope fields.
        if "event" in event:
            nested = event["event"]
            envelope_keys = _ENVELOPE_KEYS | {"event"}
            if not isinstance(nested, dict):
                return reject_envelope()
            payload: dict[str, object] = nested
            # The wrapper may only carry envelope fields plus the payload.
            if not set(event) <= envelope_keys:
                return reject_envelope()
        else:
            # The inline form may carry envelope fields plus exactly the fields
            # of its own payload kind.
            allowed_payload_keys = self._allowed_payload_keys(event.get("type"))
            if allowed_payload_keys is None or not set(event) <= (
                _ENVELOPE_KEYS | allowed_payload_keys
            ):
                return reject_envelope()
            # Keep event_id/type (payload fields); strip only symbol,
            # sequence and timestamp, which are envelope-only.
            payload = {
                key: value
                for key, value in event.items()
                if key not in ("symbol", "sequence", "timestamp")
            }
        event_type = payload.get("type")
        if not isinstance(event_type, str) or event_type not in SUPPORTED_TYPES:
            return reject_envelope()
        # Validate the full payload schema up front, so malformed content is
        # classified INVALID_EVENT and reaches neither ordering nor the book.
        # The wrapper and the payload must identify the same event.
        if payload.get("event_id") != event_id:
            return reject_envelope()
        if event_type in _TWAP_TYPES:
            schema_error = _twap_schema_error(payload)
        elif event_type in _VWAP_TYPES:
            schema_error = _vwap_schema_error(payload)
        elif event_type == PORTFOLIO_REPORT:
            schema_error = _portfolio_report_schema_error(payload)
        elif event_type == PRICE_LIMIT_UPDATE:
            schema_error = _price_limit_update_schema_error(payload)
        else:
            schema_error = Engine._schema_error(payload)
        if schema_error is not None:
            return reject_envelope()

        # A symbol state is created only once the event is fully well formed;
        # failures that precede dispatch must not register an empty book.
        state = self._symbols.get(symbol)
        known_symbol = state is not None
        if state is None:
            state = _SymbolState()
            # The active interval starts at the static configuration; an
            # accepted PRICE_LIMIT_UPDATE replaces it from then on.
            state.price_limits = self.price_limits.get(symbol)

        # ---- global idempotency (checked before sequencing) --------------
        # A retried delivery carries its original, now-stale sequence; it must
        # still be recognized as a duplicate rather than as out-of-order.
        content = canonical_json(payload).decode("utf-8")
        prior = self._events.get(event_id)
        if prior is not None:
            prior_symbol, prior_content = prior
            if prior_symbol == symbol and prior_content == content:
                bids, asks = state.engine.snapshot()
                return {
                    "event_id": event_id,
                    "symbol": symbol,
                    "sequence": sequence,
                    "status": DUPLICATE,
                    "trades": [],
                    "book_changes": {"bids": [], "asks": []},
                    "bids": bids,
                    "asks": asks,
                }
            # Same id, different symbol or different normalized content: a
            # conflict consumes neither sequence nor book state, so the
            # correct next event can follow immediately.
            return self._invalid(
                event, EVENT_ID_CONFLICT,
                expected_sequence=state.last_sequence + 1 if known_symbol else None,
                state=state if known_symbol else None,
            )

        # ---- per-symbol strict sequence ordering -------------------------
        expected = state.last_sequence + 1
        if sequence < expected or sequence > expected:
            code = OUT_OF_ORDER if sequence < expected else SEQUENCE_GAP
            return self._invalid(
                event, code, expected_sequence=expected,
                state=state if known_symbol else None,
            )

        # ---- committed application ---------------------------------------
        # From here on the event is dispatched: register a brand new symbol
        # book and commit id/sequence exactly as the baseline does.
        if not known_symbol:
            self._symbols[symbol] = state

        # A derived child id (``plan_id#slice``) is reserved from plan start;
        # it occupies both the order-id and event-id role of the future child,
        # so no external event — baseline or TWAP command — may reuse it as an
        # event id.
        if event_id in state.plan_index:
            # Committed like every baseline business rejection: the well-formed
            # event occupies its id and the sequence, but nothing else moves.
            state.last_sequence = sequence
            state.seen[event_id] = content
            self._events[event_id] = (symbol, content)
            bids, asks = state.engine.snapshot()
            return {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": REJECTED,
                "rejection_code": DUPLICATE_EVENT_ID,
                "trades": [],
                "book_changes": {"bids": [], "asks": []},
                "bids": bids,
                "asks": asks,
            }

        # ---- active per-security price limits ---------------------------
        # Enforced after the envelope, idempotency, sequence and identifier
        # conflict checks, and strictly before matching or any state change.
        # The identifier clashes the baseline engine would reach first (a
        # reused order id, a replace against a missing order, a duplicate plan
        # or a clashing derived id) keep their rejection-code precedence; a
        # price breach is only reported when none applies. The interval tested
        # here is the security's *active* one: the static config interval as
        # last replaced by any accepted PRICE_LIMIT_UPDATE.
        price_rejection = self._price_limit_rejection(payload, event_type, state)
        if price_rejection is not None:
            # A committed business rejection: it occupies the event id and
            # advances the symbol sequence, but performs no matching, leaves
            # no order or plan behind and spends no trade id. Baseline event
            # ids also occupy the per-symbol engine journal, exactly as an
            # engine-side business rejection would; plan commands live solely
            # in the replay log.
            state.last_sequence = sequence
            state.seen[event_id] = content
            self._events[event_id] = (symbol, content)
            if event_type not in _PLAN_TYPES:
                state.engine.occupy_event_id(event_id)
            bids, asks = state.engine.snapshot()
            return {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": REJECTED,
                "rejection_code": price_rejection,
                "trades": [],
                "book_changes": {"bids": [], "asks": []},
                "bids": bids,
                "asks": asks,
            }

        if event_type == PRICE_LIMIT_UPDATE:
            return self._dispatch_price_limit_update(
                event_id, payload, state, symbol, sequence, content
            )

        if event_type == PORTFOLIO_REPORT:
            return self._dispatch_portfolio_report(
                event_id, payload, state, symbol, sequence, content
            )

        if event_type in _PLAN_TYPES:
            out = self._dispatch_plan(
                event, payload, event_type, state, symbol, sequence, content
            )
            return out

        before_bids, before_asks = state.engine.level_totals()
        before_bids = dict(before_bids)
        before_asks = dict(before_asks)
        _eid, engine_result, reason, trades, _stp, _analysis, _position = (
            state.engine.handle_object_position(payload)
        )

        # Dispatched: the well-formed event occupies its id however the
        # business outcome ends, and the symbol's sequence advances exactly as
        # the input did. This mirrors the baseline's id-occupancy rule.
        state.last_sequence = sequence
        state.seen[event_id] = content
        self._events[event_id] = (symbol, content)

        bids, asks = state.engine.snapshot()
        book_changes = self._book_changes(state.engine, before_bids, before_asks)
        if reason is None:
            out: dict[str, object] = {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": ACCEPTED,
                "result": engine_result,
                "trades": trades,
                "book_changes": book_changes,
                "bids": bids,
                "asks": asks,
            }
        else:
            out = {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": REJECTED,
                # Baseline business rejection codes (UNKNOWN_ORDER,
                # DUPLICATE_ORDER_ID, ...) are preserved verbatim.
                "rejection_code": reason,
                "trades": trades,
                "book_changes": book_changes,
                "bids": bids,
                "asks": asks,
            }
        return out

    def _price_limit_rejection(
        self,
        payload: dict[str, object],
        event_type: str,
        state: _SymbolState,
    ) -> str | None:
        """Decide a price-limit breach against the active interval.

        Only the price of a LIMIT/ICEBERG ADD, of a REPLACE and of a LIMIT
        TWAP/VWAP plan is tested against the security's currently active
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

        if event_type in (TWAP_START, VWAP_START):
            if payload["order_type"] != LIMIT:
                # MARKET plans are exempt, exactly like MARKET orders.
                return None
            plan_id: str = payload["plan_id"]
            if plan_id in state.plans:
                # Mirror _register_plan: the plan id clash wins.
                return None
            if event_type == TWAP_START:
                child_count: int = payload["slice_count"]
            else:
                child_count = len(payload["volume_weights"])
            for index in range(child_count):
                child_id = f"{plan_id}#{index + 1}"
                if state.engine.has_order_id(child_id) or child_id in state.plan_index:
                    # A derived id clash rejects DUPLICATE_ORDER_ID first.
                    return None
            return PRICE_LIMIT_EXCEEDED if out_of_bounds(payload["price"]) else None

        return None

    @staticmethod
    def _allowed_payload_keys(inline_type: object) -> frozenset[str] | None:
        """The payload fields an inline event of ``inline_type`` may carry."""
        if inline_type in (ADD, CANCEL, REPLACE):
            return _BASELINE_KEYS
        if inline_type == TWAP_START:
            return _TWAP_START_KEYS
        if inline_type == VWAP_START:
            return _VWAP_START_KEYS
        if inline_type in (TWAP_SLICE, TWAP_CANCEL, TWAP_REPORT,
                           VWAP_SLICE, VWAP_CANCEL, VWAP_REPORT):
            return _TWAP_PLAN_REF_KEYS
        if inline_type == PORTFOLIO_REPORT:
            return _PORTFOLIO_REPORT_KEYS
        if inline_type == PRICE_LIMIT_UPDATE:
            return _PRICE_LIMIT_UPDATE_KEYS
        return None

    # -- parent-order plan handling ------------------------------------------

    def _dispatch_plan(
        self,
        event: dict[str, object],
        payload: dict[str, object],
        event_type: str,
        state: _SymbolState,
        symbol: str,
        sequence: int,
        content: str,
    ) -> dict[str, object]:
        """Dispatch a structurally valid, in-sequence parent-order command.

        Every branch commits the well-formed event id and advances the symbol
        sequence, including business rejections: this matches the baseline
        rule that a valid event occupies its id whatever the business result.
        """
        event_id: str = event["event_id"]

        before_bids, before_asks = state.engine.level_totals()
        before_bids = dict(before_bids)
        before_asks = dict(before_asks)

        if event_type == TWAP_START:
            result_out = self._twap_start(payload, state)
        elif event_type == VWAP_START:
            result_out = self._vwap_start(payload, state)
        elif event_type in (TWAP_SLICE, VWAP_SLICE):
            result_out = self._plan_slice(payload, state)
        elif event_type in (TWAP_CANCEL, VWAP_CANCEL):
            result_out = self._plan_cancel(payload, state)
        else:
            result_out = self._plan_report(payload, state)

        # The structurally valid command occupies its event id and the symbol
        # sequence whatever its business outcome.
        state.last_sequence = sequence
        state.seen[event_id] = content
        self._events[event_id] = (symbol, content)

        # Only a released slice can move the book; compute the change set with
        # the same level-diff routine the baseline path uses, so drained
        # levels and iceberg replenishment are reported identically.
        book_changes = self._book_changes(state.engine, before_bids, before_asks)
        return self._plan_response(
            event_id, symbol, sequence, state, result_out, book_changes
        )

    def _twap_start(
        self, payload: dict[str, object], state: _SymbolState
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
        return self._register_plan(state, plan)

    def _vwap_start(
        self, payload: dict[str, object], state: _SymbolState
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
        return self._register_plan(state, plan)

    @staticmethod
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
        self, payload: dict[str, object], state: _SymbolState
    ) -> tuple[str, str | None, ExecutionPlan | None, dict[str, object] | None]:
        """Release the next slice as an IOC child order against current book."""
        plan_id: str = payload["plan_id"]
        plan = state.plans.get(plan_id)
        if plan is None:
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
        self, payload: dict[str, object], state: _SymbolState
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
        self, payload: dict[str, object], state: _SymbolState
    ) -> tuple[str, str | None, ExecutionPlan | None, None]:
        """Read-only cumulative plan summary; closed plans stay queryable."""
        plan_id: str = payload["plan_id"]
        plan = state.plans.get(plan_id)
        if plan is None:
            return REJECTED, UNKNOWN_EXECUTION_PLAN, None, None
        return ACCEPTED, None, plan, None

    @staticmethod
    def _plan_response(
        event_id: str,
        symbol: str,
        sequence: int,
        state: _SymbolState,
        result_out: tuple[str, str | None, ExecutionPlan | None, dict[str, object] | None],
        book_changes: dict[str, list[dict[str, int]]],
    ) -> dict[str, object]:
        """Assemble the plan command result, including book and plan summary."""
        status, code, plan, slice_info = result_out
        bids, asks = state.engine.snapshot()
        if status == ACCEPTED:
            if slice_info is None:
                # START, CANCEL and REPORT never move the book.
                trades: list[dict[str, object]] = []
                result: str | None = None
                slice_number = child_order_id = None
                target_weight = scheduled_quantity = None
            else:
                trades = slice_info["trades"]
                result = slice_info["engine_result"]
                slice_number = slice_info["slice_number"]
                child_order_id = slice_info["child_order_id"]
                target_weight = slice_info.get("target_weight")
                scheduled_quantity = slice_info.get("scheduled_quantity")
            out: dict[str, object] = {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": ACCEPTED,
                "trades": trades,
                "book_changes": book_changes,
                "bids": bids,
                "asks": asks,
            }
            if result is not None:
                out["result"] = result
            out["execution_plan"] = plan.summary(
                slice_number=slice_number,
                child_order_id=child_order_id,
                target_weight=target_weight,
                scheduled_quantity=scheduled_quantity,
            ) if plan is not None else None
            return out
        # Business rejection: the book is untouched by the command itself.
        return {
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "status": REJECTED,
            "rejection_code": code,
            "trades": [],
            "book_changes": {"bids": [], "asks": []},
            "bids": bids,
            "asks": asks,
        }

    # -- intraday price-limit adjustment -------------------------------------

    def _dispatch_price_limit_update(
        self,
        event_id: str,
        payload: dict[str, object],
        state: _SymbolState,
        symbol: str,
        sequence: int,
        content: str,
    ) -> dict[str, object]:
        """Replace the security's active price-limit interval wholesale.

        Purely a replay-layer state change: no order, trade, plan or book is
        touched, resting orders outside the new interval keep their queue
        priority and may still become makers, and no trade id is spent. Like
        every other structurally valid event the command occupies its event
        id and advances the symbol sequence; its id lives solely in the
        replay log, exactly like a parent-order command id. Re-applying the
        current bounds is a successful update, not a conflict.
        """
        lower_price: int = payload["lower_price"]
        upper_price: int = payload["upper_price"]
        state.price_limits = (lower_price, upper_price)
        state.last_sequence = sequence
        state.seen[event_id] = content
        self._events[event_id] = (symbol, content)
        bids, asks = state.engine.snapshot()
        return {
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "status": ACCEPTED,
            "result": PRICE_LIMIT_UPDATED,
            "trades": [],
            "book_changes": {"bids": [], "asks": []},
            "bids": bids,
            "asks": asks,
            "active_price_limits": {
                "lower_price": lower_price,
                "upper_price": upper_price,
            },
        }

    # -- cross-security portfolio report -------------------------------------

    def _account_symbols(self, account_id: str) -> set[str]:
        """The securities on which an accepted event named ``account_id``.

        An account is known on a security once either an accepted ADD carried
        it or an accepted TWAP/VWAP plan carried it; released slices inherit
        the plan's account and therefore already show up in the engine's
        account set, while a started-but-never-sliced plan is picked up
        directly. This mirrors the baseline rule that an accepted ADD makes an
        account known whatever later happens to the order.
        """
        symbols: set[str] = set()
        for sym, symbol_state in self._symbols.items():
            if symbol_state.engine.knows_account(account_id):
                symbols.add(sym)
                continue
            for plan in symbol_state.plans.values():
                if plan.account_id == account_id:
                    symbols.add(sym)
                    break
        return symbols

    def _dispatch_portfolio_report(
        self,
        event_id: str,
        payload: dict[str, object],
        state: _SymbolState,
        symbol: str,
        sequence: int,
        content: str,
    ) -> dict[str, object]:
        """Answer a read-only cross-security portfolio query.

        The query only reads trades produced by earlier accepted events: it
        never matches, never releases a plan slice and never moves a book, a
        trade id, a plan or an account set. Like every other structurally
        valid event it occupies its event id and advances the envelope
        symbol's sequence, including the ``UNKNOWN_ACCOUNT`` and
        ``MARK_PRICE_MISMATCH`` business rejections. Its id lives solely in
        the replay log, exactly like a parent-order command id.
        """
        account_id: str = payload["account_id"]
        mark_prices: dict[str, object] = payload["mark_prices"]

        def reject(code: str) -> dict[str, object]:
            state.last_sequence = sequence
            state.seen[event_id] = content
            self._events[event_id] = (symbol, content)
            bids, asks = state.engine.snapshot()
            return {
                "event_id": event_id,
                "symbol": symbol,
                "sequence": sequence,
                "status": REJECTED,
                "rejection_code": code,
                "trades": [],
                "book_changes": {"bids": [], "asks": []},
                "bids": bids,
                "asks": asks,
            }

        known_symbols = self._account_symbols(account_id)
        if not known_symbols:
            return reject(UNKNOWN_ACCOUNT)
        # The mark map must name exactly the securities the account ever
        # appeared on: one missing or one extra is a business rejection, not a
        # schema error, and still consumes the id and the sequence.
        if set(mark_prices) != known_symbols:
            return reject(MARK_PRICE_MISMATCH)

        positions: list[dict[str, object]] = []
        total_buy_notional = 0
        total_sell_notional = 0
        total_turnover = 0
        total_market_value = 0
        total_exposure = 0
        total_pnl = 0
        for sym in sorted(known_symbols):
            mark_price = mark_prices[sym]
            engine = self._symbols[sym].engine
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

        state.last_sequence = sequence
        state.seen[event_id] = content
        self._events[event_id] = (symbol, content)
        bids, asks = state.engine.snapshot()
        return {
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "status": ACCEPTED,
            "result": REPORTED,
            "trades": [],
            "book_changes": {"bids": [], "asks": []},
            "bids": bids,
            "asks": asks,
            "portfolio_analysis": analysis,
        }


# ---------------------------------------------------------------------------
# Snapshot export / restoration
# ---------------------------------------------------------------------------


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
    _require(
        isinstance(raw_slices, list) and len(raw_slices) > 0,
        f"plan {plan_id} slice_quantities must be a non-empty list",
    )
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
    slice_count = len(slice_quantities)
    _require(0 <= released <= slice_count, f"plan {plan_id} released slice index out of range")
    total_quantity = sum(slice_quantities)
    algorithm = entry.get("algorithm", ALGORITHM_TWAP)
    volume_weights: list[int] | None = None
    if algorithm == ALGORITHM_VWAP:
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
    else:
        _require(
            algorithm == ALGORITHM_TWAP,
            f"plan {plan_id} has an unknown algorithm",
        )
    _require(
        released_quantity == sum(slice_quantities[:released]),
        f"plan {plan_id} released_quantity does not match its slices",
    )
    _require(
        filled_quantity <= released_quantity,
        f"plan {plan_id} filled quantity exceeds released quantity",
    )
    if status == PLAN_ACTIVE:
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
    # child orders synthesized for released slices, while every TWAP/VWAP
    # command id and every read-only PORTFOLIO_REPORT id lives solely in the
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
            set(entry) == _PLAN_KEYS or set(entry) == _VWAP_PLAN_KEYS,
            "plan entry has unknown fields",
        )
        plan = _parse_plan_entry(entry)
        plan_id = plan.plan_id
        _require(plan_id not in plans, f"duplicate execution plan id {plan_id}")
        for index in range(plan.slice_count):
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
    # TWAP/VWAP command ids live solely in the replay layer's event log.
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


# ---------------------------------------------------------------------------
# Public one-call entry point
# ---------------------------------------------------------------------------


def replay_events(
    events: list[dict[str, object]],
    config: dict[str, object] | None = None,
    snapshot: dict[str, object] | None = None,
    snapshot_after: object = "last",
) -> dict[str, object]:
    """Replay one ordered multi-security event stream in a single public call.

    Parameters
    ----------
    events:
        Event objects in strict submission order. Each event carries
        ``event_id`` (non-empty string, unique across the whole stream),
        ``symbol`` (non-empty string), ``sequence`` (positive integer,
        strictly increasing per symbol) and a payload — either inline
        (``"type"`` plus the fields of that event kind) or nested under an
        ``"event"`` object that repeats ``event_id`` and ``type``. Supported
        kinds are the baseline ADD/CANCEL/REPLACE events, the TWAP/VWAP
        TWAP_START/TWAP_SLICE/TWAP_CANCEL/TWAP_REPORT and
        VWAP_START/VWAP_SLICE/VWAP_CANCEL/VWAP_REPORT commands, the
        read-only cross-security PORTFOLIO_REPORT query and the intraday
        PRICE_LIMIT_UPDATE adjustment.
    config:
        Matching configuration summary. Defaults to :data:`DEFAULT_CONFIG`;
        a resumed run must pass the same configuration the snapshot carries.
    snapshot:
        Optional snapshot returned by an earlier :func:`export_snapshot` /
        :func:`replay_events` call. Restoration is fully verified before any
        event is processed.
    snapshot_after:
        ``"last"`` (default) attaches the snapshot after the final event;
        ``None`` omits it; ``{"symbol": ..., "sequence": ...}`` attaches the
        snapshot right after that identified successful event.

    Returns
    -------
    dict
        ``{"results": [...], "snapshot": {...} | None}``. Each result is
        associated with ``event_id``/``symbol``/``sequence``, carries a
        ``status`` of ``ACCEPTED``, ``REJECTED`` or ``DUPLICATE`` (with
        ``rejection_code`` for rejections), and lists the event's trades,
        book changes and the post-event ``bids``/``asks``.
    """
    if not isinstance(events, list):
        raise TypeError("events must be a list of event objects")

    if snapshot is not None:
        replayer = restore_replayer(snapshot, config)
    else:
        replayer = EventReplayer(config)

    if snapshot_after is not None and snapshot_after != "last":
        if not (
            isinstance(snapshot_after, dict)
            and isinstance(snapshot_after.get("symbol"), str)
            and isinstance(snapshot_after.get("sequence"), int)
        ):
            raise ValueError(
                "snapshot_after must be 'last', None or "
                "{'symbol': str, 'sequence': int}"
            )

    results: list[dict[str, object]] = []
    exported: dict[str, object] | None = None
    marker_matched = snapshot_after is None or snapshot_after == "last"
    for position, event in enumerate(events):
        result = replayer._submit_one(event)
        results.append(result)
        if (
            snapshot_after is not None
            and snapshot_after != "last"
            and result.get("symbol") == snapshot_after["symbol"]
            and result.get("sequence") == snapshot_after["sequence"]
        ):
            marker_matched = True
            if result.get("status") != ACCEPTED:
                raise ValueError(
                    "snapshot_after must identify an ACCEPTED event; "
                    f"{snapshot_after['symbol']} sequence {snapshot_after['sequence']} "
                    f"was {result.get('status')}"
                )
            exported = export_snapshot(replayer)

    if snapshot_after != "last" and not marker_matched:
        raise ValueError("snapshot_after did not match any event in the stream")
    if snapshot_after == "last":
        exported = export_snapshot(replayer)

    return {"results": results, "snapshot": exported}
