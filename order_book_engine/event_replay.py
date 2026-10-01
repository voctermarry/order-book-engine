"""Deterministic multi-symbol ordered event replay with resumable snapshots.

This module adds a new public entry point on top of the baseline single-book
:class:`~order_book_engine.engine.Engine`; it never changes the baseline
matching rules, priorities, rejection semantics or trade record shapes:

* A single public call (:func:`replay_events`) accepts an ordered stream of
  order events for one or several securities. Every security keeps its own
  sequence counter and its own book; events are applied strictly in input
  order, so identical timestamps never reorder anything.
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
  results identical to an uninterrupted one-shot run.

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
    CANCEL,
    REPLACE,
    Engine,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FORMAT_VERSION = "event-replay/1"

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"
DUPLICATE = "DUPLICATE"

# New validation / ordering rejection codes.
INVALID_EVENT = "INVALID_EVENT"
SEQUENCE_GAP = "SEQUENCE_GAP"
OUT_OF_ORDER = "OUT_OF_ORDER"
EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"

# Snapshot restoration failure codes.
SNAPSHOT_CORRUPT = "SNAPSHOT_CORRUPT"
SNAPSHOT_VERSION_UNSUPPORTED = "SNAPSHOT_VERSION_UNSUPPORTED"
CONFIG_MISMATCH = "CONFIG_MISMATCH"

#: Event types this replay layer accepts. Reports stay exclusive to the
#: baseline JSON Lines entry point, so the ordered stream covers exactly the
#: baseline mutating behaviours.
SUPPORTED_TYPES = frozenset({ADD, CANCEL, REPLACE})

_ENVELOPE_KEYS = frozenset({"event_id", "symbol", "sequence", "timestamp"})
_BASELINE_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity", "account_id"}
)


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


class _SymbolState:
    __slots__ = ("engine", "last_sequence", "seen")

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine or Engine()
        self.last_sequence = 0
        # eventId -> canonical payload content, in first-seen input order.
        self.seen: dict[str, str] = {}


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
            if not set(event) <= _ENVELOPE_KEYS | _BASELINE_KEYS:
                return reject_envelope()
            # Keep event_id/type (baseline fields); strip only symbol,
            # sequence and timestamp, which are envelope-only.
            payload = {
                key: value
                for key, value in event.items()
                if key not in ("symbol", "sequence", "timestamp")
            }
        event_type = payload.get("type")
        if not isinstance(event_type, str) or event_type not in SUPPORTED_TYPES:
            return reject_envelope()
        # Validate the full baseline schema up front, so malformed content is
        # classified INVALID_EVENT and reaches neither ordering nor the book.
        if (
            payload.get("event_id") != event_id
            or Engine._schema_error(payload) is not None
        ):
            # The wrapper and the baseline payload must identify the same event.
            return reject_envelope()

        # A symbol state is created only once the event is fully well formed;
        # failures that precede dispatch must not register an empty book.
        state = self._symbols.get(symbol)
        known_symbol = state is not None
        if state is None:
            state = _SymbolState()

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

        # ---- committed application through the baseline engine ----------
        # From here on the event is dispatched: register a brand new symbol
        # book and commit id/sequence exactly as the baseline does.
        if not known_symbol:
            self._symbols[symbol] = state
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


# ---------------------------------------------------------------------------
# Snapshot export / restoration
# ---------------------------------------------------------------------------


def _engine_to_json(state: _SymbolState) -> dict[str, object]:
    raw = state.engine.dump_state()
    return {
        "last_sequence": state.last_sequence,
        "event_log": [
            {"event_id": event_id, "content": content}
            for event_id, content in state.seen.items()
        ],
        "engine": {
            "event_ids": sorted(raw["event_ids"]),
            "order_ids": sorted(raw["order_ids"]),
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


_ORDER_RECORD_KEYS = frozenset(
    {"side", "price", "remaining", "status", "account_id",
     "display_quantity", "visible"}
)
_TRADE_KEYS = frozenset(
    {"trade_id", "maker_order_id", "taker_order_id", "price", "quantity", "event_id"}
)
_SYMBOL_STATE_KEYS = frozenset({"last_sequence", "event_log", "engine"})
_ENGINE_KEYS = frozenset(
    {"event_ids", "order_ids", "orders", "bids", "asks",
     "bid_totals", "ask_totals", "next_trade_id", "accounts", "trade_log"}
)
_ENVELOPE_KEYS_SNAPSHOT = frozenset(
    {"format_version", "engine_version", "config", "config_digest",
     "content", "content_digest"}
)
_CONTENT_KEYS = frozenset({"symbols", "events"})


def _engine_from_json(data: dict[str, object]) -> _SymbolState:
    _require(isinstance(data, dict), "symbol state must be an object")
    _require(set(data) == _SYMBOL_STATE_KEYS, "symbol state has unknown fields")
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

    engine_data = data.get("engine")
    _require(isinstance(engine_data, dict), "engine state must be an object")
    _require(set(engine_data) == _ENGINE_KEYS, "engine state has unknown fields")

    event_ids = engine_data.get("event_ids")
    order_ids = engine_data.get("order_ids")
    accounts = engine_data.get("accounts")
    _require(_is_str_set_list(event_ids), "engine.event_ids must be a list of strings")
    _require(_is_str_set_list(order_ids), "engine.order_ids must be a list of strings")
    _require(_is_str_set_list(accounts), "engine.accounts must be a list of strings")

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

    # The id sets mirror exactly what the baseline engine would hold.
    _require(set(order_ids) == set(orders), "order_ids and order records disagree")
    _require(set(event_ids) == set(seen), "engine event ids and event log disagree")
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

    # Deep copy so later caller mutation of the snapshot document can never
    # reach the live engine.
    engine = Engine(
        _state={
            "event_ids": set(event_ids),
            "order_ids": set(order_ids),
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
    return state


def export_snapshot(replayer: EventReplayer) -> dict[str, object]:
    """Export a resumable, JSON-compatible snapshot of a replay session.

    The snapshot fully preserves (per security): price-time queue priority,
    order remainders, the iceberg current slice and replenishment state, the
    last sequence, cumulative trades and the next trade id counter. It also
    carries the format version, the matching configuration (plus its digest)
    and a SHA-256 digest over the normalized whole document.
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
            symbols[symbol] = _engine_from_json(entry["state"])
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
        strictly increasing per symbol) and the baseline mutating payload —
        either inline (``"type"`` plus the baseline ADD/CANCEL/REPLACE fields)
        or nested under an ``"event"`` object that repeats ``event_id`` and
        ``type``.
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
