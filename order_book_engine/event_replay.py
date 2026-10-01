"""Deterministic multi-instrument event replay with resumable snapshots.

This layer adds nothing to the baseline matching semantics: each financial
event (``ADD`` / ``CANCEL`` / ``REPLACE`` and the baseline query events) is
handled by an independent per-symbol :class:`~order_book_engine.engine.Engine`
exactly as the baseline does.  It additionally provides:

* strict per-symbol ``sequence`` streams (gaps and regressions rejected);
* idempotent replay keyed by ``event_id`` over normalized event content;
* per-event results carrying trades and explicit before/after book changes;
* canonical, byte-comparable JSON serialization;
* self-describing, checksummed in-memory snapshots that resume a replay so
  trade ids, trade order and final results are identical to an uninterrupted
  run.

No file is ever read or written here: events, results and snapshots are plain
JSON-compatible Python objects handed to or returned by the public entry
points.  Persistence, if desired, is the caller's responsibility.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from .engine import Engine

# --- Replay outcome statuses ------------------------------------------------

ACCEPTED = "ACCEPTED"
"""The event was well formed, in sequence and produced a baseline result."""

REJECTED = "REJECTED"
"""The event was invalid, out of sequence or refused by baseline rules."""

DUPLICATE = "DUPLICATE"
"""A seen ``event_id`` was replayed with byte-identical normalized content."""

# --- Replay rejection codes -------------------------------------------------

INVALID_EVENT = "INVALID_EVENT"
"""Missing fields, illegal values, or an unknown event type."""

SEQUENCE_GAP = "SEQUENCE_GAP"
"""The symbol's sequence jumped over one or more expected values."""

OUT_OF_ORDER = "OUT_OF_ORDER"
"""The symbol's sequence did not strictly increase."""

EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
"""A seen ``event_id`` arrived with different normalized content."""

# --- Snapshot rejection codes ----------------------------------------------

SNAPSHOT_CORRUPT = "SNAPSHOT_CORRUPT"
SNAPSHOT_VERSION_UNSUPPORTED = "SNAPSHOT_VERSION_UNSUPPORTED"
CONFIG_MISMATCH = "CONFIG_MISMATCH"

FORMAT_VERSION = "order-book-replay/1"

# Baseline behaviour summarized inside every snapshot.  The contents pin the
# matching semantics a resume expects; lists are kept sorted so the summary
# serializes canonically.
_CONFIG: dict[str, Any] = {
    "matching": "price-time-priority",
    "engine_state_format": 1,
    "trade_id_start": 1,
    "supported_order_types": ["ICEBERG", "LIMIT", "MARKET"],
    "supported_sides": ["BUY", "SELL"],
    "supported_time_in_force": ["FOK", "GTC", "IOC"],
    "order_event_types": ["ADD", "CANCEL", "REPLACE"],
}

_ENVELOPE_KEYS = frozenset({"event_id", "symbol", "sequence", "type", "event"})


class CanonicalError(ValueError):
    """Raised when an object cannot be represented in canonical JSON."""


class SnapshotError(ValueError):
    """Raised when a snapshot cannot be restored.

    ``code`` is one of :data:`SNAPSHOT_CORRUPT`,
    :data:`SNAPSHOT_VERSION_UNSUPPORTED` or :data:`CONFIG_MISMATCH`.  A failed
    restoration never creates a partially restored replayer.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_dumps(obj: Any) -> bytes:
    """Serialize *obj* to deterministic, byte-comparable canonical JSON.

    Compact separators are used, non-ASCII characters pass through unescaped
    (matching the baseline replay output) and object keys are serialized in
    sorted order, so identical content yields identical bytes regardless of
    insertion order.  The output is ordinary JSON and round-trips through
    :func:`json.loads`.  Non-JSON-native content (including non-string object
    keys and non-finite floats) raises :class:`CanonicalError`.
    """
    try:
        return json.dumps(
            obj,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CanonicalError(str(exc)) from exc


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_int(value: Any) -> bool:
    # ``bool`` is a subclass of ``int`` and is not a sequence number.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _public_book(engine: Engine) -> dict[str, list[dict[str, int]]]:
    bids, asks = engine.snapshot()
    return {"bids": bids, "asks": asks}


_EMPTY_BOOK: dict[str, list[dict[str, int]]] = {"bids": [], "asks": []}


class _SeenEvent:
    __slots__ = ("symbol", "sequence", "content_digest", "result")

    def __init__(
        self,
        symbol: str,
        sequence: int,
        content_digest: str,
        result: dict[str, Any],
    ) -> None:
        self.symbol = symbol
        self.sequence = sequence
        self.content_digest = content_digest
        self.result = result


class EventReplayer:
    """Replays ordered order events for any number of instruments.

    Each symbol owns an independent baseline :class:`Engine`, its own sequence
    cursor and book state; event ids and the cross-symbol submission order are
    tracked for the whole replay.
    """

    def __init__(self) -> None:
        self._engines: dict[str, Engine] = {}
        self._last_sequence: dict[str, int] = {}
        self._seen: dict[str, _SeenEvent] = {}
        # event_ids of well-formed events in cross-symbol submission order.
        self._order: list[str] = []

    # -- public use ---------------------------------------------------------

    def submit(self, event: Any) -> dict[str, Any]:
        """Submit a single envelope and return its result dictionary.

        Processing is per-event: accepted earlier events stay committed no
        matter what later events do.
        """
        return self._submit(event)

    def replay(self, events: Any) -> dict[str, Any]:
        """Submit an ordered list of envelopes in input order.

        Returns ``{"results", "final_books", "trades", "snapshot"}`` with one
        result per event, the final per-symbol books, every trade produced in
        cross-symbol submission order and a snapshot of the resulting state.
        """
        if not isinstance(events, list):
            raise CanonicalError("events must be a list of event objects")
        results = [self._submit(event) for event in events]
        return {
            "results": results,
            "final_books": self.final_books(),
            "trades": self.all_trades(),
            "snapshot": self.export_snapshot(),
        }

    def final_books(self) -> dict[str, dict[str, list[dict[str, int]]]]:
        """Current public books of every symbol, keyed in sorted symbol order."""
        return {
            symbol: _public_book(self._engines[symbol])
            for symbol in sorted(self._engines)
        }

    def all_trades(self) -> list[dict[str, Any]]:
        """All trades of every symbol, in cross-symbol submission order."""
        trades: list[dict[str, Any]] = []
        for event_id in self._order:
            seen = self._seen[event_id]
            for trade in seen.result["trades"]:
                trades.append(
                    {
                        "symbol": seen.symbol,
                        "sequence": seen.sequence,
                        "event_id": event_id,
                        "trade_id": trade["trade_id"],
                        "maker_order_id": trade["maker_order_id"],
                        "taker_order_id": trade["taker_order_id"],
                        "price": trade["price"],
                        "quantity": trade["quantity"],
                    }
                )
        return trades

    # -- envelope processing ------------------------------------------------

    def _rejection(
        self,
        code: str,
        event_id: Any,
        symbol: Any,
        sequence: Any,
        event_type: Any,
        book: dict[str, list[dict[str, int]]],
    ) -> dict[str, Any]:
        return {
            "status": REJECTED,
            "code": code,
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "type": event_type,
            "result": None,
            "reason": None,
            "self_trade_prevention": None,
            "execution_analysis": None,
            "position_analysis": None,
            "trades": [],
            "book_changes": {"before": book, "after": book},
        }

    def _submit(self, event: Any) -> dict[str, Any]:
        # --- envelope validation: nothing here mutates replay state --------
        if not isinstance(event, dict):
            return self._rejection(
                INVALID_EVENT, None, None, None, None, _EMPTY_BOOK
            )
        event_id = event.get("event_id")
        symbol = event.get("symbol")
        sequence = event.get("sequence")
        event_type = event.get("type")
        if not _is_non_empty_str(event_id):
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _EMPTY_BOOK,
            )
        if not _is_non_empty_str(symbol):
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _EMPTY_BOOK,
            )
        if not _is_int(sequence) or sequence <= 0:
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _EMPTY_BOOK,
            )
        if not _is_non_empty_str(event_type):
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _EMPTY_BOOK,
            )
        engine = self._engines.get(symbol)
        if set(event) != _ENVELOPE_KEYS:
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )
        order_event = event.get("event")
        if not isinstance(order_event, dict):
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )
        # The inner event is a verbatim baseline object; its identifiers must
        # agree with the envelope so the engine sees exactly the stream the
        # caller declared.
        if (
            order_event.get("event_id") != event_id
            or order_event.get("type") != event_type
        ):
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )
        # The normalized content covers the whole envelope, so any difference
        # (including a changed nested order event or reordered keys) is caught.
        try:
            canonical = canonical_dumps(event)
        except CanonicalError:
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )

        # --- idempotency: seen ids never reach an engine again -------------
        seen = self._seen.get(event_id)
        if seen is not None:
            if _digest(canonical) == seen.content_digest:
                duplicate = copy.deepcopy(seen.result)
                duplicate["status"] = DUPLICATE
                duplicate["code"] = None
                return duplicate
            return self._rejection(
                EVENT_ID_CONFLICT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )

        # Structural validation of the verbatim baseline event happens before
        # any symbol state exists, so an invalid event can neither create the
        # instrument nor occupy its sequence slot (mirroring the baseline,
        # where schema errors consume no event id).
        if Engine._schema_error(order_event) is not None:
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _public_book(engine) if engine is not None else _EMPTY_BOOK,
            )

        # --- per-symbol sequence ordering ----------------------------------
        is_new_symbol = engine is None
        if is_new_symbol:
            if sequence != 1:
                return self._rejection(
                    SEQUENCE_GAP, event_id, symbol, sequence, event_type,
                    _EMPTY_BOOK,
                )
            engine = Engine()
        else:
            expected = self._last_sequence[symbol] + 1
            if sequence < expected:
                return self._rejection(
                    OUT_OF_ORDER, event_id, symbol, sequence, event_type,
                    _public_book(engine),
                )
            if sequence > expected:
                return self._rejection(
                    SEQUENCE_GAP, event_id, symbol, sequence, event_type,
                    _public_book(engine),
                )

        before = _public_book(engine)

        # --- baseline dispatch (matching rules are never redefined) --------
        _eid, base_result, reason, trades, stp, analysis, position = (
            engine.handle_object_position(order_event)
        )

        if base_result == "REJECTED" and reason == "INVALID_SCHEMA":
            # The remaining schema rule is state dependent (a display slice may
            # only replace an iceberg target); it behaves exactly like the
            # structural check above and consumes neither id nor sequence, so a
            # rejected first event never even creates the instrument.
            return self._rejection(
                INVALID_EVENT, event_id, symbol, sequence, event_type,
                _EMPTY_BOOK if is_new_symbol else before,
            )

        # The event is well formed and in sequence; the instrument now exists.
        if is_new_symbol:
            self._engines[symbol] = engine

        # A well-formed event occupies its sequence and event id whether the
        # baseline accepted it or refused it for business reasons; failed
        # events leave no order, trade, counter or book change behind.
        self._last_sequence[symbol] = sequence
        after = _public_book(engine)
        result: dict[str, Any] = {
            "status": ACCEPTED if base_result != "REJECTED" else REJECTED,
            "code": None,
            "event_id": event_id,
            "symbol": symbol,
            "sequence": sequence,
            "type": event_type,
            "result": base_result,
            "reason": reason,
            "self_trade_prevention": copy.deepcopy(stp),
            "execution_analysis": copy.deepcopy(analysis),
            "position_analysis": copy.deepcopy(position),
            "trades": [dict(trade) for trade in trades],
            "book_changes": {"before": before, "after": after},
        }
        self._seen[event_id] = _SeenEvent(
            symbol, sequence, _digest(canonical), result
        )
        self._order.append(event_id)
        return result

    # -- snapshots ----------------------------------------------------------

    def export_snapshot(self) -> dict[str, Any]:
        """Export a complete, checksummed snapshot of the current state.

        May be called after any successfully processed event.  The result is
        an in-memory, JSON-compatible object; this method never writes files.
        The snapshot preserves price-time priority, remaining quantities,
        iceberg visible slices and replenishment state, each symbol's last
        sequence, the cumulative trade journal and the trade id counters.
        """
        config = _canonical_config()
        config_digest = _digest(canonical_dumps(config))
        instruments: dict[str, Any] = {}
        for symbol in sorted(self._engines):
            instruments[symbol] = {
                "last_sequence": self._last_sequence[symbol],
                "engine": self._engines[symbol].dump_state(),
            }
        events = [
            {
                "event_id": event_id,
                "symbol": self._seen[event_id].symbol,
                "sequence": self._seen[event_id].sequence,
                "content_digest": self._seen[event_id].content_digest,
                "result": copy.deepcopy(self._seen[event_id].result),
            }
            for event_id in self._order
        ]
        state: dict[str, Any] = {
            "config_digest": config_digest,
            "instruments": instruments,
            "event_order": list(self._order),
            "events": events,
        }
        return {
            "format_version": format_version(),
            "config": config,
            "config_digest": config_digest,
            "state": state,
            "state_digest": _digest(canonical_dumps(state)),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Any) -> EventReplayer:
        """Validate and restore :meth:`export_snapshot` output.

        Version, configuration and the state SHA-256 digest are checked before
        any state is adopted; on failure :class:`SnapshotError` carries the
        precise code and no partial state is created.  On success the resumed
        replay generates identical trade ids, trade order and final results.
        """
        if not isinstance(snapshot, dict):
            raise SnapshotError(SNAPSHOT_CORRUPT)

        if snapshot.get("format_version") != format_version():
            raise SnapshotError(SNAPSHOT_VERSION_UNSUPPORTED)

        try:
            config = snapshot.get("config")
            expected_config = _canonical_config()
            expected_config_digest = _digest(canonical_dumps(expected_config))
            try:
                config_matches = (
                    isinstance(config, dict)
                    and canonical_dumps(config) == canonical_dumps(expected_config)
                )
            except CanonicalError:
                config_matches = False
            if not config_matches or snapshot.get("config_digest") != expected_config_digest:
                raise SnapshotError(CONFIG_MISMATCH)

            state = snapshot.get("state")
            state_digest = snapshot.get("state_digest")
            if (
                not isinstance(state, dict)
                or not isinstance(state_digest, str)
                or _digest(canonical_dumps(state)) != state_digest
            ):
                raise SnapshotError(SNAPSHOT_CORRUPT)

            return cls._build_state(state, expected_config_digest)
        except SnapshotError:
            raise
        except (KeyError, TypeError, ValueError, CanonicalError) as exc:
            raise SnapshotError(SNAPSHOT_CORRUPT) from exc

    @classmethod
    def _build_state(cls, state: dict[str, Any], config_digest: str) -> EventReplayer:
        if state.get("config_digest") != config_digest:
            raise SnapshotError(CONFIG_MISMATCH)
        instruments = state["instruments"]
        entries = state["events"]
        order = state["event_order"]
        if (
            not isinstance(instruments, dict)
            or not isinstance(entries, list)
            or not isinstance(order, list)
        ):
            raise SnapshotError(SNAPSHOT_CORRUPT)

        replayer = cls()
        engines: dict[str, Engine] = {}
        last_sequence: dict[str, int] = {}
        raw_states: dict[str, dict[str, Any]] = {}
        for symbol, instrument in instruments.items():
            if not isinstance(symbol, str) or not isinstance(instrument, dict):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            last = instrument["last_sequence"]
            raw_engine = instrument["engine"]
            if not _is_int(last) or last < 0 or not isinstance(raw_engine, dict):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            raw_states[symbol] = raw_engine
            engines[symbol] = Engine.from_state(raw_engine)
            last_sequence[symbol] = last

        seen: dict[str, _SeenEvent] = {}
        symbol_events: dict[str, set[str]] = {symbol: set() for symbol in engines}
        sequences: dict[str, set[int]] = {symbol: set() for symbol in engines}
        for entry in entries:
            if not isinstance(entry, dict):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            event_id = entry["event_id"]
            symbol = entry["symbol"]
            seq = entry["sequence"]
            content_digest = entry["content_digest"]
            stored_result = entry["result"]
            if (
                not _is_non_empty_str(event_id)
                or not isinstance(symbol, str)
                or not _is_int(seq)
                or not isinstance(content_digest, str)
                or not isinstance(stored_result, dict)
                or symbol not in engines
                or event_id in seen
            ):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            sequences.setdefault(symbol, set()).add(seq)
            symbol_events.setdefault(symbol, set()).add(event_id)
            seen[event_id] = _SeenEvent(symbol, seq, content_digest, stored_result)

        if sorted(order) != sorted(seen) or any(
            not isinstance(event_id, str) for event_id in order
        ):
            raise SnapshotError(SNAPSHOT_CORRUPT)
        for symbol, seqs in sequences.items():
            if seqs != set(range(1, last_sequence[symbol] + 1)):
                raise SnapshotError(SNAPSHOT_CORRUPT)
        for symbol, raw_engine in raw_states.items():
            cls._validate_engine_state(symbol, raw_engine, symbol_events[symbol])

        replayer._engines = engines
        replayer._last_sequence = last_sequence
        replayer._seen = seen
        replayer._order = list(order)
        return replayer

    @staticmethod
    def _validate_engine_state(
        symbol: str, state: dict[str, Any], symbol_event_ids: set[str]
    ) -> None:
        """Defence in depth: even a re-checksummed snapshot must be internally
        consistent, or restoration is refused as corrupt."""
        required = {
            "event_ids", "order_ids", "orders", "bids", "asks",
            "bid_totals", "ask_totals", "next_trade_id", "accounts",
            "trade_log",
        }
        if set(state) != required:
            raise SnapshotError(SNAPSHOT_CORRUPT)
        next_trade_id = state["next_trade_id"]
        trade_log = state["trade_log"]
        if (
            not _is_int(next_trade_id)
            or next_trade_id < 1
            or not isinstance(trade_log, list)
            or len(trade_log) != next_trade_id - 1
        ):
            raise SnapshotError(SNAPSHOT_CORRUPT)
        orders = state["orders"]
        order_ids = state["order_ids"]
        event_ids = state["event_ids"]
        if not isinstance(orders, dict) or not isinstance(order_ids, list) or not isinstance(event_ids, list):
            raise SnapshotError(SNAPSHOT_CORRUPT)
        if set(orders) != set(order_ids):
            raise SnapshotError(SNAPSHOT_CORRUPT)
        if not symbol_event_ids <= set(event_ids):
            raise SnapshotError(SNAPSHOT_CORRUPT)

        resting_membership: dict[str, int] = {}
        for side, side_book, side_totals in (
            ("BUY", state["bids"], state["bid_totals"]),
            ("SELL", state["asks"], state["ask_totals"]),
        ):
            if not isinstance(side_book, dict) or not isinstance(side_totals, dict):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            if set(side_book) != set(side_totals):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            for price_key, queue in side_book.items():
                if not isinstance(queue, list):
                    raise SnapshotError(SNAPSHOT_CORRUPT)
                visible_total = 0
                for order_id in queue:
                    record = orders.get(order_id)
                    if (
                        not isinstance(order_id, str)
                        or record is None
                        or record.get("status") != "RESTING"
                        or record.get("side") != side
                        or str(record.get("price")) != price_key
                    ):
                        raise SnapshotError(SNAPSHOT_CORRUPT)
                    visible = record.get("visible", record.get("remaining"))
                    if not _is_int(visible):
                        raise SnapshotError(SNAPSHOT_CORRUPT)
                    visible_total += visible
                    resting_membership[order_id] = resting_membership.get(order_id, 0) + 1
                if visible_total != side_totals[price_key]:
                    raise SnapshotError(SNAPSHOT_CORRUPT)

        for order_id, record in orders.items():
            if not isinstance(record, dict):
                raise SnapshotError(SNAPSHOT_CORRUPT)
            if record.get("status") == "RESTING" and resting_membership.get(order_id) != 1:
                raise SnapshotError(SNAPSHOT_CORRUPT)

        for trade in trade_log:
            if not isinstance(trade, dict) or trade.get("event_id") not in symbol_event_ids:
                raise SnapshotError(SNAPSHOT_CORRUPT)


def format_version() -> str:
    """Return the snapshot format version supported by this implementation."""
    return FORMAT_VERSION


def _canonical_config() -> dict[str, Any]:
    return json.loads(json.dumps(_CONFIG, ensure_ascii=False))


# --- Module-level convenience entry points ----------------------------------


def replay_events(events: Any, *, snapshot: Any = None) -> dict[str, Any]:
    """Replay one ordered batch of envelopes in a single public call.

    With *snapshot* omitted the replay starts from an empty state; otherwise the
    snapshot is validated and restored first (raising :class:`SnapshotError`).
    Different symbols keep independent sequences and books, while input order
    within and across symbols is preserved exactly.
    """
    replayer = (
        EventReplayer()
        if snapshot is None
        else EventReplayer.from_snapshot(snapshot)
    )
    return replayer.replay(events)


def restore_snapshot(snapshot: Any) -> EventReplayer:
    """Validate and restore a snapshot, returning a ready :class:`EventReplayer`."""
    return EventReplayer.from_snapshot(snapshot)
