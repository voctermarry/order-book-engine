"""The multi-symbol replay session: orchestration only.

:class:`EventReplayer` owns the session state (per-symbol books, sequence
counters, the global idempotency log, the matching configuration) and the
per-event pipeline: envelope validation, payload normalization, structural
validation, global idempotency, per-symbol sequencing and the single
deterministic commit. Everything type-specific — schemas, inline key sets,
business handlers, price-limit pre-checks — comes from the event-kind
registry, so this module never switches on an event type itself.
"""

from __future__ import annotations

from ..engine import DUPLICATE_EVENT_ID
from .constants import (
    DEFAULT_CONFIG,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    SUPPORTED_TYPES,
    _ENVELOPE_KEYS,
)
from .handlers import price_limit_rejection
from .registry import KINDS, kind_for
from .serialization import _digest, canonical_json
from .state import (
    _Applied,
    _CommitContext,
    _SymbolState,
    invalid_result,
)
from .validation import _validate_config, _valid_timestamp


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

    def _submit_one(self, event: object) -> dict[str, object]:
        # ---- envelope validation (nothing is consumed on failure) --------
        if not isinstance(event, dict):
            return invalid_result(event)
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
            return invalid_result(event)

        def reject_envelope(code: str = INVALID_EVENT) -> dict[str, object]:
            # A structurally invalid event for a recognizable symbol echoes the
            # untouched book but never mutates or registers anything.
            return invalid_result(event, code, state=self._symbols.get(symbol))

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
            inline_kind = kind_for(event.get("type"))
            if inline_kind is None or not set(event) <= (
                _ENVELOPE_KEYS | inline_kind.payload_keys
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
        kind = KINDS[event_type]
        if kind.schema_error(payload) is not None:
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
            return invalid_result(
                event, EVENT_ID_CONFLICT,
                expected_sequence=state.last_sequence + 1 if known_symbol else None,
                state=state if known_symbol else None,
            )

        # ---- per-symbol strict sequence ordering -------------------------
        expected = state.last_sequence + 1
        if sequence < expected or sequence > expected:
            code = OUT_OF_ORDER if sequence < expected else SEQUENCE_GAP
            return invalid_result(
                event, code, expected_sequence=expected,
                state=state if known_symbol else None,
            )

        # ---- committed application ---------------------------------------
        # From here on the event is structurally valid and in sequence: it is
        # a committed application. The context owns the single symbol
        # registration, the deterministic id/sequence commit, the post-event
        # book echo and the response assembly; the registered kind describes
        # everything type-specific.
        ctx = _CommitContext(
            self._symbols, self._events,
            event_id, event_type, symbol, sequence, content, state, known_symbol,
        )
        ctx.register_symbol()

        # A derived child id (``plan_id#slice``) is reserved from plan start;
        # it occupies both the order-id and event-id role of the future child,
        # so no external event — baseline or parent-order command — may reuse
        # it as an event id.
        if event_id in state.plan_index:
            # Committed like every baseline business rejection: the well-formed
            # event occupies its id and the sequence, but nothing else moves.
            return ctx.finish(_Applied(REJECTED, code=DUPLICATE_EVENT_ID))

        # ---- active per-security price limits ---------------------------
        # Enforced after the envelope, idempotency, sequence and identifier
        # conflict checks, and strictly before matching or any state change.
        # The identifier clashes the baseline engine would reach first (a
        # reused order id, a replace against a missing order, a duplicate plan
        # or a clashing derived id) keep their rejection-code precedence; a
        # price breach is only reported when none applies. The interval tested
        # here is the security's *active* one: the static config interval as
        # last replaced by any accepted PRICE_LIMIT_UPDATE.
        if kind.price_limit_checked:
            price_rejection = price_limit_rejection(payload, event_type, state)
            if price_rejection is not None:
                # A committed business rejection: it occupies the event id and
                # advances the symbol sequence, but performs no matching, leaves
                # no order or plan behind and spends no trade id. Baseline event
                # ids also occupy the per-symbol engine journal, exactly as an
                # engine-side business rejection would; plan commands live solely
                # in the replay log.
                return ctx.finish(_Applied(
                    REJECTED, code=price_rejection,
                    occupy_engine_id=not kind.replay_only,
                ))

        # Only book-moving kinds capture the visible levels up front, so the
        # level-diff reports drained levels and iceberg replenishment.
        if kind.captures_book:
            ctx.capture_book()
        return ctx.finish(kind.handler(self, ctx, payload))
