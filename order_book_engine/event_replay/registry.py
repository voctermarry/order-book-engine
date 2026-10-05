"""The event-kind registry: one entry per supported event type.

Each :class:`EventKind` binds together everything the orchestrator needs to
know about one event type — its structural validator, the payload fields the
inline envelope form may carry, its business handler and its commit
behaviour (whether the book is captured before dispatch and whether the
active price-limit interval is consulted first). The id-classification
flags derive from the type sets in :mod:`.constants`, which stay the single
source of truth for snapshot and historical-replay classification.

Registering a new event type is a single entry here plus its validator and
handler; the orchestrator, the snapshot layer and the historical rebuild
never grow another type-switch.
"""

from __future__ import annotations

from ..engine import (
    ADD,
    CANCEL,
    EXECUTION_REPORT,
    IMPACT_REPORT,
    REPLACE,
    Engine,
)
from .constants import (
    BOOK_LIQUIDITY_REPORT,
    BOOK_RECONSTRUCTION_REPORT,
    PLAN_TCA_REPORT,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_UPDATE,
    SESSION_RECONCILIATION,
    SUPPORTED_TYPES,
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    VWAP_CANCEL,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    _BASELINE_KEYS,
    _BOOK_LIQUIDITY_REPORT_KEYS,
    _BOOK_RECONSTRUCTION_REPORT_KEYS,
    _EXECUTION_REPORT_KEYS,
    _IMPACT_REPORT_KEYS,
    _PLAN_TCA_REPORT_KEYS,
    _PORTFOLIO_REPORT_KEYS,
    _PORTFOLIO_STRESS_REPORT_KEYS,
    _POV_START_KEYS,
    _POV_VOLUME_KEYS,
    _PRICE_LIMIT_UPDATE_KEYS,
    _REPLAY_NOOP_TYPES,
    _REPLAY_ONLY_TYPES,
    _SESSION_RECONCILIATION_KEYS,
    _TWAP_PLAN_REF_KEYS,
    _TWAP_START_KEYS,
    _VWAP_START_KEYS,
)
from .handlers import (
    apply_baseline,
    apply_book_liquidity_report,
    apply_execution_report,
    apply_impact_report,
    apply_plan,
    apply_plan_tca_report,
    apply_portfolio_report,
    apply_portfolio_stress_report,
    apply_price_limit_update,
    apply_session_reconciliation,
)
from .history import apply_book_reconstruction_report
from .validation import (
    _book_liquidity_report_schema_error,
    _book_reconstruction_schema_error,
    _execution_report_schema_error,
    _impact_report_schema_error,
    _plan_tca_report_schema_error,
    _portfolio_report_schema_error,
    _portfolio_stress_report_schema_error,
    _pov_schema_error,
    _price_limit_update_schema_error,
    _session_reconciliation_schema_error,
    _twap_schema_error,
    _vwap_schema_error,
)


class EventKind:
    """How the replay layer treats one event type.

    ``schema_error`` structurally validates the normalized payload (an
    ``INVALID_EVENT`` consumes neither the event id nor the sequence);
    ``payload_keys`` bounds the fields the inline envelope form may carry;
    ``handler`` produces the business outcome of a committed event;
    ``captures_book`` asks the commit context to snapshot the visible
    levels before dispatch so the change diff reports drained levels and
    iceberg replenishment; ``price_limit_checked`` consults the security's
    active price-limit interval strictly before matching or any state
    change. ``replay_only`` marks ids that live solely in the replay log
    (never in the per-symbol engine journal) and ``noop`` marks read-only
    reports that a historical rebuild commits as pure id/sequence markers.
    """

    __slots__ = ("schema_error", "payload_keys", "handler",
                 "captures_book", "price_limit_checked",
                 "replay_only", "noop")

    def __init__(
        self,
        event_type: str,
        schema_error,
        payload_keys: frozenset[str],
        handler,
        *,
        captures_book: bool = False,
        price_limit_checked: bool = False,
    ) -> None:
        self.schema_error = schema_error
        self.payload_keys = payload_keys
        self.handler = handler
        self.captures_book = captures_book
        self.price_limit_checked = price_limit_checked
        self.replay_only = event_type in _REPLAY_ONLY_TYPES
        self.noop = event_type in _REPLAY_NOOP_TYPES


def _kind(event_type: str, schema_error, payload_keys, handler, **flags) -> EventKind:
    return EventKind(event_type, schema_error, payload_keys, handler, **flags)


#: Every supported event type, mapped to its kind. Baseline ADD/CANCEL/REPLACE
#: share the baseline engine handler and schema; the twelve parent-order
#: commands share the plan handler (which keeps its own per-algorithm
#: dispatch); each replay-only report and the price-limit adjustment has its
#: own handler.
KINDS: dict[str, EventKind] = {
    event_type: _kind(event_type, schema_error, payload_keys, handler, **flags)
    for event_type, schema_error, payload_keys, handler, flags in (
        # Baseline mutating events: the book may move, and LIMIT/ICEBERG
        # prices are checked against the active interval first.
        (ADD, Engine._schema_error, _BASELINE_KEYS, apply_baseline,
         {"captures_book": True, "price_limit_checked": True}),
        (CANCEL, Engine._schema_error, _BASELINE_KEYS, apply_baseline,
         {"captures_book": True}),
        (REPLACE, Engine._schema_error, _BASELINE_KEYS, apply_baseline,
         {"captures_book": True, "price_limit_checked": True}),
        # Parent-order commands: only a released slice can move the book;
        # LIMIT plan starts are checked against the active interval.
        (TWAP_START, _twap_schema_error, _TWAP_START_KEYS, apply_plan,
         {"captures_book": True, "price_limit_checked": True}),
        (TWAP_SLICE, _twap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (TWAP_CANCEL, _twap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (TWAP_REPORT, _twap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (VWAP_START, _vwap_schema_error, _VWAP_START_KEYS, apply_plan,
         {"captures_book": True, "price_limit_checked": True}),
        (VWAP_SLICE, _vwap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (VWAP_CANCEL, _vwap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (VWAP_REPORT, _vwap_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (POV_START, _pov_schema_error, _POV_START_KEYS, apply_plan,
         {"captures_book": True, "price_limit_checked": True}),
        (POV_VOLUME, _pov_schema_error, _POV_VOLUME_KEYS, apply_plan,
         {"captures_book": True}),
        (POV_CANCEL, _pov_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        (POV_REPORT, _pov_schema_error, _TWAP_PLAN_REF_KEYS, apply_plan,
         {"captures_book": True}),
        # Replay-only commands and read-only reports never move the book.
        (PRICE_LIMIT_UPDATE, _price_limit_update_schema_error,
         _PRICE_LIMIT_UPDATE_KEYS, apply_price_limit_update, {}),
        (EXECUTION_REPORT, _execution_report_schema_error,
         _EXECUTION_REPORT_KEYS, apply_execution_report, {}),
        (IMPACT_REPORT, _impact_report_schema_error,
         _IMPACT_REPORT_KEYS, apply_impact_report, {}),
        (PORTFOLIO_REPORT, _portfolio_report_schema_error,
         _PORTFOLIO_REPORT_KEYS, apply_portfolio_report, {}),
        (PORTFOLIO_STRESS_REPORT, _portfolio_stress_report_schema_error,
         _PORTFOLIO_STRESS_REPORT_KEYS, apply_portfolio_stress_report, {}),
        (SESSION_RECONCILIATION, _session_reconciliation_schema_error,
         _SESSION_RECONCILIATION_KEYS, apply_session_reconciliation, {}),
        (PLAN_TCA_REPORT, _plan_tca_report_schema_error,
         _PLAN_TCA_REPORT_KEYS, apply_plan_tca_report, {}),
        (BOOK_RECONSTRUCTION_REPORT, _book_reconstruction_schema_error,
         _BOOK_RECONSTRUCTION_REPORT_KEYS, apply_book_reconstruction_report, {}),
        (BOOK_LIQUIDITY_REPORT, _book_liquidity_report_schema_error,
         _BOOK_LIQUIDITY_REPORT_KEYS, apply_book_liquidity_report, {}),
    )
}

# The registry is the dispatch table for exactly the supported types: a
# missing or extra entry is a programming error, not a runtime condition.
if frozenset(KINDS) != SUPPORTED_TYPES:  # pragma: no cover - defensive
    raise RuntimeError("event-kind registry does not match SUPPORTED_TYPES")


def kind_for(inline_type: object) -> EventKind | None:
    """The registered kind for ``inline_type``, or ``None`` when unknown."""
    if not isinstance(inline_type, str):
        return None
    return KINDS.get(inline_type)
