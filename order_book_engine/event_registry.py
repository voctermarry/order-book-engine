"""Dispatch registry: one static description per supported event type.

Every event type the replay layer accepts is described exactly once here, by
an :class:`EventSpec` naming its payload field set, its structural validator
and its business handler, plus the flags the shared machinery derives from
the type's nature:

* ``capture_book`` — the event may move the book, so the orchestration
  pipeline captures the visible levels before dispatch for the change diff
  (baseline events and parent-order commands);
* ``engine_journal`` — the event's id also occupies the per-symbol engine
  journal on a pre-matching business rejection (baseline ADD/CANCEL/REPLACE
  only; every replay-only id lives solely in the replay log);
* ``read_only`` — the event never changes any state, so historical book
  reconstruction commits it as a pure id/sequence marker without
  re-dispatching it.

The orchestration pipeline in :mod:`order_book_engine.event_replayer`, the
snapshot restore in :mod:`order_book_engine.event_snapshot` and the
historical reconstruction in :mod:`order_book_engine.event_handlers` all
read this table (directly or through the derived sets below) instead of
enumerating event types themselves, so adding an event type means adding one
entry here plus its validator and handler — nothing else to touch.
"""

from __future__ import annotations

from typing import Callable, NamedTuple

from .engine import (
    ADD,
    CANCEL,
    EXECUTION_REPORT,
    IMPACT_REPORT,
    REPLACE,
    Engine,
)
from .event_types import (
    _BASELINE_KEYS,
    _BOOK_RECONSTRUCTION_REPORT_KEYS,
    _EXECUTION_REPORT_KEYS,
    _IMPACT_REPORT_KEYS,
    _PLAN_TCA_REPORT_KEYS,
    _PORTFOLIO_REPORT_KEYS,
    _PORTFOLIO_STRESS_REPORT_KEYS,
    _POV_START_KEYS,
    _POV_VOLUME_KEYS,
    _PRICE_LIMIT_UPDATE_KEYS,
    _SESSION_RECONCILIATION_KEYS,
    _TWAP_PLAN_REF_KEYS,
    _TWAP_START_KEYS,
    _VWAP_START_KEYS,
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
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    VWAP_CANCEL,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
)
from .event_validation import (
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


class EventSpec(NamedTuple):
    """Static description of one supported event type.

    ``handler`` is the name of the :class:`~order_book_engine.event_replayer.EventReplayer`
    method (inherited from the handler mixin) that produces the business
    outcome; it is invoked as
    ``handler(payload, event_type, state, symbol)`` and returns an
    :class:`~order_book_engine.replay_state._Applied`.
    """

    type: str
    payload_keys: frozenset[str]
    validate: Callable[[dict[str, object]], "str | None"]
    handler: str
    capture_book: bool = False
    engine_journal: bool = False
    read_only: bool = False


def _baseline_spec(event_type: str) -> EventSpec:
    # Baseline ADD/CANCEL/REPLACE share the field set, the baseline engine's
    # own schema validator and the engine-journal id occupancy rule.
    return EventSpec(
        event_type, _BASELINE_KEYS, Engine._schema_error, "_apply_baseline",
        capture_book=True, engine_journal=True,
    )


def _plan_spec(event_type: str, payload_keys: frozenset[str], validate) -> EventSpec:
    # Every parent-order command goes through the shared plan dispatcher; a
    # released slice may move the book, so the levels are captured up front.
    return EventSpec(
        event_type, payload_keys, validate, "_apply_plan", capture_book=True,
    )


def _report_spec(event_type: str, payload_keys: frozenset[str], validate, handler: str) -> EventSpec:
    # A read-only query never moves any state; reconstruction commits it as a
    # pure id/sequence marker.
    return EventSpec(
        event_type, payload_keys, validate, handler, read_only=True,
    )


_EVENT_SPECS: dict[str, EventSpec] = {
    spec.type: spec
    for spec in (
        _baseline_spec(ADD),
        _baseline_spec(CANCEL),
        _baseline_spec(REPLACE),
        _plan_spec(TWAP_START, _TWAP_START_KEYS, _twap_schema_error),
        _plan_spec(TWAP_SLICE, _TWAP_PLAN_REF_KEYS, _twap_schema_error),
        _plan_spec(TWAP_CANCEL, _TWAP_PLAN_REF_KEYS, _twap_schema_error),
        _plan_spec(TWAP_REPORT, _TWAP_PLAN_REF_KEYS, _twap_schema_error),
        _plan_spec(VWAP_START, _VWAP_START_KEYS, _vwap_schema_error),
        _plan_spec(VWAP_SLICE, _TWAP_PLAN_REF_KEYS, _vwap_schema_error),
        _plan_spec(VWAP_CANCEL, _TWAP_PLAN_REF_KEYS, _vwap_schema_error),
        _plan_spec(VWAP_REPORT, _TWAP_PLAN_REF_KEYS, _vwap_schema_error),
        _plan_spec(POV_START, _POV_START_KEYS, _pov_schema_error),
        _plan_spec(POV_VOLUME, _POV_VOLUME_KEYS, _pov_schema_error),
        _plan_spec(POV_CANCEL, _TWAP_PLAN_REF_KEYS, _pov_schema_error),
        _plan_spec(POV_REPORT, _TWAP_PLAN_REF_KEYS, _pov_schema_error),
        _report_spec(
            EXECUTION_REPORT, _EXECUTION_REPORT_KEYS,
            _execution_report_schema_error, "_apply_execution_report",
        ),
        _report_spec(
            IMPACT_REPORT, _IMPACT_REPORT_KEYS,
            _impact_report_schema_error, "_apply_impact_report",
        ),
        _report_spec(
            PORTFOLIO_REPORT, _PORTFOLIO_REPORT_KEYS,
            _portfolio_report_schema_error, "_apply_portfolio_report",
        ),
        _report_spec(
            PORTFOLIO_STRESS_REPORT, _PORTFOLIO_STRESS_REPORT_KEYS,
            _portfolio_stress_report_schema_error, "_apply_portfolio_stress_report",
        ),
        _report_spec(
            SESSION_RECONCILIATION, _SESSION_RECONCILIATION_KEYS,
            _session_reconciliation_schema_error, "_apply_session_reconciliation",
        ),
        _report_spec(
            PLAN_TCA_REPORT, _PLAN_TCA_REPORT_KEYS,
            _plan_tca_report_schema_error, "_apply_plan_tca_report",
        ),
        _report_spec(
            BOOK_RECONSTRUCTION_REPORT, _BOOK_RECONSTRUCTION_REPORT_KEYS,
            _book_reconstruction_schema_error, "_apply_book_reconstruction_report",
        ),
        # The intraday adjustment mutates the active interval but never the
        # book, so it is neither captured nor a reconstruction no-op.
        EventSpec(
            PRICE_LIMIT_UPDATE, _PRICE_LIMIT_UPDATE_KEYS,
            _price_limit_update_schema_error, "_apply_price_limit_update",
        ),
    )
}


def event_spec(event_type: object) -> EventSpec | None:
    """The spec for ``event_type``, or ``None`` when the type is unsupported.

    Safe on arbitrary payload content: a non-string (or unhashable) ``type``
    value simply has no spec.
    """
    if not isinstance(event_type, str):
        return None
    return _EVENT_SPECS.get(event_type)


#: Event types this replay layer accepts. The TWAP/VWAP parent-order commands
#: join the baseline mutating behaviours; of the baseline single-security
#: read-only reports the per-order EXECUTION_REPORT query and the
#: what-if IMPACT_REPORT query join them here (ACCOUNT_REPORT and
#: DAY_END_RECONCILIATION stay exclusive to the JSON Lines entry point),
#: while the cross-security
#: PORTFOLIO_REPORT query, the cross-security PORTFOLIO_STRESS_REPORT query,
#: the whole-session SESSION_RECONCILIATION query,
#: the per-plan PLAN_TCA_REPORT query, the historical
#: BOOK_RECONSTRUCTION_REPORT query and the intraday PRICE_LIMIT_UPDATE
#: adjustment are exclusive to this layer.
SUPPORTED_TYPES = frozenset(_EVENT_SPECS)

#: Event types whose ids live solely in the replay log: parent-order commands
#: never touch the engine journal, the per-order execution query, the
#: per-symbol book-impact what-if query, the cross-security portfolio and
#: portfolio-stress queries
#: and the whole-session reconciliation are read-only and matched by no
#: engine, and a price-limit adjustment only rewrites replay-layer state.
#: Baseline ADD/CANCEL/REPLACE ids occupy the per-symbol engine journal
#: instead.
_REPLAY_ONLY_TYPES = frozenset(
    event_type for event_type, spec in _EVENT_SPECS.items()
    if not spec.engine_journal
)

#: Accepted replay-only events that never move any state themselves: every
#: read-only report. A historical reconstruction rebuild replays the logged
#: stream and skips these, since they occupied a sequence but changed
#: nothing. PRICE_LIMIT_UPDATE is intentionally absent: it rewrites the
#: active interval and must be replayed up to the target point.
_REPLAY_NOOP_TYPES = frozenset(
    event_type for event_type, spec in _EVENT_SPECS.items() if spec.read_only
)
