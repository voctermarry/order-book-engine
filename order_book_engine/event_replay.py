"""Deterministic multi-symbol ordered event replay with resumable snapshots.

This package facade adds a new public entry point on top of the baseline
single-book :class:`~order_book_engine.engine.Engine`; it never changes the
baseline matching rules, priorities, rejection semantics or trade record
shapes:

* A single public call (:func:`replay_events`) accepts an ordered stream of
  order events for one or several securities. Every security keeps its own
  sequence counter and its own book; events are applied strictly in input
  order, so identical timestamps never reorder anything. The stream covers
  the baseline ADD/CANCEL/REPLACE behaviours, resumable TWAP/VWAP parent
  orders (TWAP_START/TWAP_SLICE/TWAP_CANCEL/TWAP_REPORT and
  VWAP_START/VWAP_SLICE/VWAP_CANCEL/VWAP_REPORT), resumable POV parent
  orders (POV_START/POV_VOLUME/POV_CANCEL/POV_REPORT), the read-only
  per-order EXECUTION_REPORT query, the read-only cross-security
  PORTFOLIO_REPORT query, the read-only cross-security
  PORTFOLIO_STRESS_REPORT query (which stresses one account's existing
  positions against several caller-supplied price scenarios), the
  read-only whole-session
  SESSION_RECONCILIATION query (which reconciles every security's
  trades and account books at once), the read-only per-plan
  PLAN_TCA_REPORT query (which prices a plan's implementation shortfall
  gap against a caller-supplied evaluation price), the read-only
  per-symbol IMPACT_REPORT what-if query (which estimates an anonymous
  market order's executable quantity and cost against one security's
  current book), the read-only historical BOOK_RECONSTRUCTION_REPORT
  query (which rebuilds one security's price-time queues as of a
  previously committed sequence) and the intraday
  PRICE_LIMIT_UPDATE adjustment, which replaces one security's active price-limit interval
  (seeded from the static ``price_limits`` configuration) for all
  subsequently submitted limit prices; plans never read a wall clock and
  are advanced solely by their command events. TWAP slices divide
  the total evenly; VWAP slices follow a caller-supplied volume-weight
  curve; POV releases follow a participation rate against caller-fed
  cumulative market volume.
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

The layer never opens files: events and snapshots are received from, and
returned to, the caller.

Structure
---------
The implementation is split by concern, and this module simply re-exports
the public surface so existing imports keep working unchanged:

* :mod:`order_book_engine.event_types` — event-type vocabulary and field sets;
* :mod:`order_book_engine.event_validation` — payload and config validation;
* :mod:`order_book_engine.event_registry` — the per-type dispatch table;
* :mod:`order_book_engine.event_handlers` — the business handlers;
* :mod:`order_book_engine.event_replayer` — the orchestration pipeline;
* :mod:`order_book_engine.replay_state` — the shared commit machinery;
* :mod:`order_book_engine.execution_plans` — the parent-order state;
* :mod:`order_book_engine.event_snapshot` — snapshot export/restoration;
* :mod:`order_book_engine.replay_api` — the one-call entry point.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import deque

from . import __version__
from .engine import (
    ADD,
    BREAKS_FOUND,
    BUY,
    CANCEL,
    DUPLICATE_EVENT_ID,
    DUPLICATE_ORDER_ID,
    EXECUTION_REPORT,
    FIELD_MISMATCH,
    ICEBERG,
    IMPACT_REPORT,
    IOC,
    LIMIT,
    MARKET,
    MISSING_ACTUAL,
    MISSING_EXPECTED,
    RECONCILED,
    REPLACE,
    REPORTED,
    SELL,
    Engine,
    UNKNOWN_ACCOUNT,
    UNKNOWN_ORDER,
    _is_int,
    _is_non_empty_str,
    _is_positive_int,
)
from .event_registry import (
    _REPLAY_NOOP_TYPES,
    _REPLAY_ONLY_TYPES,
    SUPPORTED_TYPES,
)
from .event_replayer import EventReplayer
from .event_snapshot import (
    _CONTENT_KEYS,
    _ENGINE_KEYS,
    _ENVELOPE_KEYS_SNAPSHOT,
    _ORDER_RECORD_KEYS,
    _PLAN_KEYS,
    _POV_PLAN_KEYS,
    _SYMBOL_STATE_KEYS,
    _SYMBOL_STATE_KEYS_LEGACY,
    _TRADE_KEYS,
    _VWAP_PLAN_KEYS,
    SnapshotError,
    _engine_from_json,
    _engine_to_json,
    _is_str_set_list,
    _non_negative_int,
    _parse_plan_entry,
    _require,
    export_snapshot,
    restore_replayer,
)
from .event_types import (
    _BASELINE_KEYS,
    _BOOK_RECONSTRUCTION_REPORT_KEYS,
    _ENVELOPE_KEYS,
    _EXECUTION_REPORT_KEYS,
    _IMPACT_REPORT_KEYS,
    _PLAN_TCA_REPORT_KEYS,
    _PLAN_TYPES,
    _PORTFOLIO_REPORT_KEYS,
    _PORTFOLIO_STRESS_REPORT_KEYS,
    _POV_START_KEYS,
    _POV_START_REQUIRED,
    _POV_TYPES,
    _POV_VOLUME_KEYS,
    _PRICE_LIMIT_UPDATE_KEYS,
    _SESSION_EXPECTED_ACCOUNT_KEYS,
    _SESSION_EXPECTED_TRADE_KEYS,
    _SESSION_RECONCILIATION_KEYS,
    _STRESS_SCENARIO_KEYS,
    _TWAP_PLAN_REF_KEYS,
    _TWAP_START_KEYS,
    _TWAP_START_REQUIRED,
    _TWAP_TYPES,
    _VWAP_START_KEYS,
    _VWAP_START_REQUIRED,
    _VWAP_TYPES,
    ACCEPTED,
    ALGORITHM_POV,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
    BOOK_RECONSTRUCTION_REPORT,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EXECUTION_PLAN_CLOSED,
    FORMAT_VERSION,
    INVALID_EVENT,
    MARK_PRICE_MISMATCH,
    OUT_OF_ORDER,
    PLAN_ACTIVE,
    PLAN_CANCELLED,
    PLAN_COMPLETED,
    PLAN_TCA_REPORT,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    TARGET_SEQUENCE_NOT_FOUND,
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
    VWAP_CANCEL,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    DUPLICATE_EXECUTION_PLAN,
)
from .event_validation import (
    DEFAULT_CONFIG,
    _book_reconstruction_schema_error,
    _execution_report_schema_error,
    _impact_report_schema_error,
    _plan_tca_report_schema_error,
    _portfolio_report_schema_error,
    _portfolio_stress_report_schema_error,
    _pov_schema_error,
    _price_limit_error,
    _price_limit_update_schema_error,
    _price_map_error,
    _session_reconciliation_schema_error,
    _twap_schema_error,
    _validate_config,
    _valid_timestamp,
    _vwap_schema_error,
)
from .execution_plans import ExecutionPlan, _allocate_slices
from .replay_api import replay_events
from .replay_json import _digest, canonical_json
from .replay_state import _Applied, _CommitContext, _SymbolState
