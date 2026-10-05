"""Deterministic multi-symbol ordered event replay with resumable snapshots.

This package adds a new public entry point on top of the baseline single-book
:class:`~order_book_engine.engine.Engine`; it never changes the baseline
matching rules, priorities, rejection semantics or trade record shapes:

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
  previously committed sequence), the read-only BOOK_LIQUIDITY_REPORT
  query (which summarizes the current book's public depth) and the intraday
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

The package never opens files: events and snapshots are received from, and
returned to, the caller.

Internal structure (all imports below keep working exactly as they did when
this was a single module):

* :mod:`.constants` — event types, rejection codes, key sets and the default
  configuration;
* :mod:`.validation` — structural payload and configuration validators;
* :mod:`.plan` — the resumable parent-order :class:`ExecutionPlan` state;
* :mod:`.state` — per-symbol state and the shared commit/response machinery;
* :mod:`.handlers` — the business handler of every committed event kind;
* :mod:`.history` — historical book reconstruction on a throwaway session;
* :mod:`.registry` — the event-kind registry binding each type to its
  validator, inline key set, handler and commit behaviour;
* :mod:`.replayer` — the :class:`EventReplayer` orchestration session;
* :mod:`.snapshot` — snapshot export and restoration;
* :mod:`.api` — the public one-call entry points.
"""

from __future__ import annotations

from .. import __version__  # noqa: F401
from ..engine import (  # noqa: F401
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
    UNKNOWN_ACCOUNT,
    UNKNOWN_ORDER,
    Engine,
    _is_int,
    _is_non_empty_str,
    _is_positive_int,
)
from .api import export_snapshot, replay_events, restore_replayer  # noqa: F401
from .constants import (  # noqa: F401
    ACCEPTED,
    ALGORITHM_POV,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
    BOOK_LIQUIDITY_REPORT,
    BOOK_RECONSTRUCTION_REPORT,
    CONFIG_MISMATCH,
    DEFAULT_CONFIG,
    DUPLICATE,
    DUPLICATE_EXECUTION_PLAN,
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
    SUPPORTED_TYPES,
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
)
from .errors import SnapshotError  # noqa: F401
from .plan import ExecutionPlan  # noqa: F401
from .replayer import EventReplayer  # noqa: F401
from .serialization import _digest, canonical_json  # noqa: F401
from .state import (  # noqa: F401
    _Applied,
    _CommitContext,
    _SymbolState,
    book_changes,
    invalid_result,
    price_limits_view,
)
from .validation import (  # noqa: F401
    _allocate_slices,
    _book_liquidity_report_schema_error,
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
