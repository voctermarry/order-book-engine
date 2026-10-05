"""Event-type, rejection-code and configuration constants for event replay.

This module is the single source of truth for the replay layer's vocabulary:
the public event-type names, the validation/ordering/business rejection
codes, the per-kind payload key sets used by the inline envelope form, the
type-classification sets (plan commands, replay-only ids, read-only no-ops)
and the default matching configuration. It deliberately depends on nothing
but the baseline engine's own constants, so every other replay module can
import it without creating a cycle.
"""

from __future__ import annotations

from .. import __version__
from ..engine import (
    ADD,
    CANCEL,
    EXECUTION_REPORT,
    IMPACT_REPORT,
    REPLACE,
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

# Cross-security portfolio stress report event: a read-only query that
# re-prices one account's existing positions under several caller-supplied
# price scenarios. It shares PORTFOLIO_REPORT's business rejection codes
# (UNKNOWN_ACCOUNT and MARK_PRICE_MISMATCH).
PORTFOLIO_STRESS_REPORT = "PORTFOLIO_STRESS_REPORT"

# Read-only whole-session reconciliation event. Unlike the baseline
# single-security DAY_END_RECONCILIATION (which stays exclusive to the JSON
# Lines entry point), this query reconciles every security of the session at
# once; external records name their security explicitly.
SESSION_RECONCILIATION = "SESSION_RECONCILIATION"

# Read-only per-plan implementation-shortfall query. A replay-only event like
# SESSION_RECONCILIATION: the baseline engine and the JSON Lines entry point
# never accept it.
PLAN_TCA_REPORT = "PLAN_TCA_REPORT"

# Read-only historical book reconstruction query and its single business
# rejection code. Like the other replay-only reports it never reaches the
# baseline engine or the JSON Lines entry point; the baseline rejects the
# type as INVALID_SCHEMA.
BOOK_RECONSTRUCTION_REPORT = "BOOK_RECONSTRUCTION_REPORT"
TARGET_SEQUENCE_NOT_FOUND = "TARGET_SEQUENCE_NOT_FOUND"

# Read-only current-book depth summary query. Like the other replay-only
# reports it never reaches the baseline engine or the JSON Lines entry
# point; the baseline rejects the type as INVALID_SCHEMA.
BOOK_LIQUIDITY_REPORT = "BOOK_LIQUIDITY_REPORT"

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

# POV event types.
POV_START = "POV_START"
POV_VOLUME = "POV_VOLUME"
POV_CANCEL = "POV_CANCEL"
POV_REPORT = "POV_REPORT"

# Plan algorithm labels: TWAP plans keep the historical summary shape, VWAP
# plans additionally report their algorithm and per-slice schedule, and POV
# plans track a participation rate against a caller-fed market volume.
ALGORITHM_TWAP = "TWAP"
ALGORITHM_VWAP = "VWAP"
ALGORITHM_POV = "POV"

# TWAP plan lifecycle statuses.
PLAN_ACTIVE = "ACTIVE"
PLAN_COMPLETED = "COMPLETED"
PLAN_CANCELLED = "CANCELLED"

# Snapshot restoration failure codes.
SNAPSHOT_CORRUPT = "SNAPSHOT_CORRUPT"
SNAPSHOT_VERSION_UNSUPPORTED = "SNAPSHOT_VERSION_UNSUPPORTED"
CONFIG_MISMATCH = "CONFIG_MISMATCH"

#: Event types this replay layer accepts. The TWAP/VWAP parent-order commands
#: join the baseline mutating behaviours; of the baseline single-security
#: read-only reports the per-order EXECUTION_REPORT query and the
#: what-if IMPACT_REPORT query join them here (ACCOUNT_REPORT and
#: DAY_END_RECONCILIATION stay exclusive to the JSON Lines entry point),
#: while the cross-security
#: PORTFOLIO_REPORT query, the cross-security PORTFOLIO_STRESS_REPORT query,
#: the whole-session SESSION_RECONCILIATION query,
#: the per-plan PLAN_TCA_REPORT query, the historical
#: BOOK_RECONSTRUCTION_REPORT query, the current-book depth-summary
#: BOOK_LIQUIDITY_REPORT query and the intraday PRICE_LIMIT_UPDATE
#: adjustment are exclusive to this layer.
SUPPORTED_TYPES = frozenset(
    {ADD, CANCEL, REPLACE,
     TWAP_START, TWAP_SLICE, TWAP_CANCEL, TWAP_REPORT,
     VWAP_START, VWAP_SLICE, VWAP_CANCEL, VWAP_REPORT,
     POV_START, POV_VOLUME, POV_CANCEL, POV_REPORT,
     EXECUTION_REPORT, IMPACT_REPORT, PORTFOLIO_REPORT, PORTFOLIO_STRESS_REPORT,
     SESSION_RECONCILIATION,
     PLAN_TCA_REPORT, BOOK_RECONSTRUCTION_REPORT, BOOK_LIQUIDITY_REPORT,
     PRICE_LIMIT_UPDATE}
)

_ENVELOPE_KEYS = frozenset({"event_id", "symbol", "sequence", "timestamp"})
_BASELINE_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity", "account_id"}
)
_PORTFOLIO_REPORT_KEYS = frozenset(
    {"event_id", "type", "account_id", "mark_prices"}
)
_PORTFOLIO_STRESS_REPORT_KEYS = frozenset(
    {"event_id", "type", "account_id", "mark_prices", "scenarios"}
)
_STRESS_SCENARIO_KEYS = frozenset({"name", "prices"})
_SESSION_RECONCILIATION_KEYS = frozenset(
    {"event_id", "type", "expected_trades", "expected_accounts"}
)
_SESSION_EXPECTED_TRADE_KEYS = frozenset(
    {"symbol", "trade_id", "maker_order_id", "taker_order_id", "price", "quantity"}
)
_SESSION_EXPECTED_ACCOUNT_KEYS = frozenset(
    {"symbol", "account_id", "net_position", "cash_balance"}
)
_PLAN_TCA_REPORT_KEYS = frozenset(
    {"event_id", "type", "plan_id", "mark_price"}
)
_BOOK_RECONSTRUCTION_REPORT_KEYS = frozenset(
    {"event_id", "type", "target_sequence"}
)
_BOOK_LIQUIDITY_REPORT_KEYS = frozenset(
    {"event_id", "type", "depth"}
)
_EXECUTION_REPORT_KEYS = frozenset(
    {"event_id", "type", "order_id", "benchmark_price"}
)
_IMPACT_REPORT_KEYS = frozenset(
    {"event_id", "type", "side", "quantity", "benchmark_price"}
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
_POV_START_KEYS = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity",
     "participation_bps", "order_type", "benchmark_price", "price",
     "account_id"}
)
_POV_START_REQUIRED = frozenset(
    {"event_id", "type", "plan_id", "side", "total_quantity",
     "participation_bps", "order_type", "benchmark_price"}
)
_POV_VOLUME_KEYS = frozenset(
    {"event_id", "type", "plan_id", "market_volume_increment"}
)
_POV_TYPES = frozenset({POV_START, POV_VOLUME, POV_CANCEL, POV_REPORT})
#: Every parent-order command type, across algorithms.
_PLAN_TYPES = _TWAP_TYPES | _VWAP_TYPES | _POV_TYPES
#: Event types whose ids live solely in the replay log: parent-order commands
#: never touch the engine journal, the per-order execution query, the
#: per-symbol book-impact what-if query, the cross-security portfolio and
#: portfolio-stress queries
#: and the whole-session reconciliation are read-only and matched by no
#: engine, and a price-limit adjustment only rewrites replay-layer state.
#: Baseline ADD/CANCEL/REPLACE ids occupy the per-symbol engine journal
#: instead.
_REPLAY_ONLY_TYPES = _PLAN_TYPES | frozenset(
    {EXECUTION_REPORT, IMPACT_REPORT, PORTFOLIO_REPORT, PORTFOLIO_STRESS_REPORT,
     SESSION_RECONCILIATION,
     PLAN_TCA_REPORT, BOOK_RECONSTRUCTION_REPORT, BOOK_LIQUIDITY_REPORT,
     PRICE_LIMIT_UPDATE}
)
#: Accepted replay-only events that never move any state themselves: every
#: read-only report. A historical reconstruction rebuild replays the logged
#: stream and skips these, since they occupied a sequence but changed
#: nothing. PRICE_LIMIT_UPDATE is intentionally absent: it rewrites the
#: active interval and must be replayed up to the target point.
_REPLAY_NOOP_TYPES = frozenset(
    {EXECUTION_REPORT, IMPACT_REPORT, PORTFOLIO_REPORT, PORTFOLIO_STRESS_REPORT,
     SESSION_RECONCILIATION,
     PLAN_TCA_REPORT, BOOK_RECONSTRUCTION_REPORT, BOOK_LIQUIDITY_REPORT}
)

DEFAULT_CONFIG: dict[str, object] = {
    "matching_engine": "order-book-engine",
    "engine_version": __version__,
    "price_time_priority": True,
    "trade_id_scheme": "per_symbol_monotonic_from_1",
    "iceberg_replenishment": "tail_of_price_level",
    "self_trade_prevention": "same_account_only",
}
