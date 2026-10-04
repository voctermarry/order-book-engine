"""Event-type vocabulary of the multi-symbol replay layer.

This module only names things: the snapshot format version, the event type
strings, the status and rejection codes, the plan lifecycle vocabulary and
the exact field sets every event kind may carry. It has no behaviour of its
own — payload validation lives in
:mod:`order_book_engine.event_validation`, the dispatch table in
:mod:`order_book_engine.event_registry`.
"""

from __future__ import annotations

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
