"""order-book-engine — Deterministic limit order book matching and execution analytics"""

__version__ = "0.1.0"

from .event_replay import (
    ACCEPTED,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    CanonicalError,
    EventReplayer,
    SnapshotError,
    canonical_dumps,
    format_version,
    replay_events,
    restore_snapshot,
)

__all__ = [
    "ACCEPTED",
    "CONFIG_MISMATCH",
    "DUPLICATE",
    "EVENT_ID_CONFLICT",
    "INVALID_EVENT",
    "OUT_OF_ORDER",
    "REJECTED",
    "SEQUENCE_GAP",
    "SNAPSHOT_CORRUPT",
    "SNAPSHOT_VERSION_UNSUPPORTED",
    "CanonicalError",
    "EventReplayer",
    "SnapshotError",
    "canonical_dumps",
    "format_version",
    "replay_events",
    "restore_snapshot",
]
