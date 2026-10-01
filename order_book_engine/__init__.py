"""order-book-engine — Deterministic limit order book matching and execution analytics"""

__version__ = "0.1.0"

from .event_replay import (
    ACCEPTED,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EventReplayer,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    SnapshotError,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)

__all__ = [
    "ACCEPTED",
    "CONFIG_MISMATCH",
    "DUPLICATE",
    "EVENT_ID_CONFLICT",
    "EventReplayer",
    "FORMAT_VERSION",
    "INVALID_EVENT",
    "OUT_OF_ORDER",
    "REJECTED",
    "SEQUENCE_GAP",
    "SNAPSHOT_CORRUPT",
    "SNAPSHOT_VERSION_UNSUPPORTED",
    "SnapshotError",
    "canonical_json",
    "export_snapshot",
    "replay_events",
    "restore_replayer",
]
