"""Public one-call entry points: replay, snapshot export and restoration.

These functions wire the orchestrator (:class:`EventReplayer`) to the
persistence layer (:mod:`.snapshot`) without either depending on the other.
"""

from __future__ import annotations

from .constants import ACCEPTED
from .replayer import EventReplayer
from .snapshot import dump_session, parse_snapshot


def export_snapshot(replayer: EventReplayer) -> dict[str, object]:
    """Export a resumable, JSON-compatible snapshot of a replay session.

    The snapshot fully preserves (per security): price-time queue priority,
    order remainders, the iceberg current slice and replenishment state, the
    last sequence, cumulative trades, the next trade id counter and the
    active price-limit interval. It also carries the format version, the
    matching configuration (plus its digest) and a SHA-256 digest over the
    normalized whole document.
    """
    return dump_session(replayer)


def restore_replayer(
    snapshot: dict[str, object],
    config: dict[str, object] | None = None,
) -> EventReplayer:
    """Rebuild an :class:`EventReplayer` from :func:`export_snapshot` output.

    Version, configuration digest and content SHA-256 are verified before any
    state is adopted. Any failure raises :class:`SnapshotError` and leaves no
    partially recovered session behind.
    """
    expected_config, symbols, events = parse_snapshot(snapshot, config)
    replayer = EventReplayer(expected_config)
    replayer._symbols = symbols
    replayer._events = events
    return replayer


def replay_events(
    events: list[dict[str, object]],
    config: dict[str, object] | None = None,
    snapshot: dict[str, object] | None = None,
    snapshot_after: object = "last",
) -> dict[str, object]:
    """Replay one ordered multi-security event stream in a single public call.

    Parameters
    ----------
    events:
        Event objects in strict submission order. Each event carries
        ``event_id`` (non-empty string, unique across the whole stream),
        ``symbol`` (non-empty string), ``sequence`` (positive integer,
        strictly increasing per symbol) and a payload — either inline
        (``"type"`` plus the fields of that event kind) or nested under an
        ``"event"`` object that repeats ``event_id`` and ``type``. Supported
        kinds are the baseline ADD/CANCEL/REPLACE events, the TWAP/VWAP
        TWAP_START/TWAP_SLICE/TWAP_CANCEL/TWAP_REPORT and
        VWAP_START/VWAP_SLICE/VWAP_CANCEL/VWAP_REPORT commands, the POV
        POV_START/POV_VOLUME/POV_CANCEL/POV_REPORT commands, the read-only
        per-order EXECUTION_REPORT query, the read-only per-symbol
        IMPACT_REPORT what-if query, the read-only cross-security
        PORTFOLIO_REPORT query, the read-only cross-security
        PORTFOLIO_STRESS_REPORT query, the read-only whole-session
        SESSION_RECONCILIATION query, the read-only per-plan
        PLAN_TCA_REPORT query, the read-only historical
        BOOK_RECONSTRUCTION_REPORT query and the intraday PRICE_LIMIT_UPDATE
        adjustment.
    config:
        Matching configuration summary. Defaults to :data:`DEFAULT_CONFIG`;
        a resumed run must pass the same configuration the snapshot carries.
    snapshot:
        Optional snapshot returned by an earlier :func:`export_snapshot` /
        :func:`replay_events` call. Restoration is fully verified before any
        event is processed.
    snapshot_after:
        ``"last"`` (default) attaches the snapshot after the final event;
        ``None`` omits it; ``{"symbol": ..., "sequence": ...}`` attaches the
        snapshot right after that identified successful event.

    Returns
    -------
    dict
        ``{"results": [...], "snapshot": {...} | None}``. Each result is
        associated with ``event_id``/``symbol``/``sequence``, carries a
        ``status`` of ``ACCEPTED``, ``REJECTED`` or ``DUPLICATE`` (with
        ``rejection_code`` for rejections), and lists the event's trades,
        book changes and the post-event ``bids``/``asks``.
    """
    if not isinstance(events, list):
        raise TypeError("events must be a list of event objects")

    if snapshot is not None:
        replayer = restore_replayer(snapshot, config)
    else:
        replayer = EventReplayer(config)

    if snapshot_after is not None and snapshot_after != "last":
        if not (
            isinstance(snapshot_after, dict)
            and isinstance(snapshot_after.get("symbol"), str)
            and isinstance(snapshot_after.get("sequence"), int)
        ):
            raise ValueError(
                "snapshot_after must be 'last', None or "
                "{'symbol': str, 'sequence': int}"
            )

    results: list[dict[str, object]] = []
    exported: dict[str, object] | None = None
    marker_matched = snapshot_after is None or snapshot_after == "last"
    for position, event in enumerate(events):
        result = replayer._submit_one(event)
        results.append(result)
        if (
            snapshot_after is not None
            and snapshot_after != "last"
            and result.get("symbol") == snapshot_after["symbol"]
            and result.get("sequence") == snapshot_after["sequence"]
        ):
            marker_matched = True
            if result.get("status") != ACCEPTED:
                raise ValueError(
                    "snapshot_after must identify an ACCEPTED event; "
                    f"{snapshot_after['symbol']} sequence {snapshot_after['sequence']} "
                    f"was {result.get('status')}"
                )
            exported = export_snapshot(replayer)

    if snapshot_after != "last" and not marker_matched:
        raise ValueError("snapshot_after did not match any event in the stream")
    if snapshot_after == "last":
        exported = export_snapshot(replayer)

    return {"results": results, "snapshot": exported}
