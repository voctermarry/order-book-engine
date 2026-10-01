"""Single-document JSON entry point for ordered multi-symbol event replay.

The command reads exactly one UTF-8 JSON request document from standard
input and writes exactly one UTF-8 JSON response document to standard
output. It never opens files; snapshots and results only travel through the
caller-supplied document.
"""

from __future__ import annotations

import json
import sys

from .event_replay import (
    SnapshotError,
    canonical_json,
    replay_events,
)

_ERROR_IO = "ERROR_IO\n"
_INVALID_REQUEST = "INVALID_REQUEST"


def _write_error(stdout: object, stderr: object, code: str, message: str) -> int:
    try:
        stdout.buffer.write(
            canonical_json({"error": {"code": code, "message": message}}) + b"\n"
        )
        stdout.buffer.flush()
    except OSError:
        try:
            stderr.write(_ERROR_IO)
            stderr.flush()
        except OSError:
            pass
        return 1
    return 2


def serve_events(
    stdin: object = sys.stdin,
    stdout: object = sys.stdout,
    stderr: object = sys.stderr,
) -> int:
    """Run one events request. Returns the process exit code."""
    try:
        raw = stdin.buffer.read()
        text = raw.decode("utf-8")
    except (OSError, UnicodeError):
        try:
            stderr.write(_ERROR_IO)
            stderr.flush()
        except OSError:
            pass
        return 1

    try:
        request = json.loads(text)
    except json.JSONDecodeError:
        return _write_error(stdout, stderr, _INVALID_REQUEST, "request must be one JSON document")
    if (
        not isinstance(request, dict)
        or not isinstance(request.get("events"), list)
        or not set(request) <= {"events", "config", "snapshot", "snapshot_after"}
        or (
            "config" in request
            and not isinstance(request["config"], dict)
        )
        or (
            "snapshot" in request
            and request["snapshot"] is not None
            and not isinstance(request["snapshot"], dict)
        )
    ):
        return _write_error(
            stdout, stderr, _INVALID_REQUEST,
            'request must be an object with an "events" list and optional '
            '"config" and "snapshot" objects',
        )

    marker = request.get("snapshot_after", "last")
    if marker is not None and marker != "last":
        if not (
            isinstance(marker, dict)
            and isinstance(marker.get("symbol"), str)
            and isinstance(marker.get("sequence"), int)
        ):
            return _write_error(
                stdout, stderr, _INVALID_REQUEST,
                'snapshot_after must be "last", null or '
                '{"symbol": str, "sequence": int}',
            )

    try:
        response = replay_events(
            request["events"],
            config=request.get("config"),
            snapshot=request.get("snapshot"),
            snapshot_after=marker,
        )
    except SnapshotError as exc:
        return _write_error(stdout, stderr, exc.code, str(exc))
    except (TypeError, ValueError) as exc:
        return _write_error(stdout, stderr, _INVALID_REQUEST, str(exc))

    try:
        stdout.buffer.write(canonical_json(response) + b"\n")
        stdout.buffer.flush()
    except OSError:
        try:
            stderr.write(_ERROR_IO)
            stderr.flush()
        except OSError:
            pass
        return 1
    return 0
