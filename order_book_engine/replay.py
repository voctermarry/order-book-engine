"""JSON Lines event replay over standard input/output.

Each non-empty input line is handled in order and produces one JSON object
on standard output. Empty lines are ignored. The program never opens any
files itself.
"""

from __future__ import annotations

import json
import sys

from .engine import Engine

_ERROR_IO = "ERROR_IO\n"


def _encode(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def replay(stdin: object = sys.stdin, stdout: object = sys.stdout, stderr: object = sys.stderr) -> int:
    """Run the replay loop. Returns the process exit code."""
    engine = Engine()
    out_parts: list[bytes] = []

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

    for line in text.split("\n"):
        if line.endswith("\r"):
            line = line[:-1]
        if line == "":
            continue

        event_id, result, reason, trades, stp, analysis = engine.handle_line_extended(line)
        bids, asks = engine.snapshot()
        payload: dict[str, object] = {
            "input_line": line,
            "event_id": event_id,
            "result": result,
        }
        if reason is not None:
            payload["reason"] = reason
        if stp is not None:
            payload["self_trade_prevention"] = stp
        if analysis is not None:
            payload["execution_analysis"] = analysis
        payload["trades"] = trades
        payload["bids"] = bids
        payload["asks"] = asks
        out_parts.append(_encode(payload))
        out_parts.append(b"\n")

    try:
        stdout.buffer.write(b"".join(out_parts))
        stdout.buffer.flush()
    except OSError:
        try:
            stderr.write(_ERROR_IO)
            stderr.flush()
        except OSError:
            pass
        return 1
    return 0
