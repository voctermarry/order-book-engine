"""Command line entry point for order-book-engine."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from . import engine
from .engine import INVALID_JSON, OrderBook, validate_event


def _reject_constant(value: str):
    raise ValueError(f"invalid JSON constant: {value}")


def _reject(input_line: str, event_id, reason: str, book: dict) -> dict:
    return {
        "input_line": input_line,
        "event_id": event_id,
        "result": "REJECTED",
        "rejection_reason": reason,
        "trades": [],
        "order_book": book,
    }


def replay(stdin_buffer, stdout_buffer) -> int:
    """Consume UTF-8 JSON Lines from stdin and emit one JSON object per line.

    Returns 0 on clean completion, 1 when reading or writing fails; a single
    ``ERROR_IO`` line is written to stderr on such failures.
    """
    book = OrderBook()

    def emit(obj: dict) -> bool:
        payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        try:
            stdout_buffer.write(payload + b"\n")
        except (OSError, ValueError):
            return False
        return True

    def fail_io() -> int:
        try:
            sys.stderr.write("ERROR_IO\n")
            sys.stderr.flush()
        except (OSError, ValueError):
            pass
        return 1

    def reject(input_line: str, event_id, reason: str) -> int | None:
        obj = _reject(input_line, event_id, reason, book.snapshot())
        return None if emit(obj) else fail_io()

    try:
        for raw in stdin_buffer:
            if raw.endswith(b"\n"):
                raw = raw[:-1]
            if raw.endswith(b"\r"):
                raw = raw[:-1]
            if not raw:
                # Blank lines are ignored; whitespace-only lines are non-empty
                # and are processed (and rejected) like any malformed event.
                continue

            try:
                input_line = raw.decode("utf-8")
            except UnicodeDecodeError:
                return fail_io()

            # NaN/Infinity are rejected by parse_constant: Python's json
            # parser accepts them by default although they are not JSON.
            try:
                parsed = json.loads(input_line, parse_constant=_reject_constant)
            except (json.JSONDecodeError, ValueError):
                outcome = reject(input_line, None, INVALID_JSON)
                if outcome is not None:
                    return outcome
                continue

            event, schema_error = validate_event(parsed)
            if schema_error is not None:
                event_id = (
                    parsed.get("event_id") if isinstance(parsed, dict) else None
                )
                if not isinstance(event_id, str):
                    event_id = None
                outcome = reject(input_line, event_id, schema_error)
                if outcome is not None:
                    return outcome
                continue

            event_id = event["event_id"]
            if event_id in book.event_ids:
                outcome = reject(input_line, event_id, engine.DUPLICATE_EVENT_ID)
                if outcome is not None:
                    return outcome
                continue

            if event["kind"] == "ADD":
                order_id = event["order_id"]
                if order_id in book.order_ids:
                    outcome = reject(input_line, event_id, engine.DUPLICATE_ORDER_ID)
                    if outcome is not None:
                        return outcome
                    continue

                book.event_ids.add(event_id)
                book.order_ids.add(order_id)
                status, trades = book.add(
                    order_id,
                    event["side"],
                    event["order_type"],
                    event["quantity"],
                    event["price"],
                )
            else:
                status = book.cancel(event["order_id"])
                if status is None:
                    outcome = reject(input_line, event_id, engine.UNKNOWN_ORDER)
                    if outcome is not None:
                        return outcome
                    continue
                book.event_ids.add(event_id)
                trades = []

            obj = {
                "input_line": input_line,
                "event_id": event_id,
                "result": status,
                "trades": trades,
                "order_book": book.snapshot(),
            }
            if not emit(obj):
                return fail_io()
    except (OSError, UnicodeError, ValueError):
        return fail_io()

    try:
        stdout_buffer.flush()
    except (OSError, ValueError):
        return fail_io()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="order-book-engine",
        description="Deterministic limit order book matching and execution analytics",
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")
    sub.add_parser(
        "replay",
        help="replay JSON Lines order events from stdin and emit match results",
    )
    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0

    if args.command == "replay":
        return replay(sys.stdin.buffer, sys.stdout.buffer)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
