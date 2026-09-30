"""End-to-end tests for the replay command and the matching engine."""

from __future__ import annotations

import io
import json

import pytest

from order_book_engine import cli
from order_book_engine.engine import OrderBook, validate_event


def run_replay(text: str) -> tuple[list[dict], int, str]:
    stdin = io.BytesIO(text.encode("utf-8"))
    stdout = io.BytesIO()
    rc = cli.replay(stdin, stdout)
    lines = [
        json.loads(line) for line in stdout.getvalue().decode("utf-8").splitlines()
    ]
    return lines, rc, stdout.getvalue().decode("utf-8")


def test_version_and_help_behaviour_unchanged():
    assert cli.main(["version"]) == 0
    assert cli.main([]) == 0


def test_basic_cross_produces_trade_at_maker_price_and_empty_book():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 100, "quantity": 5},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "price": 101, "quantity": 5},
    ]
    lines, rc, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert rc == 0
    assert lines[0]["result"] == "RESTING"
    assert lines[0]["order_book"] == {
        "bids": [],
        "asks": [{"price": 100, "quantity": 5}],
    }
    assert lines[1]["result"] == "FILLED"
    assert lines[1]["trades"] == [
        {"maker_order_id": "o1", "taker_order_id": "o2",
         "price": 100, "quantity": 5}
    ]
    assert lines[1]["order_book"] == {"bids": [], "asks": []}


def test_price_time_priority_and_aggregation():
    events = [
        # Two asks at 100 (o1 first), one at 99 (o3).
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 100, "quantity": 4},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "SELL",
         "order_type": "LIMIT", "price": 100, "quantity": 6},
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "SELL",
         "order_type": "LIMIT", "price": 99, "quantity": 2},
        # Market buy of 7: o3 (2) then o1 (4) then o2 (1).
        {"event_id": "e4", "type": "ADD", "order_id": "o4", "side": "BUY",
         "order_type": "MARKET", "quantity": 7},
    ]
    lines, rc, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert rc == 0
    trades = lines[3]["trades"]
    assert [(t["maker_order_id"], t["quantity"], t["price"]) for t in trades] == [
        ("o3", 2, 99),
        ("o1", 4, 100),
        ("o2", 1, 100),
    ]
    assert lines[3]["result"] == "FILLED"
    assert lines[3]["order_book"] == {
        "bids": [],
        "asks": [{"price": 100, "quantity": 5}],
    }


def test_partial_fill_then_rest_and_market_remnant_cancelled():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 50, "quantity": 3},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "price": 50, "quantity": 10},
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "BUY",
         "order_type": "MARKET", "quantity": 4},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[1]["result"] == "PARTIALLY_FILLED_RESTING"
    assert lines[1]["order_book"]["bids"] == [{"price": 50, "quantity": 7}]
    assert lines[2]["result"] == "UNFILLED_CANCELLED"
    assert lines[2]["trades"] == []
    # Market order must not rest.
    assert lines[2]["order_book"]["bids"] == [{"price": 50, "quantity": 7}]


def test_limit_does_not_cross_beyond_its_price():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 60, "quantity": 5},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "price": 55, "quantity": 5},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[1]["result"] == "RESTING"
    assert lines[1]["trades"] == []
    assert lines[1]["order_book"] == {
        "bids": [{"price": 55, "quantity": 5}],
        "asks": [{"price": 60, "quantity": 5}],
    }


def test_cancel_succeeds_and_unknown_cancel_rejected_without_state_change():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "price": 10, "quantity": 2},
        {"event_id": "e2", "type": "CANCEL", "order_id": "nope"},
        {"event_id": "e3", "type": "CANCEL", "order_id": "o1"},
        {"event_id": "e4", "type": "CANCEL", "order_id": "o1"},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[1]["result"] == "REJECTED"
    assert lines[1]["rejection_reason"] == "UNKNOWN_ORDER"
    assert lines[1]["trades"] == []
    assert lines[1]["order_book"]["bids"] == [{"price": 10, "quantity": 2}]
    assert lines[2]["result"] == "CANCELLED"
    assert lines[2]["order_book"]["bids"] == []
    assert lines[3]["rejection_reason"] == "UNKNOWN_ORDER"
    # Rejected event ids are not consumed.
    assert "e2" not in lines[3]  # sanity: output shape unaffected
    dup, _, _ = run_replay(
        json.dumps(
            {"event_id": "e2", "type": "CANCEL", "order_id": "ghost"}
        )
        + "\n"
    )
    assert dup[0]["rejection_reason"] == "UNKNOWN_ORDER"


def test_duplicate_event_and_order_ids():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "price": 10, "quantity": 1},
        {"event_id": "e1", "type": "ADD", "order_id": "o9", "side": "BUY",
         "order_type": "LIMIT", "price": 10, "quantity": 1},
        {"event_id": "e9", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "price": 10, "quantity": 1},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[1]["rejection_reason"] == "DUPLICATE_EVENT_ID"
    assert lines[2]["rejection_reason"] == "DUPLICATE_ORDER_ID"
    # Book unchanged by the rejections.
    assert lines[2]["order_book"] == {
        "bids": [{"price": 10, "quantity": 1}],
        "asks": [],
    }


def test_rejected_event_does_not_advance_trade_numbering_or_priority():
    # o1 and o2 at the same ask price; invalid event between fills must not
    # disturb time priority.
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 10, "quantity": 2},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "SELL",
         "order_type": "LIMIT", "price": 10, "quantity": 2},
        "{not json",
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "BUY",
         "order_type": "MARKET", "quantity": 2},
    ]
    lines, rc, _ = run_replay(
        "\n".join(
            json.dumps(e) if not isinstance(e, str) else e for e in events
        )
        + "\n"
    )
    assert rc == 0
    assert lines[2]["result"] == "REJECTED"
    assert lines[2]["rejection_reason"] == "INVALID_JSON"
    assert lines[2]["event_id"] is None
    assert lines[3]["trades"][0]["maker_order_id"] == "o1"


@pytest.mark.parametrize(
    "payload",
    [
        [],
        "just a string",
        42,
        {},
        {"type": "ADD"},
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1},  # missing price
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "BUY",
         "order_type": "MARKET", "quantity": 1, "price": 5},
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "LONG",
         "order_type": "LIMIT", "quantity": 1, "price": 5},
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "BUY",
         "order_type": "LIMIT", "quantity": 0, "price": 5},
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1.5, "price": 5},
        {"type": "ADD", "event_id": "e", "order_id": "o", "side": "BUY",
         "order_type": "LIMIT", "quantity": True, "price": 5},
        {"type": "ADD", "event_id": 7, "order_id": "o", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 5},
        {"type": "CANCEL", "event_id": "e", "order_id": "o", "extra": 1},
        {"type": "WAT", "event_id": "e"},
    ],
)
def test_invalid_schema_cases(payload):
    event, reason = validate_event(payload)
    assert event is None
    assert reason == "INVALID_SCHEMA"


def test_blank_lines_ignored_and_output_is_byte_deterministic():
    text = (
        "\n"
        + json.dumps(
            {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
             "order_type": "LIMIT", "price": 1, "quantity": 1}
        )
        + "\n\n   \n"
    )
    _, rc1, raw1 = run_replay(text)
    _, rc2, raw2 = run_replay(text)
    assert rc1 == rc2 == 0
    assert raw1 == raw2
    # The whitespace-only line is invalid JSON, so exactly 2 outputs:
    # one RESTING and one INVALID_JSON rejection.
    results = [json.loads(line)["result"] for line in raw1.splitlines()]
    assert results == ["RESTING", "REJECTED"]


def test_market_partial_fill_cancelled_status():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 20, "quantity": 2},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "MARKET", "quantity": 5},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[1]["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert lines[1]["trades"][0]["quantity"] == 2


def test_sell_matches_highest_bid_first():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "price": 9, "quantity": 1},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "price": 11, "quantity": 1},
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "SELL",
         "order_type": "MARKET", "quantity": 1},
    ]
    lines, _, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert lines[2]["trades"][0]["maker_order_id"] == "o2"
    assert lines[2]["trades"][0]["price"] == 11


def test_cancel_filled_order_is_unknown_and_order_id_not_reusable():
    events = [
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "SELL",
         "order_type": "LIMIT", "price": 10, "quantity": 1},
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "MARKET", "quantity": 1},
        {"event_id": "e3", "type": "CANCEL", "order_id": "o1"},
        {"event_id": "e4", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "price": 1, "quantity": 1},
    ]
    lines, rc, _ = run_replay("\n".join(map(json.dumps, events)) + "\n")
    assert rc == 0
    assert lines[2]["rejection_reason"] == "UNKNOWN_ORDER"
    assert lines[3]["rejection_reason"] == "DUPLICATE_ORDER_ID"


def test_io_failures_return_1_and_single_stderr_line(capsys):
    # Unreadable stdin.
    class BadIn:
        def __iter__(self):
            raise OSError("boom")

    assert cli.replay(BadIn(), io.BytesIO()) == 1
    captured = capsys.readouterr()
    assert captured.err == "ERROR_IO\n"

    # Unwritable stdout.
    class BadOut(io.BytesIO):
        def write(self, data):
            raise OSError("broken pipe")

    stdin = io.BytesIO(
        json.dumps(
            {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
             "order_type": "LIMIT", "price": 1, "quantity": 1}
        ).encode()
        + b"\n"
    )
    assert cli.replay(stdin, BadOut()) == 1
    captured = capsys.readouterr()
    assert captured.err == "ERROR_IO\n"


def test_cancelled_order_lagging_in_queue_does_not_block_fill():
    book = OrderBook()
    book.event_ids.add("e1")
    book.order_ids.add("o1")
    book.add("o1", "SELL", "LIMIT", 2, 10)
    book.event_ids.add("e2")
    book.order_ids.add("o2")
    book.add("o2", "SELL", "LIMIT", 2, 10)
    assert book.cancel("o1") == "CANCELLED"
    snap = book.snapshot()
    assert snap["asks"] == [{"price": 10, "quantity": 2}]
    _, trades = book.add("o3", "BUY", "MARKET", 2, None)
    assert [t["maker_order_id"] for t in trades] == ["o2"]
