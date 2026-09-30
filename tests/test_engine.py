"""Tests for the matching engine and JSON Lines replay."""

from __future__ import annotations

import io
import json

import pytest

from order_book_engine import replay
from order_book_engine.engine import Engine


def add(event_id, order_id, side, order_type, quantity, price=None, **extra):
    obj = {
        "event_id": event_id,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
    }
    if price is not None:
        obj["price"] = price
    obj.update(extra)
    return json.dumps(obj, ensure_ascii=False)


def cancel(event_id, order_id):
    return json.dumps({"event_id": event_id, "type": "CANCEL", "order_id": order_id})


def book_after(lines):
    engine = Engine()
    results = []
    for line in lines:
        results.append(engine.handle_line(line))
    return engine, results


def test_limit_orders_rest_and_snapshot_is_sorted():
    engine, [(eid, result, reason, trades)] = book_after(
        [add("e1", "o1", "BUY", "LIMIT", 5, 100)]
    )
    assert (eid, result, reason, trades) == ("e1", "RESTING", None, [])
    bids, asks = engine.snapshot()
    assert bids == [{"price": 100, "quantity": 5}]
    assert asks == []

    engine.handle_line(add("e2", "o2", "BUY", "LIMIT", 3, 100))
    engine.handle_line(add("e3", "o3", "BUY", "LIMIT", 4, 99))
    engine.handle_line(add("e4", "o4", "SELL", "LIMIT", 2, 101))
    bids, asks = engine.snapshot()
    assert bids == [
        {"price": 100, "quantity": 8},
        {"price": 99, "quantity": 4},
    ]
    assert asks == [{"price": 101, "quantity": 2}]


def test_trade_takes_passive_price_and_fills_taker():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100)])
    eid, result, reason, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 5, 100))
    assert result == "FILLED"
    assert reason is None
    assert trades == [
        {
            "trade_id": 1,
            "maker_order_id": "s1",
            "taker_order_id": "b1",
            "price": 100,
            "quantity": 5,
        }
    ]
    bids, asks = engine.snapshot()
    assert bids == []
    assert asks == []


def test_partial_fill_maker_keeps_resting_aggregate():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 2, 100))
    assert result == "FILLED"
    assert trades[0]["quantity"] == 2
    bids, asks = engine.snapshot()
    assert asks == [{"price": 100, "quantity": 3}]
    assert bids == []


def test_taker_partially_filled_then_rests():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 5, 100))
    assert result == "PARTIALLY_FILLED_RESTING"
    assert [t["quantity"] for t in trades] == [2]
    bids, asks = engine.snapshot()
    assert bids == [{"price": 100, "quantity": 3}]
    assert asks == []


def test_buy_hits_lowest_ask_and_sell_hits_highest_bid():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 1, 101),
            add("e2", "s2", "SELL", "LIMIT", 1, 99),
            add("e3", "s3", "SELL", "LIMIT", 1, 100),
        ]
    )
    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "MARKET", 2))
    assert [(t["price"], t["maker_order_id"]) for t in trades] == [(99, "s2"), (100, "s3")]

    engine, _ = book_after(
        [
            add("e5", "b1", "BUY", "LIMIT", 1, 98),
            add("e6", "b2", "BUY", "LIMIT", 1, 100),
            add("e7", "b3", "BUY", "LIMIT", 1, 99),
        ]
    )
    _, _, _, trades = engine.handle_line(add("e8", "s4", "SELL", "MARKET", 2))
    assert [(t["price"], t["maker_order_id"]) for t in trades] == [(100, "b2"), (99, "b3")]


def test_price_time_priority_within_one_level():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 1, 100),
            add("e2", "s2", "SELL", "LIMIT", 1, 100),
            add("e3", "s3", "SELL", "LIMIT", 1, 100),
        ]
    )
    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "LIMIT", 2, 100))
    assert [t["maker_order_id"] for t in trades] == ["s1", "s2"]
    # Cancelling s3 must not reorder the remaining queue.
    assert engine.handle_line(cancel("e5", "s3"))[1] == "CANCELLED"
    _, _, _, trades = engine.handle_line(add("e6", "s4", "SELL", "LIMIT", 1, 100))
    assert [t["maker_order_id"] for t in trades] == []
    _, _, _, trades = engine.handle_line(add("e7", "b2", "BUY", "LIMIT", 1, 100))
    assert [t["maker_order_id"] for t in trades] == ["s4"]


def test_limit_order_does_not_cross_its_own_price():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 1, 100)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 1, 99))
    assert result == "RESTING"
    assert trades == []
    bids, asks = engine.snapshot()
    assert bids == [{"price": 99, "quantity": 1}]
    assert asks == [{"price": 100, "quantity": 1}]

    _, result, _, trades = engine.handle_line(add("e3", "s2", "SELL", "LIMIT", 1, 101))
    assert result == "RESTING"
    assert trades == []


def test_market_order_outcomes():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 3, 100)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "MARKET", 3))
    assert result == "FILLED"
    assert len(trades) == 1

    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(add("e4", "b2", "BUY", "MARKET", 5))
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [2]
    assert engine.snapshot()[1] == []

    engine = Engine()
    _, result, _, trades = engine.handle_line(add("e5", "b3", "BUY", "MARKET", 4))
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert engine.snapshot() == ([], [])


def test_market_leftover_does_not_rest_and_cannot_be_cancelled():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 1, 100)])
    engine.handle_line(add("e2", "b1", "BUY", "MARKET", 3))
    _, result, reason, _ = engine.handle_line(cancel("e3", "b1"))
    assert result == "REJECTED"
    assert reason == "UNKNOWN_ORDER"


def test_limit_ioc_outcomes():
    # Fully filled against one resting order.
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 3, 100)])
    _, result, reason, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 3, 100, time_in_force="IOC")
    )
    assert (result, reason) == ("FILLED", None)
    assert [t["quantity"] for t in trades] == [3]
    assert engine.snapshot() == ([], [])

    # Partial fill: leftover is cancelled, nothing enters the bid book.
    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "LIMIT", 5, 100, time_in_force="IOC")
    )
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [2]
    assert engine.snapshot() == ([], [])

    # No trade when nothing satisfies the limit; book stays untouched.
    engine, _ = book_after([add("e5", "s3", "SELL", "LIMIT", 1, 100)])
    _, result, _, trades = engine.handle_line(
        add("e6", "b3", "BUY", "LIMIT", 1, 99, time_in_force="IOC")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]

    # Sell-side IOC hits the highest bid and cancels the leftover.
    engine, _ = book_after([add("e7", "b4", "BUY", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e8", "s4", "SELL", "LIMIT", 4, 100, time_in_force="IOC")
    )
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["maker_order_id"] for t in trades] == ["b4"]
    assert engine.snapshot() == ([], [])


def test_ioc_leftover_cannot_be_cancelled_and_trade_ids_continue():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 5, 100, time_in_force="IOC"))
    _, result, reason, _ = engine.handle_line(cancel("e3", "b1"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")

    # The unfilled IOC must not shadow later liquidity; ids stay sequential.
    engine, _ = book_after(
        [
            add("e4", "s2", "SELL", "LIMIT", 1, 101),
            add("e5", "s3", "SELL", "LIMIT", 1, 100),
            add("e6", "s4", "SELL", "LIMIT", 1, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(
        add("e7", "b2", "BUY", "LIMIT", 1, 99, time_in_force="IOC")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    _, _, _, trades = engine.handle_line(add("e8", "b3", "BUY", "LIMIT", 2, 100))
    assert [t["trade_id"] for t in trades] == [1, 2]
    assert [t["maker_order_id"] for t in trades] == ["s3", "s4"]


def test_limit_fok_success_is_all_or_nothing():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 99),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(
        add("e3", "b1", "BUY", "LIMIT", 5, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert trades == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 99, "quantity": 2},
        {"trade_id": 2, "maker_order_id": "s2", "taker_order_id": "b1",
         "price": 100, "quantity": 3},
    ]
    assert engine.snapshot() == ([], [])

    # A partially consumed maker keeps resting with its reduced quantity.
    engine, _ = book_after([add("e4", "s3", "SELL", "LIMIT", 4, 100)])
    _, _, _, trades = engine.handle_line(
        add("e5", "b2", "BUY", "LIMIT", 3, 100, time_in_force="FOK")
    )
    assert [t["quantity"] for t in trades] == [3]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_limit_fok_failure_changes_nothing():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    line = add("e2", "b1", "BUY", "LIMIT", 5, 100, time_in_force="FOK")
    _, result, reason, trades = engine.handle_line(line)
    assert (result, reason) == ("UNFILLED_CANCELLED", None)
    assert trades == []
    # Snapshot equals the pre-event book.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]

    # Enough size exists but outside the limit price: still nothing happens.
    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 5, 101)])
    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "LIMIT", 5, 100, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 5}]

    # No trade identifiers were consumed by either failed FOK.
    _, _, _, trades = engine.handle_line(add("e5", "b3", "BUY", "LIMIT", 1, 101))
    assert [t["trade_id"] for t in trades] == [1]


def test_failed_fok_occupies_event_and_order_ids():
    engine = Engine()
    line = add("e1", "b1", "BUY", "LIMIT", 3, 100, time_in_force="FOK")
    assert engine.handle_line(line)[1] == "UNFILLED_CANCELLED"
    assert engine.handle_line(line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")
    _, result, reason, _ = engine.handle_line(
        add("e2", "b1", "SELL", "LIMIT", 1, 100, time_in_force="FOK")
    )
    assert (result, reason) == ("REJECTED", "DUPLICATE_ORDER_ID")
    # The schema-valid duplicate-order event also consumed its event id.
    assert engine.handle_line(add("e2", "o2", "SELL", "LIMIT", 1, 100))[2] == (
        "DUPLICATE_EVENT_ID"
    )


def test_market_ioc_and_fok():
    # Explicit IOC matches the historical market semantics.
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "MARKET", 5, time_in_force="IOC")
    )
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [2]
    assert engine.snapshot() == ([], [])

    # FOK market order succeeds only when the whole quantity is available.
    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "MARKET", 2, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert len(trades) == 1

    engine, _ = book_after([add("e5", "s3", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e6", "b3", "BUY", "MARKET", 3, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]

    # Empty book FOK market order spends no trade ids.
    engine = Engine()
    _, result, _, trades = engine.handle_line(
        add("e7", "b4", "SELL", "MARKET", 1, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []


@pytest.mark.parametrize("tif", ["GTC", "IOC", "FOK"])
def test_limit_explicit_tif_requires_valid_price(tif):
    engine = Engine()
    line = json.dumps(
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "time_in_force": tif}
    )
    assert engine.handle_line(line)[1:3] == ("REJECTED", "INVALID_SCHEMA")


def test_time_in_force_schema_rejections():
    engine = Engine()
    for payload in [
        # MARKET cannot be GTC.
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "MARKET", "quantity": 1, "time_in_force": "GTC"},
        # Unknown string value.
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100, "time_in_force": "DAY"},
        # Non-string values.
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100, "time_in_force": 123},
        {"event_id": "e4", "type": "ADD", "order_id": "o4", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100, "time_in_force": None},
    ]:
        assert engine.handle_line(json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), payload

    # Explicit LIMIT GTC with a valid price is accepted and behaves as before.
    eid, result, reason, _ = engine.handle_line(
        add("e5", "o5", "BUY", "LIMIT", 1, 100, time_in_force="GTC")
    )
    assert (eid, result, reason) == ("e5", "RESTING", None)


def test_cancel_results():
    engine, _ = book_after([add("e1", "b1", "BUY", "LIMIT", 3, 100)])
    _, result, reason, trades = engine.handle_line(cancel("e2", "b1"))
    assert (result, reason, trades) == ("CANCELLED", None, [])
    assert engine.snapshot() == ([], [])

    for line in (cancel("e3", "b1"), cancel("e4", "missing")):
        _, result, reason, _ = engine.handle_line(line)
        assert result == "REJECTED"
        assert reason == "UNKNOWN_ORDER"

    engine, _ = book_after([add("e5", "s1", "SELL", "LIMIT", 1, 100)])
    engine.handle_line(add("e6", "b2", "BUY", "LIMIT", 1, 100))
    _, result, reason, _ = engine.handle_line(cancel("e7", "s1"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")


def test_cancel_does_not_generate_trades_or_trade_ids():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 2, 101),
        ]
    )
    engine.handle_line(cancel("e3", "s2"))
    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "LIMIT", 2, 101))
    assert [t["trade_id"] for t in trades] == [1]
    assert [t["maker_order_id"] for t in trades] == ["s1"]


@pytest.mark.parametrize(
    "line,reason",
    [
        ("{", "INVALID_JSON"),
        ("not json", "INVALID_JSON"),
        ('{"event_id": "e1", ', "INVALID_JSON"),
        ("123", "INVALID_SCHEMA"),
        ("[1, 2, 3]", "INVALID_SCHEMA"),
        ('"a string"', "INVALID_SCHEMA"),
        ("null", "INVALID_SCHEMA"),
        ("true", "INVALID_SCHEMA"),
    ],
)
def test_rejected_json_variants(line, reason):
    engine = Engine()
    eid, result, got_reason, trades = engine.handle_line(line)
    assert (result, got_reason) == ("REJECTED", reason)
    assert trades == []


def test_invalid_json_vs_invalid_schema_distinct():
    engine = Engine()
    assert engine.handle_line("{")[1:3] == ("REJECTED", "INVALID_JSON")
    for line in [
        "{}",
        '{"event_id": "e1"}',
        '{"event_id": "e1", "type": "ADD"}',
        json.dumps(
            {"event_id": "e2", "type": "ADD", "order_id": "o1", "side": "HOLD",
             "order_type": "LIMIT", "quantity": 1, "price": 100}
        ),
        json.dumps(
            {"event_id": "e3", "type": "ADD", "order_id": "o2", "side": "BUY",
             "order_type": "LIMIT", "quantity": 1.5, "price": 100}
        ),
        json.dumps(
            {"event_id": "e4", "type": "ADD", "order_id": "o3", "side": "BUY",
             "order_type": "LIMIT", "quantity": 0, "price": 100}
        ),
        json.dumps(
            {"event_id": "e5", "type": "ADD", "order_id": "o4", "side": "BUY",
             "order_type": "LIMIT", "quantity": 1, "price": 0}
        ),
        json.dumps(
            {"event_id": "e6", "type": "ADD", "order_id": "o5", "side": "BUY",
             "order_type": "MARKET", "quantity": 1, "price": 100}
        ),
        json.dumps(
            {"event_id": "e7", "type": "ADD", "order_id": "o6", "side": "BUY",
             "order_type": "LIMIT", "quantity": 1}
        ),
        json.dumps(
            {"event_id": "e8", "type": "ADD", "order_id": "o7", "side": "BUY",
             "order_type": "MARKET", "quantity": 1, "extra": 1}
        ),
        json.dumps({"event_id": "e9", "type": "CANCEL"}),
        json.dumps({"event_id": 9, "type": "CANCEL", "order_id": "o8"}),
        json.dumps({"event_id": "e10", "type": "BOGUS"}),
    ]:
        assert engine.handle_line(line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), line


def test_market_order_allows_explicit_null_price():
    engine = Engine()
    line = json.dumps(
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "MARKET", "quantity": 1, "price": None}
    )
    eid, result, reason, _ = engine.handle_line(line)
    assert (eid, result, reason) == ("e1", "UNFILLED_CANCELLED", None)


def test_duplicate_event_and_order_ids():
    engine = Engine()
    line = add("e1", "o1", "BUY", "LIMIT", 1, 100)
    assert engine.handle_line(line)[1] == "RESTING"
    assert engine.handle_line(line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")

    dup_order = add("e2", "o1", "SELL", "LIMIT", 1, 100)
    eid, result, reason, _ = engine.handle_line(dup_order)
    assert (eid, result, reason) == ("e2", "REJECTED", "DUPLICATE_ORDER_ID")
    # The well-formed duplicate-order event still consumed its event id.
    assert engine.handle_line(add("e2", "o2", "SELL", "LIMIT", 1, 100))[2] == "DUPLICATE_EVENT_ID"


def test_event_id_is_echoed_when_obtainable():
    engine = Engine()
    assert engine.handle_line('{"event_id": "e9", "type": "BOGUS"}')[0] == "e9"
    assert engine.handle_line('{"event_id": 7, "type": "BOGUS"}')[0] is None
    assert engine.handle_line("{")[0] is None


def test_rejection_keeps_book_trade_ids_and_priority():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 2, 100),
        ]
    )
    before = engine.snapshot()
    # Bad JSON, duplicate event and duplicate order must not mutate anything.
    engine.handle_line("{")
    engine.handle_line(add("e1", "oX", "BUY", "LIMIT", 2, 100))
    engine.handle_line(add("e3", "s1", "BUY", "LIMIT", 2, 100))
    assert engine.snapshot() == before

    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "LIMIT", 2, 100))
    assert [t["trade_id"] for t in trades] == [1]
    assert [t["maker_order_id"] for t in trades] == ["s1"]


def run_replay(text: str):
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = replay.replay(stdin, stdout, stderr)
    stdout.flush()
    return code, stdout.buffer.getvalue().decode("utf-8"), stderr.getvalue()


def test_replay_end_to_end_shape_and_byte_determinism():
    stream = "\n".join(
        [
            "",
            add("e1", "s1", "SELL", "LIMIT", 5, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 99),
            add("e3", "b1", "BUY", "LIMIT", 4, 100),
            cancel("e4", "s1"),
            "{bad",
            add("e3", "dup", "BUY", "LIMIT", 1, 100),
            add("e5", "b2", "BUY", "MARKET", 10),
            "",
        ]
    ) + "\n"

    code, out1, err = run_replay(stream)
    assert code == 0
    assert err == ""

    records = [json.loads(line) for line in out1.splitlines()]
    assert [r["event_id"] for r in records] == ["e1", "e2", "e3", "e4", None, "e3", "e5"]
    assert [r["result"] for r in records] == [
        "RESTING",
        "RESTING",
        "FILLED",
        "CANCELLED",
        "REJECTED",
        "REJECTED",
        "UNFILLED_CANCELLED",
    ]
    assert records[2]["trades"] == [
        {"trade_id": 1, "maker_order_id": "s2", "taker_order_id": "b1",
         "price": 99, "quantity": 3},
        {"trade_id": 2, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 1},
    ]
    assert records[3]["trades"] == []
    assert records[3]["asks"] == []
    assert records[4]["reason"] == "INVALID_JSON"
    assert records[4]["trades"] == []
    assert records[5]["reason"] == "DUPLICATE_EVENT_ID"
    # Rejected event echoes the book that existed before it.
    assert records[5]["asks"] == records[4]["asks"]
    # The cancelled sell is gone, so the market order finds no liquidity.
    assert records[6]["trades"] == []
    assert records[6]["asks"] == []

    _, out2, _ = run_replay(stream)
    assert out2 == out1


def test_replay_empty_input_emits_nothing():
    code, out, err = run_replay("")
    assert (code, out, err) == (0, "", "")
    code, out, err = run_replay("\n\n")
    assert (code, out, err) == (0, "", "")


def test_replay_input_line_is_echoed_verbatim():
    raw = '{"event_id":"e1","type":"ADD","order_id":"o1","side":"BUY","order_type":"LIMIT","quantity":1,"price":100}'
    _, out, _ = run_replay(raw + "\n")
    record = json.loads(out)
    assert record["input_line"] == raw


def test_replay_invalid_utf8_is_io_error():
    stdin = io.TextIOWrapper(io.BytesIO(b"\xff\xfe"))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = replay.replay(stdin, stdout, stderr)
    assert code == 1
    assert stderr.getvalue() == "ERROR_IO\n"


def test_replay_write_failure_is_io_error():
    stdin = io.TextIOWrapper(io.BytesIO(b"{}\n"))

    class _BrokenBuffer:
        def write(self, data):
            raise OSError("broken pipe")

        def flush(self):
            raise OSError("broken pipe")

    class BrokenPipe:
        buffer = _BrokenBuffer()

    stderr = io.StringIO()
    code = replay.replay(stdin, BrokenPipe(), stderr)
    assert code == 1
    assert stderr.getvalue() == "ERROR_IO\n"


# --------------------------------------------------------------------------
# ICEBERG orders
# --------------------------------------------------------------------------


def iceberg(event_id, order_id, side, quantity, price, display_quantity, **extra):
    return add(
        event_id, order_id, side, "ICEBERG", quantity, price,
        display_quantity=display_quantity, **extra
    )


def test_iceberg_rests_showing_only_first_slice():
    engine = Engine()
    eid, result, reason, trades = engine.handle_line(
        iceberg("e1", "i1", "SELL", 10, 100, 3)
    )
    assert (eid, result, reason, trades) == ("e1", "RESTING", None, [])
    # Only the current slice is public; the reserve stays hidden.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 3}]


def test_iceberg_display_equal_quantity_is_fully_visible():
    engine = Engine()
    _, result, _, _ = engine.handle_line(iceberg("e1", "i1", "SELL", 4, 100, 4))
    assert result == "RESTING"
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 4}]


def test_iceberg_explicit_gtc_is_accepted():
    engine = Engine()
    eid, result, reason, _ = engine.handle_line(
        iceberg("e1", "i1", "BUY", 10, 100, 3, time_in_force="GTC")
    )
    assert (eid, result, reason) == ("e1", "RESTING", None)
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 3}]


def test_iceberg_taker_outcomes():
    # Aggressive iceberg fully filled before resting.
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 7, 100)])
    _, result, _, trades = engine.handle_line(iceberg("e2", "b1", "BUY", 7, 100, 2))
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [7]
    assert engine.snapshot() == ([], [])

    # Aggressive iceberg partially filled; only one slice of the rest is public.
    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 4, 100)])
    _, result, _, trades = engine.handle_line(iceberg("e4", "b2", "BUY", 10, 100, 3))
    assert result == "PARTIALLY_FILLED_RESTING"
    assert [t["quantity"] for t in trades] == [4]
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 3}]

    # The resting leftover (3 visible + 3 reserve) can be cancelled, removing
    # both; a second cancel is unknown.
    _, result, reason, _ = engine.handle_line(cancel("e5", "b2"))
    assert (result, reason) == ("CANCELLED", None)
    assert engine.snapshot()[0] == []
    _, result, reason, _ = engine.handle_line(cancel("e6", "b2"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")

    # The fully filled iceberg from the first scenario cannot be cancelled.
    engine2, _ = book_after([add("e7", "s3", "SELL", "LIMIT", 7, 100)])
    engine2.handle_line(iceberg("e8", "b3", "BUY", 7, 100, 2))
    _, result, reason, _ = engine2.handle_line(cancel("e9", "b3"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")


def test_passive_iceberg_replenishes_and_is_met_again_by_same_taker():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 4, 100))
    assert result == "FILLED"
    # Slice one is fully consumed, the next slice is published immediately and
    # the same taker consumes one more from it. Maker id and passive price are
    # reused; trade ids stay consecutive.
    assert [(t["maker_order_id"], t["price"], t["quantity"]) for t in trades] == [
        ("i1", 100, 3),
        ("i1", 100, 1),
    ]
    assert [t["trade_id"] for t in trades] == [1, 2]
    # Sold 4 of 10; the replenished slice (3) gave up one more unit, so 2 is
    # visible with the final 4 held as reserve.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]


def test_new_slice_joins_behind_existing_visible_orders():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
            add("e3", "s3", "SELL", "LIMIT", 2, 100),
        ]
    )
    # Buy exactly the first slice: it replenishes behind s2 and s3.
    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "LIMIT", 3, 100))
    assert [t["maker_order_id"] for t in trades] == ["i1"]
    # The replenished slice must wait behind s2 and s3.
    _, _, _, trades = engine.handle_line(add("e5", "b2", "BUY", "LIMIT", 5, 100))
    assert [t["maker_order_id"] for t in trades] == ["s2"]
    _, _, _, trades = engine.handle_line(add("e6", "b3", "BUY", "LIMIT", 2, 100))
    assert [t["maker_order_id"] for t in trades] == ["s3"]
    # Now the second iceberg slice is at the front.
    _, _, _, trades = engine.handle_line(add("e7", "b4", "BUY", "LIMIT", 3, 100))
    assert [t["maker_order_id"] for t in trades] == ["i1"]
    assert [t["trade_id"] for t in trades] == [4]
    # Sold two slices (3+3) of 10; the third slice is 3 visible with 1 held
    # back as a one-unit final reserve.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 3}]


def test_same_taker_meets_replenished_slice_after_other_same_price_orders():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
        ]
    )
    _, _, _, trades = engine.handle_line(add("e3", "b1", "BUY", "LIMIT", 10, 100))
    # i1 slice (3), then the ordinary order (5), then the fresh i1 slice (2).
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 3),
        ("s2", 5),
        ("i1", 2),
    ]
    assert [t["trade_id"] for t in trades] == [1, 2, 3]
    # i1 sold 5 of 10; the replenished slice has 1 visible, 4 in reserve.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_market_ioc_and_limit_consume_replenished_slices():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 6, 100, 2)])

    _, _, _, trades = engine.handle_line(add("e2", "b1", "BUY", "MARKET", 3))
    assert [t["quantity"] for t in trades] == [2, 1]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]

    _, _, _, trades = engine.handle_line(
        add("e3", "b2", "BUY", "LIMIT", 5, 100, time_in_force="IOC")
    )
    # The IOC drains the visible slice, the final reserve slice replenishes and
    # the IOC cancels only once no more public size exists.
    assert [t["quantity"] for t in trades] == [1, 2]
    assert engine.snapshot()[1] == []

    engine, _ = book_after([iceberg("e4", "i2", "BUY", 6, 100, 2)])
    _, _, _, trades = engine.handle_line(add("e5", "s1", "SELL", "MARKET", 6))
    assert [t["quantity"] for t in trades] == [2, 2, 2]
    assert engine.snapshot()[0] == []


def test_fok_precheck_counts_full_iceberg_reserve():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 2)])
    # Only 2 is visible, but the full 10 is available to a FOK taker.
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 10, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [2, 2, 2, 2, 2]
    assert len({t["maker_order_id"] for t in trades}) == 1
    assert engine.snapshot() == ([], [])

    # One more than the full reserve fails atomically.
    engine, _ = book_after([iceberg("e3", "i2", "SELL", 10, 100, 2)])
    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "LIMIT", 11, 100, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    # No replenishment, no book change: still one slice of 2 visible.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]
    # No trade identifiers were spent.
    _, _, _, trades = engine.handle_line(add("e5", "b3", "BUY", "LIMIT", 1, 100))
    assert trades[0]["trade_id"] == 1


def test_fok_against_iceberg_respects_queue_order_across_slices():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 6, 100, 2),
            add("e2", "s2", "SELL", "LIMIT", 2, 100),
        ]
    )
    _, _, _, trades = engine.handle_line(
        add("e3", "b1", "BUY", "LIMIT", 8, 100, time_in_force="FOK")
    )
    # i1 slice (2), s2 (2), then the replenished i1 slices twice.
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 2),
        ("s2", 2),
        ("i1", 2),
        ("i1", 2),
    ]


def test_fok_failure_mixes_visible_books_and_iceberg_reserve():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 99),
            iceberg("e2", "i1", "SELL", 3, 100, 1),
        ]
    )
    assert engine.snapshot()[1] == [
        {"price": 99, "quantity": 2},
        {"price": 100, "quantity": 1},
    ]
    # 2 visible + 1 visible, but reserve holds another 2: total available 5.
    _, result, _, _ = engine.handle_line(
        add("e3", "b1", "BUY", "LIMIT", 5, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert engine.snapshot() == ([], [])


def test_cancel_iceberg_removes_visible_and_reserve():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 4, 100),
        ]
    )
    _, result, reason, trades = engine.handle_line(cancel("e3", "i1"))
    assert (result, reason, trades) == ("CANCELLED", None, [])
    # Only the visible slice leaves the aggregate; the reserve was never in it.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 4}]

    # The reserve is gone as well: cancelling again is unknown, and future
    # trades never see the iceberg.
    _, result, reason, _ = engine.handle_line(cancel("e4", "i1"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")
    _, _, _, trades = engine.handle_line(add("e5", "b1", "BUY", "LIMIT", 10, 100))
    assert [t["maker_order_id"] for t in trades] == ["s2"]


def test_iceberg_schema_rejections_consume_no_ids():
    engine = Engine()
    base = {
        "event_id": "eX", "type": "ADD", "order_id": "oX", "side": "BUY",
        "order_type": "ICEBERG", "quantity": 10, "price": 100,
        "display_quantity": 3,
    }
    payloads = [
        {k: v for k, v in base.items() if k != "price"},            # missing price
        {k: v for k, v in base.items() if k != "display_quantity"},  # missing display
        {**base, "price": True},                                     # bool price
        {**base, "price": 0},                                        # non-positive price
        {**base, "price": 1.5},                                      # float price
        {**base, "display_quantity": True},                          # bool display
        {**base, "display_quantity": 0},                             # non-positive display
        {**base, "display_quantity": -2},                            # negative display
        {**base, "display_quantity": 2.0},                           # float display
        {**base, "display_quantity": 11},                            # display > quantity
        {**base, "quantity": True},                                  # bool quantity
        {**base, "quantity": 0},                                     # non-positive quantity
        {**base, "time_in_force": "IOC"},                            # IOC forbidden
        {**base, "time_in_force": "FOK"},                            # FOK forbidden
        {**base, "time_in_force": "DAY"},                            # unknown tif
        {**base, "time_in_force": None},                             # null tif
        {**base, "time_in_force": 123},                              # non-string tif
        {**base, "unknown_field": 1},                                # unknown field
    ]
    for payload in payloads:
        assert engine.handle_line(json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), payload

    # display_quantity is reserved for iceberg orders.
    assert engine.handle_line(
        add("eA", "oA", "BUY", "LIMIT", 10, 100, display_quantity=3)
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")
    assert engine.handle_line(
        add("eB", "oB", "BUY", "MARKET", 10, display_quantity=3)
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")

    # Nothing consumed an event id or an order id.
    assert engine.handle_line(
        iceberg("eX", "oX", "BUY", 10, 100, 3)
    )[1] == "RESTING"


def test_iceberg_duplicate_event_and_order_ids():
    engine = Engine()
    line = iceberg("e1", "i1", "SELL", 10, 100, 3)
    assert engine.handle_line(line)[1] == "RESTING"
    assert engine.handle_line(line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")

    _, result, reason, _ = engine.handle_line(
        iceberg("e2", "i1", "BUY", 10, 100, 3)
    )
    assert (result, reason) == ("REJECTED", "DUPLICATE_ORDER_ID")
    # The well-formed duplicate-order event still consumed its event id.
    assert engine.handle_line(
        iceberg("e2", "i2", "BUY", 10, 100, 3)
    )[2] == "DUPLICATE_EVENT_ID"


def test_iceberg_replay_is_byte_deterministic():
    stream = "\n".join(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
            add("e3", "b1", "BUY", "LIMIT", 10, 100),
            cancel("e4", "i1"),
            add("e5", "b2", "BUY", "MARKET", 20),
            iceberg("e6", "i9", "BUY", 10, 100, 3, time_in_force="IOC"),
        ]
    ) + "\n"
    _, out1, err1 = run_replay(stream)
    code, out2, err2 = run_replay(stream)
    assert code == 0
    assert (err1, err2) == ("", "")
    assert out2 == out1

    records = [json.loads(line) for line in out1.splitlines()]
    assert records[0]["asks"] == [{"price": 100, "quantity": 3}]
    assert [(t["maker_order_id"], t["quantity"]) for t in records[2]["trades"]] == [
        ("i1", 3),
        ("s2", 5),
        ("i1", 2),
    ]
    assert records[3]["result"] == "CANCELLED"
    assert records[3]["asks"] == []
    assert records[4]["result"] == "UNFILLED_CANCELLED"
    assert records[4]["asks"] == []
    assert records[5]["result"] == "REJECTED"
    assert records[5]["reason"] == "INVALID_SCHEMA"
    # A schema rejection leaves the book untouched.
    assert records[5]["asks"] == records[4]["asks"]

