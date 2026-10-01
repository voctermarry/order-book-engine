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


# --------------------------------------------------------------------------
# REPLACE events
# --------------------------------------------------------------------------


def replace(event_id, order_id, quantity, price, **extra):
    obj = {
        "event_id": event_id,
        "type": "REPLACE",
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
    }
    obj.update(extra)
    return json.dumps(obj, ensure_ascii=False)


def test_replace_resting_limit_moves_and_resizes():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    eid, result, reason, trades = engine.handle_line(replace("e2", "o1", 3, 101))
    assert (eid, result, reason, trades) == ("e2", "REPLACED", None, [])
    assert engine.snapshot() == ([{"price": 101, "quantity": 3}], [])


def test_replace_loses_priority_even_with_unchanged_parameters():
    engine, _ = book_after(
        [
            add("e1", "o1", "BUY", "LIMIT", 2, 100),
            add("e2", "o2", "BUY", "LIMIT", 2, 100),
        ]
    )
    _, result, _, _ = engine.handle_line(replace("e3", "o1", 2, 100))
    assert result == "REPLACED"
    # o1 rejoined behind o2, so o2 is matched first.
    _, _, _, trades = engine.handle_line(add("e4", "s1", "SELL", "LIMIT", 2, 100))
    assert [t["maker_order_id"] for t in trades] == ["o2"]
    _, _, _, trades = engine.handle_line(add("e5", "s2", "SELL", "LIMIT", 2, 100))
    assert [t["maker_order_id"] for t in trades] == ["o1"]


def test_replace_matches_as_taker_and_can_fill():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 5, 100),
            add("e2", "b1", "BUY", "LIMIT", 3, 99),
        ]
    )
    _, result, _, trades = engine.handle_line(replace("e3", "b1", 2, 100))
    assert result == "FILLED"
    assert trades == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 2}
    ]
    assert engine.snapshot() == ([], [{"price": 100, "quantity": 3}])


def test_replace_partially_filled_then_rests():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "b1", "BUY", "LIMIT", 1, 99),
        ]
    )
    _, result, _, trades = engine.handle_line(replace("e3", "b1", 5, 100))
    assert result == "PARTIALLY_FILLED_RESTING"
    assert [t["quantity"] for t in trades] == [2]
    assert engine.snapshot() == ([{"price": 100, "quantity": 3}], [])


def test_replace_never_trades_against_its_own_previous_state():
    # The only order in the book is the target itself; repricing across the
    # old position must not produce a self trade.
    engine, _ = book_after([add("e1", "b1", "BUY", "LIMIT", 5, 100)])
    _, result, _, trades = engine.handle_line(replace("e2", "b1", 5, 200))
    assert (result, trades) == ("REPLACED", [])
    assert engine.snapshot() == ([{"price": 200, "quantity": 5}], [])

    # Same on the ask side with an iceberg target.
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    _, result, _, trades = engine.handle_line(replace("e2", "i1", 10, 50))
    assert (result, trades) == ("REPLACED", [])
    assert engine.snapshot() == ([], [{"price": 50, "quantity": 3}])


def test_replace_quantity_is_new_remaining_total():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 4, 100),
            add("e2", "b1", "BUY", "LIMIT", 10, 100),  # fills 4, rests 6
        ]
    )
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 6}]
    # quantity is the new remaining total, unrelated to the 4 already filled.
    _, result, _, _ = engine.handle_line(replace("e3", "b1", 2, 100))
    assert result == "REPLACED"
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 2}]


def test_replace_unknown_or_finished_target_is_unknown_order():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "b1", "BUY", "LIMIT", 2, 100),  # fills s1
        ]
    )
    for line in (
        replace("e3", "missing", 1, 100),   # never existed
        replace("e4", "s1", 1, 100),        # filled maker
        replace("e5", "b1", 1, 100),        # filled taker
    ):
        _, result, reason, trades = engine.handle_line(line)
        assert (result, reason, trades) == ("REJECTED", "UNKNOWN_ORDER", [])

    # A cancelled order cannot be replaced either.
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 1, 100)])
    engine.handle_line(cancel("e2", "o1"))
    assert engine.handle_line(replace("e3", "o1", 1, 100))[1:3] == (
        "REJECTED", "UNKNOWN_ORDER"
    )

    # A valid event with an unknown target still occupies its event id.
    assert engine.handle_line(replace("e3", "o1", 1, 100))[1:3] == (
        "REJECTED", "DUPLICATE_EVENT_ID"
    )


def test_replace_rejections_change_nothing():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 2, 100),
        ]
    )
    before = engine.snapshot()
    engine.handle_line(replace("e3", "missing", 1, 100))     # UNKNOWN_ORDER
    engine.handle_line(replace("e4", "s1", 0, 100))          # bad quantity
    engine.handle_line(replace("e5", "s1", 1, 100, side="BUY"))  # extra field
    assert engine.snapshot() == before

    _, _, _, trades = engine.handle_line(add("e6", "b1", "BUY", "LIMIT", 2, 100))
    assert [t["trade_id"] for t in trades] == [1]
    assert [t["maker_order_id"] for t in trades] == ["s1"]


def test_replace_schema_rejections_consume_no_event_id():
    engine, _ = book_after([add("e0", "o1", "BUY", "LIMIT", 5, 100)])
    base = {"event_id": "eR", "type": "REPLACE", "order_id": "o1",
            "quantity": 3, "price": 100}
    payloads = [
        {k: v for k, v in base.items() if k != "quantity"},   # missing quantity
        {k: v for k, v in base.items() if k != "price"},      # missing price
        {k: v for k, v in base.items() if k != "order_id"},   # missing order_id
        {**base, "side": "BUY"},                              # inherited field
        {**base, "order_type": "LIMIT"},                      # inherited field
        {**base, "time_in_force": "GTC"},                     # always GTC
        {**base, "unknown_field": 1},                         # unknown field
        {**base, "quantity": True},                           # bool quantity
        {**base, "quantity": 0},                              # non-positive
        {**base, "quantity": 1.5},                            # float quantity
        {**base, "price": True},                              # bool price
        {**base, "price": 0},                                 # non-positive
        {**base, "price": 2.5},                               # float price
        {**base, "display_quantity": 2},                      # limit target
        {**base, "order_id": 7},                              # non-string id
    ]
    for payload in payloads:
        assert engine.handle_line(json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), payload
        # Structural errors never occupy the event id.
        assert engine.handle_line(json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), payload

    # The book and the target are untouched; the event id is still free.
    assert engine.snapshot() == ([{"price": 100, "quantity": 5}], [])
    _, result, reason, _ = engine.handle_line(json.dumps(base))
    assert (result, reason) == ("REPLACED", None)
    assert engine.snapshot() == ([{"price": 100, "quantity": 3}], [])


def test_replace_duplicate_event_id():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    line = replace("e2", "o1", 3, 100)
    assert engine.handle_line(line)[1] == "REPLACED"
    assert engine.handle_line(line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")


def test_replace_keeps_order_id_occupied():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    # Replacing does not trip DUPLICATE_ORDER_ID itself...
    assert engine.handle_line(replace("e2", "o1", 3, 100))[1] == "REPLACED"
    # ...but the id stays occupied for later ADDs, even after the replacement
    # is fully filled.
    engine.handle_line(add("e3", "s1", "SELL", "LIMIT", 3, 100))
    assert engine.handle_line(add("e4", "o1", "SELL", "LIMIT", 1, 100))[1:3] == (
        "REJECTED", "DUPLICATE_ORDER_ID"
    )


def test_replace_iceberg_defaults_to_original_peak_and_rests_one_slice():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    _, result, _, trades = engine.handle_line(replace("e2", "i1", 8, 101))
    assert (result, trades) == ("REPLACED", [])
    # Omitted display_quantity keeps the original peak of 3.
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 3}]
    # The reserve follows the iceberg through replenishment.
    _, _, _, trades = engine.handle_line(add("e3", "b1", "BUY", "LIMIT", 5, 101))
    assert [t["quantity"] for t in trades] == [3, 2]
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 1}]


def test_replace_iceberg_with_new_display_quantity():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    _, result, _, _ = engine.handle_line(
        replace("e2", "i1", 9, 100, display_quantity=4)
    )
    assert result == "REPLACED"
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 4}]

    # display_quantity must be a positive integer not above the new quantity.
    for bad in (0, -1, True, 2.0, 10):
        assert engine.handle_line(
            replace("eX", "i1", 9, 100, display_quantity=bad)
        )[1:3] == ("REJECTED", "INVALID_SCHEMA"), bad


def test_replace_iceberg_visible_is_capped_by_remaining():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    _, result, _, _ = engine.handle_line(replace("e2", "i1", 2, 100))
    assert result == "REPLACED"
    # min(display_quantity, remaining): only 2 of the peak 3 is shown.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]


def test_replace_iceberg_replenishes_behind_same_price_orders():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            iceberg("e2", "i1", "SELL", 6, 100, 2),
        ]
    )
    # Move the iceberg onto s1's level: it joins behind s1.
    _, result, _, _ = engine.handle_line(replace("e3", "i1", 6, 100))
    assert result == "REPLACED"
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 4}]
    _, _, _, trades = engine.handle_line(add("e4", "b1", "BUY", "LIMIT", 6, 100))
    # s1 first, then the iceberg slices replenishing at the queue tail.
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("s1", 2),
        ("i1", 2),
        ("i1", 2),
    ]


def test_replace_iceberg_fully_filled_as_taker():
    engine, _ = book_after(
        [
            add("e1", "b1", "BUY", "LIMIT", 8, 100),
            iceberg("e2", "i1", "SELL", 10, 101, 3),
        ]
    )
    _, result, _, trades = engine.handle_line(replace("e3", "i1", 8, 100))
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [8]
    assert engine.snapshot() == ([], [])


def test_replace_replay_is_byte_deterministic():
    stream = "\n".join(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
            add("e3", "b1", "BUY", "LIMIT", 4, 99),
            replace("e4", "b1", 6, 100),
            replace("e5", "i1", 8, 100, display_quantity=2),
            replace("e6", "s2", 1, 100, display_quantity=1),
            replace("e7", "gone", 1, 100),
            replace("e4", "b1", 1, 100),
        ]
    ) + "\n"
    code, out1, err1 = run_replay(stream)
    _, out2, err2 = run_replay(stream)
    assert code == 0
    assert (err1, err2) == ("", "")
    assert out2 == out1

    records = [json.loads(line) for line in out1.splitlines()]
    assert [r["result"] for r in records] == [
        "RESTING", "RESTING", "RESTING", "FILLED",
        "REPLACED", "REJECTED", "REJECTED", "REJECTED",
    ]
    # b1 replaced to 6@100 takes the iceberg slice (3) and s2 (3) in full.
    assert [(t["maker_order_id"], t["quantity"]) for t in records[3]["trades"]] == [
        ("i1", 3),
        ("s2", 3),
    ]
    # i1 re-rested at 100 showing its new slice of 2 behind s2's leftover 2.
    assert records[4]["asks"] == [{"price": 100, "quantity": 4}]
    assert records[5]["reason"] == "INVALID_SCHEMA"
    assert records[6]["reason"] == "UNKNOWN_ORDER"
    assert records[7]["reason"] == "DUPLICATE_EVENT_ID"


# --------------------------------------------------------------------------
# Self-trade prevention (account_id)
# --------------------------------------------------------------------------


def stp(line_engine, line):
    return line_engine.handle_line_full(line)


def test_account_id_is_accepted_on_every_add_variant():
    engine = Engine()
    assert stp(engine, add("e1", "o1", "BUY", "LIMIT", 1, 100, account_id="A"))[1] == "RESTING"
    assert stp(engine, add("e2", "o2", "BUY", "MARKET", 1, account_id="A"))[1] == (
        "UNFILLED_CANCELLED"
    )
    assert stp(
        engine, iceberg("e3", "o3", "SELL", 4, 101, 2, account_id="A")
    )[1] == "RESTING"
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 2}]


def test_account_id_schema_rejections_consume_no_ids():
    engine = Engine()
    base = {"event_id": "eX", "type": "ADD", "order_id": "oX", "side": "BUY",
            "order_type": "LIMIT", "quantity": 1, "price": 100}
    for bad in ("", 0, 7, True, 1.5, None, ["A"], {"x": 1}):
        payload = {**base, "account_id": bad}
        assert stp(engine, json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), bad
        # Structural rejection leaves both ids free.
        assert stp(engine, json.dumps(payload))[1:3] == (
            "REJECTED", "INVALID_SCHEMA"
        ), bad

    # account_id belongs to ADD only; CANCEL and REPLACE must not carry it.
    engine, _ = book_after([add("e0", "o0", "BUY", "LIMIT", 5, 100, account_id="A")])
    assert engine.handle_line(
        json.dumps({"event_id": "e1", "type": "CANCEL", "order_id": "o0",
                    "account_id": "A"})
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")
    assert stp(
        engine,
        json.dumps({"event_id": "e2", "type": "REPLACE", "order_id": "o0",
                    "quantity": 3, "price": 100, "account_id": "A"}),
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")

    # Nothing was consumed: the same ADD event id/order id now succeeds.
    assert stp(engine, add("eX", "oX", "BUY", "LIMIT", 1, 100, account_id="A"))[1] == (
        "RESTING"
    )
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 6}]


def test_self_trade_fires_only_when_both_sides_share_the_account():
    # Identical accounts: blocked, no trade, book untouched.
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100, account_id="A")])
    _, result, reason, trades, info = stp(
        engine, add("e2", "b1", "BUY", "LIMIT", 3, 100, account_id="A")
    )
    assert (result, reason) == ("SELF_TRADE_PREVENTED", None)
    assert trades == []
    assert info == {
        "maker_order_id": "s1",
        "taker_order_id": "b1",
        "cancelled_quantity": 3,
    }
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 5}]

    # Different accounts trade normally.
    engine, _ = book_after([add("e3", "s2", "SELL", "LIMIT", 5, 100, account_id="A")])
    assert stp(engine, add("e4", "b2", "BUY", "LIMIT", 3, 100, account_id="B"))[1] == (
        "FILLED"
    )

    # Maker tagged, taker untagged: trade.
    engine, _ = book_after([add("e5", "s3", "SELL", "LIMIT", 5, 100, account_id="A")])
    assert stp(engine, add("e6", "b3", "BUY", "LIMIT", 3, 100))[1] == "FILLED"

    # Taker tagged, maker untagged: trade.
    engine, _ = book_after([add("e7", "s4", "SELL", "LIMIT", 5, 100)])
    assert stp(engine, add("e8", "b4", "BUY", "LIMIT", 3, 100, account_id="A"))[1] == (
        "FILLED"
    )


def test_self_trade_prevention_works_on_the_sell_side():
    engine, _ = book_after([add("e1", "b1", "BUY", "LIMIT", 5, 100, account_id="A")])
    _, result, _, trades, info = stp(
        engine, add("e2", "s1", "SELL", "LIMIT", 3, 100, account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info == {
        "maker_order_id": "b1",
        "taker_order_id": "s1",
        "cancelled_quantity": 3,
    }
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 5}]


def test_self_trade_block_does_not_skip_or_change_the_passive_book():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
            add("e2", "s2", "SELL", "LIMIT", 5, 100, account_id="B"),
            add("e3", "s3", "SELL", "LIMIT", 5, 101, account_id="B"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e4", "b1", "BUY", "LIMIT", 8, 101, account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    # s1 keeps its queue position and full size; liquidity behind is untouched.
    assert info == {"maker_order_id": "s1", "taker_order_id": "b1",
                    "cancelled_quantity": 8}
    assert engine.snapshot()[1] == [
        {"price": 100, "quantity": 10},
        {"price": 101, "quantity": 5},
    ]
    # A later taker meets s1 first, proving its queue position is unchanged.
    _, _, _, trades, _ = stp(engine, add("e5", "b2", "BUY", "LIMIT", 6, 101))
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("s1", 5),
        ("s2", 1),
    ]


def test_partial_fills_then_self_trade_block_keep_trades_and_trade_ids():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100, account_id="B"),
            add("e2", "s2", "SELL", "LIMIT", 5, 100, account_id="A"),
            add("e3", "s3", "SELL", "LIMIT", 5, 101, account_id="B"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e4", "b1", "BUY", "LIMIT", 6, 101, account_id="A")
    )
    assert result == "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"
    assert trades == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 2}
    ]
    assert info == {"maker_order_id": "s2", "taker_order_id": "b1",
                    "cancelled_quantity": 4}
    # s2 and the level behind it were not touched by the block.
    assert engine.snapshot()[1] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 5},
    ]
    # The earlier trade occupies trade id 1; the next event starts at 2.
    _, _, _, trades, _ = stp(engine, add("e5", "b2", "BUY", "LIMIT", 1, 100))
    assert trades[0]["trade_id"] == 2
    assert trades[0]["maker_order_id"] == "s2"


def test_self_trade_prevented_taker_cannot_be_cancelled_replaced_or_readded():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100, account_id="A")])
    assert stp(
        engine, add("e2", "b1", "BUY", "LIMIT", 3, 100, account_id="A")
    )[1] == "SELF_TRADE_PREVENTED"

    assert engine.handle_line(cancel("e3", "b1"))[1:3] == ("REJECTED", "UNKNOWN_ORDER")
    assert stp(engine, replace("e4", "b1", 3, 100))[1:3] == (
        "REJECTED", "UNKNOWN_ORDER"
    )
    assert stp(engine, add("e5", "b1", "BUY", "LIMIT", 3, 100, account_id="A"))[1:3] == (
        "REJECTED", "DUPLICATE_ORDER_ID"
    )
    # The event id stays occupied as well.
    assert stp(engine, add("e2", "o9", "BUY", "LIMIT", 3, 100, account_id="A"))[1:3] == (
        "REJECTED", "DUPLICATE_EVENT_ID"
    )


def test_ioc_self_trade_prevention():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100, account_id="A")])
    _, result, _, trades, info = stp(
        engine, add("e2", "b1", "BUY", "LIMIT", 3, 100,
                    time_in_force="IOC", account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info["cancelled_quantity"] == 3
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 5}]

    # Partial IOC fill before the block reports the partial outcome.
    engine, _ = book_after(
        [
            add("e3", "s2", "SELL", "LIMIT", 2, 100, account_id="B"),
            add("e4", "s3", "SELL", "LIMIT", 5, 100, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e5", "b2", "BUY", "LIMIT", 5, 100,
                    time_in_force="IOC", account_id="A")
    )
    assert result == "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"
    assert [t["quantity"] for t in trades] == [2]
    assert info == {"maker_order_id": "s3", "taker_order_id": "b2",
                    "cancelled_quantity": 3}


def test_iceberg_self_trade_block_preserves_slice_and_reserve():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3, account_id="A")])
    _, result, _, trades, info = stp(
        engine, add("e2", "b1", "BUY", "LIMIT", 2, 100, account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info == {"maker_order_id": "i1", "taker_order_id": "b1",
                    "cancelled_quantity": 2}
    # The public slice is exactly as before the event.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 3}]
    # A later taker meets the untouched first slice, then replenishment.
    _, _, _, trades, _ = stp(engine, add("e3", "b2", "BUY", "LIMIT", 4, 100))
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 3),
        ("i1", 1),
    ]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]


def test_fok_self_trade_prevention_is_atomic():
    # Same-account order reached before the full quantity can be assembled.
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 99, account_id="B"),
            add("e2", "s2", "SELL", "LIMIT", 5, 100, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e3", "b1", "BUY", "LIMIT", 6, 100,
                    time_in_force="FOK", account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info == {"maker_order_id": "s2", "taker_order_id": "b1",
                    "cancelled_quantity": 6}
    # Nothing traded, nothing changed; no trade ids spent.
    assert engine.snapshot()[1] == [
        {"price": 99, "quantity": 2},
        {"price": 100, "quantity": 5},
    ]
    _, _, _, trades, _ = stp(engine, add("e4", "b2", "BUY", "LIMIT", 1, 99))
    assert trades[0]["trade_id"] == 1

    # The cancelled FOK still occupies both ids.
    assert stp(engine, add("e5", "b1", "BUY", "LIMIT", 1, 100, account_id="A"))[1:3] == (
        "REJECTED", "DUPLICATE_ORDER_ID"
    )


def test_fok_self_trade_ordering_through_iceberg_replenishment():
    # Slice 2 is public; s1 queues ahead of the replenished slice.
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 2),
            add("e2", "s1", "SELL", "LIMIT", 2, 100, account_id="A"),
        ]
    )
    # 2 fits in the first slice: filled before the same-account order.
    assert stp(
        engine, add("e3", "b1", "BUY", "LIMIT", 2, 100,
                    time_in_force="FOK", account_id="A")
    )[1] == "FILLED"

    # A 4-lot would need s1 (same account) before the replenished slice: blocked
    # atomically with no trades and an unchanged book.
    engine, _ = book_after(
        [
            iceberg("e4", "i2", "SELL", 10, 100, 2),
            add("e5", "s2", "SELL", "LIMIT", 2, 100, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e6", "b2", "BUY", "LIMIT", 4, 100,
                    time_in_force="FOK", account_id="A")
    )
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info["cancelled_quantity"] == 4
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 4}]


def test_fok_self_trade_prevention_vs_unfilled_cancelled():
    # Same-account liquidity outside the limit is never reached: shortfall wins.
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 1, 100, account_id="B"),
            add("e2", "s2", "SELL", "LIMIT", 10, 101, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e3", "b1", "BUY", "LIMIT", 6, 100,
                    time_in_force="FOK", account_id="A")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert info is None
    assert engine.snapshot()[1] == [
        {"price": 100, "quantity": 1},
        {"price": 101, "quantity": 10},
    ]

    # Enough fillable liquidity before the same-account order: still FILLED.
    engine, _ = book_after(
        [
            add("e4", "s3", "SELL", "LIMIT", 5, 100, account_id="B"),
            add("e5", "s4", "SELL", "LIMIT", 5, 101, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(
        engine, add("e6", "b2", "BUY", "LIMIT", 5, 101,
                    time_in_force="FOK", account_id="A")
    )
    assert result == "FILLED"
    assert info is None
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("s3", 5),
    ]
    # The same-account order at 101 never participated and rests untouched.
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 5}]


def test_replace_inherits_target_account_for_self_trade_prevention():
    engine, _ = book_after(
        [
            add("e1", "b1", "BUY", "LIMIT", 2, 99, account_id="A"),
            add("e2", "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(engine, replace("e3", "b1", 5, 100))
    assert result == "SELF_TRADE_PREVENTED"
    assert trades == []
    assert info == {"maker_order_id": "s1", "taker_order_id": "b1",
                    "cancelled_quantity": 5}
    # The old bid remainder was removed and is not restored; the ask is intact.
    assert engine.snapshot() == ([], [{"price": 100, "quantity": 5}])
    # The replaced order is finished: no cancel or further replace possible.
    assert engine.handle_line(cancel("e4", "b1"))[1:3] == ("REJECTED", "UNKNOWN_ORDER")
    assert stp(engine, replace("e5", "b1", 5, 100))[1:3] == (
        "REJECTED", "UNKNOWN_ORDER"
    )


def test_replace_partial_self_trade_prevention_does_not_restore_old_remainder():
    engine, _ = book_after(
        [
            add("e1", "b1", "BUY", "LIMIT", 2, 99, account_id="A"),
            add("e2", "s1", "SELL", "LIMIT", 2, 100, account_id="B"),
            add("e3", "s2", "SELL", "LIMIT", 5, 100, account_id="A"),
        ]
    )
    _, result, _, trades, info = stp(engine, replace("e4", "b1", 5, 100))
    assert result == "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [("s1", 2)]
    assert info == {"maker_order_id": "s2", "taker_order_id": "b1",
                    "cancelled_quantity": 3}
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 5}]
    assert engine.snapshot()[0] == []


def test_replace_without_account_does_not_inherit_protection():
    # Neither side carries an account, so an aggressive replacement trades.
    engine, _ = book_after(
        [
            add("e1", "b1", "BUY", "LIMIT", 2, 99),
            add("e2", "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        ]
    )
    assert stp(engine, replace("e3", "b1", 5, 100))[1] == "FILLED"


def test_handle_line_keeps_four_tuple_shape():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 5, 100, account_id="A")])
    eid, result, reason, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 3, 100, account_id="A")
    )
    assert (eid, result, reason, trades) == ("e2", "SELF_TRADE_PREVENTED", None, [])


def test_replay_self_trade_prevention_shape_and_byte_determinism():
    stream = "\n".join(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100, account_id="B"),
            add("e2", "s2", "SELL", "LIMIT", 5, 100, account_id="A"),
            add("e3", "b1", "BUY", "LIMIT", 6, 100, account_id="A"),
            add("e4", "b2", "BUY", "LIMIT", 1, 100, account_id="A"),
        ]
    ) + "\n"
    code, out1, err = run_replay(stream)
    assert code == 0
    assert err == ""
    _, out2, _ = run_replay(stream)
    assert out2 == out1

    lines = out1.splitlines()
    # Only the two prevented results carry the descriptor; others never do.
    assert [json.loads(line).get("self_trade_prevention") is not None
            for line in lines] == [False, False, True, True]
    records = [json.loads(line) for line in lines]
    assert records[2]["result"] == "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"
    assert records[2]["self_trade_prevention"] == {
        "maker_order_id": "s2", "taker_order_id": "b1", "cancelled_quantity": 4
    }
    assert records[3]["result"] == "SELF_TRADE_PREVENTED"
    assert records[3]["self_trade_prevention"] == {
        "maker_order_id": "s2", "taker_order_id": "b2", "cancelled_quantity": 1
    }
    # The descriptor is serialized between result/reason and trades.
    stp_line = lines[2]
    assert (
        stp_line.index('"self_trade_prevention"')
        < stp_line.index('"trades"')
    )


def test_replay_without_account_id_is_byte_for_byte_compatible():
    # Streams that never mention account_id must serialize exactly as before.
    first_input = add("e1", "s1", "SELL", "LIMIT", 5, 100)
    stream = "\n".join(
        [
            first_input,
            add("e2", "b1", "BUY", "LIMIT", 3, 100),
            cancel("e3", "s1"),
        ]
    ) + "\n"
    _, out, _ = run_replay(stream)
    records = [json.loads(line) for line in out.splitlines()]
    for record in records:
        assert "self_trade_prevention" not in record

    # The first record matches the historical compact serialization exactly.
    expected_first = (
        '{"input_line":'
        + json.dumps(first_input, ensure_ascii=False)
        + ',"event_id":"e1","result":"RESTING","trades":[],'
        '"bids":[],"asks":[{"price":100,"quantity":5}]}'
    )
    assert out.splitlines()[0] == expected_first


# --------------------------------------------------------------------------
# EXECUTION_REPORT events
# --------------------------------------------------------------------------


def report(event_id, order_id, benchmark_price):
    return json.dumps(
        {
            "event_id": event_id,
            "type": "EXECUTION_REPORT",
            "order_id": order_id,
            "benchmark_price": benchmark_price,
        }
    )


def detailed(engine, line):
    return engine.handle_line_detailed(line)


def test_execution_report_resting_limit_with_no_fills():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    eid, result, reason, trades, stp, analysis = detailed(
        engine, report("q1", "o1", 102)
    )
    assert (eid, result, reason, trades, stp) == ("q1", "REPORTED", None, [], None)
    assert analysis == {
        "side": "BUY",
        "current_status": "RESTING",
        "open_quantity": 5,
        "filled_quantity": 0,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
        "trade_attribution": [],
    }
    # The query leaves the book untouched.
    assert engine.snapshot() == ([{"price": 100, "quantity": 5}], [])


def test_execution_report_aggregates_maker_and_taker_fills():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 5, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 101),
        ]
    )
    detailed(engine, add("e3", "b1", "BUY", "LIMIT", 10, 101))  # fills s1 and s2, rests 2

    _, _, _, _, _, seller = detailed(engine, report("q1", "s1", 102))
    assert seller["current_status"] == "FILLED"
    assert seller["open_quantity"] == 0
    assert seller["filled_quantity"] == 5
    assert seller["executed_notional"] == 500
    assert seller["vwap"] == {"numerator": 500, "denominator": 5}
    # A sell at 100 against a 102 benchmark earns 2 per unit more: +10.
    assert seller["slippage_notional"] == 10
    assert seller["trade_attribution"] == [
        {
            "trade_id": 1,
            "role": "MAKER",
            "counterparty_order_id": "b1",
            "event_id": "e3",
            "price": 100,
            "quantity": 5,
        }
    ]

    _, _, _, _, _, buyer = detailed(engine, report("q2", "b1", 100))
    assert buyer["current_status"] == "RESTING"
    assert buyer["open_quantity"] == 2
    assert buyer["filled_quantity"] == 8
    assert buyer["executed_notional"] == 5 * 100 + 3 * 101
    assert buyer["vwap"] == {"numerator": 803, "denominator": 8}
    # A buy paying 803 against a 100 benchmark for 8 units costs 3 more.
    assert buyer["slippage_notional"] == 3
    assert [
        (t["trade_id"], t["role"], t["counterparty_order_id"], t["event_id"],
         t["price"], t["quantity"])
        for t in buyer["trade_attribution"]
    ] == [
        (1, "TAKER", "s1", "e3", 100, 5),
        (2, "TAKER", "s2", "e3", 101, 3),
    ]


def test_execution_report_negative_slippage_means_improvement():
    # Buying below the benchmark is an improvement (negative slippage).
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 4, 99)])
    detailed(engine, add("e2", "b1", "BUY", "LIMIT", 4, 99))
    _, _, _, _, _, buyer = detailed(engine, report("q1", "b1", 100))
    assert buyer["slippage_notional"] == 4 * 99 - 100 * 4

    # Selling above the benchmark is an improvement (negative slippage).
    engine, _ = book_after([add("e3", "b2", "BUY", "LIMIT", 4, 101)])
    detailed(engine, add("e4", "s2", "SELL", "LIMIT", 4, 101))
    _, _, _, _, _, seller = detailed(engine, report("q2", "s2", 100))
    assert seller["slippage_notional"] == 100 * 4 - 4 * 101


def test_execution_report_attributes_iceberg_slices_under_one_order():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
        ]
    )
    detailed(engine, add("e3", "b1", "BUY", "LIMIT", 10, 100))
    _, _, _, _, _, analysis = detailed(engine, report("q1", "i1", 100))
    assert analysis["current_status"] == "RESTING"
    # 5 sold (3 + 2); the remaining 5 includes the visible slice and reserve.
    assert analysis["open_quantity"] == 5
    assert analysis["filled_quantity"] == 5
    assert analysis["executed_notional"] == 500
    assert [
        (t["trade_id"], t["role"], t["counterparty_order_id"], t["quantity"])
        for t in analysis["trade_attribution"]
    ] == [
        (1, "MAKER", "b1", 3),
        (3, "MAKER", "b1", 2),
    ]


def test_execution_report_aggregates_fills_before_and_after_replace():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "b1", "BUY", "LIMIT", 5, 100),  # partial fill, rests 3
            add("e3", "s2", "SELL", "LIMIT", 4, 101),
        ]
    )
    detailed(engine, replace("e4", "b1", 4, 101))  # fills 4 as taker
    _, _, _, _, _, analysis = detailed(engine, report("q1", "b1", 100))
    assert analysis["current_status"] == "FILLED"
    assert analysis["filled_quantity"] == 6
    assert analysis["executed_notional"] == 2 * 100 + 4 * 101
    assert [
        (t["trade_id"], t["role"], t["event_id"], t["price"], t["quantity"])
        for t in analysis["trade_attribution"]
    ] == [
        (1, "TAKER", "e2", 100, 2),
        (2, "TAKER", "e4", 101, 4),
    ]


def test_execution_report_works_for_every_finished_status():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    # A partially filled IOC ends cancelled but stays queryable.
    detailed(
        engine, add("e2", "b1", "BUY", "LIMIT", 5, 100, time_in_force="IOC")
    )
    _, _, _, _, _, partial = detailed(engine, report("q1", "b1", 100))
    assert partial["current_status"] == "CANCELLED"
    assert partial["open_quantity"] == 0
    assert partial["filled_quantity"] == 2

    # An explicitly cancelled resting order reports as cancelled with no fills.
    detailed(engine, add("e3", "b2", "BUY", "LIMIT", 3, 99))
    detailed(engine, cancel("e4", "b2"))
    _, _, _, _, _, cancelled = detailed(engine, report("q2", "b2", 100))
    assert cancelled["current_status"] == "CANCELLED"
    assert cancelled["open_quantity"] == 0
    assert cancelled["vwap"] is None
    assert cancelled["trade_attribution"] == []

    # A fully filled maker reports FILLED.
    _, _, _, _, _, filled = detailed(engine, report("q3", "s1", 100))
    assert filled["current_status"] == "FILLED"


def test_execution_report_unknown_order_is_rejected_and_consumes_event_id():
    engine = Engine()
    eid, result, reason, trades, stp, analysis = detailed(
        engine, report("q1", "missing", 100)
    )
    assert (eid, result, reason, trades, stp, analysis) == (
        "q1", "REJECTED", "UNKNOWN_ORDER", [], None, None
    )
    # The well-formed unknown-order query did occupy its event id.
    assert detailed(engine, report("q1", "missing", 100))[1:3] == (
        "REJECTED", "DUPLICATE_EVENT_ID"
    )


def test_execution_report_changes_nothing():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    detailed(engine, add("e2", "b1", "BUY", "LIMIT", 2, 100))
    before = engine.snapshot()
    detailed(engine, report("q1", "s1", 100))
    detailed(engine, report("q2", "b1", 100))
    assert engine.snapshot() == before
    # No trade ids are spent: the next trade is number 2.
    detailed(engine, add("e3", "s2", "SELL", "LIMIT", 2, 100))
    _, _, _, trades, _, _ = detailed(
        engine, add("e4", "b2", "BUY", "LIMIT", 2, 100)
    )
    assert [t["trade_id"] for t in trades] == [2]


def test_execution_report_schema_rejections_consume_no_event_id():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    base = {"event_id": "eR", "type": "EXECUTION_REPORT",
            "order_id": "o1", "benchmark_price": 100}
    payloads = [
        {k: v for k, v in base.items() if k != "benchmark_price"},  # missing
        {k: v for k, v in base.items() if k != "order_id"},         # missing id
        {**base, "extra": 1},                                       # unknown field
        {**base, "benchmark_price": True},                          # bool
        {**base, "benchmark_price": False},
        {**base, "benchmark_price": 0},                             # non-positive
        {**base, "benchmark_price": -1},
        {**base, "benchmark_price": 1.5},                           # float
        {**base, "benchmark_price": "100"},                         # string
        {**base, "benchmark_price": None},                          # null
        {**base, "order_id": 7},                                    # non-string id
        {"event_id": 9, "type": "EXECUTION_REPORT",                 # bad event id
         "order_id": "o1", "benchmark_price": 100},
    ]
    for payload in payloads:
        line = json.dumps(payload)
        assert detailed(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload
        # Structural errors never occupy the event id.
        assert detailed(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload

    # The rejected queries changed neither book nor trade ids, and eR is free.
    assert engine.snapshot() == ([{"price": 100, "quantity": 5}], [])
    assert detailed(engine, json.dumps(base))[1] == "REPORTED"


def test_execution_report_duplicate_event_id():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 5, 100)])
    line = report("q1", "o1", 100)
    assert detailed(engine, line)[1] == "REPORTED"
    assert detailed(engine, line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")


def test_execution_report_benchmark_field_is_not_allowed_elsewhere():
    engine = Engine()
    assert detailed(
        engine,
        add("e1", "o1", "BUY", "LIMIT", 1, 100, benchmark_price=100),
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")
    assert detailed(
        engine,
        json.dumps({"event_id": "e2", "type": "BOGUS", "order_id": "o1",
                    "benchmark_price": 100}),
    )[1:3] == ("REJECTED", "INVALID_SCHEMA")
    # Nothing was consumed.
    assert detailed(engine, add("e1", "o1", "BUY", "LIMIT", 1, 100))[1] == "RESTING"


def test_handle_line_and_full_keep_their_shapes_for_report():
    engine, _ = book_after([add("e1", "o1", "BUY", "LIMIT", 1, 100)])
    eid, result, reason, trades = engine.handle_line(report("q1", "o1", 100))
    assert (eid, result, reason, trades) == ("q1", "REPORTED", None, [])
    eid, result, reason, trades, stp = engine.handle_line_full(report("q2", "o1", 100))
    assert (eid, result, reason, trades, stp) == ("q2", "REPORTED", None, [], None)


def test_replay_execution_report_shape_and_byte_determinism():
    stream = "\n".join(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s2", "SELL", "LIMIT", 5, 100),
            add("e3", "b1", "BUY", "LIMIT", 10, 100),
            report("q1", "i1", 99),
            report("q2", "missing", 1),
            report("q3", "b1", "not-a-number"),
        ]
    ) + "\n"
    code, out1, err = run_replay(stream)
    assert code == 0
    assert err == ""
    _, out2, _ = run_replay(stream)
    assert out2 == out1

    records = [json.loads(line) for line in out1.splitlines()]
    assert records[3]["result"] == "REPORTED"
    assert records[3]["trades"] == []
    # execution_analysis is serialized between result/reason/stp and trades.
    assert (
        out1.splitlines()[3].index('"execution_analysis"')
        < out1.splitlines()[3].index('"trades"')
    )
    analysis = records[3]["execution_analysis"]
    assert analysis["side"] == "SELL"
    assert analysis["current_status"] == "RESTING"
    assert analysis["open_quantity"] == 5
    assert analysis["filled_quantity"] == 5
    assert analysis["executed_notional"] == 500
    assert analysis["vwap"] == {"numerator": 500, "denominator": 5}
    assert analysis["slippage_notional"] == -5
    assert [(t["trade_id"], t["role"]) for t in analysis["trade_attribution"]] == [
        (1, "MAKER"),
        (3, "MAKER"),
    ]
    # A query echoes the same book the preceding event left.
    assert records[3]["asks"] == records[2]["asks"]
    # Unknown order and schema rejection shapes.
    assert (records[4]["result"], records[4]["reason"]) == ("REJECTED", "UNKNOWN_ORDER")
    assert "execution_analysis" not in records[4]
    assert (records[5]["result"], records[5]["reason"]) == ("REJECTED", "INVALID_SCHEMA")
    assert "execution_analysis" not in records[5]


def test_replay_without_execution_reports_is_byte_for_byte_compatible():
    first_input = add("e1", "s1", "SELL", "LIMIT", 5, 100)
    stream = "\n".join(
        [
            first_input,
            add("e2", "b1", "BUY", "LIMIT", 3, 100),
            cancel("e3", "s1"),
        ]
    ) + "\n"
    _, out, _ = run_replay(stream)
    assert "execution_analysis" not in out
    expected_first = (
        '{"input_line":'
        + json.dumps(first_input, ensure_ascii=False)
        + ',"event_id":"e1","result":"RESTING","trades":[],'
        '"bids":[],"asks":[{"price":100,"quantity":5}]}'
    )
    assert out.splitlines()[0] == expected_first
