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


def iceberg(event_id, order_id, side, quantity, price, display_quantity, **extra):
    obj = {
        "event_id": event_id,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "order_type": "ICEBERG",
        "quantity": quantity,
        "price": price,
        "display_quantity": display_quantity,
    }
    obj.update(extra)
    return json.dumps(obj)


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


# ---------------------------------------------------------------------------
# ICEBERG
# ---------------------------------------------------------------------------


def test_iceberg_resting_exposes_only_display_slice():
    engine = Engine()
    eid, result, reason, trades = engine.handle_line(
        iceberg("e1", "i1", "SELL", 10, 100, 3)
    )
    assert (eid, result, reason, trades) == ("e1", "RESTING", None, [])
    assert engine.snapshot() == ([], [{"price": 100, "quantity": 3}])

    # display == quantity is allowed and looks like an ordinary limit.
    eid, result, _, _ = engine.handle_line(iceberg("e2", "i2", "BUY", 4, 99, 4))
    assert (eid, result) == ("e2", "RESTING")
    assert engine.snapshot()[0] == [{"price": 99, "quantity": 4}]


def test_iceberg_explicit_gtc_and_default_match():
    engine = Engine()
    assert engine.handle_line(
        iceberg("e1", "i1", "SELL", 5, 100, 2, time_in_force="GTC")
    )[1] == "RESTING"
    assert engine.handle_line(iceberg("e2", "i2", "SELL", 5, 100, 2))[1] == "RESTING"


def test_iceberg_passive_slice_consumed_then_requeued_behind_peers():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            add("e2", "s1", "SELL", "LIMIT", 2, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(add("e3", "b1", "BUY", "LIMIT", 7, 100))
    assert result == "FILLED"
    # First slice of i1 (3), then the later ordinary order s1 (2), then the
    # replenished slice of i1 (2) -- the taker meets i1 twice with s1 in between.
    assert [(t["trade_id"], t["maker_order_id"], t["quantity"], t["price"]) for t in trades] == [
        (1, "i1", 3, 100),
        (2, "s1", 2, 100),
        (3, "i1", 2, 100),
    ]
    # i1 has 5 left but the partially consumed slice only shows 1.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_iceberg_replenishment_keeps_price_time_priority_across_events():
    # A replenished slice goes to the back of the same-price queue; an order
    # arriving later must trade before the next taker meets the iceberg again.
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 8, 100, 2)])
    engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 2, 100))  # exhaust first slice
    # Between events the replenished slice rests; a new resting seller lands
    # ahead of it in arrival order.
    engine.handle_line(add("e3", "s1", "SELL", "LIMIT", 3, 100))
    _, _, _, trades = engine.handle_line(add("e4", "b2", "BUY", "LIMIT", 6, 100))
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 2),
        ("s1", 3),
        ("i1", 1),
    ]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_iceberg_partial_slice_does_not_replenish_until_exhausted():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 1, 100))
    # Slice partially consumed: 2 visible, reserve untouched, no requeue.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]
    # It keeps its place at the front; the next taker hits it first.
    _, _, _, trades = engine.handle_line(add("e3", "b2", "BUY", "LIMIT", 1, 100))
    assert [t["maker_order_id"] for t in trades] == ["i1"]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_iceberg_final_slice_is_smaller_and_order_fills():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 7, 100, 3)])
    _, result, _, trades = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 7, 100))
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [3, 3, 1]
    assert [t["maker_order_id"] for t in trades] == ["i1", "i1", "i1"]
    assert [t["trade_id"] for t in trades] == [1, 2, 3]
    assert engine.snapshot() == ([], [])


def test_iceberg_passive_fill_status_and_cancel_unknown():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 3, 100, 3)])
    engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 3, 100))
    # Fully filled iceberg cannot be cancelled.
    _, result, reason, _ = engine.handle_line(cancel("e3", "i1"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")


def test_iceberg_taker_uses_full_remaining_not_display():
    # An aggressive iceberg itself matches with all of its quantity, then rests
    # only the display slice of its leftover.
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(
        iceberg("e3", "i9", "BUY", 10, 100, 2)
    )
    assert result == "PARTIALLY_FILLED_RESTING"
    assert [t["quantity"] for t in trades] == [2, 3]
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 2}]


def test_iceberg_taker_fully_filled_has_no_rest():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 4, 100)])
    _, result, _, trades = engine.handle_line(
        iceberg("e2", "i1", "BUY", 4, 100, 2)
    )
    assert result == "FILLED"
    assert len(trades) == 1
    assert engine.snapshot() == ([], [])


def test_market_order_consumes_iceberg_replenishment():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 2)])
    _, result, _, trades = engine.handle_line(add("e2", "m1", "BUY", "MARKET", 7))
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [2, 2, 2, 1]
    # Remaining 3 still hides behind the partially consumed slice of 1.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_ioc_consumes_iceberg_replenishment():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 2)])
    _, result, _, trades = engine.handle_line(
        add("e2", "q1", "BUY", "LIMIT", 5, 100, time_in_force="IOC")
    )
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [2, 2, 1]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 1}]


def test_ordinary_limit_consumes_iceberg_replenishment_and_rests():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 6, 100, 2),
            add("e2", "s1", "SELL", "LIMIT", 1, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(add("e3", "b1", "BUY", "LIMIT", 5, 100))
    assert result == "FILLED"
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 2),
        ("s1", 1),
        ("i1", 2),
    ]
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]


def test_iceberg_cancel_removes_visible_and_reserve():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    assert engine.handle_line(cancel("e2", "i1"))[1] == "CANCELLED"
    assert engine.snapshot() == ([], [])
    # Second cancel is unknown; the reserve cannot leak back.
    assert engine.handle_line(cancel("e3", "i1"))[2] == "UNKNOWN_ORDER"


def test_cancel_partially_consumed_iceberg_removes_visible_slice():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 3)])
    engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 4, 100))  # 3 + replenished 1
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]
    assert engine.handle_line(cancel("e3", "i1"))[1] == "CANCELLED"
    assert engine.snapshot() == ([], [])


def test_fok_precheck_counts_full_iceberg_reserve():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 2)])
    _, result, _, trades = engine.handle_line(
        add("e2", "f1", "BUY", "LIMIT", 10, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [2, 2, 2, 2, 2]
    assert engine.snapshot() == ([], [])


def test_fok_precheck_combines_visible_and_reserve_with_other_orders():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 7, 100, 2),
            add("e2", "s1", "SELL", "LIMIT", 3, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(
        add("e3", "f1", "BUY", "LIMIT", 10, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert [t["quantity"] for t in trades] == [2, 3, 2, 2, 1]


def test_fok_failure_against_iceberg_changes_nothing():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 100, 2)])
    _, result, reason, trades = engine.handle_line(
        add("e2", "f1", "BUY", "LIMIT", 11, 100, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert reason is None
    assert trades == []
    # Visible slice untouched: the book does not reveal an exhausted slice and
    # the reserve is unchanged.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]
    # No trade identifier spent.
    _, _, _, trades = engine.handle_line(add("e3", "b1", "BUY", "LIMIT", 1, 100))
    assert trades[0]["trade_id"] == 1
    # The failed FOK still occupies its ids.
    assert engine.handle_line(
        add("e2", "x", "BUY", "LIMIT", 1, 100, time_in_force="FOK")
    )[2] == "DUPLICATE_EVENT_ID"
    assert engine.handle_line(
        add("e4", "f1", "BUY", "LIMIT", 1, 100)
    )[2] == "DUPLICATE_ORDER_ID"


def test_fok_partial_reserve_outside_limit_does_not_count():
    engine, _ = book_after([iceberg("e1", "i1", "SELL", 10, 101, 2)])
    _, result, _, trades = engine.handle_line(
        add("e2", "f1", "BUY", "LIMIT", 1, 100, time_in_force="FOK")
    )
    assert result == "UNFILLED_CANCELLED"
    assert trades == []
    assert engine.snapshot()[1] == [{"price": 101, "quantity": 2}]


def test_iceberg_schema_rejections():
    engine = Engine()
    payloads = [
        # Missing display_quantity.
        {"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100},
        # Missing price.
        {"event_id": "e2", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "display_quantity": 3},
        # Non-positive / boolean / non-integer values.
        {"event_id": "e3", "type": "ADD", "order_id": "o3", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100, "display_quantity": 0},
        {"event_id": "e4", "type": "ADD", "order_id": "o4", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100, "display_quantity": -2},
        {"event_id": "e5", "type": "ADD", "order_id": "o5", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100, "display_quantity": True},
        {"event_id": "e6", "type": "ADD", "order_id": "o6", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100, "display_quantity": 2.5},
        {"event_id": "e7", "type": "ADD", "order_id": "o7", "side": "BUY",
         "order_type": "ICEBERG", "quantity": True, "price": 100, "display_quantity": 1},
        {"event_id": "e8", "type": "ADD", "order_id": "o8", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": False, "display_quantity": 1},
        # display exceeds total quantity.
        {"event_id": "e9", "type": "ADD", "order_id": "o9", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100, "display_quantity": 11},
        # Forbidden time-in-force values (including null / non-string).
        {"event_id": "e10", "type": "ADD", "order_id": "o10", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3, "time_in_force": "IOC"},
        {"event_id": "e11", "type": "ADD", "order_id": "o11", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3, "time_in_force": "FOK"},
        {"event_id": "e12", "type": "ADD", "order_id": "o12", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3, "time_in_force": None},
        {"event_id": "e13", "type": "ADD", "order_id": "o13", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3, "time_in_force": 123},
        # Unknown field.
        {"event_id": "e14", "type": "ADD", "order_id": "o14", "side": "BUY",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3, "hidden": True},
        # display_quantity is iceberg-only.
        {"event_id": "e15", "type": "ADD", "order_id": "o15", "side": "BUY",
         "order_type": "LIMIT", "quantity": 10, "price": 100, "display_quantity": 3},
        {"event_id": "e16", "type": "ADD", "order_id": "o16", "side": "BUY",
         "order_type": "MARKET", "quantity": 10, "display_quantity": 3},
    ]
    for payload in payloads:
        line = json.dumps(payload)
        eid, result, reason, trades = engine.handle_line(line)
        assert (result, reason, trades) == ("REJECTED", "INVALID_SCHEMA", []), payload

    # Nothing was occupied: the same ids and order ids are all still free.
    assert engine.handle_line(
        iceberg("e1", "o1", "BUY", 1, 100, 1)
    )[1] == "RESTING"


def test_iceberg_duplicate_event_and_order_ids():
    engine = Engine()
    line = iceberg("e1", "i1", "SELL", 10, 100, 2)
    assert engine.handle_line(line)[1] == "RESTING"
    assert engine.handle_line(line)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")
    _, result, reason, _ = engine.handle_line(
        iceberg("e2", "i1", "BUY", 2, 100, 1)
    )
    assert (result, reason) == ("REJECTED", "DUPLICATE_ORDER_ID")
    assert engine.handle_line(
        add("e2", "o2", "SELL", "LIMIT", 1, 100)
    )[2] == "DUPLICATE_EVENT_ID"


def test_cancel_unknown_or_cancelled_or_filled_iceberg():
    engine = Engine()
    assert engine.handle_line(cancel("e1", "missing"))[2] == "UNKNOWN_ORDER"
    engine.handle_line(iceberg("e2", "i1", "SELL", 3, 100, 3))
    assert engine.handle_line(cancel("e3", "i1"))[1] == "CANCELLED"
    assert engine.handle_line(cancel("e4", "i1"))[2] == "UNKNOWN_ORDER"

    engine.handle_line(iceberg("e5", "i2", "SELL", 3, 100, 3))
    engine.handle_line(add("e6", "b1", "BUY", "LIMIT", 3, 100))
    assert engine.handle_line(cancel("e7", "i2"))[2] == "UNKNOWN_ORDER"


def test_iceberg_rejection_keeps_book_trade_ids_and_queue():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "SELL", 10, 100, 2),
            add("e2", "s1", "SELL", "LIMIT", 2, 100),
        ]
    )
    before = engine.snapshot()
    # Schema rejections and duplicate ids must leave everything untouched.
    engine.handle_line(
        json.dumps({"event_id": "e9", "type": "ADD", "order_id": "o9", "side": "BUY",
                    "order_type": "ICEBERG", "quantity": 10, "price": 100})
    )
    engine.handle_line(iceberg("e1", "dup1", "BUY", 2, 100, 1))
    engine.handle_line(iceberg("e8", "i1", "BUY", 2, 100, 1))
    assert engine.snapshot() == before

    _, _, _, trades = engine.handle_line(add("e10", "b1", "BUY", "LIMIT", 3, 100))
    assert [t["trade_id"] for t in trades] == [1, 2]
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 2),
        ("s1", 1),
    ]


def test_bid_side_iceberg_replenishes_at_passive_price():
    engine, _ = book_after(
        [
            iceberg("e1", "i1", "BUY", 8, 100, 2),
            add("e2", "b1", "BUY", "LIMIT", 2, 100),
        ]
    )
    _, result, _, trades = engine.handle_line(add("e3", "s1", "SELL", "LIMIT", 7, 100))
    assert result == "FILLED"
    # Passive price 100 throughout; iceberg re-meets the taker behind b1.
    assert [(t["maker_order_id"], t["quantity"], t["price"]) for t in trades] == [
        ("i1", 2, 100),
        ("b1", 2, 100),
        ("i1", 2, 100),
        ("i1", 1, 100),
    ]
    assert engine.snapshot()[0] == [{"price": 100, "quantity": 1}]


def test_replay_iceberg_end_to_end_byte_determinism():
    stream = "\n".join(
        [
            iceberg("e1", "i1", "SELL", 5, 100, 2),
            add("e2", "b1", "BUY", "LIMIT", 3, 100),
            cancel("e3", "i1"),
            iceberg("e4", "i2", "BUY", 5, 100, 2, time_in_force="IOC"),
        ]
    ) + "\n"
    code, out1, err = run_replay(stream)
    assert code == 0
    assert err == ""
    records = [json.loads(line) for line in out1.splitlines()]
    assert [r["event_id"] for r in records] == ["e1", "e2", "e3", "e4"]
    assert [r["result"] for r in records] == [
        "RESTING",
        "FILLED",
        "CANCELLED",
        "REJECTED",
    ]
    assert records[0]["asks"] == [{"price": 100, "quantity": 2}]
    assert records[1]["trades"] == [
        {"trade_id": 1, "maker_order_id": "i1", "taker_order_id": "b1",
         "price": 100, "quantity": 2},
        {"trade_id": 2, "maker_order_id": "i1", "taker_order_id": "b1",
         "price": 100, "quantity": 1},
    ]
    assert records[3]["reason"] == "INVALID_SCHEMA"
    assert records[3]["trades"] == []
    _, out2, _ = run_replay(stream)
    assert out2 == out1
