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


def test_limit_ioc_fills_what_it_can_and_never_rests():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 5, 100, time_in_force="IOC")
    )
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [2]
    # The leftover must not rest on the book.
    assert engine.snapshot() == ([], [])
    _, result, reason, _ = engine.handle_line(cancel("e3", "b1"))
    assert (result, reason) == ("REJECTED", "UNKNOWN_ORDER")

    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "LIMIT", 1, 100, time_in_force="IOC")
    )
    assert (result, trades) == ("UNFILLED_CANCELLED", [])

    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 3, 100)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 3, 100, time_in_force="IOC")
    )
    assert result == "FILLED"
    assert len(trades) == 1


def test_limit_ioc_respects_its_own_price():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 101)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "LIMIT", 2, 100, time_in_force="IOC")
    )
    assert (result, trades) == ("UNFILLED_CANCELLED", [])
    assert engine.snapshot() == ([], [{"price": 101, "quantity": 2}])


def test_limit_fok_fills_in_full_when_liquidity_suffices():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 101),
        ]
    )
    _, result, _, trades = engine.handle_line(
        add("e3", "b1", "BUY", "LIMIT", 4, 101, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert [(t["trade_id"], t["maker_order_id"], t["price"], t["quantity"]) for t in trades] == [
        (1, "s1", 100, 2),
        (2, "s2", 101, 2),
    ]
    assert engine.snapshot() == ([], [{"price": 101, "quantity": 1}])


def test_limit_fok_fails_without_trading_or_mutating_state():
    engine, _ = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 101),
        ]
    )
    before = engine.snapshot()
    # Total liquidity is 5 but only 2 within the limit price.
    _, result, _, trades = engine.handle_line(
        add("e3", "b1", "BUY", "LIMIT", 3, 100, time_in_force="FOK")
    )
    assert (result, trades) == ("UNFILLED_CANCELLED", [])
    assert engine.snapshot() == before

    # Enough in total but the FOK quantity exceeds everything available.
    _, result, _, trades = engine.handle_line(
        add("e4", "b2", "BUY", "LIMIT", 6, 200, time_in_force="FOK")
    )
    assert (result, trades) == ("UNFILLED_CANCELLED", [])
    assert engine.snapshot() == before

    # Failed FOK consumed no trade ids: the next fill starts at 1.
    _, result, _, trades = engine.handle_line(
        add("e5", "b3", "BUY", "LIMIT", 2, 100, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert [t["trade_id"] for t in trades] == [1]

    # Failed FOK still occupied its event id and order id.
    assert engine.handle_line(
        add("e4", "oX", "BUY", "LIMIT", 1, 100)
    )[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")
    assert engine.handle_line(
        add("e6", "b2", "BUY", "LIMIT", 1, 100)
    )[1:3] == ("REJECTED", "DUPLICATE_ORDER_ID")


def test_market_explicit_ioc_matches_default_behaviour():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "MARKET", 5, time_in_force="IOC")
    )
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [2]
    assert engine.snapshot() == ([], [])


def test_market_fok():
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 3, 100)])
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "MARKET", 3, time_in_force="FOK")
    )
    assert result == "FILLED"
    assert len(trades) == 1

    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    before = engine.snapshot()
    _, result, _, trades = engine.handle_line(
        add("e2", "b1", "BUY", "MARKET", 3, time_in_force="FOK")
    )
    assert (result, trades) == ("UNFILLED_CANCELLED", [])
    assert engine.snapshot() == before


def test_time_in_force_schema_validation():
    engine = Engine()
    bad = [
        # MARKET may not be combined with GTC.
        add("e1", "o1", "BUY", "MARKET", 1, time_in_force="GTC"),
        # Non-string time_in_force values.
        add("e2", "o2", "BUY", "LIMIT", 1, 100, time_in_force=1),
        add("e3", "o3", "BUY", "LIMIT", 1, 100, time_in_force=None),
        add("e4", "o4", "BUY", "LIMIT", 1, 100, time_in_force=True),
        # Unknown time_in_force value.
        add("e5", "o5", "BUY", "LIMIT", 1, 100, time_in_force="DAY"),
        add("e6", "o6", "BUY", "MARKET", 1, time_in_force="DAY"),
        # CANCEL must not carry time_in_force.
        json.dumps({"event_id": "e7", "type": "CANCEL", "order_id": "o1",
                    "time_in_force": "GTC"}),
        # LIMIT with IOC/FOK still requires a valid price.
        add("e8", "o8", "BUY", "LIMIT", 1, time_in_force="IOC"),
        add("e9", "o9", "BUY", "LIMIT", 1, price=0, time_in_force="FOK"),
    ]
    for line in bad:
        assert engine.handle_line(line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), line

    # Explicit GTC on LIMIT behaves exactly like the default.
    engine, _ = book_after([add("e1", "b1", "BUY", "LIMIT", 2, 100, time_in_force="GTC")])
    assert engine.snapshot() == ([{"price": 100, "quantity": 2}], [])


def test_old_streams_produce_identical_results_with_tif_defaults():
    # The default (no time_in_force) semantics are unchanged: LIMIT rests,
    # MARKET cancels its leftover.
    engine, _ = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    _, result, _, _ = engine.handle_line(add("e2", "b1", "BUY", "LIMIT", 5, 100))
    assert result == "PARTIALLY_FILLED_RESTING"
    assert engine.snapshot() == ([{"price": 100, "quantity": 3}], [])
    _, result, _, _ = engine.handle_line(add("e3", "b2", "BUY", "MARKET", 9))
    assert result == "UNFILLED_CANCELLED"
    assert engine.snapshot() == ([{"price": 100, "quantity": 3}], [])
    _, result, _, trades = engine.handle_line(add("e4", "s2", "SELL", "MARKET", 9))
    assert result == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in trades] == [3]
    assert engine.snapshot() == ([], [])


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
