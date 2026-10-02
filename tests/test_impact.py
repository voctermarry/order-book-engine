"""Tests for the read-only IMPACT_REPORT replay query."""

from __future__ import annotations

import io
import json

from order_book_engine import replay
from order_book_engine.engine import (
    IMPACT_REPORT,
    Engine,
)


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


def iceberg(event_id, order_id, side, quantity, price, display_quantity, **extra):
    return add(
        event_id, order_id, side, "ICEBERG", quantity, price,
        display_quantity=display_quantity, **extra,
    )


def impact(event_id, side, quantity, benchmark_price):
    return json.dumps(
        {
            "event_id": event_id,
            "type": IMPACT_REPORT,
            "side": side,
            "quantity": quantity,
            "benchmark_price": benchmark_price,
        }
    )


def impact_query(engine, line):
    return engine.handle_line_impact(line)


def book_after(lines):
    engine = Engine()
    for line in lines:
        engine.handle_line(line)
    return engine


def run_replay(text: str):
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = replay.replay(stdin, stdout, stderr)
    stdout.flush()
    return code, stdout.buffer.getvalue().decode("utf-8"), stderr.getvalue()


# --------------------------------------------------------------------------
# Analysis content
# --------------------------------------------------------------------------


def test_buy_walks_asks_price_time_priority_and_reports_costs():
    engine = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 101),
        ]
    )
    eid, result, reason, trades, stp, analysis, position, recon, impact_obj = (
        impact_query(engine, impact("q1", "BUY", 4, 99))
    )
    assert (eid, result, reason) == ("q1", "REPORTED", None)
    assert trades == []
    assert stp is analysis is position is recon
    assert impact_obj == {
        "side": "BUY",
        "requested_quantity": 4,
        "benchmark_price": 99,
        "executable_quantity": 4,
        "unfilled_quantity": 0,
        "executed_notional": 2 * 100 + 2 * 101,
        "best_price": 100,
        "vwap": {"numerator": 2 * 100 + 2 * 101, "denominator": 4},
        # 402 - 99*4 = 6
        "slippage_notional": 6,
        # 402 - 100*4 = 2
        "impact_notional": 2,
        "price_breakdown": [
            {"price": 100, "quantity": 2},
            {"price": 101, "quantity": 2},
        ],
    }


def test_sell_walks_bids_and_mirrors_cost_signs():
    engine = book_after(
        [
            add("e1", "b1", "BUY", "LIMIT", 3, 100),
            add("e2", "b2", "BUY", "LIMIT", 2, 99),
        ]
    )
    impact_obj = impact_query(engine, impact("q1", "SELL", 4, 101))[8]
    assert impact_obj["side"] == "SELL"
    assert impact_obj["executable_quantity"] == 4
    assert impact_obj["unfilled_quantity"] == 0
    assert impact_obj["executed_notional"] == 3 * 100 + 99
    assert impact_obj["best_price"] == 100
    assert impact_obj["vwap"] == {"numerator": 3 * 100 + 99, "denominator": 4}
    # raw = 399 - 101*4 = -5 (an improvement); sell side reports the opposite.
    assert impact_obj["slippage_notional"] == 5
    # raw = 399 - 100*4 = -1; mirrored to 1.
    assert impact_obj["impact_notional"] == 1
    assert impact_obj["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 99, "quantity": 1},
    ]


def test_no_liquidity_reports_null_prices_zero_costs_and_empty_breakdown():
    engine = Engine()
    impact_obj = impact_query(engine, impact("q1", "BUY", 5, 100))[8]
    assert impact_obj == {
        "side": "BUY",
        "requested_quantity": 5,
        "benchmark_price": 100,
        "executable_quantity": 0,
        "unfilled_quantity": 5,
        "executed_notional": 0,
        "best_price": None,
        "vwap": None,
        "slippage_notional": 0,
        "impact_notional": 0,
        "price_breakdown": [],
    }


def test_partial_fill_costs_use_actual_executable_quantity():
    engine = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 103)])
    impact_obj = impact_query(engine, impact("q1", "BUY", 7, 100))[8]
    assert impact_obj["executable_quantity"] == 2
    assert impact_obj["unfilled_quantity"] == 5
    assert impact_obj["executed_notional"] == 206
    assert impact_obj["best_price"] == 103
    assert impact_obj["vwap"] == {"numerator": 206, "denominator": 2}
    assert impact_obj["slippage_notional"] == 6
    assert impact_obj["impact_notional"] == 0
    assert impact_obj["price_breakdown"] == [{"price": 103, "quantity": 2}]


def test_breakdown_consolidates_each_price_level_into_one_entry():
    engine = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
        ]
    )
    impact_obj = impact_query(engine, impact("q1", "BUY", 4, 100))[8]
    assert impact_obj["price_breakdown"] == [{"price": 100, "quantity": 4}]


# --------------------------------------------------------------------------
# Iceberg simulation
# --------------------------------------------------------------------------


def test_iceberg_public_slice_replenishes_at_level_tail():
    # One price level, in arrival order: iceberg i1 (5 total, 2 displayed),
    # plain s2 (3), iceberg i2 (4 total, 2 displayed). Total available 12.
    engine = book_after(
        [
            iceberg("e1", "i1", "SELL", 5, 100, 2),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
            iceberg("e3", "i2", "SELL", 4, 100, 2),
        ]
    )
    impact_obj = impact_query(engine, impact("q1", "BUY", 12, 100))[8]
    assert impact_obj["executable_quantity"] == 12
    assert impact_obj["unfilled_quantity"] == 0
    assert impact_obj["executed_notional"] == 1200
    assert impact_obj["best_price"] == 100
    assert impact_obj["vwap"] == {"numerator": 1200, "denominator": 12}
    assert impact_obj["slippage_notional"] == 0
    assert impact_obj["impact_notional"] == 0
    # Every fill happens at the single price; consolidation still yields one
    # breakdown entry even though i1 and i2 replenish behind s2.
    assert impact_obj["price_breakdown"] == [{"price": 100, "quantity": 12}]


def test_iceberg_replenishment_reserve_counts_toward_impact():
    # Requesting more than the visible total still reaches iceberg reserve.
    engine = book_after([iceberg("e1", "i1", "SELL", 5, 100, 2)])
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]
    impact_obj = impact_query(engine, impact("q1", "BUY", 5, 100))[8]
    assert impact_obj["executable_quantity"] == 5
    assert impact_obj["unfilled_quantity"] == 0
    assert impact_obj["price_breakdown"] == [{"price": 100, "quantity": 5}]


def test_iceberg_tail_order_matches_a_real_market_fill():
    # Cross-check the simulated walk against the actual matching engine:
    # i1 (5@100 peak 2), s2 (3@100), i2 (4@100 peak 2); a market buy of 12
    # must see fills in queue order, with replenishment slices behind s2.
    sim_engine = book_after(
        [
            iceberg("e1", "i1", "SELL", 5, 100, 2),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
            iceberg("e3", "i2", "SELL", 4, 100, 2),
        ]
    )
    real_engine = book_after(
        [
            iceberg("e1", "i1", "SELL", 5, 100, 2),
            add("e2", "s2", "SELL", "LIMIT", 3, 100),
            iceberg("e3", "i2", "SELL", 4, 100, 2),
        ]
    )
    sim = impact_query(sim_engine, impact("qX", "BUY", 12, 100))[8]
    _, _, _, real_trades, _ = real_engine.handle_object(
        {
            "event_id": "e4",
            "type": "ADD",
            "order_id": "m1",
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": 12,
        }
    )
    # Simulated price/quantity sequence equals the real trade sequence.
    simulated_steps: list[tuple[int, int]] = []
    for level in sim["price_breakdown"]:
        simulated_steps.append((level["price"], level["quantity"]))
    real_by_price: dict[int, int] = {}
    for trade in real_trades:
        real_by_price[trade["price"]] = (
            real_by_price.get(trade["price"], 0) + trade["quantity"]
        )
    assert sum(qty for _, qty in simulated_steps) == sum(real_by_price.values())
    assert dict(simulated_steps) == real_by_price


# --------------------------------------------------------------------------
# Read-only guarantees
# --------------------------------------------------------------------------


def test_query_changes_nothing_and_spends_no_trade_id():
    engine = book_after(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            iceberg("e2", "i1", "SELL", 5, 101, 2),
        ]
    )
    before = engine.dump_state()
    impact_query(engine, impact("q1", "BUY", 100, 90))
    after = engine.dump_state()
    # A valid query occupies only its event id: every other piece of engine
    # state is byte-identical.
    assert after.pop("event_ids") == before.pop("event_ids") | {"q1"}
    assert before == after

    # A later real trade still takes trade id 1 and sees the full book.
    _, _, _, trades = engine.handle_line(add("e3", "b1", "BUY", "MARKET", 7))
    assert [t["trade_id"] for t in trades] == [1, 2, 3, 4]
    assert sum(t["quantity"] for t in trades) == 7


def test_repeated_queries_are_independent_simulations():
    engine = book_after([iceberg("e1", "i1", "SELL", 5, 100, 2)])
    first = impact_query(engine, impact("q1", "BUY", 5, 100))[8]
    second = impact_query(engine, impact("q2", "BUY", 5, 100))[8]
    assert first == second
    # The visible aggregate still shows only the peak slice.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 2}]


def test_anonymous_simulation_ignores_account_self_trade_prevention():
    engine = book_after(
        [add("e1", "s1", "SELL", "LIMIT", 3, 100, account_id="acct")]
    )
    impact_obj = impact_query(engine, impact("q1", "BUY", 3, 100))[8]
    # No account is attributed to the simulated taker, so the resting order is
    # fully available even though it carries an account.
    assert impact_obj["executable_quantity"] == 3


# --------------------------------------------------------------------------
# Schema and idempotency
# --------------------------------------------------------------------------


def test_schema_rejections_consume_no_event_id():
    engine = book_after([add("e0", "s1", "SELL", "LIMIT", 5, 100)])
    base = {
        "event_id": "qR",
        "type": IMPACT_REPORT,
        "side": "BUY",
        "quantity": 3,
        "benchmark_price": 100,
    }
    payloads = [
        {k: v for k, v in base.items() if k != "side"},
        {k: v for k, v in base.items() if k != "quantity"},
        {k: v for k, v in base.items() if k != "benchmark_price"},
        {k: v for k, v in base.items() if k != "event_id"},
        {**base, "order_id": "o1"},
        {**base, "price": 100},
        {**base, "side": "LONGSIDE"},
        {**base, "side": None},
        {**base, "side": "buy"},
        {**base, "quantity": 0},
        {**base, "quantity": -5},
        {**base, "quantity": True},
        {**base, "quantity": 1.0},
        {**base, "quantity": "3"},
        {**base, "quantity": None},
        {**base, "benchmark_price": False},
        {**base, "benchmark_price": 0},
        {**base, "benchmark_price": 1.5},
        {**base, "benchmark_price": "100"},
        {**base, "benchmark_price": None},
        {**base, "event_id": 7},
        {**base, "event_id": None},
        {**base, "type": "ADD"},
    ]
    for payload in payloads:
        line = json.dumps(payload)
        assert impact_query(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload
        # Structural errors never occupy the event id.
        assert impact_query(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload

    assert engine.snapshot()[1] == [{"price": 100, "quantity": 5}]
    assert impact_query(engine, json.dumps(base))[1] == "REPORTED"


def test_valid_query_occupies_event_id_and_duplicate_is_rejected():
    engine = book_after([add("e1", "s1", "SELL", "LIMIT", 2, 100)])
    line = impact("q1", "BUY", 2, 100)
    assert impact_query(engine, line)[1] == "REPORTED"
    eid, result, reason, trades, *_ = impact_query(engine, line)
    assert (eid, result, reason) == ("q1", "REJECTED", "DUPLICATE_EVENT_ID")
    assert trades == []


def test_query_shares_the_engine_event_id_namespace_with_order_events():
    engine = Engine()
    engine.handle_line(add("e1", "o1", "BUY", "LIMIT", 1, 100))
    # An impact query reusing an order event id is a duplicate.
    duplicate = impact("e1", "SELL", 1, 100)
    assert impact_query(engine, duplicate)[1:3] == ("REJECTED", "DUPLICATE_EVENT_ID")
    # And a later ADD may not reuse a consumed impact query id.
    impact_query(engine, impact("q1", "SELL", 1, 100))
    eid, result, reason, _trades = engine.handle_line(
        add("q1", "o2", "BUY", "LIMIT", 1, 99)
    )
    assert (eid, result, reason) == ("q1", "REJECTED", "DUPLICATE_EVENT_ID")


# --------------------------------------------------------------------------
# JSON Lines entry point
# --------------------------------------------------------------------------


def test_replay_output_shape_and_serialization_order():
    stream = "\n".join(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            impact("q1", "BUY", 5, 99),
        ]
    ) + "\n"
    code, out, err = run_replay(stream)
    assert (code, err) == (0, "")
    record = json.loads(out.splitlines()[1])
    assert record["event_id"] == "q1"
    assert record["result"] == "REPORTED"
    assert record["trades"] == []
    assert record["asks"] == [{"price": 100, "quantity": 2}]
    assert record["impact_analysis"]["executable_quantity"] == 2
    assert record["impact_analysis"]["unfilled_quantity"] == 3
    assert record["impact_analysis"]["slippage_notional"] == 2
    assert "execution_analysis" not in record
    assert "position_analysis" not in record
    assert "reconciliation" not in record
    # The analysis is serialized after the result and before the trades.
    line = out.splitlines()[1]
    assert (
        line.index('"result"')
        < line.index('"impact_analysis"')
        < line.index('"trades"')
    )


def test_replay_echoes_input_line_verbatim():
    raw = (
        '{"event_id":"q1","type":"IMPACT_REPORT","side":"BUY",'
        '"quantity":1,"benchmark_price":100}'
    )
    _, out, _ = run_replay(raw + "\n")
    assert json.loads(out)["input_line"] == raw


def test_replay_rejected_query_uses_reason_and_no_analysis():
    raw = '{"event_id":"q1","type":"IMPACT_REPORT","side":"BUY","quantity":1}'
    _, out, _ = run_replay(raw + "\n")
    record = json.loads(out)
    assert record["result"] == "REJECTED"
    assert record["reason"] == "INVALID_SCHEMA"
    assert "impact_analysis" not in record
    assert record["trades"] == []


def test_replay_is_byte_for_byte_deterministic():
    stream = "\n".join(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "s2", "SELL", "LIMIT", 3, 101),
            iceberg("e3", "i1", "SELL", 4, 102, 2),
            impact("q1", "BUY", 8, 100),
            impact("q2", "SELL", 2, 103),
        ]
    ) + "\n"
    _, out1, _ = run_replay(stream)
    _, out2, _ = run_replay(stream)
    assert out1 == out2
