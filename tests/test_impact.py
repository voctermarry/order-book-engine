"""Tests for the read-only IMPACT_REPORT what-if query (JSON Lines replay)."""

from __future__ import annotations

import io
import json

from order_book_engine import replay as replay_cli
from order_book_engine.engine import Engine
from order_book_engine.event_replay import INVALID_EVENT, replay_events


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
        display_quantity=display_quantity, **extra
    )


def impact_line(event_id, side, quantity, benchmark_price):
    return json.dumps(
        {
            "event_id": event_id,
            "type": "IMPACT_REPORT",
            "side": side,
            "quantity": quantity,
            "benchmark_price": benchmark_price,
        }
    )


def impact_obj(event_id, side, quantity, benchmark_price):
    return {
        "event_id": event_id,
        "type": "IMPACT_REPORT",
        "side": side,
        "quantity": quantity,
        "benchmark_price": benchmark_price,
    }


def query(engine, line):
    return engine.handle_line_impact(line)


def build_ask_book():
    engine = Engine()
    engine.handle_line(add("e1", "s1", "SELL", "LIMIT", 5, 100))
    engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 3, 101))
    return engine


# --------------------------------------------------------------------------
# Basic estimation
# --------------------------------------------------------------------------


def test_buy_walks_asks_at_maker_prices_with_full_fill():
    engine = build_ask_book()
    eid, result, reason, trades, stp, analysis, position, recon, impact = query(
        engine, impact_line("q1", "BUY", 6, 99)
    )
    assert (eid, result, reason) == ("q1", "REPORTED", None)
    assert trades == []
    assert stp is None and analysis is None and position is None and recon is None
    assert impact == {
        "side": "BUY",
        "requested_quantity": 6,
        "benchmark_price": 99,
        "executable_quantity": 6,
        "unfilled_quantity": 0,
        "executed_notional": 5 * 100 + 101,
        "best_price": 100,
        "vwap": {"numerator": 601, "denominator": 6},
        # 601 - 99*6 against the caller benchmark; 601 - 100*6 versus best ask.
        "slippage_notional": 7,
        "impact_notional": 1,
        "price_breakdown": [
            {"price": 100, "quantity": 5},
            {"price": 101, "quantity": 1},
        ],
    }


def test_buy_chooses_lowest_ask_first():
    engine = Engine()
    engine.handle_line(add("e1", "s1", "SELL", "LIMIT", 1, 102))
    engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 2, 100))
    engine.handle_line(add("e3", "s3", "SELL", "LIMIT", 3, 101))
    *_, impact = query(engine, impact_line("q1", "BUY", 4, 100))
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 2},
        {"price": 101, "quantity": 2},
    ]
    assert impact["best_price"] == 100
    assert impact["executed_notional"] == 402


def test_price_breakdown_has_one_entry_per_fill_not_per_price():
    # Two makers queue at the best price; a partial taker produces one entry
    # per consumed passive slice, never merged across makers.
    engine = Engine()
    engine.handle_line(add("e1", "s1", "SELL", "LIMIT", 3, 100))
    engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 4, 100))
    *_, impact = query(engine, impact_line("q1", "BUY", 5, 100))
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 2},
    ]
    assert impact["executable_quantity"] == 5
    assert impact["executed_notional"] == 500


def test_sell_walks_bids_and_mirrors_slippage_sign():
    engine = Engine()
    engine.handle_line(add("e1", "b1", "BUY", "LIMIT", 3, 100))
    engine.handle_line(add("e2", "b2", "BUY", "LIMIT", 4, 99))
    *_, impact = query(engine, impact_line("q1", "SELL", 5, 101))
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 99, "quantity": 2},
    ]
    assert impact["executable_quantity"] == 5
    assert impact["unfilled_quantity"] == 0
    assert impact["best_price"] == 100
    assert impact["vwap"] == {"numerator": 498, "denominator": 5}
    # Selling below the benchmark is worse: raw 498 - 101*5 = -7 is mirrored.
    assert impact["slippage_notional"] == 7
    # Versus the pre-query best bid: raw 498 - 100*5 = -2 is mirrored.
    assert impact["impact_notional"] == 2


def test_partial_fill_costs_use_only_executable_quantity():
    engine = build_ask_book()
    *_, impact = query(engine, impact_line("q1", "BUY", 100, 100))
    assert impact["requested_quantity"] == 100
    assert impact["executable_quantity"] == 8
    assert impact["unfilled_quantity"] == 92
    assert impact["executed_notional"] == 5 * 100 + 3 * 101
    assert impact["vwap"] == {"numerator": 803, "denominator": 8}
    assert impact["slippage_notional"] == 803 - 800
    assert impact["impact_notional"] == 803 - 800
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 3},
    ]


def test_no_liquidity_reports_nulls_zeros_and_empty_breakdown():
    # Asks exist, so a sell market finds no bids.
    engine = build_ask_book()
    *_, impact = query(engine, impact_line("q1", "SELL", 4, 102))
    assert impact == {
        "side": "SELL",
        "requested_quantity": 4,
        "benchmark_price": 102,
        "executable_quantity": 0,
        "unfilled_quantity": 4,
        "executed_notional": 0,
        "best_price": None,
        "vwap": None,
        "slippage_notional": 0,
        "impact_notional": 0,
        "price_breakdown": [],
    }

    # A fresh book has no liquidity on either side.
    engine = Engine()
    *_, impact = query(engine, impact_line("q2", "BUY", 1, 50))
    assert (impact["best_price"], impact["vwap"]) == (None, None)
    assert impact["executable_quantity"] == 0
    assert impact["slippage_notional"] == 0
    assert impact["impact_notional"] == 0


def test_benchmark_below_market_makes_buy_slippage_negative():
    engine = build_ask_book()
    *_, impact = query(engine, impact_line("q1", "BUY", 5, 102))
    # Buying at 100 against a benchmark of 102 is improvement.
    assert impact["executed_notional"] == 500
    assert impact["slippage_notional"] == 500 - 510
    # Impact is always measured from the best consumed price, so it stays zero
    # when only the best level is touched.
    assert impact["impact_notional"] == 0


# --------------------------------------------------------------------------
# Iceberg handling in the simulation
# --------------------------------------------------------------------------


def test_simulation_includes_full_iceberg_remaining_via_replenishment():
    engine = Engine()
    # Visible slice of 3, reserve of 7: all ten units are executable.
    engine.handle_line(iceberg("e1", "i1", "SELL", 10, 100, 3))
    *_, impact = query(engine, impact_line("q1", "BUY", 10, 100))
    assert impact["executable_quantity"] == 10
    assert impact["unfilled_quantity"] == 0
    assert impact["executed_notional"] == 1000
    # One entry per consumed visible slice, in fill order: 3 + 3 + 3 + 1.
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 1},
    ]
    # The live aggregate still shows only the first slice.
    assert engine.snapshot()[1] == [{"price": 100, "quantity": 3}]


def test_replenished_slice_waits_at_level_tail_before_next_price():
    engine = Engine()
    # Iceberg 2 visible / 4 total, then 3 plain at the same price, then 3 one
    # tick worse. Queue order: i1(2), s2(3); i1's replenishment joins behind s2.
    engine.handle_line(iceberg("e1", "i1", "SELL", 4, 100, 2))
    engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 3, 100))
    engine.handle_line(add("e3", "s3", "SELL", "LIMIT", 3, 101))
    *_, impact = query(engine, impact_line("q1", "BUY", 8, 100))
    # i1 slice, s2, then i1's replenished slice at 100 before 101 is touched.
    assert impact["price_breakdown"] == [
        {"price": 100, "quantity": 2},
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 2},
        {"price": 101, "quantity": 1},
    ]
    assert impact["executable_quantity"] == 8
    assert impact["executed_notional"] == 801


def test_simulated_walk_matches_a_real_market_order():
    """Differential check: the what-if aggregation equals an actual market
    order on an identical book, while leaving its own book untouched."""

    def seeded_engine():
        engine = Engine()
        engine.handle_line(iceberg("e1", "i1", "SELL", 9, 100, 2))
        engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 3, 100))
        engine.handle_line(add("e3", "s3", "SELL", "LIMIT", 4, 101))
        engine.handle_line(add("e4", "s4", "SELL", "LIMIT", 2, 102))
        return engine

    simulated = seeded_engine()
    *_, impact = query(simulated, impact_line("q1", "BUY", 15, 100))

    actual = seeded_engine()
    _, _, _, real_trades = actual.handle_line(
        add("q1", "bX", "BUY", "MARKET", 15)
    )
    # The simulated per-fill sequence matches the real trade sequence exactly,
    # including the iceberg's tail-replenished slices.
    assert [(t["price"], t["quantity"]) for t in real_trades] == [
        (item["price"], item["quantity"]) for item in impact["price_breakdown"]
    ]
    real_notional = sum(t["price"] * t["quantity"] for t in real_trades)
    assert (impact["executable_quantity"], impact["executed_notional"]) == (
        sum(t["quantity"] for t in real_trades), real_notional
    )

    # The simulation leaves book, iceberg slices and the trade id counter as
    # they were; the real order does not.
    assert simulated.snapshot()[1] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 4},
        {"price": 102, "quantity": 2},
    ]
    _, _, _, next_trades = simulated.handle_line(
        add("q2", "bY", "BUY", "LIMIT", 1, 100)
    )
    assert next_trades[0]["trade_id"] == 1
    assert real_trades[0]["trade_id"] == 1


def test_simulation_ignores_account_self_trade_prevention():
    # The anonymous query has no account, so a tagged resting order is simply
    # liquidity; no self-trade prevention descriptor may appear.
    engine = Engine()
    engine.handle_line(add("e1", "s1", "SELL", "LIMIT", 4, 100, account_id="A"))
    eid, result, reason, trades, stp, _a, _p, _r, impact = query(
        engine, impact_line("q1", "BUY", 4, 100)
    )
    assert (result, reason, stp) == ("REPORTED", None, None)
    assert impact["executable_quantity"] == 4
    assert trades == []


# --------------------------------------------------------------------------
# Read-only guarantees and identifier semantics
# --------------------------------------------------------------------------


def test_impact_query_changes_no_state_at_all():
    engine = build_ask_book()
    before = engine.dump_state()
    bids_before, asks_before = engine.snapshot()
    query(engine, impact_line("q1", "BUY", 6, 99))
    query(engine, impact_line("q2", "BUY", 1000, 99))
    query(engine, impact_line("q3", "SELL", 5, 200))
    assert engine.snapshot() == (bids_before, asks_before)
    after = engine.dump_state()
    # Only the query event ids were registered; every other component is
    # identical (queues, orders, totals, trade log, accounts, trade counter).
    assert after.keys() == before.keys()
    for key in (
        "order_ids", "reserved_order_ids", "orders", "bids", "asks",
        "bid_totals", "ask_totals", "next_trade_id", "accounts", "trade_log",
    ):
        assert after[key] == before[key]
    assert after["event_ids"] == before["event_ids"] | {"q1", "q2", "q3"}
    assert after["next_trade_id"] == 1


def test_valid_query_occupies_event_id_and_duplicate_is_rejected():
    engine = build_ask_book()
    line = impact_line("q1", "BUY", 2, 100)
    assert query(engine, line)[1] == "REPORTED"
    eid, result, reason, *_ = query(engine, line)
    assert (eid, result, reason) == ("q1", "REJECTED", "DUPLICATE_EVENT_ID")

    # The id blocks a later baseline event with the same event id.
    assert engine.handle_line(
        add("q1", "o9", "BUY", "LIMIT", 1, 100)
    )[2] == "DUPLICATE_EVENT_ID"


def test_schema_rejections_consume_no_event_id():
    engine = build_ask_book()
    base = impact_obj("qX", "BUY", 3, 100)
    payloads = [
        {k: v for k, v in base.items() if k != "side"},             # missing side
        {k: v for k, v in base.items() if k != "quantity"},         # missing quantity
        {k: v for k, v in base.items() if k != "benchmark_price"},  # missing benchmark
        {k: v for k, v in base.items() if k != "event_id"},         # missing event id
        {**base, "order_id": "o1"},                                 # extra field
        {**base, "side": "LONG"},                                   # bad side
        {**base, "side": "buy"},                                    # bad side casing
        {**base, "side": 1},                                        # non-string side
        {**base, "quantity": True},                                 # bool quantity
        {**base, "quantity": False},
        {**base, "quantity": 0},                                    # non-positive
        {**base, "quantity": -2},
        {**base, "quantity": 1.5},                                  # float
        {**base, "quantity": "3"},                                  # string
        {**base, "quantity": None},                                 # null
        {**base, "benchmark_price": True},                          # bool benchmark
        {**base, "benchmark_price": 0},
        {**base, "benchmark_price": -1},
        {**base, "benchmark_price": 1.5},
        {**base, "benchmark_price": "100"},
        {**base, "benchmark_price": None},
        {"event_id": 9, "type": "IMPACT_REPORT", "side": "BUY",
         "quantity": 1, "benchmark_price": 100},                    # non-string id
    ]
    for payload in payloads:
        line = json.dumps(payload)
        assert query(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload
        # Structural errors never occupy the event id.
        assert query(engine, line)[1:3] == ("REJECTED", "INVALID_SCHEMA"), payload

    # Non-object input is a schema error too.
    assert query(engine, "[1, 2]")[1:3] == ("REJECTED", "INVALID_SCHEMA")
    # The book is untouched and the rejected id is still free.
    assert engine.snapshot()[1] == [
        {"price": 100, "quantity": 5}, {"price": 101, "quantity": 3}
    ]
    assert query(engine, json.dumps(base))[1] == "REPORTED"


def test_malformed_json_keeps_invalid_json_reason():
    engine = Engine()
    eid, result, reason, *_ = query(engine, "{not json")
    assert (eid, result, reason) == (None, "REJECTED", "INVALID_JSON")


def test_legacy_handle_chain_keeps_its_tuple_shapes():
    engine = build_ask_book()
    line = impact_line("q1", "BUY", 2, 100)
    # The deepest entry point exposes the impact analysis as a ninth element.
    assert len(engine.handle_line_impact(line)) == 9
    # Every earlier entry point keeps its historical arity and reports None for
    # the analyses it does not surface.
    assert len(engine.handle_line_reconciliation(line)) == 8
    assert engine.handle_line_reconciliation(line)[1:3] == (
        "REJECTED", "DUPLICATE_EVENT_ID"
    )
    assert engine.handle_object({"type": "nope"})[2] == "INVALID_SCHEMA"


# --------------------------------------------------------------------------
# JSON Lines CLI integration
# --------------------------------------------------------------------------


def test_cli_serializes_impact_analysis_between_result_and_trades():
    lines = "\n".join(
        [
            add("e1", "s1", "SELL", "LIMIT", 2, 100),
            impact_line("q1", "BUY", 3, 99),
        ]
    ) + "\n"
    stdin = io.TextIOWrapper(io.BytesIO(lines.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO(), write_through=True)
    code = replay_cli.replay(stdin, stdout, io.StringIO())
    assert code == 0
    stdout.buffer.seek(0)
    records = [json.loads(line) for line in stdout.buffer.read().decode("utf-8").splitlines()]

    record = records[1]
    assert record["input_line"] == impact_line("q1", "BUY", 3, 99)
    assert record["event_id"] == "q1"
    assert record["result"] == "REPORTED"
    assert record["trades"] == []
    assert record["impact_analysis"]["executable_quantity"] == 2
    assert record["impact_analysis"]["unfilled_quantity"] == 1
    assert record["asks"] == [{"price": 100, "quantity": 2}]
    assert "execution_analysis" not in record
    assert "position_analysis" not in record
    assert "reconciliation" not in record

    # impact_analysis is serialized after result/reason and before trades.
    text = stdout.buffer.getvalue().decode("utf-8").splitlines()[1]
    assert text.index('"result"') < text.index('"impact_analysis"')
    assert text.index('"impact_analysis"') < text.index('"trades"')


# --------------------------------------------------------------------------
# The events (multi-symbol replay) entry point does not know the query
# --------------------------------------------------------------------------


def test_events_entry_rejects_impact_report_without_consuming_sequence():
    events = [
        {
            "event_id": "e1", "symbol": "AAA", "sequence": 1,
            "type": "ADD", "order_id": "s1", "side": "SELL",
            "order_type": "LIMIT", "quantity": 2, "price": 100,
        },
        {
            "event_id": "e2", "symbol": "AAA", "sequence": 2,
            "type": "IMPACT_REPORT", "side": "BUY",
            "quantity": 1, "benchmark_price": 100,
        },
    ]
    out = replay_events(events)
    results = out["results"]
    assert results[0]["status"] == "ACCEPTED"
    assert (results[1]["status"], results[1]["rejection_code"]) == (
        "REJECTED", INVALID_EVENT
    )
    # A structural rejection consumed neither the id nor the sequence, and no
    # impact analysis leaked into the events response.
    assert "impact_analysis" not in results[1]
    follow_up = replay_events(
        events
        + [
            {
                "event_id": "e3", "symbol": "AAA", "sequence": 2,
                "type": "CANCEL", "order_id": "s1",
            }
        ],
        snapshot=out["snapshot"],
    )
    assert follow_up["results"][-1]["status"] == "ACCEPTED"
