"""Tests for the read-only IMPACT_REPORT what-if query.

Covers both the single-security JSON Lines replay entry point and the
ordered multi-symbol event stream (``replay_events`` / ``EventReplayer``).
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    IMPACT_REPORT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine import replay as replay_cli
from order_book_engine.engine import Engine
from order_book_engine.event_replay import INVALID_EVENT


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
# Multi-symbol ordered event stream (replay_events / EventReplayer)
# --------------------------------------------------------------------------


def m_add(event_id, symbol, sequence, order_id, side, order_type, quantity, price=None, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "ADD", "order_id": order_id, "side": side,
        "order_type": order_type, "quantity": quantity,
    }
    if price is not None:
        event["price"] = price
    event.update(extra)
    return event


def m_iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return m_add(event_id, symbol, sequence, order_id, side, "ICEBERG",
                 quantity, price, display_quantity=display, **extra)


def m_impact(event_id, symbol, sequence, side, quantity, benchmark_price, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": IMPACT_REPORT, "side": side, "quantity": quantity,
        "benchmark_price": benchmark_price,
    }
    event.update(extra)
    return event


def m_impact_nested(event_id, symbol, sequence, side, quantity, benchmark_price):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "event": {
            "event_id": event_id, "type": IMPACT_REPORT, "side": side,
            "quantity": quantity, "benchmark_price": benchmark_price,
        },
    }


def _ask_book_stream(prefix="AAA"):
    return [
        m_add("e1", prefix, 1, "s1", "SELL", "LIMIT", 5, 100),
        m_add("e2", prefix, 2, "s2", "SELL", "LIMIT", 3, 101),
    ]


# -- success shape ----------------------------------------------------------


def test_stream_impact_success_shape():
    out = replay_events(
        _ask_book_stream()
        + [m_impact("q1", "AAA", 3, "BUY", 6, 99)],
        snapshot_after=None,
    )
    r = out["results"][2]
    assert (r["status"], r["result"]) == (ACCEPTED, "REPORTED")
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["bids"] == []
    assert r["asks"] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 3},
    ]
    assert r["impact_analysis"] == {
        "side": "BUY",
        "requested_quantity": 6,
        "benchmark_price": 99,
        "executable_quantity": 6,
        "unfilled_quantity": 0,
        "executed_notional": 601,
        "best_price": 100,
        "vwap": {"numerator": 601, "denominator": 6},
        "slippage_notional": 7,
        "impact_notional": 1,
        "price_breakdown": [
            {"price": 100, "quantity": 5},
            {"price": 101, "quantity": 1},
        ],
    }


def test_stream_impact_sell_side_mirrors_costs():
    out = replay_events(
        [
            m_add("e1", "AAA", 1, "b1", "BUY", "LIMIT", 3, 100),
            m_add("e2", "AAA", 2, "b2", "BUY", "LIMIT", 4, 99),
            m_impact("q1", "AAA", 3, "SELL", 5, 101),
        ],
        snapshot_after=None,
    )
    analysis = out["results"][2]["impact_analysis"]
    assert analysis["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 99, "quantity": 2},
    ]
    assert analysis["vwap"] == {"numerator": 498, "denominator": 5}
    assert analysis["slippage_notional"] == 7
    assert analysis["impact_notional"] == 2


def test_stream_impact_nested_payload_form():
    out = replay_events(
        _ask_book_stream()
        + [m_impact_nested("q1", "AAA", 3, "BUY", 6, 99)],
        snapshot_after=None,
    )
    r = out["results"][2]
    assert (r["status"], r["result"]) == (ACCEPTED, "REPORTED")
    assert r["impact_analysis"]["executable_quantity"] == 6


def test_stream_impact_matches_single_security_entry_point_exactly():
    # Differential check: the stream analysis is byte-identical to the
    # baseline single-security engine's own impact analysis on an equal book.
    stream = [
        m_iceberg("e1", "AAA", 1, "i1", "SELL", 9, 100, 2),
        m_add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
        m_add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 4, 101),
        m_add("e4", "AAA", 4, "s4", "SELL", "LIMIT", 2, 102),
        m_impact("q1", "AAA", 5, "BUY", 15, 100),
    ]
    out = replay_events(copy.deepcopy(stream), snapshot_after=None)
    stream_analysis = out["results"][4]["impact_analysis"]

    engine = Engine()
    engine.handle_line(add("e1", "i1", "SELL", "ICEBERG", 9, 100, display_quantity=2))
    engine.handle_line(add("e2", "s2", "SELL", "LIMIT", 3, 100))
    engine.handle_line(add("e3", "s3", "SELL", "LIMIT", 4, 101))
    engine.handle_line(add("e4", "s4", "SELL", "LIMIT", 2, 102))
    *_, engine_analysis = query(engine, impact_line("q1", "BUY", 15, 100))
    assert stream_analysis == engine_analysis


def test_stream_impact_iceberg_replenishes_at_level_tail():
    out = replay_events(
        [
            m_iceberg("e1", "AAA", 1, "i1", "SELL", 4, 100, 2),
            m_add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
            m_add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 3, 101),
            m_impact("q1", "AAA", 4, "BUY", 8, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["impact_analysis"]["price_breakdown"] == [
        {"price": 100, "quantity": 2},
        {"price": 100, "quantity": 3},
        {"price": 100, "quantity": 2},
        {"price": 101, "quantity": 1},
    ]


def test_stream_impact_partial_fill_and_empty_book():
    out = replay_events(
        _ask_book_stream()
        + [m_impact("q1", "AAA", 3, "BUY", 100, 100)],
        snapshot_after=None,
    )
    analysis = out["results"][2]["impact_analysis"]
    assert (analysis["executable_quantity"], analysis["unfilled_quantity"]) == (8, 92)
    assert analysis["executed_notional"] == 803
    assert analysis["vwap"] == {"numerator": 803, "denominator": 8}
    assert analysis["impact_notional"] == 3

    # A sell finds no bids in the ask-only book.
    out = replay_events(
        _ask_book_stream("BBB")
        + [m_impact("q2", "BBB", 3, "SELL", 4, 102)],
        snapshot_after=None,
    )
    analysis = out["results"][2]["impact_analysis"]
    assert analysis == {
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


def test_stream_impact_on_first_seen_symbol_succeeds_against_empty_book():
    out = replay_events(
        [m_impact("q1", "ZZZ", 1, "BUY", 7, 50)]
    )
    r = out["results"][0]
    assert (r["status"], r["result"]) == (ACCEPTED, "REPORTED")
    assert r["bids"] == [] and r["asks"] == []
    analysis = r["impact_analysis"]
    assert analysis["executable_quantity"] == 0
    assert analysis["unfilled_quantity"] == 7
    assert analysis["best_price"] is None and analysis["vwap"] is None
    # The well-formed query registered the symbol and advanced its sequence.
    follow = replay_events(
        [m_impact("q2", "ZZZ", 2, "SELL", 1, 60)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert follow["results"][0]["status"] == ACCEPTED


def test_stream_impact_uses_envelope_symbol_book_only():
    out = replay_events(
        [
            m_add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
            m_add("e2", "BBB", 1, "w1", "SELL", "LIMIT", 2, 50),
            m_impact("q1", "AAA", 2, "BUY", 9, 100),
            m_impact("q2", "BBB", 2, "BUY", 9, 50),
        ],
        snapshot_after=None,
    )
    aaa = out["results"][2]["impact_analysis"]
    bbb = out["results"][3]["impact_analysis"]
    assert (aaa["executable_quantity"], aaa["executed_notional"]) == (5, 500)
    assert (bbb["executable_quantity"], bbb["executed_notional"]) == (2, 100)
    assert out["results"][2]["asks"] == [{"price": 100, "quantity": 5}]
    assert out["results"][3]["asks"] == [{"price": 50, "quantity": 2}]


# -- read-only guarantees ----------------------------------------------------


def test_stream_impact_changes_no_matching_state():
    stream = [
        m_iceberg("e1", "AAA", 1, "i1", "SELL", 9, 100, 2),
        m_add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
        m_add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 4, 101),
        m_add("e4", "BBB", 1, "w1", "BUY", "LIMIT", 6, 40),
    ]
    queries = [
        m_impact("q1", "AAA", 4, "BUY", 20, 100),
        m_impact("q2", "AAA", 5, "SELL", 20, 100),
        m_impact("q3", "BBB", 2, "SELL", 20, 40),
    ]
    before = replay_events(copy.deepcopy(stream))
    after = replay_events(copy.deepcopy(stream) + queries)

    def strip_query_records(snapshot):
        doc = copy.deepcopy(snapshot)
        doc["content"]["events"] = []
        for symbol in doc["content"]["symbols"]:
            symbol["state"]["event_log"] = []
            symbol["state"]["last_sequence"] = 0
        doc["content_digest"] = ""
        return doc

    assert canonical_json(strip_query_records(after["snapshot"])) == canonical_json(
        strip_query_records(before["snapshot"])
    )

    # The queries' own ids live solely in the replay log, never in the engine
    # journal; the trade id counter did not move.
    snap = after["snapshot"]["content"]["symbols"]
    for symbol in snap:
        engine = symbol["state"]["engine"]
        assert set(engine["event_ids"]).isdisjoint({"q1", "q2", "q3"})
        assert engine["next_trade_id"] == 1


def test_stream_impact_ignores_self_trade_prevention():
    out = replay_events(
        [
            m_add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100, account_id="A"),
            m_impact("q1", "AAA", 2, "BUY", 4, 100),
        ],
        snapshot_after=None,
    )
    r = out["results"][1]
    assert (r["status"], r["result"]) == (ACCEPTED, "REPORTED")
    assert "self_trade_prevention" not in r
    assert r["impact_analysis"]["executable_quantity"] == 4
    assert r["trades"] == []
    # The tagged resting order is untouched and still visible.
    assert r["asks"] == [{"price": 100, "quantity": 4}]


def test_stream_impact_is_not_blocked_by_active_price_limits():
    # Rest the book first, then narrow the active band below its prices:
    # new priced orders would be rejected, the read-only query is not.
    out = replay_events(
        [
            m_add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
            m_add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 2, 101),
            {
                "event_id": "u1", "symbol": "AAA", "sequence": 3,
                "type": "PRICE_LIMIT_UPDATE", "lower_price": 500, "upper_price": 600,
            },
            m_impact("q1", "AAA", 4, "BUY", 5, 100),
            # Sanity: a priced order at the book's prices is now blocked, so
            # the accepted impact is demonstrably exempt from the band.
            m_add("e3", "AAA", 5, "s3", "SELL", "LIMIT", 1, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["status"] == ACCEPTED
    assert out["results"][3]["impact_analysis"]["executable_quantity"] == 5
    assert out["results"][4]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    assert out["results"][3]["asks"] == [
        {"price": 100, "quantity": 3},
        {"price": 101, "quantity": 2},
    ]


# -- schema validation -------------------------------------------------------


@pytest.mark.parametrize("mutation", [
    lambda e: e.pop("side"),
    lambda e: e.pop("quantity"),
    lambda e: e.pop("benchmark_price"),
    lambda e: e.pop("type"),
    lambda e: e.update(side="LONG"),
    lambda e: e.update(side="buy"),
    lambda e: e.update(side=1),
    lambda e: e.update(side=None),
    lambda e: e.update(quantity=True),
    lambda e: e.update(quantity=False),
    lambda e: e.update(quantity=0),
    lambda e: e.update(quantity=-2),
    lambda e: e.update(quantity=1.5),
    lambda e: e.update(quantity="3"),
    lambda e: e.update(quantity=None),
    lambda e: e.update(benchmark_price=True),
    lambda e: e.update(benchmark_price=0),
    lambda e: e.update(benchmark_price=-1),
    lambda e: e.update(benchmark_price=1.5),
    lambda e: e.update(benchmark_price="100"),
    lambda e: e.update(benchmark_price=None),
    lambda e: e.update(order_id="o1"),
])
def test_stream_malformed_inline_impact_is_invalid(mutation):
    event = m_impact("q1", "AAA", 2, "BUY", 3, 100)
    mutation(event)
    out = replay_events(
        _ask_book_stream() + [event],
        snapshot_after=None,
    )
    r = out["results"][2]
    assert (r["status"], r["rejection_code"]) == (REJECTED, INVALID_EVENT)
    assert "impact_analysis" not in r
    # The untouched book is echoed and no id/sequence was consumed.
    assert r["asks"] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 3},
    ]


@pytest.mark.parametrize("mutation", [
    lambda e: e["event"].pop("side"),
    lambda e: e["event"].update(quantity=0),
    lambda e: e["event"].update(benchmark_price="100"),
    lambda e: e["event"].update(extra=1),
    lambda e: e.update(extra=1),
])
def test_stream_malformed_nested_impact_is_invalid(mutation):
    event = m_impact_nested("q1", "AAA", 2, "BUY", 3, 100)
    mutation(event)
    out = replay_events(_ask_book_stream() + [event], snapshot_after=None)
    assert out["results"][2]["rejection_code"] == INVALID_EVENT


def test_stream_nested_impact_event_id_mismatch_is_invalid():
    event = m_impact_nested("q1", "AAA", 2, "BUY", 3, 100)
    event["event"]["event_id"] = "other"
    out = replay_events(_ask_book_stream() + [event], snapshot_after=None)
    assert out["results"][2]["rejection_code"] == INVALID_EVENT


def test_stream_invalid_impact_consumes_neither_id_nor_sequence():
    out = replay_events(
        _ask_book_stream()
        + [
            m_impact("q1", "AAA", 3, "LONG", 3, 100),
            m_impact("q1", "AAA", 3, "BUY", 3, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][2]["rejection_code"] == INVALID_EVENT
    assert out["results"][3]["status"] == ACCEPTED


def test_stream_invalid_impact_on_unknown_symbol_does_not_create_it():
    invalid = m_impact("q1", "ZZZ", 1, "BUY", 3, 100)
    invalid["side"] = "LONG"
    out = replay_events(
        [
            invalid,
            m_impact("q2", "ZZZ", 1, "BUY", 3, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    # Sequence 1 was not consumed and the symbol was not created...
    assert out["results"][0]["bids"] == [] and out["results"][0]["asks"] == []
    assert out["results"][1]["status"] == ACCEPTED


# -- idempotency, conflicts and ordering -------------------------------------


def test_stream_impact_duplicate_is_idempotent_with_stale_sequence():
    query = m_impact("q1", "AAA", 3, "BUY", 6, 99)
    out = replay_events(_ask_book_stream() + [query])
    repeat = replay_events([query], snapshot=out["snapshot"], snapshot_after=None)
    r = repeat["results"][0]
    assert r["status"] == DUPLICATE
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert "impact_analysis" not in r
    assert r["asks"] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 3},
    ]
    # Sequence stayed at 3: the next in-sequence event is accepted.
    follow = replay_events(
        [m_impact("q2", "AAA", 4, "BUY", 1, 100)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert follow["results"][0]["status"] == ACCEPTED


def test_stream_impact_same_id_different_content_or_symbol_conflicts():
    out = replay_events(
        _ask_book_stream()
        + [
            m_impact("q1", "AAA", 3, "BUY", 6, 99),
            m_impact("q1", "AAA", 4, "BUY", 6, 100),
            m_impact("q1", "BBB", 1, "BUY", 6, 99),
            m_impact("q2", "AAA", 4, "BUY", 1, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][4]["rejection_code"] == EVENT_ID_CONFLICT
    # Conflicts consume neither the id nor the sequence.
    assert out["results"][5]["status"] == ACCEPTED


def test_stream_impact_sequence_gap_and_out_of_order():
    out = replay_events(
        _ask_book_stream()
        + [
            m_impact("q1", "AAA", 5, "BUY", 1, 100),
            m_impact("q2", "AAA", 2, "BUY", 1, 100),
            m_impact("q3", "AAA", 3, "BUY", 1, 100),
        ],
        snapshot_after=None,
    )
    assert out["results"][2]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["expected_sequence"] == 3
    assert out["results"][3]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][3]["expected_sequence"] == 3
    # Neither consumed its slot.
    assert out["results"][4]["status"] == ACCEPTED


# -- determinism, snapshots and the events CLI -------------------------------


def _impact_rich_stream():
    return [
        m_iceberg("e1", "AAA", 1, "i1", "SELL", 9, 100, 2),
        m_add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100, account_id="fund"),
        m_add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 4, 101),
        m_add("b1", "BBB", 1, "w1", "SELL", "LIMIT", 3, 50),
        m_impact("q1", "AAA", 4, "BUY", 15, 100),
        m_impact("q2", "AAA", 5, "SELL", 2, 102),
        m_impact("q3", "BBB", 2, "BUY", 9, 50),
        m_impact("q4", "CCC", 1, "BUY", 1, 70),
    ]


def test_stream_impact_output_is_byte_for_byte_deterministic():
    a = canonical_json(replay_events(_impact_rich_stream()))
    b = canonical_json(replay_events(copy.deepcopy(_impact_rich_stream())))
    assert a == b
    parsed = json.loads(a)

    def no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float leaked into output")
        if isinstance(value, dict):
            for item in value.values():
                no_floats(item)
        elif isinstance(value, list):
            for item in value:
                no_floats(item)

    no_floats(parsed)


def test_stream_impact_resumed_replay_matches_one_shot():
    stream = _impact_rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    # Split right after the first impact query.
    snapshot = replay_events(copy.deepcopy(stream[:5]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[5:]), snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][5:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(
        one_shot["snapshot"]
    )


def test_stream_impact_snapshot_roundtrip_and_post_restore_duplicate():
    stream = _impact_rich_stream()
    out = replay_events(copy.deepcopy(stream))
    restored = restore_replayer(copy.deepcopy(out["snapshot"]))
    assert canonical_json(export_snapshot(restored)) == canonical_json(out["snapshot"])
    # The already-seen query stays a duplicate after restore.
    duplicate = restored.submit([copy.deepcopy(stream[4])])[0]
    assert duplicate["status"] == DUPLICATE
    fresh = restored.submit([m_impact("q9", "AAA", 6, "BUY", 1, 100)])[0]
    assert (fresh["status"], fresh["result"]) == (ACCEPTED, "REPORTED")


def test_stream_impact_snapshot_after_named_query_event():
    out = replay_events(
        _impact_rich_stream(),
        snapshot_after={"symbol": "AAA", "sequence": 4},
    )
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
    resumed = replay_events(
        [m_impact("qx", "AAA", 5, "SELL", 1, 102)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert resumed["results"][0]["status"] == ACCEPTED


def _run_events_cli(request_obj):
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    body = stdout.buffer.getvalue().decode("utf-8")
    parsed = json.loads(body) if body else None
    return code, parsed, stderr.getvalue()


def test_cli_events_supports_impact_report_end_to_end():
    code, out, err = _run_events_cli({
        "events": _ask_book_stream()
        + [
            m_impact("q1", "AAA", 3, "BUY", 6, 99),
            m_impact("q1", "AAA", 4, "BUY", 6, 99),
        ]
    })
    assert code == 0
    assert err == ""
    reported, duplicate = out["results"][2], out["results"][3]
    assert reported["status"] == ACCEPTED
    assert reported["result"] == "REPORTED"
    assert reported["impact_analysis"]["executable_quantity"] == 6
    assert duplicate["status"] == DUPLICATE
    assert "impact_analysis" not in duplicate
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
