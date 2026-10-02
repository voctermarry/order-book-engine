"""Tests for the per-order EXECUTION_REPORT query in the multi-symbol stream."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EXECUTION_REPORT,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    POV_START,
    POV_VOLUME,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    VWAP_SLICE,
    VWAP_START,
    canonical_json,
    replay_events,
)
from order_book_engine import event_cli
from order_book_engine.engine import UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def add(event_id, symbol, sequence, order_id, side, order_type, quantity, price=None, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
    }
    if price is not None:
        event["price"] = price
    event.update(extra)
    return event


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def report(event_id, symbol, sequence, order_id, benchmark_price, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": EXECUTION_REPORT,
        "order_id": order_id,
        "benchmark_price": benchmark_price,
    }
    event.update(extra)
    return event


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
               price, benchmark_price=None, **extra):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": TWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price, **extra,
    }


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


def vwap_start(event_id, symbol, sequence, plan_id, side, total_quantity, weights,
               price, benchmark_price=None, **extra):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": weights,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price, **extra,
    }


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


def pov_start(event_id, symbol, sequence, plan_id, side, total_quantity,
              participation_bps, price, benchmark_price=None, **extra):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": POV_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "participation_bps": participation_bps,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price, **extra,
    }


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_VOLUME, "plan_id": plan_id,
            "market_volume_increment": increment}


# ---------------------------------------------------------------------------
# Success path and result shape
# ---------------------------------------------------------------------------


def test_execution_report_success_shape_and_analysis():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
        report("r1", "AAA", 3, "s1", 99),
    ])
    r = out["results"][-1]
    assert r["event_id"] == "r1"
    assert r["symbol"] == "AAA"
    assert r["sequence"] == 3
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The security's book is echoed unchanged (2 units of s1 still rest).
    assert r["bids"] == []
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert "execution_plan" not in r
    assert "rejection_code" not in r
    assert r["execution_analysis"] == {
        "side": "SELL",
        "current_status": "RESTING",
        "open_quantity": 2,
        "filled_quantity": 3,
        "executed_notional": 300,
        "vwap": {"numerator": 300, "denominator": 3},
        # Sold at 100 against a benchmark of 99: 3 units of improvement.
        "slippage_notional": -3,
        "trade_attribution": [
            {"trade_id": 1, "role": "MAKER", "counterparty_order_id": "b1",
             "event_id": "a2", "price": 100, "quantity": 3},
        ],
    }


def test_execution_report_taker_view_and_slippage_sign():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 3, "b1", 110),
        report("r2", "AAA", 4, "s1", 110),
    ])
    buy = out["results"][2]["execution_analysis"]
    assert buy["current_status"] == "FILLED"
    assert buy["open_quantity"] == 0
    # Bought at 100 against a benchmark of 110: improvement is negative.
    assert buy["slippage_notional"] == 500 - 550
    assert buy["trade_attribution"] == [
        {"trade_id": 1, "role": "TAKER", "counterparty_order_id": "s1",
         "event_id": "a2", "price": 100, "quantity": 5},
    ]
    sell = out["results"][3]["execution_analysis"]
    assert sell["slippage_notional"] == 550 - 500


def test_execution_report_without_trades_has_null_vwap_and_empty_attribution():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 10),
        report("r1", "AAA", 2, "o1", 10),
    ])
    analysis = out["results"][-1]["execution_analysis"]
    assert analysis["current_status"] == "RESTING"
    assert analysis["open_quantity"] == 3
    assert analysis["filled_quantity"] == 0
    assert analysis["executed_notional"] == 0
    assert analysis["vwap"] is None
    assert analysis["slippage_notional"] == 0
    assert analysis["trade_attribution"] == []


def test_execution_report_cancelled_order_stays_queryable():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 10),
        cancel("a2", "AAA", 2, "o1"),
        report("r1", "AAA", 3, "o1", 10),
    ])
    analysis = out["results"][-1]["execution_analysis"]
    assert analysis["current_status"] == "CANCELLED"
    assert analysis["open_quantity"] == 0
    assert analysis["filled_quantity"] == 0


def test_execution_report_iceberg_open_quantity_and_replenished_slices():
    out = replay_events([
        iceberg("a1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        report("r1", "AAA", 2, "i1", 100),
        add("a2", "AAA", 3, "b1", "BUY", "LIMIT", 4, 100),
        report("r2", "AAA", 4, "i1", 100),
    ])
    resting = out["results"][1]["execution_analysis"]
    # The open quantity includes the hidden reserve.
    assert resting["current_status"] == "RESTING"
    assert resting["open_quantity"] == 10
    assert resting["vwap"] is None
    assert resting["trade_attribution"] == []
    filled = out["results"][3]["execution_analysis"]
    # Replenished slices trade under the same maker id and one event id.
    assert filled["open_quantity"] == 6
    assert filled["filled_quantity"] == 4
    assert filled["executed_notional"] == 400
    assert filled["vwap"] == {"numerator": 400, "denominator": 4}
    assert filled["trade_attribution"] == [
        {"trade_id": 1, "role": "MAKER", "counterparty_order_id": "b1",
         "event_id": "a2", "price": 100, "quantity": 3},
        {"trade_id": 2, "role": "MAKER", "counterparty_order_id": "b1",
         "event_id": "a2", "price": 100, "quantity": 1},
    ]


def test_execution_report_aggregates_across_replace():
    out = replay_events([
        add("a1", "AAA", 1, "b1", "BUY", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "s1", "SELL", "LIMIT", 2, 100),   # b1 makes 2@100
        replace("a3", "AAA", 3, "b1", 4, 101),
        add("a4", "AAA", 4, "s2", "SELL", "LIMIT", 4, 101),   # b1 makes 4@101
        report("r1", "AAA", 5, "b1", 100),
    ])
    assert out["results"][2]["result"] == "REPLACED"
    analysis = out["results"][-1]["execution_analysis"]
    assert analysis["current_status"] == "FILLED"
    assert analysis["filled_quantity"] == 6
    assert analysis["executed_notional"] == 2 * 100 + 4 * 101
    assert analysis["vwap"] == {"numerator": 604, "denominator": 6}
    assert analysis["slippage_notional"] == 604 - 600
    assert analysis["trade_attribution"] == [
        {"trade_id": 1, "role": "MAKER", "counterparty_order_id": "s1",
         "event_id": "a2", "price": 100, "quantity": 2},
        {"trade_id": 2, "role": "MAKER", "counterparty_order_id": "s2",
         "event_id": "a4", "price": 101, "quantity": 4},
    ]


# ---------------------------------------------------------------------------
# Plan child orders
# ---------------------------------------------------------------------------


def test_released_twap_child_is_queryable_but_plan_and_unreleased_ids_are_not():
    out = replay_events([
        add("a1", "AAA", 1, "w1", "SELL", "LIMIT", 5, 70),
        twap_start("a2", "AAA", 2, "p1", "BUY", 3, 3, 70),
        twap_slice("a3", "AAA", 3, "p1"),
        report("r1", "AAA", 4, "p1#1", 70),     # released child: an order
        report("r2", "AAA", 5, "p1", 70),       # the parent plan: not an order
        report("r3", "AAA", 6, "p1#2", 70),     # unreleased slice: not an order
    ])
    r = out["results"][3]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    analysis = r["execution_analysis"]
    assert analysis["side"] == "BUY"
    # The IOC child took its full unit from w1 and ended FILLED.
    assert analysis["current_status"] == "FILLED"
    assert analysis["open_quantity"] == 0
    assert analysis["filled_quantity"] == 1
    assert analysis["executed_notional"] == 70
    assert analysis["trade_attribution"] == [
        {"trade_id": 1, "role": "TAKER", "counterparty_order_id": "w1",
         "event_id": "p1#1", "price": 70, "quantity": 1},
    ]
    assert out["results"][4]["rejection_code"] == UNKNOWN_ORDER
    assert out["results"][5]["rejection_code"] == UNKNOWN_ORDER


def test_released_vwap_and_pov_children_are_queryable():
    out = replay_events([
        add("a1", "AAA", 1, "w1", "SELL", "LIMIT", 100, 70),
        vwap_start("a2", "AAA", 2, "v1", "BUY", 10, [1, 3], 70),
        vwap_slice("a3", "AAA", 3, "v1"),
        pov_start("a4", "AAA", 4, "p1", "BUY", 100, 2000, 70),
        pov_volume("a5", "AAA", 5, "p1", 50),
        report("r1", "AAA", 6, "v1#1", 70),
        report("r2", "AAA", 7, "p1#1", 70),
        report("r3", "AAA", 8, "p1#2", 70),     # no second release yet
    ])
    vwap_child = out["results"][5]["execution_analysis"]
    # 10 units over weights [1, 3]: the first bucket releases 3 units.
    assert vwap_child["filled_quantity"] == 3
    assert vwap_child["executed_notional"] == 210
    pov_child = out["results"][6]["execution_analysis"]
    # floor(50 * 2000 / 10000) = 10 units released by the POV_VOLUME event.
    assert pov_child["filled_quantity"] == 10
    assert pov_child["executed_notional"] == 700
    assert out["results"][7]["rejection_code"] == UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# Read-only guarantees
# ---------------------------------------------------------------------------


def _engine_states(snapshot):
    return {s["symbol"]: s["state"]["engine"]
            for s in snapshot["content"]["symbols"]}


def test_execution_report_is_read_only():
    events = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
    ]
    before = replay_events(events)["snapshot"]
    with_report = replay_events(
        events + [report("r1", "AAA", 3, "s1", 100)]
    )["snapshot"]
    # The only differences the query leaves are the replay-log entries; no
    # engine state (orders, trades, accounts, next trade id) moves.
    before_states = _engine_states(before)
    after_states = _engine_states(with_report)
    assert set(before_states) == set(after_states)
    for sym in before_states:
        assert canonical_json(before_states[sym]) == canonical_json(after_states[sym])
    # The query id lives solely in the replay log, not in the engine journal.
    assert "r1" not in after_states["AAA"]["event_ids"]
    aaa_state = [s for s in with_report["content"]["symbols"]
                 if s["symbol"] == "AAA"][0]["state"]
    assert aaa_state["last_sequence"] == 3
    # The next AAA trade still takes id 2 (one trade happened so far).
    follow = replay_events(
        [add("a3", "AAA", 4, "b2", "BUY", "LIMIT", 2, 100)],
        snapshot=with_report,
    )
    assert [t["trade_id"] for t in follow["results"][0]["trades"]] == [2]


def test_report_does_not_release_plan_slices_or_move_plans():
    stream = [
        add("a1", "AAA", 1, "w1", "SELL", "LIMIT", 5, 70),
        twap_start("a2", "AAA", 2, "p1", "BUY", 3, 3, 70),
        report("r1", "AAA", 3, "w1", 70),
    ]
    out = replay_events(stream)
    plan = out["results"][2]
    assert plan["status"] == ACCEPTED
    snapshot = out["snapshot"]
    aaa = [s for s in snapshot["content"]["symbols"] if s["symbol"] == "AAA"][0]
    (stored_plan,) = aaa["state"]["plans"]
    assert stored_plan["released"] == 0
    assert stored_plan["released_quantity"] == 0
    # The reserved derived ids stay reserved.
    assert aaa["state"]["engine"]["reserved_order_ids"] == ["p1#1", "p1#2", "p1#3"]


# ---------------------------------------------------------------------------
# UNKNOWN_ORDER
# ---------------------------------------------------------------------------


def test_unknown_order_rejects_and_consumes_id_and_sequence():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        report("r1", "AAA", 2, "gone", 100),
    ])
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ORDER)
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert "execution_analysis" not in r
    # Sequence 2 was consumed: another event at sequence 2 is out of order.
    stale = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        report("r1", "AAA", 2, "gone", 100),
        report("r2", "AAA", 2, "s1", 100),
    ])
    assert stale["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        report("r1", "AAA", 2, "gone", 100),
        report("r2", "AAA", 3, "s1", 100),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


def test_unknown_order_on_brand_new_symbol_registers_symbol():
    out = replay_events([report("r1", "EEE", 1, "gone", 100)])
    r = out["results"][0]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ORDER)
    assert r["bids"] == [] and r["asks"] == []
    symbols = [s["symbol"] for s in out["snapshot"]["content"]["symbols"]]
    assert symbols == ["EEE"]


def test_same_order_id_on_different_symbols_does_not_cross():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "SELL", "LIMIT", 2, 100),
        add("b1", "BBB", 1, "o1", "BUY", "LIMIT", 3, 50),
        report("r1", "AAA", 2, "o1", 100),
        report("r2", "BBB", 2, "o1", 50),
        report("r3", "CCC", 1, "o1", 100),
    ])
    aaa = out["results"][2]["execution_analysis"]
    assert aaa["side"] == "SELL"
    assert aaa["open_quantity"] == 2
    bbb = out["results"][3]["execution_analysis"]
    assert bbb["side"] == "BUY"
    assert bbb["open_quantity"] == 3
    # CCC has no such order (and no orders at all).
    assert out["results"][4]["rejection_code"] == UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# INVALID_EVENT: structural validation consumes nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"type": EXECUTION_REPORT, "order_id": "o1", "benchmark_price": 100},  # no event_id
    {"event_id": "r1", "order_id": "o1", "benchmark_price": 100},          # missing type
    {"event_id": "r1", "type": EXECUTION_REPORT, "benchmark_price": 100},  # no order_id
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1"},        # no benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": 100, "side": "BUY"},                               # extra field
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "",
     "benchmark_price": 100},                                              # empty order id
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": 7,
     "benchmark_price": 100},                                              # non-string order id
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": None,
     "benchmark_price": 100},                                              # null order id
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": True},                                             # bool benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": 0},                                                # zero benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": -3},                                               # negative benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": 1.5},                                              # float benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": "100"},                                            # string benchmark
    {"event_id": "r1", "type": EXECUTION_REPORT, "order_id": "o1",
     "benchmark_price": None},                                             # null benchmark
])
def test_malformed_report_events_are_invalid(bad):
    event = {"symbol": "AAA", "sequence": 2, **bad}
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        event,
    ])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT, bad
    assert out["results"][-1]["trades"] == []


def test_invalid_event_consumes_neither_id_nor_sequence():
    base = [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100)]
    bad = report("r1", "AAA", 2, "s1", True)
    # The malformed event consumed no sequence slot: another event at
    # sequence 2 is accepted.
    out = replay_events(base + [bad, report("r2", "AAA", 2, "s1", 100)])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    # It also consumed no event id: r1 itself is usable at sequence 2 in a
    # fresh run over the same prefix.
    retry = replay_events(base + [bad, report("r1", "AAA", 2, "s1", 100)])
    assert retry["results"][1]["rejection_code"] == INVALID_EVENT
    assert retry["results"][2]["status"] == ACCEPTED


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([report("r1", "EEE", 1, "o1", 0)])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "r1", "type": EXECUTION_REPORT,
                   "order_id": "o1", "benchmark_price": 100}},
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert r["execution_analysis"]["current_status"] == "RESTING"


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "OTHER", "type": EXECUTION_REPORT,
                   "order_id": "o1", "benchmark_price": 100}},
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Envelope, idempotency and ordering precedence
# ---------------------------------------------------------------------------


def test_identical_report_replay_is_duplicate_and_not_recomputed():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        report("r1", "AAA", 2, "o1", 100),
        # Stale sequence 2, identical content: idempotent duplicate.
        report("r1", "AAA", 2, "o1", 100),
        add("a2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][2]["trades"] == []
    assert out["results"][2]["book_changes"] == {"bids": [], "asks": []}
    assert "execution_analysis" not in out["results"][2]
    assert out["results"][3]["result"] == "FILLED"


def test_same_report_id_with_different_content_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        report("r1", "AAA", 2, "o1", 100),
        report("r1", "AAA", 3, "o1", 101),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_same_report_id_on_another_symbol_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        report("r1", "AAA", 2, "o1", 100),
        report("r1", "BBB", 1, "o1", 100),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_business_checks():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        report("r1", "AAA", 3, "o1", 100),
        report("r2", "AAA", 1, "gone", 100),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    # Neither occupied its slot: sequence 2 still works.
    follow = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        report("r1", "AAA", 3, "o1", 100),
        report("r3", "AAA", 2, "o1", 100),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


def test_business_rejected_report_id_is_occupied():
    base = [report("r1", "AAA", 1, "gone", 100)]
    out = replay_events(base + [report("r1", "AAA", 1, "gone", 100)])
    # Identical retry is a DUPLICATE even though the order is still unknown.
    assert out["results"][1]["status"] == DUPLICATE
    out2 = replay_events(base + [report("r1", "AAA", 2, "gone", 101)])
    assert out2["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_execution_report_output_is_byte_for_byte_deterministic():
    events = [
        iceberg("a1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 100),
        report("r1", "AAA", 3, "i1", 99),
        report("r2", "AAA", 4, "gone", 100),
    ]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
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


# ---------------------------------------------------------------------------
# Snapshot export / restore
# ---------------------------------------------------------------------------


def _report_stream():
    return [
        iceberg("a1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 100),
        report("r1", "AAA", 3, "i1", 99),
        add("b1", "BBB", 1, "w1", "SELL", "LIMIT", 5, 70),
        twap_start("b2", "BBB", 2, "p1", "BUY", 3, 3, 70),
        twap_slice("b3", "BBB", 3, "p1"),
        report("r2", "BBB", 4, "p1#1", 70),
        report("r3", "BBB", 5, "p1#2", 70),     # business reject: unreleased
        report("r4", "ZZZ", 1, "o1", 100),      # business reject: new symbol
    ]


def test_resumed_replay_with_execution_reports_matches_one_shot():
    stream = _report_stream()
    part1 = stream[:4]
    one_shot = replay_events(stream)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(stream[4:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][4:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_execution_report_works_after_restore_and_snapshot_format_is_unchanged():
    snapshot = replay_events(_report_stream()[:2])["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    out = replay_events([report("r1", "AAA", 3, "i1", 99)], snapshot=snapshot)
    analysis = out["results"][0]["execution_analysis"]
    assert analysis["filled_quantity"] == 4
    assert analysis["open_quantity"] == 6


def test_duplicate_report_is_still_idempotent_after_restore():
    snapshot = replay_events(_report_stream())["snapshot"]
    out = replay_events([report("r1", "AAA", 3, "i1", 99)], snapshot=snapshot)
    assert out["results"][0]["status"] == DUPLICATE


def test_business_rejected_report_roundtrips_through_snapshot():
    from order_book_engine import export_snapshot, restore_replayer
    out = replay_events([report("r1", "AAA", 1, "gone", 100)])
    snapshot = out["snapshot"]
    # Export/restore must accept the replay-only id and keep it occupied.
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [report("r1", "AAA", 1, "gone", 100)],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


def test_snapshot_after_named_execution_report_event():
    out = replay_events(
        [
            add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
            report("r1", "AAA", 2, "o1", 100),
        ],
        snapshot_after={"symbol": "AAA", "sequence": 2},
    )
    aaa = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 2
    assert "r1" in {e["event_id"] for e in aaa["state"]["event_log"]}
    assert "r1" not in aaa["state"]["engine"]["event_ids"]


# ---------------------------------------------------------------------------
# EventReplayer stateful session
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_execution_reports():
    from order_book_engine import EventReplayer
    replayer = EventReplayer()
    r1 = replayer.submit([add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100)])
    assert r1[0]["result"] == "RESTING"
    r2 = replayer.submit([report("r1", "AAA", 2, "o1", 100)])
    assert r2[0]["status"] == ACCEPTED
    assert r2[0]["execution_analysis"]["open_quantity"] == 2
    r3 = replayer.submit([report("r2", "AAA", 3, "gone", 100)])
    assert r3[0]["rejection_code"] == UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# CLI: events subcommand
# ---------------------------------------------------------------------------


def _run_cli(request_obj):
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    body = stdout.buffer.getvalue().decode("utf-8")
    parsed = json.loads(body) if body else None
    return code, parsed, stderr.getvalue()


def test_cli_events_supports_execution_report_end_to_end():
    request = {"events": [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
        report("r1", "AAA", 3, "s1", 99),
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["execution_analysis"]["filled_quantity"] == 3
    assert r["execution_analysis"]["slippage_notional"] == -3
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_execution_report_business_rejection_roundtrip():
    code, out, _ = _run_cli({"events": [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        report("r1", "AAA", 2, "gone", 100),
    ]})
    assert code == 0
    assert out["results"][1]["rejection_code"] == UNKNOWN_ORDER
