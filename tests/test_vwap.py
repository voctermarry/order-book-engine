"""Tests for resumable VWAP parent orders in the multi-symbol event stream.

VWAP plans are the volume-curve sibling of the TWAP parent order: the total
quantity is split across one bucket per volume weight using a deterministic
largest-remainder allocation, and slices are advanced solely by VWAP_SLICE
events without reading a wall clock.
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
    DUPLICATE,
    DUPLICATE_EXECUTION_PLAN,
    EVENT_ID_CONFLICT,
    EXECUTION_PLAN_CLOSED,
    FORMAT_VERSION,
    INVALID_EVENT,
    PLAN_ACTIVE,
    PLAN_CANCELLED,
    PLAN_COMPLETED,
    REJECTED,
    SNAPSHOT_CORRUPT,
    EventReplayer,
    SnapshotError,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
    VWAP_CANCEL,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.event_replay import ExecutionPlan


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


def vwap_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, volume_weights,
    order_type, benchmark_price, price=None, account_id=None,
):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": VWAP_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "volume_weights": list(volume_weights),
        "order_type": order_type,
        "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    elif price is not None:
        event["price"] = price
    if account_id is not None:
        event["account_id"] = account_id
    return event


def twap_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
    order_type, benchmark_price, price=None,
):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": TWAP_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "slice_count": slice_count,
        "order_type": order_type,
        "benchmark_price": benchmark_price,
    }
    if price is not None:
        event["price"] = price
    return event


def plan_cmd(event_id, symbol, sequence, cmd, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": cmd, "plan_id": plan_id}


def slice_event(eid, sym, seq, plan):
    return plan_cmd(eid, sym, seq, VWAP_SLICE, plan)


def cancel_plan(eid, sym, seq, plan):
    return plan_cmd(eid, sym, seq, VWAP_CANCEL, plan)


def report_plan(eid, sym, seq, plan):
    return plan_cmd(eid, sym, seq, VWAP_REPORT, plan)


# ---------------------------------------------------------------------------
# Bucket allocation (largest remainder, earlier bucket wins ties)
# ---------------------------------------------------------------------------


def test_every_bucket_gets_one_unit_before_proportional_split():
    # Total equals the number of buckets: each gets exactly one.
    assert ExecutionPlan._vwap_schedule(3, [5, 3, 2]) == [1, 1, 1]


def test_equal_weights_spread_remainder_over_earliest_buckets():
    # R = 7 over three equal weights: quotient 2 each leaves one unit, handed
    # to the earliest bucket.
    assert ExecutionPlan._vwap_schedule(10, [1, 1, 1]) == [4, 3, 3]


def test_proportional_quotients_are_respected():
    # R = 7, weights 3:2:1 over total 6 -> quotients 3,2,1 (6 units) and one
    # left over; the largest remainder (3/6) is bucket 0, so it rounds up.
    assert ExecutionPlan._vwap_schedule(10, [3, 2, 1]) == [5, 3, 2]
    # An exact proportional split needs no rounding-up at all.
    # total 9 -> R = 6 = total weight, so quotients are exactly 3,2,1.
    assert ExecutionPlan._vwap_schedule(9, [3, 2, 1]) == [4, 3, 2]


def test_remainder_units_follow_largest_fractional_parts():
    # R = 2, weights 2:1:1 over total 4 -> quotients 1,0,0 and one left over;
    # bucket 0's division is exact (remainder 0), so the unit goes to bucket 1.
    assert ExecutionPlan._vwap_schedule(5, [2, 1, 1]) == [2, 2, 1]
    # R = 2, equal weights -> two earliest buckets round up.
    assert ExecutionPlan._vwap_schedule(5, [1, 1, 1]) == [2, 2, 1]


def test_equal_remainder_tie_favours_the_earlier_bucket():
    # R = 10 over three equal weights: quotient 3 each (9), one left over; the
    # earliest bucket takes it.
    assert ExecutionPlan._vwap_schedule(13, [7, 7, 7]) == [5, 4, 4]


@pytest.mark.parametrize("total,weights", [
    (4, [1, 1, 1]),
    (7, [3, 1, 2]),
    (100, [1, 2, 3, 4]),
    (11, [5, 5, 5, 5, 5]),
    (1, [1]),
])
def test_schedule_always_sums_to_total_and_is_positive(total, weights):
    quantities = ExecutionPlan._vwap_schedule(total, weights)
    assert len(quantities) == len(weights)
    assert sum(quantities) == total
    assert all(q >= 1 for q in quantities)


def test_plan_constructor_builds_the_weight_schedule():
    plan = ExecutionPlan(
        plan_id="p", side="BUY", order_type="LIMIT", total_quantity=10,
        benchmark_price=100, price=100, account_id=None,
        algorithm=ALGORITHM_VWAP, volume_weights=[3, 2, 1],
    )
    assert plan.algorithm == ALGORITHM_VWAP
    assert plan.slice_quantities == [5, 3, 2]
    assert plan.target_weights == [3, 2, 1]
    assert plan.total_quantity == 10


# ---------------------------------------------------------------------------
# Start: no matching, reservations, summary shape
# ---------------------------------------------------------------------------


def test_vwap_start_does_not_match_and_echoes_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        vwap_start("e2", "AAA", 2, "p1", "BUY", 5, [1, 1], "LIMIT", 99, price=100),
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert "result" not in r
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    plan = r["execution_plan"]
    assert plan == {
        "algorithm": ALGORITHM_VWAP,
        "status": PLAN_ACTIVE,
        "released_quantity": 0,
        "filled_quantity": 0,
        "cancelled_quantity": 0,
        "remaining_slices": 2,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
    }
    # The start consumed no trade id.
    assert out["snapshot"]["content"]["symbols"][0]["state"]["engine"]["next_trade_id"] == 1


def test_start_reserves_one_derived_id_per_bucket():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 7, [3, 2, 1], "MARKET", 100),
    ])
    reserved = out["snapshot"]["content"]["symbols"][0]["state"]["engine"]["reserved_order_ids"]
    assert reserved == ["p#1", "p#2", "p#3"]
    plans = out["snapshot"]["content"]["symbols"][0]["state"]["plans"]
    assert plans[0]["slice_quantities"] == [3, 2, 2]
    assert plans[0]["target_weights"] == [3, 2, 1]


# ---------------------------------------------------------------------------
# Slice execution
# ---------------------------------------------------------------------------


def test_slice_reports_target_weight_and_scheduled_quantity():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [3, 2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
    ])
    first = out["results"][2]["execution_plan"]
    assert first["slice_number"] == 1
    assert first["child_order_id"] == "p#1"
    assert first["target_weight"] == 3
    assert first["scheduled_quantity"] == 6
    second = out["results"][3]["execution_plan"]
    assert second["slice_number"] == 2
    assert second["child_order_id"] == "p#2"
    assert second["target_weight"] == 2
    assert second["scheduled_quantity"] == 4
    assert second["status"] == PLAN_COMPLETED


def test_report_summary_does_not_carry_slice_fields():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 6, [2, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        report_plan("e3", "AAA", 4, "p"),
    ])
    report = out["results"][3]["execution_plan"]
    assert "slice_number" not in report
    assert "child_order_id" not in report
    assert "target_weight" not in report
    assert "scheduled_quantity" not in report


def test_limit_slice_is_ioc_at_plan_price():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 3, [1, 1], "LIMIT", 99, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [1]
    assert r["bids"] == []
    assert r["execution_plan"]["scheduled_quantity"] == 2
    assert r["execution_plan"]["filled_quantity"] == 1


def test_market_slice_uses_market_child_and_ioc_semantics():
    out = replay_events([
        add("e0", "AAA", 1, "b1", "BUY", "LIMIT", 2, 100),
        vwap_start("e1", "AAA", 2, "p", "SELL", 5, [1, 1], "MARKET", 101),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [2]
    plan = r["execution_plan"]
    assert plan["executed_notional"] == 200
    # Sell improvement: -(100*2 - 101*2) = 2.
    assert plan["slippage_notional"] == 2


def test_unfilled_slice_keeps_plan_active():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "SELL", 2, [1, 1], "MARKET", 100),
        slice_event("e2", "AAA", 2, "p"),
    ])
    plan = out["results"][1]["execution_plan"]
    assert plan["status"] == PLAN_ACTIVE
    assert plan["vwap"] is None
    assert plan["released_quantity"] == 1
    assert plan["remaining_slices"] == 1


def test_vwap_accumulates_across_slices():
    out = replay_events([
        add("a", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        add("b", "AAA", 2, "s2", "SELL", "LIMIT", 6, 102),
        vwap_start("t", "AAA", 3, "p", "BUY", 10, [4, 6], "LIMIT", 100, price=102),
        slice_event("s1e", "AAA", 4, "p"),
        slice_event("s2e", "AAA", 5, "p"),
    ])
    final = out["results"][4]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert final["scheduled_quantity"] == 6
    assert final["filled_quantity"] == 10
    assert final["executed_notional"] == 4 * 100 + 6 * 102
    assert final["vwap"] == {"numerator": 4 * 100 + 6 * 102, "denominator": 10}
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [2]


def test_slice_respects_iceberg_replenishment():
    out = replay_events([
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 4, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        vwap_start("t", "AAA", 3, "p", "BUY", 4, [1], "LIMIT", 100, price=100),
        slice_event("s", "AAA", 4, "p"),
    ])
    assert [(t["maker_order_id"], t["quantity"]) for t in out["results"][3]["trades"]] == [
        ("i1", 2), ("s2", 1), ("i1", 1),
    ]


# ---------------------------------------------------------------------------
# Completion / cancellation / report lifecycle
# ---------------------------------------------------------------------------


def test_plan_completes_after_final_bucket_and_stays_reportable():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 2, [1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
    ])
    assert out["results"][3]["execution_plan"]["status"] == PLAN_COMPLETED
    report = out["results"][4]
    assert report["status"] == ACCEPTED
    assert report["execution_plan"]["status"] == PLAN_COMPLETED
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_cancel_counts_unreleased_quantity():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [3, 2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),                  # scheduled 6, all filled
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
        cancel_plan("e6", "AAA", 7, "p"),
    ])
    cancel = out["results"][3]["execution_plan"]
    assert cancel["status"] == PLAN_CANCELLED
    assert cancel["released_quantity"] == 6
    assert cancel["cancelled_quantity"] == 4
    assert cancel["remaining_slices"] == 0
    assert out["results"][4]["execution_plan"] == cancel
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][6]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_unknown_plan_commands_use_unknown_code():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        slice_event("e2", "AAA", 2, "ghost"),
        cancel_plan("e3", "AAA", 3, "ghost"),
    ])
    assert [r["rejection_code"] for r in out["results"]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]


# ---------------------------------------------------------------------------
# Shared plan-id namespace with TWAP
# ---------------------------------------------------------------------------


def test_vwap_and_twap_share_the_plan_id_namespace():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100),
        twap_start("e2", "AAA", 2, "p", "SELL", 2, 2, "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN


def test_twap_then_vwap_same_plan_id_duplicates():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 1, "MARKET", 100),
        vwap_start("e2", "AAA", 2, "p", "SELL", 2, [1, 1], "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN


def test_distinct_plan_ids_of_both_algorithms_coexist():
    out = replay_events([
        twap_start("e1", "AAA", 1, "tw", "BUY", 1, 1, "MARKET", 100),
        vwap_start("e2", "AAA", 2, "vw", "SELL", 1, [1], "MARKET", 100),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED, ACCEPTED]


def test_derived_id_clash_with_existing_order_rejects_vwap_start():
    out = replay_events([
        add("e0", "AAA", 1, "p#1", "SELL", "LIMIT", 1, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 2, [1, 1], "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    assert out["snapshot"]["content"]["symbols"][0]["state"]["plans"] == []


def test_cross_algorithm_derived_id_clash_rejects_second_start():
    # A TWAP plan reserves q#1; a VWAP plan with the same plan id derives the
    # same first child id.
    out = replay_events([
        twap_start("e1", "AAA", 1, "q", "BUY", 1, 1, "MARKET", 100),
        vwap_start("e2", "AAA", 2, "q", "BUY", 2, [1, 1], "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN


def test_released_child_id_cannot_be_reused_by_external_add():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 1, [1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        add("e3", "AAA", 4, "p#1", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][3]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_external_event_cannot_reuse_reserved_child_event_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 2, [1, 1], "LIMIT", 100, price=100),
        add("p#2", "AAA", 3, "other", "BUY", "LIMIT", 1, 100),
        slice_event("e4", "AAA", 4, "p"),
        slice_event("e5", "AAA", 5, "p"),
    ])
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EVENT_ID"
    assert out["results"][4]["execution_plan"]["child_order_id"] == "p#2"
    assert out["results"][4]["execution_plan"]["status"] == PLAN_COMPLETED


# ---------------------------------------------------------------------------
# Schema validation: INVALID_EVENT consumes neither id nor sequence
# ---------------------------------------------------------------------------


def start_base(eid="e1", **overrides):
    event = {
        "event_id": eid, "symbol": "AAA", "sequence": 1, "type": VWAP_START,
        "plan_id": "p1", "side": "BUY", "total_quantity": 4,
        "volume_weights": [1, 1], "order_type": "LIMIT",
        "benchmark_price": 100, "price": 100,
    }
    event.update(overrides)
    return event


@pytest.mark.parametrize("event", [
    start_base(plan_id=""),
    start_base(plan_id=3),
    start_base(side="ACROSS"),
    start_base(order_type="ICEBERG"),
    start_base(total_quantity=0),
    start_base(total_quantity=True),
    start_base(benchmark_price=0),
    start_base(benchmark_price=1.5),
    start_base(volume_weights=[]),                    # empty array
    start_base(volume_weights=[1, 0]),                # non-positive weight
    start_base(volume_weights=[1, -2]),
    start_base(volume_weights=[1, "2"]),              # non-integer weight
    start_base(volume_weights=[True, 1]),             # bool is not int
    start_base(total_quantity=1, volume_weights=[1, 1]),  # total < bucket count
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_START, "plan_id": "p1", "side": "BUY",
     "total_quantity": 4, "order_type": "LIMIT",
     "benchmark_price": 100, "price": 100},            # missing volume_weights
    start_base(price=0),                               # non-positive LIMIT price
    start_base(price="100"),
    start_base(order_type="MARKET", price=99),         # MARKET with price
    start_base(account_id=""),
    start_base(account_id=7),
    start_base(extra_field=1),
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_SLICE, "plan_id": "p1", "bogus": 1},
    {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": VWAP_CANCEL},
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_REPORT, "plan_id": ""},
])
def test_malformed_vwap_events_are_invalid(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


def test_market_vwap_explicit_null_price_is_accepted():
    event = start_base(order_type="MARKET", price=None)
    out = replay_events([event])
    assert out["results"][0]["status"] == ACCEPTED


def test_invalid_vwap_event_consumes_neither_sequence_nor_id():
    out = replay_events([
        start_base("e1", total_quantity=1),            # invalid (2 buckets)
        start_base("e1"),                              # same id/seq now valid
        slice_event("e2", "AAA", 2, "p1"),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][2]["status"] == ACCEPTED


def test_unknown_vwap_type_is_invalid_event():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "type": "VWAP_PAUSE", "plan_id": "p1"},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_nested_vwap_payload_is_supported():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": VWAP_START, "plan_id": "p1",
                   "side": "BUY", "total_quantity": 2, "volume_weights": [1, 1],
                   "order_type": "MARKET", "benchmark_price": 100}},
    ])
    assert out["results"][0]["status"] == ACCEPTED


def test_nested_vwap_event_id_mismatch_is_invalid():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "OTHER", "type": VWAP_SLICE, "plan_id": "p1"}},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_inline_baseline_event_cannot_smuggle_vwap_fields():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, volume_weights=[1]),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Business rejection occupancy and idempotency
# ---------------------------------------------------------------------------


def test_business_rejection_occupies_event_id_and_advances_sequence():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        report_plan("e1", "AAA", 3, "other-plan"),
        report_plan("e2", "AAA", 2, "ghost"),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_closed_plan_rejection_occupies_sequence():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
        slice_event("e2", "AAA", 2, "p"),
        slice_event("e3", "AAA", 3, "p"),                 # closed, occupies seq
        add("e4", "AAA", 4, "o1", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][2]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][3]["status"] == ACCEPTED


def test_retried_slice_delivery_is_recognized_as_duplicate():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        vwap_start("t", "AAA", 2, "p", "BUY", 2, [1], "LIMIT", 100, price=100),
        slice_event("s", "AAA", 3, "p"),
        slice_event("s", "AAA", 3, "p"),
        report_plan("r", "AAA", 4, "p"),
    ])
    assert out["results"][3]["status"] == DUPLICATE
    assert out["results"][4]["execution_plan"]["filled_quantity"] == 2


# ---------------------------------------------------------------------------
# Multi-symbol isolation and determinism
# ---------------------------------------------------------------------------


def test_vwap_plans_are_per_symbol():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
        vwap_start("e2", "BBB", 1, "p", "SELL", 1, [1], "MARKET", 100),
        slice_event("e3", "AAA", 2, "p"),
        slice_event("e4", "BBB", 2, "p"),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED] * 4
    symbols = {s["symbol"] for s in out["snapshot"]["content"]["symbols"]}
    assert symbols == {"AAA", "BBB"}


def _full_vwap_stream():
    return [
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 6, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        vwap_start("t1", "AAA", 3, "p", "BUY", 9, [5, 3, 1], "LIMIT", 99,
                   price=101, account_id="acct"),
        slice_event("t2", "AAA", 4, "p"),
        add("x1", "BBB", 1, "xb", "BUY", "LIMIT", 2, 50, account_id="z"),
        slice_event("t3", "AAA", 5, "p"),
        vwap_start("m1", "BBB", 2, "mp", "SELL", 2, [1, 1], "MARKET", 51),
        slice_event("m2", "BBB", 3, "mp"),
        cancel_plan("t4", "AAA", 6, "p"),
        report_plan("t5", "AAA", 7, "p"),
    ]


def test_vwap_output_is_byte_for_byte_deterministic():
    events = _full_vwap_stream()
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
    assert a == b

    def _no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float leaked into deterministic output")
        if isinstance(value, dict):
            for item in value.values():
                _no_floats(item)
        elif isinstance(value, list):
            for item in value:
                _no_floats(item)

    _no_floats(json.loads(a))


def test_snapshot_format_stays_version_2():
    snap = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
    ])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION == "event-replay/2"
    plan = snap["content"]["symbols"][0]["state"]["plans"][0]
    assert plan["algorithm"] == ALGORITHM_VWAP
    assert plan["target_weights"] == [1]


# ---------------------------------------------------------------------------
# Snapshot / resume equivalence and integrity
# ---------------------------------------------------------------------------


def test_resumed_vwap_matches_uninterrupted_run_exactly():
    events = _full_vwap_stream()
    cut = 5
    one_shot = replay_events(events)
    snapshot = replay_events(events[:cut])["snapshot"]
    segmented = replay_events(events[cut:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][cut:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_keeps_releasing_buckets_and_counters():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 20, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [3, 2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),                  # qty 6, trade id 1
    ]
    snapshot = replay_events(part1)["snapshot"]
    part2 = [slice_event("e3", "AAA", 4, "p")]            # qty 4, trade id 2
    out = replay_events(part2, snapshot=snapshot)
    assert [t["trade_id"] for t in out["results"][0]["trades"]] == [2]
    final = out["results"][0]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert (final["released_quantity"], final["filled_quantity"]) == (10, 10)


def test_cancelled_vwap_plan_survives_snapshot_and_stays_queryable():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [3, 2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        cancel_plan("e3", "AAA", 4, "p"),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
    ], snapshot=snapshot)
    plan = out["results"][0]["execution_plan"]
    assert plan["status"] == PLAN_CANCELLED
    assert plan["cancelled_quantity"] == 4
    assert out["results"][1]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_reserved_vwap_ids_remain_reserved_after_restore():
    snapshot = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 3, [1, 1, 1], "MARKET", 100),
    ])["snapshot"]
    out = replay_events([
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ], snapshot=snapshot)
    assert out["results"][0]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_vwap_snapshot_roundtrip_is_byte_stable():
    snapshot = replay_events(_full_vwap_stream())["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_tampered_vwap_target_weights_is_corrupt():
    good = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 10, [3, 2], "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("target_weights", [3, 9]),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_vwap_schedule_is_corrupt_even_with_recomputed_digest():
    from order_book_engine.event_replay import _digest

    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [3, 2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])["snapshot"]
    broken = copy.deepcopy(good)
    plan = broken["content"]["symbols"][0]["state"]["plans"][0]
    plan["slice_quantities"] = [9, 1]
    broken["content_digest"] = _digest(
        {k: v for k, v in broken.items() if k != "content_digest"}
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_dropping_target_weights_is_corrupt():
    good = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0].pop("target_weights"),
    )
    with pytest.raises(SnapshotError):
        restore_replayer(broken)


def test_stateful_replayer_runs_vwap_across_submissions():
    replayer = EventReplayer()
    replayer.submit([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 5, [1, 1], "LIMIT", 100, price=100),
    ])
    second = replayer.submit([slice_event("e2", "AAA", 3, "p")])
    assert second[0]["result"] == "FILLED"
    assert second[0]["execution_plan"]["remaining_slices"] == 1


# ---------------------------------------------------------------------------
# CLI
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


def test_cli_vwap_end_to_end_is_deterministic():
    request = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 4, [1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    assert out1["results"][2]["execution_plan"]["child_order_id"] == "p#1"
    assert out1["results"][2]["execution_plan"]["algorithm"] == ALGORITHM_VWAP
    assert out1["snapshot"]["format_version"] == "event-replay/2"


def test_cli_resumes_vwap_from_snapshot():
    first = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 4, [1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ]}
    _, out1, _ = _run_cli(first)
    second = {
        "events": [slice_event("e3", "AAA", 4, "p")],
        "snapshot": out1["snapshot"],
    }
    code, out2, _ = _run_cli(second)
    assert code == 0
    assert out2["results"][0]["execution_plan"]["status"] == PLAN_COMPLETED
    assert [t["trade_id"] for t in out2["results"][0]["trades"]] == [2]
