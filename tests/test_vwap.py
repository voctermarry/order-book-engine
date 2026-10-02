"""Tests for resumable VWAP parent orders in the multi-symbol event stream."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
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
    SEQUENCE_GAP,
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
        "volume_weights": volume_weights,
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


def vwap_cmd(event_id, symbol, sequence, cmd, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": cmd, "plan_id": plan_id}


def slice_event(eid, sym, seq, plan):
    return vwap_cmd(eid, sym, seq, VWAP_SLICE, plan)


def cancel_plan(eid, sym, seq, plan):
    return vwap_cmd(eid, sym, seq, VWAP_CANCEL, plan)


def report_plan(eid, sym, seq, plan):
    return vwap_cmd(eid, sym, seq, VWAP_REPORT, plan)


def plan_states(out, symbol_index=0):
    return out["snapshot"]["content"]["symbols"][symbol_index]["state"]["plans"]


# ---------------------------------------------------------------------------
# Volume-curve allocation
# ---------------------------------------------------------------------------


def test_allocation_distributes_by_integer_quotient_then_residual():
    # remainder 7 over weights 1:2:3 -> quotients 1/2/3, residuals 1/2/3;
    # the single leftover unit goes to the largest residual (bucket 3).
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 10, [1, 2, 3], "MARKET", 100),
    ])
    assert plan_states(out)[0]["slice_quantities"] == [2, 3, 5]


def test_equal_residuals_prefer_the_earlier_bucket():
    # remainder 2 over three equal weights -> all residuals tie at 2, so the
    # two leftover units go to the two earliest buckets.
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 5, [1, 1, 1], "MARKET", 100),
    ])
    assert plan_states(out)[0]["slice_quantities"] == [2, 2, 1]


def test_larger_residual_beats_earlier_bucket():
    # remainder 3 over weights 2:1:1 -> residuals 2/3/3, so the two leftover
    # units skip the earlier bucket and land on the larger residuals.
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 6, [2, 1, 1], "MARKET", 100),
    ])
    assert plan_states(out)[0]["slice_quantities"] == [2, 2, 2]


def test_exact_division_leaves_no_leftover():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 7, [1, 1, 2], "MARKET", 100),
    ])
    assert plan_states(out)[0]["slice_quantities"] == [2, 2, 3]


@pytest.mark.parametrize("total,weights", [
    (3, [1, 1, 1]),
    (10, [1, 2, 3]),
    (100, [7, 13, 29, 1]),
    (5, [4]),
    (11, [3, 3, 3, 3]),
    (8, [5, 1, 5, 1, 5]),
])
def test_slice_quantities_always_sum_to_total(total, weights):
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "SELL", total, weights, "MARKET", 100),
    ])
    quantities = plan_states(out)[0]["slice_quantities"]
    assert sum(quantities) == total
    assert len(quantities) == len(weights)
    assert all(q >= 1 for q in quantities)


# ---------------------------------------------------------------------------
# Start: no matching, summary shape, reservations
# ---------------------------------------------------------------------------


def test_vwap_start_does_not_match_and_echoes_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        vwap_start("e2", "AAA", 2, "p1", "BUY", 5, [2, 3], "LIMIT", 99, price=100),
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert "result" not in r
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    assert r["execution_plan"] == {
        "status": PLAN_ACTIVE,
        "released_quantity": 0,
        "filled_quantity": 0,
        "cancelled_quantity": 0,
        "remaining_slices": 2,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
        "algorithm": "VWAP",
    }
    # The start consumed no trade id: the engine counter still starts at one.
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    assert state["engine"]["next_trade_id"] == 1


def test_start_reserves_every_derived_child_id():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 3, [1, 1, 1], "MARKET", 100),
    ])
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    assert state["engine"]["reserved_order_ids"] == ["p#1", "p#2", "p#3"]


def test_market_plan_explicit_null_price_is_accepted():
    event = vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100)
    event["price"] = None
    out = replay_events([event])
    assert out["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Slice execution
# ---------------------------------------------------------------------------


def test_slice_releases_buckets_in_order_with_schedule_fields():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [1, 2, 3], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
        slice_event("e4", "AAA", 5, "p"),
    ])
    # Allocation [2, 3, 5] is released one bucket per slice event.
    for index, (quantity, weight) in enumerate([(2, 1), (3, 2), (5, 3)]):
        r = out["results"][2 + index]
        assert r["result"] == "FILLED"
        assert [t["quantity"] for t in r["trades"]] == [quantity]
        plan = r["execution_plan"]
        assert plan["slice_number"] == index + 1
        assert plan["child_order_id"] == f"p#{index + 1}"
        assert plan["target_weight"] == weight
        assert plan["scheduled_quantity"] == quantity
        assert plan["algorithm"] == "VWAP"
    assert out["results"][4]["execution_plan"]["status"] == PLAN_COMPLETED


def test_limit_slice_is_ioc_and_leftover_never_rests():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 3, [3], "LIMIT", 99, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [1]
    assert r["bids"] == []
    plan = r["execution_plan"]
    assert plan["released_quantity"] == 3
    assert plan["filled_quantity"] == 1
    assert plan["executed_notional"] == 100
    assert plan["vwap"] == {"numerator": 100, "denominator": 1}
    # 100*1 - 99*1 = 1 for a buy.
    assert plan["slippage_notional"] == 1


def test_market_slice_uses_ioc_semantics():
    out = replay_events([
        add("e0", "AAA", 1, "b1", "BUY", "LIMIT", 2, 100),
        vwap_start("e1", "AAA", 2, "p", "SELL", 5, [5], "MARKET", 101),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [2]
    plan = r["execution_plan"]
    assert plan["filled_quantity"] == 2
    assert plan["executed_notional"] == 200
    # Sell slippage negates: -(100*2 - 101*2) = 2 (improvement).
    assert plan["slippage_notional"] == 2


def test_vwap_metrics_accumulate_across_slices_at_different_prices():
    out = replay_events([
        add("a", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 102),
        vwap_start("t", "AAA", 3, "p", "BUY", 5, [2, 3], "LIMIT", 100, price=102),
        slice_event("s1e", "AAA", 4, "p"),
        slice_event("s2e", "AAA", 5, "p"),
    ])
    final = out["results"][4]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert final["remaining_slices"] == 0
    assert final["filled_quantity"] == 5
    assert final["executed_notional"] == 2 * 100 + 3 * 102
    assert final["vwap"] == {"numerator": 506, "denominator": 5}
    assert final["slippage_notional"] == 506 - 100 * 5
    # Trade ids run per symbol across the two slices.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [2]


def test_slice_matches_against_book_present_at_release_time():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "LIMIT", 100, price=100),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 100),
        slice_event("e3", "AAA", 3, "p"),
    ])
    assert out["results"][2]["result"] == "FILLED"
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["s1"]


def test_slice_self_trade_prevention_blocks_like_baseline():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        vwap_start("e1", "AAA", 2, "p", "BUY", 3, [3], "LIMIT", 100,
                   price=100, account_id="A"),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "SELF_TRADE_PREVENTED"
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    assert r["execution_plan"]["filled_quantity"] == 0


def test_book_changes_of_a_slice_list_drained_level_as_zero():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 2, [2], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])
    assert out["results"][2]["book_changes"] == {
        "bids": [], "asks": [{"price": 100, "quantity": 0}],
    }


# ---------------------------------------------------------------------------
# Shared plan-id namespace with TWAP
# ---------------------------------------------------------------------------


def test_vwap_and_twap_share_the_plan_id_namespace():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": TWAP_START,
         "plan_id": "p", "side": "BUY", "total_quantity": 1, "slice_count": 1,
         "order_type": "MARKET", "benchmark_price": 100},
        vwap_start("e2", "AAA", 2, "p", "SELL", 1, [1], "MARKET", 100),
        report_plan("e3", "AAA", 3, "p"),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    # The original TWAP plan is untouched and still queryable.
    assert len(plan_states(out)) == 1
    assert "algorithm" not in plan_states(out)[0]


def test_twap_start_conflicts_with_existing_vwap_plan():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
        {"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": TWAP_START,
         "plan_id": "p", "side": "SELL", "total_quantity": 1, "slice_count": 1,
         "order_type": "MARKET", "benchmark_price": 100},
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    assert len(plan_states(out)) == 1
    assert plan_states(out)[0]["algorithm"] == "VWAP"


def test_duplicate_vwap_plan_id_rejects_without_creating_second_plan():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100),
        vwap_start("e2", "AAA", 2, "p", "SELL", 2, [1, 1], "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    assert len(plan_states(out)) == 1
    assert plan_states(out)[0]["side"] == "BUY"


def test_derived_id_clash_with_existing_order_rejects_start():
    out = replay_events([
        add("e0", "AAA", 1, "p#2", "SELL", "LIMIT", 1, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 2, [1, 1], "MARKET", 100),
        add("e2", "AAA", 3, "other", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    # The rejected start created no plan or reservation; later events proceed.
    assert plan_states(out) == []
    assert out["results"][2]["status"] == ACCEPTED


def test_hash_scheme_derives_distinct_ids_across_algorithms():
    # The plan_id#slice scheme is injective for 1-based integer slices, so a
    # VWAP plan whose id looks like a TWAP derived id never collides with
    # that TWAP plan's reservations; only an external order can clash.
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": TWAP_START,
         "plan_id": "p", "side": "BUY", "total_quantity": 4, "slice_count": 2,
         "order_type": "MARKET", "benchmark_price": 100},
        vwap_start("e2", "AAA", 2, "p#2", "BUY", 2, [1, 1], "MARKET", 100),
    ])
    assert out["results"][1]["status"] == ACCEPTED
    reserved = out["snapshot"]["content"]["symbols"][0]["state"]["engine"][
        "reserved_order_ids"]
    assert reserved == ["p#1", "p#2", "p#2#1", "p#2#2"]


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


def test_external_add_cannot_reuse_reserved_child_order_id():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 3, [1, 1, 1], "MARKET", 100),
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


# ---------------------------------------------------------------------------
# Schema validation: INVALID_EVENT consumes nothing
# ---------------------------------------------------------------------------


def start_base(eid="e1", **overrides):
    event = {
        "event_id": eid, "symbol": "AAA", "sequence": 1, "type": VWAP_START,
        "plan_id": "p1", "side": "BUY", "total_quantity": 4,
        "volume_weights": [1, 3], "order_type": "LIMIT",
        "benchmark_price": 100, "price": 100,
    }
    event.update(overrides)
    return event


@pytest.mark.parametrize("event", [
    start_base(plan_id=""),                                   # empty plan id
    start_base(plan_id=3),                                    # non-string id
    start_base(side="ACROSS"),                                # bad side
    start_base(order_type="ICEBERG"),                         # unsupported type
    start_base(total_quantity=0),                             # non-positive
    start_base(total_quantity=True),                          # bool is not int
    start_base(volume_weights=[]),                            # empty weights
    start_base(volume_weights="1,3"),                         # non-list weights
    start_base(volume_weights=[1, 0]),                        # non-positive weight
    start_base(volume_weights=[1, True]),                     # bool is not int
    start_base(volume_weights=[1, 1.5]),                      # non-integer weight
    start_base(total_quantity=1, volume_weights=[1, 1]),      # total < buckets
    start_base(benchmark_price=0),
    start_base(benchmark_price=1.5),
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_START, "plan_id": "p1", "side": "BUY",
     "total_quantity": 4, "volume_weights": [1, 3], "order_type": "LIMIT",
     "benchmark_price": 100},                                 # LIMIT without price
    start_base(price=0),                                      # non-positive price
    start_base(price="100"),
    start_base(order_type="MARKET", price=99),                # MARKET with price
    start_base(account_id=""),                                # empty account
    start_base(account_id=7),
    start_base(slice_count=2),                                # TWAP-only field
    start_base(extra_field=1),                                # unknown field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_SLICE, "plan_id": "p1", "bogus": 1},        # ref with extra field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_CANCEL},                                    # ref missing plan_id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": VWAP_REPORT, "plan_id": ""},                     # empty ref id
])
def test_malformed_vwap_events_are_invalid(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


def test_invalid_vwap_event_consumes_neither_sequence_nor_id():
    out = replay_events([
        start_base("e1", total_quantity=1),                   # invalid: total < buckets
        start_base("e1"),                                     # same id/seq now valid
        slice_event("e2", "AAA", 2, "p1"),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][2]["status"] == ACCEPTED


def test_nested_vwap_payload_is_supported():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": VWAP_START, "plan_id": "p1",
                   "side": "BUY", "total_quantity": 2, "volume_weights": [1, 1],
                   "order_type": "MARKET", "benchmark_price": 100}},
    ])
    assert out["results"][0]["status"] == ACCEPTED


def test_inline_baseline_event_cannot_smuggle_vwap_fields():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, volume_weights=[1]),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Lifecycle: completion, cancellation, report, occupancy
# ---------------------------------------------------------------------------


def test_plan_completes_after_final_slice_and_is_still_queryable():
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
    assert "result" not in report
    assert report["trades"] == []
    assert report["execution_plan"]["status"] == PLAN_COMPLETED
    assert report["execution_plan"]["algorithm"] == "VWAP"
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_cancel_counts_unreleased_quantity_and_closes_plan():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 6, [1, 1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),                 # bucket qty 2
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
        cancel_plan("e6", "AAA", 7, "p"),
    ])
    cancel = out["results"][3]["execution_plan"]
    assert cancel["status"] == PLAN_CANCELLED
    assert cancel["released_quantity"] == 2
    assert cancel["filled_quantity"] == 2
    assert cancel["cancelled_quantity"] == 4
    assert cancel["remaining_slices"] == 0
    # Report returns the identical summary and changes nothing.
    assert out["results"][4]["execution_plan"] == cancel
    assert out["results"][4]["book_changes"] == {"bids": [], "asks": []}
    # Closed plan rejects further slices and cancels.
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][6]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_unknown_plan_codes():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        slice_event("e2", "AAA", 2, "ghost"),
        cancel_plan("e3", "AAA", 3, "ghost"),
    ])
    assert [r["rejection_code"] for r in out["results"]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]


def test_business_rejection_occupies_event_id_and_advances_sequence():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),               # occupies e1/seq1
        report_plan("e1", "AAA", 3, "other-plan"),          # same id, other content
        report_plan("e2", "AAA", 2, "ghost"),               # correct next seq
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_sequence_gap_vwap_event_consumes_nothing():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 2, "p"),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["status"] == ACCEPTED


def test_exact_duplicate_vwap_start_is_idempotent():
    first = vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100)
    out = replay_events([
        first,
        vwap_start("e1", "AAA", 1, "p", "BUY", 2, [1, 1], "MARKET", 100),
        report_plan("e2", "AAA", 2, "p"),
    ])
    assert out["results"][1]["status"] == DUPLICATE
    assert len(plan_states(out)) == 1


def test_retried_slice_delivery_is_recognized_as_duplicate():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        vwap_start("t", "AAA", 2, "p", "BUY", 2, [2], "LIMIT", 100, price=100),
        slice_event("s", "AAA", 3, "p"),
        slice_event("s", "AAA", 3, "p"),                     # retry, stale seq
        report_plan("r", "AAA", 4, "p"),
    ])
    assert out["results"][3]["status"] == DUPLICATE
    assert out["results"][4]["execution_plan"]["filled_quantity"] == 2


# ---------------------------------------------------------------------------
# Multi-symbol isolation
# ---------------------------------------------------------------------------


def test_plans_are_per_symbol_and_same_plan_id_is_fine_elsewhere():
    out = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 1, [1], "MARKET", 100),
        vwap_start("e2", "BBB", 1, "p", "SELL", 1, [1], "MARKET", 100),
        slice_event("e3", "AAA", 2, "p"),
        slice_event("e4", "BBB", 2, "p"),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED] * 4
    symbols = {s["symbol"] for s in out["snapshot"]["content"]["symbols"]}
    assert symbols == {"AAA", "BBB"}


# ---------------------------------------------------------------------------
# Determinism and snapshot format
# ---------------------------------------------------------------------------


def _full_vwap_stream():
    return [
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 6, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        vwap_start("t1", "AAA", 3, "p", "BUY", 9, [1, 2, 1], "LIMIT", 99,
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


def test_snapshot_format_stays_version_2_with_vwap_state():
    snap = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 3, [1, 2], "MARKET", 100),
    ])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION == "event-replay/2"
    state = snap["content"]["symbols"][0]["state"]
    assert set(state) == {"last_sequence", "price_limits", "event_log",
                          "plans", "engine"}
    assert state["price_limits"] is None
    plan = state["plans"][0]
    assert plan["algorithm"] == "VWAP"
    assert plan["volume_weights"] == [1, 2]
    assert plan["slice_quantities"] == [1, 2]
    assert state["engine"]["reserved_order_ids"] == ["p#1", "p#2"]


# ---------------------------------------------------------------------------
# Snapshot / resume equivalence
# ---------------------------------------------------------------------------


def test_resumed_vwap_matches_uninterrupted_run_exactly():
    events = _full_vwap_stream()
    cut = 5
    part1, part2 = events[:cut], events[cut:]
    one_shot = replay_events(events)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][cut:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_keeps_releasing_buckets_and_counters():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 6, [1, 1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),                 # qty 2, trade id 1
    ]
    snapshot = replay_events(part1)["snapshot"]
    part2 = [
        slice_event("e3", "AAA", 4, "p"),                 # qty 2, trade id 2
        slice_event("e4", "AAA", 5, "p"),                 # qty 2, trade id 3
    ]
    out = replay_events(part2, snapshot=snapshot)
    assert [t["trade_id"] for t in out["results"][0]["trades"]] == [2]
    final = out["results"][1]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert (final["released_quantity"], final["filled_quantity"]) == (6, 6)


def test_cancelled_vwap_plan_survives_snapshot_and_stays_queryable():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 4, [1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        cancel_plan("e3", "AAA", 4, "p"),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
    ], snapshot=snapshot)
    assert out["results"][0]["execution_plan"]["status"] == PLAN_CANCELLED
    assert out["results"][0]["execution_plan"]["cancelled_quantity"] == 2
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


def test_stateful_replayer_runs_vwap_across_submissions():
    replayer = EventReplayer()
    replayer.submit([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 5, [2, 3], "LIMIT", 100, price=100),
    ])
    second = replayer.submit([slice_event("e2", "AAA", 3, "p")])
    assert second[0]["result"] == "FILLED"
    plan = second[0]["execution_plan"]
    assert plan["remaining_slices"] == 1
    assert plan["scheduled_quantity"] == 2
    assert plan["target_weight"] == 2


# ---------------------------------------------------------------------------
# Snapshot integrity
# ---------------------------------------------------------------------------


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_tampered_vwap_plan_state_is_corrupt():
    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        vwap_start("e1", "AAA", 2, "p", "BUY", 4, [1, 1], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("filled_quantity", 99),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_allocation_mismatch_is_rejected_even_with_recomputed_digest():
    from order_book_engine.event_replay import _digest

    good = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 10, [1, 2, 3], "MARKET", 100),
    ])["snapshot"]
    broken = copy.deepcopy(good)
    # Reweight the buckets away from the deterministic allocation: this keeps
    # the sum and every per-field range check valid, so only the allocation
    # cross-check can catch it.
    plan = broken["content"]["symbols"][0]["state"]["plans"][0]
    plan["slice_quantities"] = [4, 3, 3]
    broken["content_digest"] = _digest(
        {k: v for k, v in broken.items() if k != "content_digest"}
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_vwap_weights_are_corrupt():
    good = replay_events([
        vwap_start("e1", "AAA", 1, "p", "BUY", 10, [1, 2, 3], "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("volume_weights", [3, 2, 1]),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


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
        vwap_start("e1", "AAA", 2, "p", "BUY", 4, [1, 3], "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    plan = out1["results"][2]["execution_plan"]
    assert plan["child_order_id"] == "p#1"
    assert plan["algorithm"] == "VWAP"
    assert plan["scheduled_quantity"] == 2
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
