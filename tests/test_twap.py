"""Tests for resumable TWAP parent orders in the multi-symbol event stream."""

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
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
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


def twap_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
    order_type, benchmark_price, price=None, account_id=None,
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
    if order_type == "LIMIT":
        event["price"] = price
    elif price is not None:
        event["price"] = price
    if account_id is not None:
        event["account_id"] = account_id
    return event


def twap_cmd(event_id, symbol, sequence, cmd, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": cmd, "plan_id": plan_id}


def slice_event(eid, sym, seq, plan):
    return twap_cmd(eid, sym, seq, TWAP_SLICE, plan)


def cancel_plan(eid, sym, seq, plan):
    return twap_cmd(eid, sym, seq, TWAP_CANCEL, plan)


def report_plan(eid, sym, seq, plan):
    return twap_cmd(eid, sym, seq, TWAP_REPORT, plan)


def statuses(out):
    return [(r["event_id"], r["status"], r.get("result") or r.get("rejection_code"))
            for r in out["results"]]


# ---------------------------------------------------------------------------
# Start: no matching, slice allocation, summary shape
# ---------------------------------------------------------------------------


def test_twap_start_does_not_match_and_echoes_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("e2", "AAA", 2, "p1", "BUY", 5, 2, "LIMIT", 99, price=100),
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert "result" not in r
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    plan = r["execution_plan"]
    assert plan == {
        "status": PLAN_ACTIVE,
        "released_quantity": 0,
        "filled_quantity": 0,
        "cancelled_quantity": 0,
        "remaining_slices": 2,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
    }
    # The start consumed no trade id: the engine counter still starts at one.
    assert out["snapshot"]["content"]["symbols"][0]["state"]["engine"]["next_trade_id"] == 1


def test_remainder_is_spread_over_the_earliest_slices():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
    ])
    assert [t["quantity"] for t in out["results"][2]["trades"]] == [3]
    assert [t["quantity"] for t in out["results"][3]["trades"]] == [2]
    plans = out["snapshot"]["content"]["symbols"][0]["state"]["plans"]
    assert plans[0]["slice_quantities"] == [3, 2]


def test_exact_division_gives_equal_slices():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 9, 3, "LIMIT", 100, price=100),
    ])
    plans = out["snapshot"]["content"]["symbols"][0]["state"]["plans"]
    assert plans[0]["slice_quantities"] == [3, 3, 3]


def test_slice_child_id_is_plan_hash_number():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p1", "BUY", 1, 1, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p1"),
    ])
    r = out["results"][2]
    assert r["execution_plan"]["slice_number"] == 1
    assert r["execution_plan"]["child_order_id"] == "p1#1"
    assert r["trades"][0]["taker_order_id"] == "p1#1"


# ---------------------------------------------------------------------------
# Slice execution: limit / market, partial fill, vwap and slippage
# ---------------------------------------------------------------------------


def test_limit_slice_is_ioc_and_leftover_never_rests():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 3, 1, "LIMIT", 99, price=100),
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


def test_unfilled_slice_has_null_vwap_and_plan_stays_active():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "SELL", 2, 2, "MARKET", 100),
        slice_event("e2", "AAA", 2, "p"),
    ])
    r = out["results"][1]
    assert r["result"] == "UNFILLED_CANCELLED"
    assert r["trades"] == []
    plan = r["execution_plan"]
    assert plan["status"] == PLAN_ACTIVE
    assert plan["vwap"] is None
    assert plan["released_quantity"] == 1
    assert plan["filled_quantity"] == 0
    assert plan["remaining_slices"] == 1


def test_market_slice_uses_ioc_semantics():
    out = replay_events([
        add("e0", "AAA", 1, "b1", "BUY", "LIMIT", 2, 100),
        twap_start("e1", "AAA", 2, "p", "SELL", 5, 1, "MARKET", 101),
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


def test_vwap_accumulates_across_slices_at_different_prices():
    out = replay_events([
        add("a", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("b", "AAA", 2, "s2", "SELL", "LIMIT", 2, 102),
        twap_start("t", "AAA", 3, "p", "BUY", 4, 2, "LIMIT", 100, price=102),
        slice_event("s1e", "AAA", 4, "p"),
        slice_event("s2e", "AAA", 5, "p"),
    ])
    final = out["results"][4]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert final["remaining_slices"] == 0
    assert final["filled_quantity"] == 4
    assert final["executed_notional"] == 2 * 100 + 2 * 102
    assert final["vwap"] == {"numerator": 404, "denominator": 4}
    assert final["slippage_notional"] == 404 - 100 * 4
    # Each slice is one trade against a single maker; ids run per symbol.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [2]


def test_slice_matches_against_book_present_at_release_time():
    # The plan starts against an empty book; liquidity arrives before release.
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "LIMIT", 100, price=100),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 100),
        slice_event("e3", "AAA", 3, "p"),
    ])
    assert out["results"][2]["result"] == "FILLED"
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["s1"]


def test_slice_respects_iceberg_replenishment_and_stp_rules():
    out = replay_events([
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 4, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        twap_start("t", "AAA", 3, "p", "BUY", 4, 1, "LIMIT", 100, price=100),
        slice_event("s", "AAA", 4, "p"),
    ])
    # First iceberg slice (2), then s2 (1), then the replenished slice (1).
    assert [(t["maker_order_id"], t["quantity"]) for t in out["results"][3]["trades"]] == [
        ("i1", 2), ("s2", 1), ("i1", 1),
    ]


def test_slice_self_trade_prevention_blocks_like_baseline():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        twap_start("e1", "AAA", 2, "p", "BUY", 3, 1, "LIMIT", 100,
                   price=100, account_id="A"),
        slice_event("e2", "AAA", 3, "p"),
    ])
    r = out["results"][2]
    assert r["result"] == "SELF_TRADE_PREVENTED"
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    assert r["execution_plan"]["filled_quantity"] == 0


def test_slice_without_account_does_not_trigger_stp():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="A"),
        twap_start("e1", "AAA", 2, "p", "BUY", 1, 1, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])
    assert out["results"][2]["result"] == "FILLED"


def test_book_changes_of_a_slice_list_drained_level_as_zero():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 1, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])
    changes = out["results"][2]["book_changes"]
    assert changes == {"bids": [], "asks": [{"price": 100, "quantity": 0}]}


# ---------------------------------------------------------------------------
# Completion / cancellation / report
# ---------------------------------------------------------------------------


def test_plan_completes_after_final_slice_and_is_still_queryable():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 2, "LIMIT", 100, price=100),
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
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_cancel_counts_unreleased_quantity_and_closes_plan():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 3, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),                 # slice qty 2
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        slice_event("e5", "AAA", 6, "p"),
        cancel_plan("e6", "AAA", 7, "p"),
    ])
    cancel = out["results"][3]["execution_plan"]
    assert cancel["status"] == PLAN_CANCELLED
    assert cancel["released_quantity"] == 2
    assert cancel["filled_quantity"] == 2
    assert cancel["cancelled_quantity"] == 3
    assert cancel["remaining_slices"] == 0
    # Report returns the identical summary and changes nothing.
    assert out["results"][4]["execution_plan"] == cancel
    assert out["results"][4]["book_changes"] == {"bids": [], "asks": []}
    # Closed plan rejects further slices and cancels.
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][6]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_cancel_does_not_change_book_or_trade_numbers():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 3, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        cancel_plan("e3", "AAA", 4, "p"),
    ])
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    # Only the one slice traded; the cancel spent no trade id.
    assert state["engine"]["next_trade_id"] == 2
    assert state["engine"]["asks"] == [{"price": 100, "order_ids": ["s1"]}]


def test_report_then_cancel_and_unknown_plan_codes():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        slice_event("e2", "AAA", 2, "ghost"),
        cancel_plan("e3", "AAA", 3, "ghost"),
    ])
    assert [r["rejection_code"] for r in out["results"]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]


# ---------------------------------------------------------------------------
# Schema validation: INVALID_EVENT consumes nothing
# ---------------------------------------------------------------------------


def start_base(eid="e1", **overrides):
    event = {
        "event_id": eid, "symbol": "AAA", "sequence": 1, "type": TWAP_START,
        "plan_id": "p1", "side": "BUY", "total_quantity": 4, "slice_count": 2,
        "order_type": "LIMIT", "benchmark_price": 100, "price": 100,
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
    start_base(slice_count=0),
    start_base(slice_count="2"),
    start_base(benchmark_price=0),
    start_base(benchmark_price=1.5),
    start_base(total_quantity=1, slice_count=2),              # total < slices
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": TWAP_START, "plan_id": "p1", "side": "BUY",
     "total_quantity": 4, "slice_count": 2, "order_type": "LIMIT",
     "benchmark_price": 100},                                 # LIMIT without price
    start_base(price=0),                                      # non-positive price
    start_base(price="100"),
    start_base(order_type="MARKET", price=99),                # MARKET with price
    start_base(account_id=""),                                # empty account
    start_base(account_id=7),
    start_base(extra_field=1),                                # unknown field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": TWAP_SLICE, "plan_id": "p1", "bogus": 1},        # ref with extra field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": TWAP_CANCEL},                                    # ref missing plan_id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": TWAP_REPORT, "plan_id": ""},                     # empty ref id
])
def test_malformed_twap_events_are_invalid(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


def test_market_plan_explicit_null_price_is_accepted():
    event = start_base(order_type="MARKET", price=None)
    out = replay_events([event])
    assert out["results"][0]["status"] == ACCEPTED


def test_invalid_twap_event_consumes_neither_sequence_nor_id():
    out = replay_events([
        start_base("e1", total_quantity=1, slice_count=9),     # invalid
        start_base("e1"),                                      # same id/seq now valid
        slice_event("e2", "AAA", 2, "p1"),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][2]["status"] == ACCEPTED


def test_unknown_twap_type_is_invalid_event():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "type": "TWAP_PAUSE", "plan_id": "p1"},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_nested_twap_payload_is_supported():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": TWAP_START, "plan_id": "p1",
                   "side": "BUY", "total_quantity": 2, "slice_count": 1,
                   "order_type": "MARKET", "benchmark_price": 100}},
    ])
    assert out["results"][0]["status"] == ACCEPTED


def test_nested_twap_event_id_mismatch_is_invalid():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "OTHER", "type": TWAP_SLICE, "plan_id": "p1"}},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_inline_baseline_event_cannot_smuggle_twap_fields():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, plan_id="p1"),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Business rejection occupancy and plan-id rules
# ---------------------------------------------------------------------------


def test_duplicate_plan_id_rejects_without_creating_second_plan():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 1, "MARKET", 100),
        twap_start("e2", "AAA", 2, "p", "SELL", 2, 1, "MARKET", 100),
        report_plan("e3", "AAA", 3, "p"),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    # The original plan (a BUY) is untouched and still queryable.
    snap = out["snapshot"]["content"]["symbols"][0]["state"]
    assert len(snap["plans"]) == 1
    assert snap["plans"][0]["side"] == "BUY"


def test_derived_id_clash_with_existing_order_rejects_start():
    out = replay_events([
        add("e0", "AAA", 1, "p#1", "SELL", "LIMIT", 1, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 2, "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    # No plan was registered.
    assert out["snapshot"]["content"]["symbols"][0]["state"]["plans"] == []


def test_hash_scheme_derives_distinct_ids_across_hash_named_plans():
    # The plan_id#slice scheme (slice is the part after the last '#') is
    # injective for 1-based integer slices, so plans with '#' in their ids
    # never collide with each other; only an external order can clash.
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "MARKET", 100),
        twap_start("e2", "AAA", 2, "p#1", "BUY", 1, 1, "MARKET", 100),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED, ACCEPTED]
    reserved = out["snapshot"]["content"]["symbols"][0]["state"]["engine"][
        "reserved_order_ids"]
    assert reserved == ["p#1", "p#1#1"]


def test_external_order_clash_with_any_reserved_slice_rejects_start():
    # An external order already spent "p#2"; the new plan reserves that id for
    # its second slice, so the start must be rejected before registration.
    out = replay_events([
        add("e0", "AAA", 1, "p#2", "SELL", "LIMIT", 1, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 2, "MARKET", 100),
        add("e2", "AAA", 3, "other", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    # The rejected start created no plan or reservation; later events proceed.
    assert out["snapshot"]["content"]["symbols"][0]["state"]["plans"] == []
    assert out["results"][2]["status"] == ACCEPTED


def test_released_child_id_cannot_be_reused_by_external_add():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 1, 1, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        add("e3", "AAA", 4, "p#1", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][3]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_external_event_cannot_reuse_reserved_child_event_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 2, "LIMIT", 100, price=100),
        add("p#2", "AAA", 3, "other", "BUY", "LIMIT", 1, 100),
        slice_event("e4", "AAA", 4, "p"),
        slice_event("e5", "AAA", 5, "p"),
    ])
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EVENT_ID"
    # Both slices still execute normally; the rejected event changed nothing.
    assert out["results"][3]["execution_plan"]["child_order_id"] == "p#1"
    assert out["results"][4]["execution_plan"]["child_order_id"] == "p#2"
    assert out["results"][4]["execution_plan"]["status"] == PLAN_COMPLETED


def test_twap_command_cannot_reuse_reserved_child_event_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 2, 2, "LIMIT", 100, price=100),
        report_plan("p#2", "AAA", 3, "p"),
        slice_event("e4", "AAA", 4, "p"),
        slice_event("e5", "AAA", 5, "p"),
    ])
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EVENT_ID"
    # The child can still be released under its reserved id afterwards.
    assert out["results"][4]["execution_plan"]["child_order_id"] == "p#2"


def test_twap_events_accept_optional_timestamp_without_reordering():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "timestamp": "t-100",
         "type": TWAP_START, "plan_id": "p", "side": "BUY", "total_quantity": 1,
         "slice_count": 1, "order_type": "MARKET", "benchmark_price": 100},
        {"event_id": "e2", "symbol": "AAA", "sequence": 2, "timestamp": 5,
         "type": TWAP_SLICE, "plan_id": "p"},
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED, ACCEPTED]


def test_business_rejection_occupies_event_id_and_advances_sequence():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),               # occupies e1/seq1
        report_plan("e1", "AAA", 3, "other-plan"),         # same id, other content
        report_plan("e2", "AAA", 2, "ghost"),              # correct next seq
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["status"] == REJECTED
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_closed_plan_rejection_occupies_sequence():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "MARKET", 100),
        slice_event("e2", "AAA", 2, "p"),                    # completes
        slice_event("e3", "AAA", 3, "p"),                    # closed, occupies seq
        add("e4", "AAA", 4, "o1", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][2]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][3]["status"] == ACCEPTED


def test_sequence_gap_twap_event_consumes_nothing():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "MARKET", 100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 2, "p"),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_exact_duplicate_twap_start_is_idempotent():
    first = twap_start("e1", "AAA", 1, "p", "BUY", 2, 2, "MARKET", 100)
    out = replay_events([
        first,
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 2, "MARKET", 100),
        report_plan("e2", "AAA", 2, "p"),
    ])
    assert out["results"][1]["status"] == DUPLICATE
    assert out["results"][1]["trades"] == []
    # Only one plan exists.
    assert len(out["snapshot"]["content"]["symbols"][0]["state"]["plans"]) == 1


def test_same_twap_event_id_with_different_plan_content_conflicts():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 2, "MARKET", 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 3, 3, "MARKET", 100),
        report_plan("e2", "AAA", 2, "p"),
    ])
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["status"] == ACCEPTED


def test_retried_slice_delivery_is_recognized_as_duplicate():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        twap_start("t", "AAA", 2, "p", "BUY", 2, 1, "LIMIT", 100, price=100),
        slice_event("s", "AAA", 3, "p"),
        slice_event("s", "AAA", 3, "p"),                     # retry, stale seq
        report_plan("r", "AAA", 4, "p"),
    ])
    dup = out["results"][3]
    assert dup["status"] == DUPLICATE
    assert dup["trades"] == []
    # The slice filled exactly once.
    assert out["results"][4]["execution_plan"]["filled_quantity"] == 2


# ---------------------------------------------------------------------------
# Multi-symbol isolation
# ---------------------------------------------------------------------------


def test_plans_are_per_symbol_and_same_plan_id_is_fine_elsewhere():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "MARKET", 100),
        twap_start("e2", "BBB", 1, "p", "SELL", 1, 1, "MARKET", 100),
        slice_event("e3", "AAA", 2, "p"),
        slice_event("e4", "BBB", 2, "p"),
    ])
    assert statuses(out) == [
        ("e1", ACCEPTED, None),
        ("e2", ACCEPTED, None),
        ("e3", ACCEPTED, "UNFILLED_CANCELLED"),
        ("e4", ACCEPTED, "UNFILLED_CANCELLED"),
    ]
    symbols = {s["symbol"] for s in out["snapshot"]["content"]["symbols"]}
    assert symbols == {"AAA", "BBB"}


def test_twap_trade_ids_are_per_symbol():
    out = replay_events([
        add("a0", "AAA", 1, "sa", "SELL", "LIMIT", 1, 100),
        add("b0", "BBB", 1, "sb", "SELL", "LIMIT", 1, 50),
        twap_start("ta", "AAA", 2, "pa", "BUY", 1, 1, "LIMIT", 100, price=100),
        twap_start("tb", "BBB", 2, "pb", "BUY", 1, 1, "LIMIT", 50, price=50),
        slice_event("x", "AAA", 3, "pa"),
        slice_event("y", "BBB", 3, "pb"),
    ])
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [1]
    assert [t["trade_id"] for t in out["results"][5]["trades"]] == [1]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def _full_twap_stream():
    return [
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 6, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        twap_start("t1", "AAA", 3, "p", "BUY", 7, 3, "LIMIT", 99,
                   price=101, account_id="acct"),
        slice_event("t2", "AAA", 4, "p"),
        add("x1", "BBB", 1, "xb", "BUY", "LIMIT", 2, 50, account_id="z"),
        slice_event("t3", "AAA", 5, "p"),
        twap_start("m1", "BBB", 2, "mp", "SELL", 2, 1, "MARKET", 51),
        slice_event("m2", "BBB", 3, "mp"),
        cancel_plan("t4", "AAA", 6, "p"),
        report_plan("t5", "AAA", 7, "p"),
    ]


def test_twap_output_is_byte_for_byte_deterministic():
    events = _full_twap_stream()
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


def test_snapshot_format_is_version_2():
    snap = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 1, 1, "MARKET", 100),
    ])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION == "event-replay/2"
    state = snap["content"]["symbols"][0]["state"]
    assert set(state) == {"last_sequence", "event_log", "plans", "engine"}
    assert "reserved_order_ids" in state["engine"]
    assert state["engine"]["reserved_order_ids"] == ["p#1"]


# ---------------------------------------------------------------------------
# Snapshot / resume equivalence
# ---------------------------------------------------------------------------


def test_resumed_twap_matches_uninterrupted_run_exactly():
    events = _full_twap_stream()
    cut = 5
    part1, part2 = events[:cut], events[cut:]
    one_shot = replay_events(events)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][cut:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_keeps_releasing_slices_and_counters():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 6, 3, "LIMIT", 100, price=100),
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


def test_cancelled_plan_survives_snapshot_and_stays_queryable():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 4, 2, "LIMIT", 100, price=100),
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


def test_reserved_ids_remain_reserved_after_restore():
    part1 = [
        twap_start("e1", "AAA", 1, "p", "BUY", 3, 3, "MARKET", 100),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ], snapshot=snapshot)
    assert out["results"][0]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_snapshot_roundtrip_is_byte_stable():
    events = _full_twap_stream()
    snapshot = replay_events(events)["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)


def test_stateful_replayer_runs_twap_across_submissions():
    replayer = EventReplayer()
    replayer.submit([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "LIMIT", 100, price=100),
    ])
    second = replayer.submit([slice_event("e2", "AAA", 3, "p")])
    assert second[0]["result"] == "FILLED"
    assert second[0]["execution_plan"]["remaining_slices"] == 1


# ---------------------------------------------------------------------------
# Snapshot integrity
# ---------------------------------------------------------------------------


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_tampered_plan_state_is_corrupt():
    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 4, 2, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])["snapshot"]
    # Mutate a plan counter so it disagrees with the trade journal.
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("filled_quantity", 99),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_reserved_ids_are_corrupt():
    good = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 2, "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["engine"]
        .__setitem__("reserved_order_ids", ["p#9"]),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_digest_mismatch_on_twap_snapshot_is_corrupt():
    good = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 2, 2, "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("benchmark_price", 1),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_plan_journal_mismatch_is_rejected_even_with_recomputed_digest():
    from order_book_engine.event_replay import _digest

    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 4, 2, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
    ])["snapshot"]
    broken = copy.deepcopy(good)
    # Claim a smaller fill than the single child trade records: this passes the
    # plan's own range checks but must be caught by the trade-journal
    # cross-validation.
    broken["content"]["symbols"][0]["state"]["plans"][0]["filled_quantity"] = 1
    broken["content_digest"] = _digest(
        {k: v for k, v in broken.items() if k != "content_digest"}
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


def test_cli_twap_end_to_end_is_deterministic():
    request = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 4, 2, "LIMIT", 100, price=100),
        slice_event("e2", "AAA", 3, "p"),
        slice_event("e3", "AAA", 4, "p"),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    assert out1["results"][2]["execution_plan"]["child_order_id"] == "p#1"
    assert out1["snapshot"]["format_version"] == "event-replay/2"


def test_cli_resumes_twap_from_snapshot():
    first = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e1", "AAA", 2, "p", "BUY", 4, 2, "LIMIT", 100, price=100),
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
