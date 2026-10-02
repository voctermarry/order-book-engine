"""Tests for resumable POV parent orders in the multi-symbol event stream."""

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
    TWAP_START,
    TWAP_SLICE,
    UNKNOWN_EXECUTION_PLAN,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    EventReplayer,
    SnapshotError,
    VWAP_SLICE,
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


def pov_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, participation_bps,
    order_type, benchmark_price, price=None, account_id=None,
):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": POV_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "participation_bps": participation_bps,
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


def volume_event(eid, sym, seq, plan, increment):
    return {
        "event_id": eid, "symbol": sym, "sequence": seq,
        "type": POV_VOLUME, "plan_id": plan,
        "market_volume_increment": increment,
    }


def cancel_plan(eid, sym, seq, plan):
    return {"event_id": eid, "symbol": sym, "sequence": seq,
            "type": POV_CANCEL, "plan_id": plan}


def report_plan(eid, sym, seq, plan):
    return {"event_id": eid, "symbol": sym, "sequence": seq,
            "type": POV_REPORT, "plan_id": plan}


def limit_update(eid, sym, seq, lower, upper):
    return {"event_id": eid, "symbol": sym, "sequence": seq,
            "type": PRICE_LIMIT_UPDATE,
            "lower_price": lower, "upper_price": upper}


def plan_states(out, symbol_index=0):
    return out["snapshot"]["content"]["symbols"][symbol_index]["state"]["plans"]


def symbol_state(response_or_snapshot, symbol):
    snapshot = response_or_snapshot.get("snapshot", response_or_snapshot)
    for entry in snapshot["content"]["symbols"]:
        if entry["symbol"] == symbol:
            return entry["state"]
    raise KeyError(symbol)


# ---------------------------------------------------------------------------
# Participation target arithmetic
# ---------------------------------------------------------------------------


def test_target_is_floor_of_cumulative_volume_times_bps():
    # 10% of 25 = 2.5 -> floor 2 on the first event; cumulative 40 -> 4 next.
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 10, 1000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 25),
        volume_event("e3", "AAA", 4, "p", 15),
    ])
    first, second = out["results"][2]["execution_plan"], out["results"][3]["execution_plan"]
    assert first["release_number"] == 1
    assert first["child_order_id"] == "p#1"
    assert first["released_quantity"] == 2
    assert second["release_number"] == 2
    assert second["child_order_id"] == "p#2"
    assert second["released_quantity"] == 4


def test_target_is_capped_at_total_quantity():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 1_000_000),
    ])
    plan = out["results"][2]["execution_plan"]
    assert plan["status"] == PLAN_COMPLETED
    assert plan["released_quantity"] == 3
    assert plan["unreleased_quantity"] == 0
    # Only one child order exists despite the huge market volume.
    assert [t["quantity"] for t in out["results"][2]["trades"]] == [3]


def test_releases_accumulate_until_the_target_catches_up():
    # bps 1000 (10%): volumes 4 (target 0), 6 (cum 10 -> 1), 50 (cum 60 -> 6).
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 10, 1000, "LIMIT", 100, price=100),
        volume_event("v0", "AAA", 3, "p", 4),
        volume_event("v1", "AAA", 4, "p", 6),
        volume_event("v2", "AAA", 5, "p", 50),
    ])
    zero, first, second = (out["results"][i]["execution_plan"] for i in (2, 3, 4))
    assert zero["released_quantity"] == 0
    assert zero["child_order_id"] is None
    assert "release_number" not in zero
    assert first["released_quantity"] == 1
    assert first["release_number"] == 1
    assert first["child_order_id"] == "p#1"
    assert second["released_quantity"] == 6
    assert second["release_number"] == 2
    assert second["child_order_id"] == "p#2"
    assert second["status"] == PLAN_ACTIVE


def test_full_participation_releases_one_unit_per_volume_unit():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 100, price=100),
        volume_event("v1", "AAA", 3, "p", 1),
        volume_event("v2", "AAA", 4, "p", 1),
        volume_event("v3", "AAA", 5, "p", 1),
    ])
    for index, result_index in enumerate((2, 3, 4)):
        r = out["results"][result_index]
        assert r["result"] == "FILLED"
        plan = r["execution_plan"]
        assert plan["release_number"] == index + 1
        assert plan["child_order_id"] == f"p#{index + 1}"
    assert out["results"][4]["execution_plan"]["status"] == PLAN_COMPLETED


# ---------------------------------------------------------------------------
# Start: no matching, summary shape, reservations
# ---------------------------------------------------------------------------


def test_pov_start_does_not_match_and_echoes_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e2", "AAA", 2, "p1", "BUY", 5, 2000, "LIMIT", 99, price=100),
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
        "unreleased_quantity": 5,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
        "algorithm": "POV",
    }
    # The start consumed no trade id and reserves one derived id per unit.
    state = symbol_state(out, "AAA")
    assert state["engine"]["next_trade_id"] == 1
    assert state["engine"]["reserved_order_ids"] == ["p1#1", "p1#2", "p1#3", "p1#4", "p1#5"]


def test_pov_summary_uses_unreleased_quantity_not_remaining_slices():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 4, 5000, "MARKET", 100),
    ])
    plan = out["results"][0]["execution_plan"]
    assert "remaining_slices" not in plan
    assert plan["unreleased_quantity"] == 4
    assert plan["algorithm"] == "POV"


def test_market_plan_explicit_null_price_is_accepted():
    event = pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100)
    event["price"] = None
    out = replay_events([event])
    assert out["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Zero-volume releases
# ---------------------------------------------------------------------------


def test_zero_release_succeeds_without_child_order_or_trade_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 5, 1, "LIMIT", 100, price=100),
        volume_event("v1", "AAA", 3, "p", 5000),       # floor(0.5) = 0
    ])
    r = out["results"][2]
    assert r["status"] == ACCEPTED
    assert "result" not in r
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    plan = r["execution_plan"]
    assert plan["released_quantity"] == 0
    assert plan["filled_quantity"] == 0
    assert plan["unreleased_quantity"] == 5
    assert plan["child_order_id"] is None
    assert "release_number" not in plan
    # The market volume was retained even though nothing was released.
    assert plan_states(out)[0]["market_volume"] == 5000
    assert symbol_state(out, "AAA")["engine"]["next_trade_id"] == 1


def test_first_positive_release_after_zero_events_is_child_number_one():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 5, 1, "LIMIT", 100, price=100),
        volume_event("v1", "AAA", 3, "p", 5000),       # zero
        volume_event("v2", "AAA", 4, "p", 5000),       # cumulative 10000 -> 1
    ])
    r = out["results"][3]
    assert r["result"] == "FILLED"
    plan = r["execution_plan"]
    assert plan["release_number"] == 1
    assert plan["child_order_id"] == "p#1"
    assert plan["released_quantity"] == 1
    assert [t["trade_id"] for t in r["trades"]] == [1]


def test_consecutive_zero_events_never_carry_a_release_number():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 5, 1000, "MARKET", 100),
        volume_event("v1", "AAA", 2, "p", 1),
        volume_event("v2", "AAA", 3, "p", 1),
    ])
    for index in (1, 2):
        plan = out["results"][index]["execution_plan"]
        assert plan["child_order_id"] is None
        assert "release_number" not in plan


# ---------------------------------------------------------------------------
# Child order execution
# ---------------------------------------------------------------------------


def test_limit_release_is_ioc_and_leftover_never_rests():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 99, price=100),
        volume_event("v1", "AAA", 3, "p", 3),
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


def test_market_release_uses_ioc_semantics():
    out = replay_events([
        add("e0", "AAA", 1, "b1", "BUY", "LIMIT", 2, 100),
        pov_start("e1", "AAA", 2, "p", "SELL", 5, 10000, "MARKET", 101),
        volume_event("v1", "AAA", 3, "p", 5),
    ])
    r = out["results"][2]
    assert r["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [2]
    plan = r["execution_plan"]
    assert plan["filled_quantity"] == 2
    assert plan["executed_notional"] == 200
    # Sell slippage negates: -(100*2 - 101*2) = 2 (improvement).
    assert plan["slippage_notional"] == 2


def test_metrics_accumulate_across_releases_at_different_prices():
    out = replay_events([
        add("a", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 102),
        pov_start("t", "AAA", 3, "p", "BUY", 5, 10000, "LIMIT", 100, price=102),
        volume_event("r1", "AAA", 4, "p", 2),
        volume_event("r2", "AAA", 5, "p", 3),
    ])
    final = out["results"][4]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert final["filled_quantity"] == 5
    assert final["executed_notional"] == 2 * 100 + 3 * 102
    assert final["vwap"] == {"numerator": 506, "denominator": 5}
    assert final["slippage_notional"] == 506 - 100 * 5
    # Trade ids run per symbol across releases.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [2]


def test_release_matches_against_book_present_at_release_time():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 10000, "LIMIT", 100, price=100),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 100),
        volume_event("e3", "AAA", 3, "p", 1),
    ])
    assert out["results"][2]["result"] == "FILLED"
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["s1"]


def test_release_self_trade_prevention_blocks_like_baseline():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 100,
                  price=100, account_id="A"),
        volume_event("e2", "AAA", 3, "p", 3),
    ])
    r = out["results"][2]
    assert r["result"] == "SELF_TRADE_PREVENTED"
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    plan = r["execution_plan"]
    assert plan["filled_quantity"] == 0
    # The blocked quantity still counts as released; the child order is spent.
    assert plan["released_quantity"] == 3


def test_book_changes_of_a_release_list_drained_level_as_zero():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 2),
    ])
    assert out["results"][2]["book_changes"] == {
        "bids": [], "asks": [{"price": 100, "quantity": 0}],
    }


def test_unfilled_market_release_spends_no_trade_id():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 10000, "MARKET", 100),
        volume_event("e2", "AAA", 2, "p", 3),
        add("e3", "AAA", 3, "s1", "SELL", "LIMIT", 1, 100),
        add("e4", "AAA", 4, "b1", "BUY", "MARKET", 1),
    ])
    assert out["results"][1]["result"] == "UNFILLED_CANCELLED"
    # The unfilled POV child spent no trade id; the later market order gets id 1.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]


# ---------------------------------------------------------------------------
# Shared plan-id namespace and derived identifiers
# ---------------------------------------------------------------------------


def test_pov_and_twap_share_the_plan_id_namespace():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": TWAP_START,
         "plan_id": "p", "side": "BUY", "total_quantity": 1, "slice_count": 1,
         "order_type": "MARKET", "benchmark_price": 100},
        pov_start("e2", "AAA", 2, "p", "SELL", 1, 5000, "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    assert len(plan_states(out)) == 1
    assert "algorithm" not in plan_states(out)[0]


def test_duplicate_pov_plan_id_rejects_without_creating_second_plan():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100),
        pov_start("e2", "AAA", 2, "p", "SELL", 2, 5000, "MARKET", 100),
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    assert len(plan_states(out)) == 1
    assert plan_states(out)[0]["side"] == "BUY"


def test_derived_id_clash_with_existing_order_rejects_start():
    out = replay_events([
        add("e0", "AAA", 1, "p#2", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 5000, "MARKET", 100),
        add("e2", "AAA", 3, "other", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    # The rejected start created no plan or reservation; later events proceed.
    assert plan_states(out) == []
    assert out["results"][2]["status"] == ACCEPTED


def test_external_event_cannot_reuse_reserved_child_event_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 2500, "LIMIT", 100, price=100),
        add("p#3", "AAA", 3, "other", "BUY", "LIMIT", 1, 100),
        volume_event("e4", "AAA", 4, "p", 8),                # releases p#1 qty 2
        volume_event("e5", "AAA", 5, "p", 8),                # releases p#2 qty 2
    ])
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EVENT_ID"
    plan = out["results"][4]["execution_plan"]
    assert plan["status"] == PLAN_COMPLETED
    assert plan["child_order_id"] == "p#2"
    # p#3 and p#4 were never released but stay reserved against external use.
    assert symbol_state(out, "AAA")["engine"]["reserved_order_ids"] == ["p#3", "p#4"]


def test_external_add_cannot_reuse_reserved_child_order_id():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 5000, "MARKET", 100),
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_unreleased_tail_ids_stay_reserved_after_completion():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 3),                # one release of all 3
    ])
    assert out["results"][2]["execution_plan"]["status"] == PLAN_COMPLETED
    assert symbol_state(out, "AAA")["engine"]["reserved_order_ids"] == [
        "p#2", "p#3",
    ]


# ---------------------------------------------------------------------------
# Schema validation: INVALID_EVENT consumes nothing
# ---------------------------------------------------------------------------


def start_base(eid="e1", **overrides):
    event = {
        "event_id": eid, "symbol": "AAA", "sequence": 1, "type": POV_START,
        "plan_id": "p1", "side": "BUY", "total_quantity": 4,
        "participation_bps": 2000, "order_type": "LIMIT",
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
    start_base(participation_bps=0),                          # rate below 1
    start_base(participation_bps=-1),
    start_base(participation_bps=10001),                      # rate above 10000
    start_base(participation_bps=True),                       # bool is not int
    start_base(participation_bps=2000.0),                     # float
    start_base(participation_bps="2000"),                     # string
    start_base(participation_bps=None),                       # missing/null
    start_base(benchmark_price=0),
    start_base(benchmark_price=1.5),
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_START, "plan_id": "p1", "side": "BUY",
     "total_quantity": 4, "participation_bps": 2000,
     "order_type": "LIMIT", "benchmark_price": 100},          # LIMIT without price
    start_base(price=0),                                      # non-positive price
    start_base(price="100"),
    start_base(order_type="MARKET", price=99),                # MARKET with price
    start_base(account_id=""),                                # empty account
    start_base(account_id=7),
    start_base(slice_count=2),                                # TWAP-only field
    start_base(volume_weights=[1]),                           # VWAP-only field
    start_base(extra_field=1),                                # unknown field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1"},                    # missing increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": 0},                           # non-positive increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": True},                        # bool increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": 1.5},                         # float increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": 1, "bogus": 1},               # extra field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_CANCEL},                                     # ref missing plan_id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_REPORT, "plan_id": ""},                      # empty ref id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_CANCEL, "plan_id": "p1", "extra": 1},        # ref extra field
])
def test_malformed_pov_events_are_invalid(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


def test_boundary_participation_rates_are_accepted():
    out = replay_events([
        pov_start("lo", "AAA", 1, "lo", "BUY", 2, 1, "MARKET", 100),
        pov_start("hi", "AAA", 2, "hi", "BUY", 2, 10000, "MARKET", 100),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED, ACCEPTED]


def test_invalid_pov_event_consumes_neither_sequence_nor_id():
    out = replay_events([
        start_base("e1", participation_bps=0),                # invalid
        start_base("e1"),                                     # same id/seq now valid
        volume_event("e2", "AAA", 2, "p1", 1),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
    # The zero-release volume event succeeds at sequence 2.
    assert out["results"][2]["status"] == ACCEPTED


def test_nested_pov_payload_is_supported():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": POV_START, "plan_id": "p1",
                   "side": "BUY", "total_quantity": 2, "participation_bps": 5000,
                   "order_type": "MARKET", "benchmark_price": 100}},
    ])
    assert out["results"][0]["status"] == ACCEPTED


def test_inline_baseline_event_cannot_smuggle_pov_fields():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, participation_bps=1),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Lifecycle: completion, cancellation, report, occupancy
# ---------------------------------------------------------------------------


def test_plan_completes_after_total_release_and_is_still_queryable():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 1),
        volume_event("e3", "AAA", 4, "p", 1),
        report_plan("e4", "AAA", 5, "p"),
        volume_event("e5", "AAA", 6, "p", 1),
    ])
    assert out["results"][3]["execution_plan"]["status"] == PLAN_COMPLETED
    report = out["results"][4]
    assert report["status"] == ACCEPTED
    assert "result" not in report
    assert report["trades"] == []
    assert report["execution_plan"]["status"] == PLAN_COMPLETED
    assert report["execution_plan"]["algorithm"] == "POV"
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_volume_after_completion_does_not_change_market_volume():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 1, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 1),
        volume_event("e3", "AAA", 4, "p", 7),
    ])
    assert out["results"][3]["rejection_code"] == EXECUTION_PLAN_CLOSED
    # The rejected increment was not retained.
    assert plan_states(out)[0]["market_volume"] == 1


def test_cancel_counts_unreleased_quantity_and_closes_plan():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 6, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 2),
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        volume_event("e5", "AAA", 6, "p", 1),
        cancel_plan("e6", "AAA", 7, "p"),
    ])
    cancel = out["results"][3]["execution_plan"]
    assert cancel["status"] == PLAN_CANCELLED
    assert cancel["released_quantity"] == 2
    assert cancel["filled_quantity"] == 2
    assert cancel["cancelled_quantity"] == 4
    assert cancel["unreleased_quantity"] == 0
    # Report returns the identical summary and changes nothing.
    assert out["results"][4]["execution_plan"] == cancel
    assert out["results"][4]["book_changes"] == {"bids": [], "asks": []}
    # Closed plan rejects further volume and cancels.
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][6]["rejection_code"] == EXECUTION_PLAN_CLOSED
    # The rejected volume did not move the cumulative market volume.
    assert plan_states(out)[0]["market_volume"] == 2


def test_cancel_before_any_release_cancels_the_full_total():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 6, 5000, "MARKET", 100),
        cancel_plan("e2", "AAA", 2, "p"),
    ])
    plan = out["results"][1]["execution_plan"]
    assert plan["status"] == PLAN_CANCELLED
    assert plan["cancelled_quantity"] == 6
    assert plan["unreleased_quantity"] == 0


def test_unknown_plan_codes():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        volume_event("e2", "AAA", 2, "ghost", 5),
        cancel_plan("e3", "AAA", 3, "ghost"),
    ])
    assert [r["rejection_code"] for r in out["results"]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]


def test_release_commands_are_kind_specific_but_cancel_and_report_are_shared():
    # A fixed-schedule TWAP plan cannot be driven by market volume ...
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": TWAP_START,
         "plan_id": "p", "side": "BUY", "total_quantity": 2, "slice_count": 2,
         "order_type": "MARKET", "benchmark_price": 100},
        volume_event("e2", "AAA", 2, "p", 10),
        # ... but the generic lifecycle commands still reach it.
        report_plan("e3", "AAA", 3, "p"),
        cancel_plan("e4", "AAA", 4, "p"),
    ])
    assert out["results"][1]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][2]["status"] == ACCEPTED
    assert out["results"][3]["execution_plan"]["status"] == PLAN_CANCELLED

    # And a fixed-schedule slice cannot drive a POV plan.
    out = replay_events([
        pov_start("e1", "AAA", 1, "q", "BUY", 2, 5000, "MARKET", 100),
        {"event_id": "e2", "symbol": "AAA", "sequence": 2,
         "type": "TWAP_SLICE", "plan_id": "q"},
        {"event_id": "e3", "symbol": "AAA", "sequence": 3,
         "type": "VWAP_SLICE", "plan_id": "q"},
    ])
    assert [r["rejection_code"] for r in out["results"][1:]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]
    # The rejected slices moved nothing; the POV plan is still active.
    assert plan_states(out)[0]["status"] == PLAN_ACTIVE


def test_business_rejection_occupies_event_id_and_advances_sequence():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),               # occupies e1/seq1
        report_plan("e1", "AAA", 3, "other-plan"),          # same id, other content
        report_plan("e2", "AAA", 2, "ghost"),               # correct next seq
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_sequence_gap_pov_event_consumes_nothing():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 5000, "MARKET", 100),
        volume_event("e2", "AAA", 3, "p", 1),
        volume_event("e3", "AAA", 2, "p", 1),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["status"] == ACCEPTED


def test_exact_duplicate_pov_start_is_idempotent():
    first = pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100)
    out = replay_events([
        first,
        pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100),
        report_plan("e2", "AAA", 2, "p"),
    ])
    assert out["results"][1]["status"] == DUPLICATE
    assert len(plan_states(out)) == 1


def test_retried_volume_delivery_is_recognized_as_duplicate():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("t", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        volume_event("s", "AAA", 3, "p", 2),
        volume_event("s", "AAA", 3, "p", 2),                  # retry, stale seq
        report_plan("r", "AAA", 4, "p"),
    ])
    assert out["results"][3]["status"] == DUPLICATE
    assert out["results"][4]["execution_plan"]["filled_quantity"] == 2
    # The retry did not double-count market volume.
    assert plan_states(out)[0]["market_volume"] == 2


# ---------------------------------------------------------------------------
# Multi-symbol isolation and portfolio visibility
# ---------------------------------------------------------------------------


def test_plans_are_per_symbol_and_same_plan_id_is_fine_elsewhere():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 5000, "MARKET", 100),
        pov_start("e2", "BBB", 1, "p", "SELL", 1, 5000, "MARKET", 100),
        volume_event("e3", "AAA", 2, "p", 10),
        volume_event("e4", "BBB", 2, "p", 10),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED] * 4
    assert {s["symbol"] for s in out["snapshot"]["content"]["symbols"]} == {"AAA", "BBB"}


def test_started_but_never_released_pov_plan_makes_account_known():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100, account_id="z"),
        {"event_id": "e2", "symbol": "AAA", "sequence": 2,
         "type": "PORTFOLIO_REPORT", "account_id": "z",
         "mark_prices": {"AAA": 100}},
    ])
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][1]["portfolio_analysis"]["positions"][0]["symbol"] == "AAA"


# ---------------------------------------------------------------------------
# Determinism and snapshot format
# ---------------------------------------------------------------------------


def _full_pov_stream():
    return [
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 6, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        pov_start("t1", "AAA", 3, "p", "BUY", 9, 2500, "LIMIT", 99,
                  price=101, account_id="acct"),
        volume_event("t0", "AAA", 4, "p", 1),                   # zero release
        volume_event("t2", "AAA", 5, "p", 20),                  # release p#1 qty 5
        add("x1", "BBB", 1, "xb", "BUY", "LIMIT", 2, 50, account_id="z"),
        pov_start("m1", "BBB", 2, "mp", "SELL", 2, 5000, "MARKET", 51),
        volume_event("m2", "BBB", 3, "mp", 4),                  # release mp#1 qty 2
        volume_event("t3", "AAA", 6, "p", 100),                 # release p#2 qty 4
        report_plan("t5", "AAA", 7, "p"),
    ]


def test_pov_output_is_byte_for_byte_deterministic():
    events = _full_pov_stream()
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


def test_snapshot_format_stays_version_2_with_pov_state():
    snap = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 2000, "MARKET", 100),
        volume_event("e2", "AAA", 2, "p", 10),                  # release p#1 qty 2
    ])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION == "event-replay/2"
    state = symbol_state(snap, "AAA")
    assert set(state) == {"last_sequence", "price_limits", "event_log",
                         "plans", "engine"}
    assert state["price_limits"] is None
    plan = state["plans"][0]
    assert plan["algorithm"] == "POV"
    assert plan["total_quantity"] == 3
    assert plan["participation_bps"] == 2000
    assert plan["market_volume"] == 10
    assert plan["slice_quantities"] == [2]
    # The unreleased unit tail stays reserved; p#1 was spent by the child.
    assert state["engine"]["reserved_order_ids"] == ["p#2", "p#3"]


def test_plan_with_no_releases_roundtrips_empty_slice_list():
    snap = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 2000, "MARKET", 100),
    ])["snapshot"]
    plan = symbol_state(snap, "AAA")["plans"][0]
    assert plan["slice_quantities"] == []
    assert plan["market_volume"] == 0
    replayer = restore_replayer(copy.deepcopy(snap))
    assert canonical_json(export_snapshot(replayer)) == canonical_json(snap)


# ---------------------------------------------------------------------------
# Snapshot / resume equivalence
# ---------------------------------------------------------------------------


def test_resumed_pov_matches_uninterrupted_run_exactly():
    events = _full_pov_stream()
    for cut in (4, 5, 6, 7):
        part1, part2 = events[:cut], events[cut:]
        one_shot = replay_events(events)
        snapshot = replay_events(part1)["snapshot"]
        segmented = replay_events(part2, snapshot=snapshot)
        assert canonical_json(segmented["results"]) == canonical_json(
            one_shot["results"][cut:]
        )
        assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_keeps_releasing_children_and_counters():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 6, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 2),                 # child p#1, trade id 1
    ]
    snapshot = replay_events(part1)["snapshot"]
    part2 = [
        volume_event("e3", "AAA", 4, "p", 2),                 # child p#2, trade id 2
        volume_event("e4", "AAA", 5, "p", 2),                 # child p#3, trade id 3
    ]
    out = replay_events(part2, snapshot=snapshot)
    assert [t["trade_id"] for t in out["results"][0]["trades"]] == [2]
    assert out["results"][1]["execution_plan"]["child_order_id"] == "p#3"
    final = out["results"][1]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert (final["released_quantity"], final["filled_quantity"]) == (6, 6)


def test_zero_release_state_survives_snapshot():
    part1 = [
        pov_start("e1", "AAA", 1, "p", "BUY", 5, 1, "MARKET", 100),
        volume_event("e2", "AAA", 2, "p", 5000),              # zero release
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        volume_event("e3", "AAA", 3, "p", 5000),              # now child p#1
    ], snapshot=snapshot)
    plan = out["results"][0]["execution_plan"]
    assert plan["release_number"] == 1
    assert plan["child_order_id"] == "p#1"
    assert plan["released_quantity"] == 1


def test_cancelled_pov_plan_survives_snapshot_and_stays_queryable():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 2),
        cancel_plan("e3", "AAA", 4, "p"),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        report_plan("e4", "AAA", 5, "p"),
        volume_event("e5", "AAA", 6, "p", 1),
    ], snapshot=snapshot)
    assert out["results"][0]["execution_plan"]["status"] == PLAN_CANCELLED
    assert out["results"][0]["execution_plan"]["cancelled_quantity"] == 2
    assert out["results"][1]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_reserved_pov_ids_remain_reserved_after_restore():
    snapshot = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 5000, "MARKET", 100),
    ])["snapshot"]
    out = replay_events([
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ], snapshot=snapshot)
    assert out["results"][0]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_pov_snapshot_roundtrip_is_byte_stable():
    snapshot = replay_events(_full_pov_stream())["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)


def test_stateful_replayer_runs_pov_across_submissions():
    replayer = EventReplayer()
    replayer.submit([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 5, 2000, "LIMIT", 100, price=100),
    ])
    second = replayer.submit([volume_event("e2", "AAA", 3, "p", 10)])
    assert second[0]["result"] == "FILLED"
    plan = second[0]["execution_plan"]
    assert plan["child_order_id"] == "p#1"
    assert plan["released_quantity"] == 2
    assert "remaining_slices" not in plan


# ---------------------------------------------------------------------------
# Snapshot integrity
# ---------------------------------------------------------------------------


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_tampered_pov_plan_counters_are_corrupt():
    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 5000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 4),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: symbol_state(s, "AAA")["plans"][0]
        .__setitem__("filled_quantity", 99),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_market_volume_breaks_the_participation_invariant():
    from order_book_engine.event_replay import _digest

    good = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 10, 2000, "MARKET", 100),
        volume_event("e2", "AAA", 2, "p", 10),                # target 2, no release fills
    ])["snapshot"]
    broken = copy.deepcopy(good)
    # Cumulative volume 100 with the same 2 released units disagrees with the
    # participation target (min(10, 20) = 10), so structural validation must
    # reject it even after a freshly recomputed digest.
    symbol_state(broken, "AAA")["plans"][0]["market_volume"] = 100
    broken["content_digest"] = _digest(
        {k: v for k, v in broken.items() if k != "content_digest"}
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_participation_bps_is_corrupt():
    good = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 10, 2000, "MARKET", 100),
        volume_event("e2", "AAA", 2, "p", 10),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: symbol_state(s, "AAA")["plans"][0]
        .__setitem__("participation_bps", 9999),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_pov_plan_record_with_unknown_field_is_corrupt():
    good = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: symbol_state(s, "AAA")["plans"][0]
        .__setitem__("volume_weights", [1]),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


# ---------------------------------------------------------------------------
# Price limits: start-time and release-time re-check
# ---------------------------------------------------------------------------


LIMITS = {"price_limits": {"AAA": {"lower": 95, "upper": 105}}}


def test_limit_pov_start_outside_band_is_rejected():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 4, 5000, "LIMIT", 100, price=106),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert plan_states(out) == []


def test_market_pov_start_ignores_the_band():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 4, 5000, "MARKET", 100),
    ], config=LIMITS)
    assert out["results"][0]["status"] == ACCEPTED


def test_derived_id_clash_takes_precedence_over_price_breach():
    out = replay_events([
        add("e0", "AAA", 1, "p#2", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 3, 5000, "LIMIT", 100, price=110),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_pov_release_is_rechecked_against_the_current_band():
    stream = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        pov_start("e2", "AAA", 2, "p1", "BUY", 4, 10000, "LIMIT", 100, price=100),
        limit_update("e3", "AAA", 3, 103, 110),               # 100 now outside
        volume_event("e4", "AAA", 4, "p1", 2),                # rejected
        report_plan("e5", "AAA", 5, "p1"),
        limit_update("e6", "AAA", 6, 95, 105),                # band restored
        volume_event("e7", "AAA", 7, "p1", 2),                # now releases
    ]
    # Export the snapshot right after the accepted report following the
    # rejected release: its increment must not have been retained.
    at_rejection = replay_events(
        stream[:5], config=LIMITS,
        snapshot_after={"symbol": "AAA", "sequence": 5},
    )["snapshot"]
    assert symbol_state(at_rejection, "AAA")["plans"][0]["market_volume"] == 0
    assert symbol_state(at_rejection, "AAA")["engine"]["next_trade_id"] == 1

    out = replay_events(stream, config=LIMITS)
    rejected = out["results"][3]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    assert "execution_plan" not in rejected
    # Nothing advanced: release counters unchanged.
    plan = out["results"][4]["execution_plan"]
    assert plan["status"] == PLAN_ACTIVE
    assert plan["released_quantity"] == 0
    assert plan["filled_quantity"] == 0
    released = out["results"][6]
    assert released["execution_plan"]["release_number"] == 1
    assert released["execution_plan"]["child_order_id"] == "p1#1"
    assert [t["trade_id"] for t in released["trades"]] == [1]
    # Only the post-widening increment was ever retained.
    assert symbol_state(out, "AAA")["plans"][0]["market_volume"] == 2


def test_rejected_pov_release_keeps_derived_ids_reserved():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p1", "BUY", 4, 10000, "LIMIT", 100, price=100),
        limit_update("e2", "AAA", 2, 103, 110),
        volume_event("e3", "AAA", 3, "p1", 2),                # rejected
        add("e4", "AAA", 4, "p1#2", "BUY", "LIMIT", 1, 105),  # still reserved
    ], config=LIMITS)
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][3]["rejection_code"] == "DUPLICATE_ORDER_ID"
    # The rejected volume was not retained.
    assert symbol_state(out, "AAA")["plans"][0]["market_volume"] == 0


def test_market_pov_releases_ignore_the_band():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("e2", "AAA", 2, "p1", "SELL", 4, 10000, "MARKET", 100),
        limit_update("e3", "AAA", 3, 1, 2),                   # absurdly narrow
        volume_event("e4", "AAA", 4, "p1", 2),
    ], config=LIMITS)
    assert out["results"][3]["status"] == ACCEPTED
    assert out["results"][3]["result"] == "UNFILLED_CANCELLED"


# ---------------------------------------------------------------------------
# Single-security entry point keeps rejecting the new types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    {"event_id": "e1", "type": POV_START, "plan_id": "p", "side": "BUY",
     "total_quantity": 3, "participation_bps": 5000,
     "order_type": "MARKET", "benchmark_price": 100},
    {"event_id": "e2", "type": POV_VOLUME, "plan_id": "p",
     "market_volume_increment": 10},
    {"event_id": "e3", "type": POV_CANCEL, "plan_id": "p"},
    {"event_id": "e4", "type": POV_REPORT, "plan_id": "p"},
])
def test_single_security_json_lines_entry_rejects_pov_types(payload):
    from order_book_engine import replay as line_replay

    stdin = io.TextIOWrapper(io.BytesIO(
        (json.dumps(payload) + "\n").encode("utf-8")
    ))
    stdout = io.TextIOWrapper(io.BytesIO())
    assert line_replay.replay(stdin, stdout, io.StringIO()) == 0
    stdout.flush()
    line = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    assert line["result"] == "REJECTED"
    assert line["reason"] == "INVALID_SCHEMA"


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


def test_cli_pov_end_to_end_is_deterministic():
    request = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 5000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 4),
        volume_event("e3", "AAA", 4, "p", 4),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    plan = out1["results"][2]["execution_plan"]
    assert plan["child_order_id"] == "p#1"
    assert plan["algorithm"] == "POV"
    assert plan["released_quantity"] == 2
    assert out1["snapshot"]["format_version"] == "event-replay/2"


def test_cli_resumes_pov_from_snapshot():
    first = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        volume_event("e2", "AAA", 3, "p", 2),
    ]}
    _, out1, _ = _run_cli(first)
    second = {
        "events": [volume_event("e3", "AAA", 4, "p", 2)],
        "snapshot": out1["snapshot"],
    }
    code, out2, _ = _run_cli(second)
    assert code == 0
    assert out2["results"][0]["execution_plan"]["status"] == PLAN_COMPLETED
    assert [t["trade_id"] for t in out2["results"][0]["trades"]] == [2]
