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
    EventReplayer,
    SnapshotError,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    VWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.engine import Engine, INVALID_SCHEMA


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


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": POV_VOLUME,
        "plan_id": plan_id,
        "market_volume_increment": increment,
    }


def pov_cmd(event_id, symbol, sequence, cmd, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": cmd, "plan_id": plan_id}


def cancel_plan(eid, sym, seq, plan):
    return pov_cmd(eid, sym, seq, POV_CANCEL, plan)


def report_plan(eid, sym, seq, plan):
    return pov_cmd(eid, sym, seq, POV_REPORT, plan)


def plan_states(out, symbol_index=0):
    return out["snapshot"]["content"]["symbols"][symbol_index]["state"]["plans"]


LIMITS = {"price_limits": {"AAA": {"lower": 95, "upper": 105}}}


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
        "remaining_slices": 5,
        "executed_notional": 0,
        "vwap": None,
        "slippage_notional": 0,
        "algorithm": "POV",
        "participation_bps": 2000,
        "market_volume": 0,
        "unreleased_quantity": 5,
    }
    # START carries no child fields and spends no trade id.
    assert "slice_number" not in r["execution_plan"]
    assert "child_order_id" not in r["execution_plan"]
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    assert state["engine"]["next_trade_id"] == 1


def test_start_reserves_one_one_unit_child_id_per_total_unit():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 5000, "MARKET", 100),
    ])
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    assert state["engine"]["reserved_order_ids"] == ["p#1", "p#2", "p#3"]
    plan = state["plans"][0]
    assert plan["algorithm"] == "POV"
    assert plan["participation_bps"] == 5000
    assert plan["market_volume"] == 0
    assert plan["slice_quantities"] == [1, 1, 1]


def test_market_plan_explicit_null_price_is_accepted():
    event = pov_start("e1", "AAA", 1, "p", "BUY", 2, 5000, "MARKET", 100)
    event["price"] = None
    assert replay_events([event])["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Release math
# ---------------------------------------------------------------------------


def test_release_target_uses_floor_of_cumulative_volume_times_bps():
    out = replay_events([
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 10, 2000, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "p", 10),   # floor(10*2000/10000) = 2
        pov_volume("v2", "AAA", 4, "p", 5),    # cum 15 -> 3, release one more
        pov_volume("v3", "AAA", 5, "p", 1),    # cum 16 -> 3, release nothing
    ])
    assert [t["quantity"] for t in out["results"][2]["trades"]] == [1, 1]
    third = out["results"][3]["execution_plan"]
    assert third["slice_number"] == 3
    assert third["child_order_id"] == "p#3"
    assert third["released_quantity"] == 3
    assert third["market_volume"] == 15
    zero = out["results"][4]
    assert zero["status"] == ACCEPTED
    assert "result" not in zero
    assert zero["trades"] == []
    assert zero["book_changes"] == {"bids": [], "asks": []}
    plan = zero["execution_plan"]
    assert plan["slice_number"] is None
    assert plan["child_order_id"] is None
    assert plan["released_quantity"] == 3
    assert plan["market_volume"] == 16
    assert plan["unreleased_quantity"] == 7


def test_zero_release_event_still_succeeds_as_the_first_command():
    out = replay_events([
        pov_start("p0", "AAA", 1, "p", "BUY", 10, 100, "MARKET", 100),
        pov_volume("v1", "AAA", 2, "p", 5),   # floor(5*1%) = 0
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert "result" not in r
    plan = r["execution_plan"]
    assert plan["child_order_id"] is None
    assert plan["slice_number"] is None
    assert plan["market_volume"] == 5
    assert plan["released_quantity"] == 0
    assert plan["vwap"] is None
    assert plan["slippage_notional"] == 0
    # No child id was spent and the reservation still covers every unit.
    reserved = out["snapshot"]["content"]["symbols"][0]["state"]["engine"][
        "reserved_order_ids"]
    assert set(reserved) == {f"p#{i}" for i in range(1, 11)}


def test_target_is_capped_at_total_quantity_and_completes_plan():
    out = replay_events([
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "p", 10),   # target min(4, 10) = 4
        report_plan("r1", "AAA", 4, "p"),
    ])
    volume_result = out["results"][2]
    assert [t["taker_order_id"] for t in volume_result["trades"]] == [
        "p#1", "p#2", "p#3", "p#4"
    ]
    # The child fields identify the LAST child released by the event.
    plan = volume_result["execution_plan"]
    assert plan["slice_number"] == 4
    assert plan["child_order_id"] == "p#4"
    assert plan["status"] == PLAN_COMPLETED
    assert plan["remaining_slices"] == 0
    assert plan["unreleased_quantity"] == 0
    assert out["results"][3]["execution_plan"]["status"] == PLAN_COMPLETED


def test_bps_boundaries_one_and_ten_thousand():
    # 1 bps on 10000 volume floors to 1.
    one = replay_events([
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 100, 1, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "p", 10000),
    ])
    assert one["results"][2]["execution_plan"]["released_quantity"] == 1
    # 10000 bps releases one unit per market unit.
    full = replay_events([
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("p0", "AAA", 2, "q", "BUY", 3, 10000, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "q", 3),
    ])
    assert full["results"][2]["execution_plan"]["released_quantity"] == 3
    assert full["results"][2]["execution_plan"]["status"] == PLAN_COMPLETED


# ---------------------------------------------------------------------------
# Child execution: limit IOC / market, metrics and self-trade prevention
# ---------------------------------------------------------------------------


def test_limit_children_are_ioc_and_leftover_never_rests():
    out = replay_events([
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 99, price=100),
        pov_volume("v1", "AAA", 3, "p", 3),
    ])
    r = out["results"][2]
    # The last two one-unit children find no liquidity; the event result is
    # the last child's engine result.
    assert r["result"] == "UNFILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [1]
    assert r["bids"] == []
    plan = r["execution_plan"]
    assert plan["released_quantity"] == 3
    assert plan["filled_quantity"] == 1
    assert plan["executed_notional"] == 100
    assert plan["vwap"] == {"numerator": 100, "denominator": 1}
    assert plan["slippage_notional"] == 1  # 100*1 - 99*1 for a buy


def test_market_children_use_ioc_semantics_and_sell_slippage():
    out = replay_events([
        add("b1", "AAA", 1, "b1", "BUY", "LIMIT", 2, 100),
        pov_start("p0", "AAA", 2, "p", "SELL", 5, 10000, "MARKET", 101),
        pov_volume("v1", "AAA", 3, "p", 5),
    ])
    r = out["results"][2]
    assert r["result"] == "UNFILLED_CANCELLED"
    assert [t["quantity"] for t in r["trades"]] == [1, 1]
    plan = r["execution_plan"]
    assert plan["filled_quantity"] == 2
    assert plan["executed_notional"] == 200
    # Sell slippage negates: -(100*2 - 101*2) = 2 (improvement).
    assert plan["slippage_notional"] == 2


def test_metrics_accumulate_across_volume_events_at_different_prices():
    out = replay_events([
        add("a", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 102),
        pov_start("t", "AAA", 3, "p", "BUY", 5, 10000, "LIMIT", 100, price=102),
        pov_volume("v1", "AAA", 4, "p", 2),
        pov_volume("v2", "AAA", 5, "p", 3),
    ])
    final = out["results"][4]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert final["filled_quantity"] == 5
    assert final["executed_notional"] == 2 * 100 + 3 * 102
    assert final["vwap"] == {"numerator": 506, "denominator": 5}
    assert final["slippage_notional"] == 506 - 100 * 5
    # Trade ids run per symbol across the two volume events.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1, 2]
    assert [t["trade_id"] for t in out["results"][4]["trades"]] == [3, 4, 5]


def test_children_match_against_the_book_present_at_release_time():
    out = replay_events([
        pov_start("p0", "AAA", 1, "p", "BUY", 1, 10000, "LIMIT", 100, price=100),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 100),
        pov_volume("v1", "AAA", 3, "p", 1),
    ])
    assert out["results"][2]["result"] == "FILLED"
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["s1"]


def test_self_trade_prevention_blocks_like_baseline():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        pov_start("p0", "AAA", 2, "p", "BUY", 3, 10000, "LIMIT", 100,
                  price=100, account_id="A"),
        pov_volume("v1", "AAA", 3, "p", 3),
    ])
    r = out["results"][2]
    assert r["result"] == "SELF_TRADE_PREVENTED"
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]
    # The child was still released: the plan counts release but no fill.
    plan = r["execution_plan"]
    assert plan["released_quantity"] == 3
    assert plan["filled_quantity"] == 0


def test_book_changes_of_a_volume_event_list_drained_level_as_zero():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "p", 2),
    ])
    assert out["results"][2]["book_changes"] == {
        "bids": [], "asks": [{"price": 100, "quantity": 0}],
    }


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
    assert "participation_bps" not in plan_states(out)[0]


def test_vwap_start_conflicts_with_existing_pov_plan():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 5000, "MARKET", 100),
        {"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": VWAP_START,
         "plan_id": "p", "side": "SELL", "total_quantity": 1, "volume_weights": [1],
         "order_type": "MARKET", "benchmark_price": 100},
    ])
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    assert plan_states(out)[0]["algorithm"] == "POV"


def test_derived_id_clash_with_existing_order_rejects_start():
    out = replay_events([
        add("e0", "AAA", 1, "p#2", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 2, 5000, "MARKET", 100),
        add("e2", "AAA", 3, "other", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    assert plan_states(out) == []
    assert out["results"][2]["status"] == ACCEPTED


def test_external_event_cannot_reuse_reserved_child_event_id():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        add("p#2", "AAA", 3, "other", "BUY", "LIMIT", 1, 100),
        pov_volume("v2", "AAA", 4, "p", 1),
        pov_volume("v3", "AAA", 5, "p", 1),
    ])
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EVENT_ID"
    assert out["results"][4]["execution_plan"]["child_order_id"] == "p#2"
    assert out["results"][4]["execution_plan"]["status"] == PLAN_COMPLETED


def test_external_add_cannot_reuse_reserved_child_order_id():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 5000, "MARKET", 100),
        add("e2", "AAA", 2, "p#2", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


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
    start_base(plan_id=""),                                  # empty plan id
    start_base(plan_id=3),                                   # non-string id
    start_base(side="ACROSS"),                               # bad side
    start_base(order_type="ICEBERG"),                        # unsupported type
    start_base(total_quantity=0),                            # non-positive
    start_base(total_quantity=True),                         # bool is not int
    start_base(participation_bps=0),                         # below 1
    start_base(participation_bps=-1),
    start_base(participation_bps=10001),                     # above 10000
    start_base(participation_bps=True),                      # bool is not int
    start_base(participation_bps=2.5),                       # float
    start_base(participation_bps="2000"),                    # string
    start_base(participation_bps=None),                      # null
    start_base(benchmark_price=0),
    start_base(benchmark_price=1.5),
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_START, "plan_id": "p1", "side": "BUY",
     "total_quantity": 4, "participation_bps": 2000,
     "order_type": "LIMIT", "benchmark_price": 100},         # LIMIT without price
    start_base(price=0),                                     # non-positive price
    start_base(price="100"),
    start_base(order_type="MARKET", price=99),               # MARKET with price
    start_base(account_id=""),                               # empty account
    start_base(account_id=7),
    start_base(slice_count=2),                               # TWAP-only field
    start_base(volume_weights=[1]),                          # VWAP-only field
    start_base(extra_field=1),                               # unknown field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1", "bogus": 1},       # volume with extra field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1"},                   # missing increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": 0},                          # non-positive increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": True},                       # bool increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_VOLUME, "plan_id": "p1",
     "market_volume_increment": 1.5},                        # float increment
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_CANCEL},                                    # ref missing plan_id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_REPORT, "plan_id": ""},                     # empty ref id
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": POV_CANCEL, "plan_id": "p", "extra": 1},        # ref with extra field
])
def test_malformed_pov_events_are_invalid(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


def test_invalid_pov_event_consumes_neither_sequence_nor_id():
    bad = start_base("e1", participation_bps=10001)
    good = start_base("e1")
    out = replay_events([
        bad,
        good,                                                  # same id/seq now valid
        pov_volume("e2", "AAA", 2, "p1", 1),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
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


def test_single_security_entry_point_rejects_pov_types():
    engine = Engine()
    line = json.dumps(start_base())
    _eid, result, reason, _trades = engine.handle_line(line)
    assert result == REJECTED
    assert reason == INVALID_SCHEMA
    volume_line = json.dumps(
        {"event_id": "e1", "type": POV_VOLUME, "plan_id": "p",
         "market_volume_increment": 1}
    )
    _eid, result, reason, _trades = engine.handle_line(volume_line)
    assert result == REJECTED
    assert reason == INVALID_SCHEMA


# ---------------------------------------------------------------------------
# Lifecycle: completion, cancellation, report, occupancy
# ---------------------------------------------------------------------------


def test_plan_completes_after_final_unit_and_is_still_queryable():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 2, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 1),
        pov_volume("e3", "AAA", 4, "p", 1),
        report_plan("e4", "AAA", 5, "p"),
        pov_volume("e5", "AAA", 6, "p", 1),
    ])
    assert out["results"][3]["execution_plan"]["status"] == PLAN_COMPLETED
    report = out["results"][4]
    assert report["status"] == ACCEPTED
    assert "result" not in report
    assert report["trades"] == []
    assert report["execution_plan"]["status"] == PLAN_COMPLETED
    # A report never carries the POV_VOLUME-only child fields.
    assert "slice_number" not in report["execution_plan"]
    assert "child_order_id" not in report["execution_plan"]
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED


def test_cancel_counts_unreleased_quantity_and_closes_plan():
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 6, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 2),
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        pov_volume("e5", "AAA", 6, "p", 10),
        cancel_plan("e6", "AAA", 7, "p"),
    ])
    cancel = out["results"][3]["execution_plan"]
    assert cancel["status"] == PLAN_CANCELLED
    assert cancel["released_quantity"] == 2
    assert cancel["filled_quantity"] == 2
    assert cancel["cancelled_quantity"] == 4
    assert cancel["unreleased_quantity"] == 4
    assert cancel["remaining_slices"] == 0
    assert cancel["market_volume"] == 2
    assert out["results"][4]["execution_plan"] == cancel
    assert out["results"][4]["book_changes"] == {"bids": [], "asks": []}
    assert out["results"][5]["rejection_code"] == EXECUTION_PLAN_CLOSED
    assert out["results"][6]["rejection_code"] == EXECUTION_PLAN_CLOSED
    # The rejected post-cancel volume did not accumulate market volume.
    assert out["results"][5]["bids"] == out["results"][4]["bids"]


def test_unknown_plan_codes():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),
        cancel_plan("e2", "AAA", 2, "ghost"),
        pov_volume("e3", "AAA", 3, "ghost", 1),
    ])
    assert [r["rejection_code"] for r in out["results"]] == [
        UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN, UNKNOWN_EXECUTION_PLAN,
    ]


def test_business_rejection_occupies_event_id_and_advances_sequence():
    out = replay_events([
        report_plan("e1", "AAA", 1, "ghost"),                # occupies e1/seq1
        report_plan("e1", "AAA", 3, "other-plan"),           # same id, other content
        report_plan("e2", "AAA", 2, "ghost"),                # correct next seq
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_sequence_gap_pov_event_consumes_nothing():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 5000, "MARKET", 100),
        pov_volume("e2", "AAA", 3, "p", 1),
        pov_volume("e3", "AAA", 2, "p", 1),
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
        pov_volume("s", "AAA", 3, "p", 2),
        pov_volume("s", "AAA", 3, "p", 2),                      # retry, stale seq
        report_plan("r", "AAA", 4, "p"),
    ])
    assert out["results"][3]["status"] == DUPLICATE
    assert out["results"][4]["execution_plan"]["filled_quantity"] == 2


# ---------------------------------------------------------------------------
# Price limits
# ---------------------------------------------------------------------------


def test_limit_pov_start_out_of_bounds_creates_no_plan():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p1", "BUY", 4, 5000, "LIMIT", 100, price=90),
        add("e2", "AAA", 2, "p1#1", "BUY", "LIMIT", 1, 100),
        report_plan("e3", "AAA", 3, "p1"),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    assert "execution_plan" not in out["results"][0]
    assert out["results"][1]["result"] == "RESTING"
    assert out["results"][2]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


def test_limit_pov_start_in_band_and_market_exempt():
    accepted = replay_events([
        pov_start("e1", "AAA", 1, "p1", "BUY", 4, 5000, "LIMIT", 100, price=105),
    ], config=LIMITS)
    assert accepted["results"][0]["status"] == ACCEPTED
    market = replay_events([
        pov_start("e1", "AAA", 1, "p2", "SELL", 4, 5000, "MARKET", 100),
    ], config=LIMITS)
    assert market["results"][0]["status"] == ACCEPTED


def test_limit_recheck_before_release_freezes_volume_and_release_state():
    events = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("p0", "AAA", 2, "p", "BUY", 10, 5000, "LIMIT", 100, price=100),
        {"event_id": "u1", "symbol": "AAA", "sequence": 3,
         "type": "PRICE_LIMIT_UPDATE", "lower_price": 90, "upper_price": 99},
        pov_volume("v1", "AAA", 4, "p", 10),                  # breaches: frozen
        {"event_id": "u2", "symbol": "AAA", "sequence": 5,
         "type": "PRICE_LIMIT_UPDATE", "lower_price": 95, "upper_price": 105},
        pov_volume("v2", "AAA", 6, "p", 10),
    ]
    out = replay_events(events, config=LIMITS)
    assert out["results"][3]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    assert out["results"][3]["trades"] == []
    plan = out["results"][5]["execution_plan"]
    # The rejected increment was not accumulated: only 10 (not 20) market
    # volume is recorded, releasing floor(10*50%) = 5 units.
    assert plan["market_volume"] == 10
    assert plan["released_quantity"] == 5
    assert plan["child_order_id"] == "p#5"


def test_price_breach_precedence_keeps_duplicate_plan_and_clash_codes():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p1", "BUY", 4, 5000, "LIMIT", 100, price=100),
        pov_start("e2", "AAA", 2, "p1", "BUY", 4, 5000, "LIMIT", 100, price=90),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == DUPLICATE_EXECUTION_PLAN
    clash = replay_events([
        add("e0", "AAA", 1, "q#1", "SELL", "LIMIT", 1, 100),
        pov_start("e1", "AAA", 2, "q", "BUY", 2, 5000, "LIMIT", 100, price=90),
    ], config=LIMITS)
    assert clash["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


# ---------------------------------------------------------------------------
# Multi-symbol isolation
# ---------------------------------------------------------------------------


def test_plans_are_per_symbol_and_same_plan_id_is_fine_elsewhere():
    out = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 1, 5000, "MARKET", 100),
        pov_start("e2", "BBB", 1, "p", "SELL", 1, 5000, "MARKET", 100),
        pov_volume("e3", "AAA", 2, "p", 1),
        pov_volume("e4", "BBB", 2, "p", 1),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED] * 4
    assert {s["symbol"] for s in out["snapshot"]["content"]["symbols"]} == {"AAA", "BBB"}


# ---------------------------------------------------------------------------
# Determinism and snapshot format
# ---------------------------------------------------------------------------


def _full_pov_stream():
    return [
        add("e0", "AAA", 1, "i1", "SELL", "ICEBERG", 6, 100, display_quantity=2),
        add("e0b", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        pov_start("t1", "AAA", 3, "p", "BUY", 9, 5000, "LIMIT", 99,
                  price=101, account_id="acct"),
        pov_volume("t2", "AAA", 4, "p", 4),       # target 2: two one-unit children
        add("x1", "BBB", 1, "xb", "BUY", "LIMIT", 2, 50, account_id="z"),
        pov_volume("t3", "AAA", 5, "p", 8),       # cum 12 -> target 6
        pov_start("m1", "BBB", 2, "mp", "SELL", 2, 10000, "MARKET", 51),
        pov_volume("m2", "BBB", 3, "mp", 1),
        {"event_id": "u1", "symbol": "AAA", "sequence": 6,
         "type": "PRICE_LIMIT_UPDATE", "lower_price": 95, "upper_price": 105},
        pov_volume("t4", "AAA", 7, "p", 6),       # cum 18 -> target 9, completes
        report_plan("t5", "AAA", 8, "p"),
        pov_volume("t6", "AAA", 9, "p", 10),      # closed
        cancel_plan("t7", "AAA", 10, "p"),        # closed
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
        pov_start("e1", "AAA", 1, "p", "BUY", 3, 2500, "MARKET", 100),
    ])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION == "event-replay/2"
    state = snap["content"]["symbols"][0]["state"]
    assert set(state) == {"last_sequence", "price_limits", "event_log",
                          "plans", "engine"}
    plan = state["plans"][0]
    assert plan["algorithm"] == "POV"
    assert plan["participation_bps"] == 2500
    assert plan["market_volume"] == 0
    assert plan["slice_quantities"] == [1, 1, 1]
    assert state["engine"]["reserved_order_ids"] == ["p#1", "p#2", "p#3"]


# ---------------------------------------------------------------------------
# Snapshot / resume equivalence
# ---------------------------------------------------------------------------


def test_resumed_pov_matches_uninterrupted_run_exactly():
    events = _full_pov_stream()
    cut = 6
    one_shot = replay_events(events)
    snapshot = replay_events(events[:cut])["snapshot"]
    segmented = replay_events(events[cut:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][cut:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_keeps_releasing_units_and_counters():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 6, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 2),
    ]
    snapshot = replay_events(part1)["snapshot"]
    part2 = [
        pov_volume("e3", "AAA", 4, "p", 2),
        pov_volume("e4", "AAA", 5, "p", 2),
    ]
    out = replay_events(part2, snapshot=snapshot)
    assert [t["trade_id"] for t in out["results"][0]["trades"]] == [3, 4]
    final = out["results"][1]["execution_plan"]
    assert final["status"] == PLAN_COMPLETED
    assert (final["released_quantity"], final["filled_quantity"]) == (6, 6)


def test_resume_through_zero_release_and_cancel():
    part1 = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 1),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        cancel_plan("e3", "AAA", 4, "p"),
        report_plan("e4", "AAA", 5, "p"),
        pov_volume("e5", "AAA", 6, "p", 10),
    ], snapshot=snapshot)
    assert out["results"][0]["execution_plan"]["status"] == PLAN_CANCELLED
    assert out["results"][0]["execution_plan"]["cancelled_quantity"] == 3
    assert out["results"][1]["execution_plan"]["status"] == PLAN_CANCELLED
    assert out["results"][2]["rejection_code"] == EXECUTION_PLAN_CLOSED


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
        pov_start("e1", "AAA", 2, "p", "BUY", 5, 10000, "LIMIT", 100, price=100),
    ])
    second = replayer.submit([pov_volume("e2", "AAA", 3, "p", 2)])
    assert second[0]["result"] == "FILLED"
    plan = second[0]["execution_plan"]
    assert plan["child_order_id"] == "p#2"
    assert plan["slice_number"] == 2
    assert plan["released_quantity"] == 2


# ---------------------------------------------------------------------------
# Snapshot integrity
# ---------------------------------------------------------------------------


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_tampered_pov_plan_state_is_corrupt():
    good = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 2),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("filled_quantity", 99),
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_pov_market_volume_mismatch_is_rejected_even_with_recomputed_digest():
    from order_book_engine.event_replay import _digest

    good = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 10, 5000, "MARKET", 100),
        pov_volume("e2", "AAA", 2, "p", 10),          # target 5, released 5
    ])["snapshot"]
    broken = copy.deepcopy(good)
    plan = broken["content"]["symbols"][0]["state"]["plans"][0]
    # Raising the frozen market volume to 12 changes the implied target to 6,
    # so only the POV market-volume cross-check can catch it.
    plan["market_volume"] = 12
    broken["content_digest"] = _digest(
        {k: v for k, v in broken.items() if k != "content_digest"}
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_tampered_pov_participation_bps_is_corrupt():
    good = replay_events([
        pov_start("e1", "AAA", 1, "p", "BUY", 10, 5000, "MARKET", 100),
    ])["snapshot"]
    broken = _tamper(
        good,
        lambda s: s["content"]["symbols"][0]["state"]["plans"][0]
        .__setitem__("participation_bps", 99999),
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


def test_cli_pov_end_to_end_is_deterministic():
    request = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 2),
        pov_volume("e3", "AAA", 4, "p", 2),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    plan = out1["results"][2]["execution_plan"]
    assert plan["child_order_id"] == "p#2"
    assert plan["algorithm"] == "POV"
    assert plan["participation_bps"] == 10000
    assert out1["snapshot"]["format_version"] == "event-replay/2"


def test_cli_resumes_pov_from_snapshot():
    first = {"events": [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 4, 10000, "LIMIT", 100, price=100),
        pov_volume("e2", "AAA", 3, "p", 2),
    ]}
    _, out1, _ = _run_cli(first)
    second = {
        "events": [pov_volume("e3", "AAA", 4, "p", 2)],
        "snapshot": out1["snapshot"],
    }
    code, out2, _ = _run_cli(second)
    assert code == 0
    assert out2["results"][0]["execution_plan"]["status"] == PLAN_COMPLETED
    assert [t["trade_id"] for t in out2["results"][0]["trades"]] == [3, 4]
