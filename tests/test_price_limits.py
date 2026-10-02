"""Tests for the optional static per-security ``price_limits`` configuration."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PRICE_LIMIT_EXCEEDED,
    REJECTED,
    SnapshotError,
    TWAP_SLICE,
    TWAP_START,
    VWAP_SLICE,
    VWAP_START,
    EventReplayer,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli

LIMITS = {"price_limits": {"AAA": {"lower": 90, "upper": 110}}}


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


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def twap_start(event_id, symbol, sequence, plan_id, order_type, price=None,
               side="BUY", total_quantity=4, slice_count=2, benchmark_price=100):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": TWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": order_type, "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def vwap_start(event_id, symbol, sequence, plan_id, order_type, price=None,
               side="BUY", total_quantity=6, weights=(1, 2, 3), benchmark_price=100):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": list(weights),
        "order_type": order_type, "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def plan_slice(event_id, symbol, sequence, plan_id, slice_type=TWAP_SLICE):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": slice_type, "plan_id": plan_id}


def statuses(out):
    return [(r["event_id"], r["status"], r.get("result") or r.get("rejection_code"))
            for r in out["results"]]


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("config", [
    None,
    {},
    {"price_limits": {}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 1}}},
    {"price_limits": {"AAA": {"lower": 90, "upper": 110},
                      "BBB": {"lower": 1, "upper": 1_000_000}}},
    {"venue": "X", "price_limits": {"甲": {"lower": 1, "upper": 2}}},
])
def test_valid_price_limits_configurations_are_accepted(config):
    replayer = EventReplayer(copy.deepcopy(config))
    if config and "price_limits" in config:
        assert replayer.config["price_limits"] == config["price_limits"]


@pytest.mark.parametrize("config", [
    {"price_limits": []},
    {"price_limits": ()},
    {"price_limits": "AAA"},
    {"price_limits": None},
    {"price_limits": {"": {"lower": 1, "upper": 2}}},
    {"price_limits": {7: {"lower": 1, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 2, "x": 3}}},
    {"price_limits": {"AAA": [1, 2]}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 2, "price": 3}}},
    {"price_limits": {"AAA": {"lower": True, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": False}}},
    {"price_limits": {"AAA": {"lower": 0, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": -5, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 0}}},
    {"price_limits": {"AAA": {"lower": 5, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": "1", "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1.0, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": None}}},
])
def test_invalid_price_limits_configuration_raises_value_error(config):
    with pytest.raises(ValueError):
        EventReplayer(copy.deepcopy(config))
    with pytest.raises(ValueError):
        replay_events([], config=copy.deepcopy(config))


def test_non_object_config_keeps_type_error():
    with pytest.raises(TypeError):
        EventReplayer([])


def test_invalid_config_raises_before_any_event_is_processed():
    # A well-formed event in the same call is never reached.
    events = [add("e1", "AAA", 1, "o1", "SELL", "LIMIT", 1, 100)]
    with pytest.raises(ValueError):
        replay_events(events, config={"price_limits": {"AAA": {"lower": 9, "upper": 1}}})


def test_invalid_config_raises_before_snapshot_state_is_adopted():
    good = replay_events([], config=LIMITS)["snapshot"]
    with pytest.raises(ValueError):
        restore_replayer(good, config={"price_limits": {"AAA": {"lower": 9, "upper": 1}}})


def test_price_limits_digest_is_insertion_order_independent():
    a = {"price_limits": {"AAA": {"lower": 90, "upper": 110},
                          "BBB": {"lower": 1, "upper": 5}}}
    b = {"price_limits": {"BBB": {"upper": 5, "lower": 1},
                          "AAA": {"upper": 110, "lower": 90}}}
    assert EventReplayer(a).config_digest == EventReplayer(b).config_digest


def test_distinct_price_limits_produce_distinct_digests():
    a = EventReplayer({"price_limits": {"AAA": {"lower": 90, "upper": 110}}})
    b = EventReplayer({"price_limits": {"AAA": {"lower": 90, "upper": 111}}})
    c = EventReplayer()
    assert a.config_digest != b.config_digest
    assert a.config_digest != c.config_digest


# ---------------------------------------------------------------------------
# ADD enforcement (LIMIT / ICEBERG bounded, MARKET open)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("price", [90, 100, 110])
def test_add_inside_the_closed_interval_is_accepted(price):
    out = replay_events(
        [add("e1", "AAA", 1, "o1", "SELL", "LIMIT", 2, price)], config=LIMITS
    )
    assert out["results"][0]["status"] == ACCEPTED


@pytest.mark.parametrize("price", [89, 111])
def test_add_outside_the_interval_is_rejected(price):
    out = replay_events(
        [add("e1", "AAA", 1, "o1", "SELL", "LIMIT", 2, price)], config=LIMITS
    )
    r = out["results"][0]
    assert r["status"] == REJECTED
    assert r["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The untouched book is echoed.
    assert r["bids"] == []
    assert r["asks"] == []


def test_iceberg_add_is_bounded():
    ok = replay_events(
        [iceberg("e1", "AAA", 1, "i1", "SELL", 6, 90, 2)], config=LIMITS
    )
    assert ok["results"][0]["status"] == ACCEPTED
    bad = replay_events(
        [iceberg("e1", "AAA", 1, "i1", "SELL", 6, 111, 2)], config=LIMITS
    )
    assert bad["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_market_add_is_not_checked_against_limits():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "m1", "BUY", "MARKET", 3),
    ], config=LIMITS)
    assert statuses(out)[1] == ("e2", ACCEPTED, "FILLED")


def test_unconfigured_symbol_keeps_baseline_behaviour():
    out = replay_events(
        [add("e1", "ZZZ", 1, "z1", "SELL", "LIMIT", 2, 999_999)], config=LIMITS
    )
    assert out["results"][0]["status"] == ACCEPTED
    assert out["results"][0]["result"] == "RESTING"


def test_absent_and_empty_configuration_leave_all_symbols_open():
    wide = add("e1", "AAA", 1, "o1", "SELL", "LIMIT", 2, 1_000_000)
    assert replay_events([copy.deepcopy(wide)])["results"][0]["status"] == ACCEPTED
    assert replay_events([copy.deepcopy(wide)], config={})["results"][0]["status"] == ACCEPTED
    assert replay_events(
        [copy.deepcopy(wide)], config={"price_limits": {}}
    )["results"][0]["status"] == ACCEPTED


def test_price_breach_occupies_event_id_and_advances_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "bad", "SELL", "LIMIT", 2, 200),   # breach, consumed
        add("e3", "AAA", 2, "o3", "SELL", "LIMIT", 2, 100),   # sequence already at 2
        add("e4", "AAA", 3, "o4", "SELL", "LIMIT", 2, 100),   # correct next sequence
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][3]["status"] == ACCEPTED


def test_price_breach_occupies_the_event_id_globally():
    out = replay_events([
        add("e2", "AAA", 1, "bad", "SELL", "LIMIT", 2, 200),
        # Same event id, different content: a conflict, proving the id is spent.
        add("e2", "AAA", 2, "o3", "SELL", "LIMIT", 2, 100),
        # Identical retried delivery is still recognized as a duplicate.
        add("e2", "AAA", 1, "bad", "SELL", "LIMIT", 2, 200),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT
    assert out["results"][2]["status"] == DUPLICATE


def test_price_breach_leaves_no_order_and_spends_no_trade_id():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "bad", "BUY", "LIMIT", 5, 111, time_in_force="IOC"),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    # The breach never matched and the resting sell is untouched.
    assert out["results"][1]["asks"] == [{"price": 100, "quantity": 2}]
    # The first real trade afterwards still takes id 1.
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]


def test_fok_price_breach_is_rejected_without_matching():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 89, time_in_force="FOK"),
    ], config=LIMITS)
    r = out["results"][1]
    assert r["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]


def test_duplicate_order_id_add_takes_priority_over_price_breach():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 200),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_price_breach_on_one_symbol_does_not_touch_another():
    out = replay_events([
        add("e1", "AAA", 1, "a1", "SELL", "LIMIT", 2, 200),
        add("e2", "BBB", 1, "b1", "SELL", "LIMIT", 2, 200),
    ], config={"price_limits": {"AAA": {"lower": 90, "upper": 110}}})
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][1]["status"] == ACCEPTED


def test_structural_invalidation_takes_priority_over_price_breach():
    # A malformed ADD (non-positive quantity) at an out-of-range price is still
    # classified INVALID_EVENT and consumes neither id nor sequence.
    out = replay_events([
        add("e1", "AAA", 1, "bad", "SELL", "LIMIT", 0, 200),
        add("e2", "AAA", 1, "ok", "SELL", "LIMIT", 2, 100),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED


def test_breaching_add_creates_no_order_so_its_order_id_is_not_retained():
    # The check runs before matching and any state change: the event id is
    # spent but no order is created, parallelling a breaching plan reserving
    # neither its plan id nor its derived ids.
    out = replay_events([
        add("e1", "AAA", 1, "same-oid", "SELL", "LIMIT", 2, 200),  # breach
        add("e2", "AAA", 2, "same-oid", "SELL", "LIMIT", 2, 100),  # in range
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][1]["result"] == "RESTING"


def test_nested_payload_form_is_price_checked():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": "ADD", "order_id": "o1",
                   "side": "SELL", "order_type": "LIMIT",
                   "quantity": 2, "price": 200}},
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED


# ---------------------------------------------------------------------------
# REPLACE enforcement: out-of-range keeps the resting order and its priority
# ---------------------------------------------------------------------------


def test_replace_inside_the_interval_replaces_normally():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        replace("e2", "AAA", 2, "o1", 2, 105),
    ], config=LIMITS)
    assert out["results"][1]["result"] == "REPLACED"
    assert out["results"][1]["bids"] == [{"price": 105, "quantity": 2}]


def test_replace_outside_interval_keeps_order_quantity_price_and_priority():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "o2", "BUY", "LIMIT", 2, 100),
        replace("e3", "AAA", 3, "o1", 2, 111),
        add("e4", "AAA", 4, "s1", "SELL", "LIMIT", 4, 100),
    ], config=LIMITS)
    r = out["results"][2]
    assert r["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # Original order keeps its price, quantity and front-of-queue priority.
    assert r["bids"] == [{"price": 100, "quantity": 4}]
    assert [t["maker_order_id"] for t in out["results"][3]["trades"]] == ["o1", "o2"]
    # The original order is still replaceable afterwards.
    follow = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        replace("e2", "AAA", 2, "o1", 2, 101),
    ], config=LIMITS)
    assert follow["results"][1]["status"] == ACCEPTED


def test_replace_of_iceberg_outside_interval_keeps_the_iceberg():
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        replace("e2", "AAA", 2, "i1", 8, 89),
    ], config=LIMITS)
    r = out["results"][1]
    assert r["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert r["asks"] == [{"price": 100, "quantity": 3}]
    # The reserve survives: a buyer crossing 100 still sees the full iceberg.
    filled = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        replace("e2", "AAA", 2, "i1", 8, 89),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 10, 100),
    ], config=LIMITS)
    trades = filled["results"][2]["trades"]
    assert sum(t["quantity"] for t in trades) == 10
    assert filled["results"][2]["asks"] == []


def test_replace_unknown_order_keeps_unknown_order_code():
    out = replay_events([replace("e1", "AAA", 1, "ghost", 2, 200)], config=LIMITS)
    assert out["results"][0]["rejection_code"] == "UNKNOWN_ORDER"


def test_replace_of_finished_order_keeps_unknown_order_code():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100),
        replace("e3", "AAA", 3, "s1", 1, 200),
    ], config=LIMITS)
    assert out["results"][2]["rejection_code"] == "UNKNOWN_ORDER"


def test_replace_display_slice_on_non_iceberg_keeps_baseline_schema_code():
    # A display slice offered for a non-iceberg resting target is an engine
    # structural rejection (INVALID_SCHEMA, the baseline code the replay layer
    # forwards verbatim); it keeps priority over the price-limit breach and the
    # regular dispatch path handles it exactly as without price_limits.
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        replace("e2", "AAA", 2, "o1", 5, 200, display_quantity=2),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "INVALID_SCHEMA"
    # Same code with and without limits.
    baseline = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        replace("e2", "AAA", 2, "o1", 5, 200, display_quantity=2),
    ])
    assert baseline["results"][1]["rejection_code"] == "INVALID_SCHEMA"


# ---------------------------------------------------------------------------
# TWAP / VWAP plan START enforcement
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("price", [89, 111])
def test_twap_start_with_limit_outside_interval_is_rejected(price):
    out = replay_events(
        [twap_start("e1", "AAA", 1, "p1", "LIMIT", price)], config=LIMITS
    )
    r = out["results"][0]
    assert r["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}


@pytest.mark.parametrize("price", [90, 110])
def test_twap_start_inside_interval_is_accepted(price):
    out = replay_events(
        [twap_start("e1", "AAA", 1, "p1", "LIMIT", price)], config=LIMITS
    )
    assert out["results"][0]["status"] == ACCEPTED
    assert out["results"][0]["execution_plan"]["status"] == "ACTIVE"


def test_out_of_range_plan_creates_no_plan_or_derived_ids():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p1", "LIMIT", 200),
        plan_slice("e2", "AAA", 2, "p1"),
        # The rejected plan reserved none of its derived ids, so a fresh
        # in-range start with the same plan_id is a new plan.
        twap_start("e3", "AAA", 3, "p1", "LIMIT", 95),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][1]["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"
    assert out["results"][2]["status"] == ACCEPTED


def test_out_of_range_plan_does_not_reserve_derived_order_ids():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p1", "LIMIT", 200),
        # p1#1 would be a reserved derived id had the plan started.
        add("e2", "AAA", 2, "p1#1", "SELL", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][1]["status"] == ACCEPTED


def test_market_twap_start_is_not_checked():
    out = replay_events([twap_start("e1", "AAA", 1, "p1", "MARKET")], config=LIMITS)
    assert out["results"][0]["status"] == ACCEPTED


@pytest.mark.parametrize("price", [89, 111])
def test_vwap_start_with_limit_outside_interval_is_rejected(price):
    out = replay_events(
        [vwap_start("e1", "AAA", 1, "p1", "LIMIT", price)], config=LIMITS
    )
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    follow = replay_events([
        vwap_start("e1", "AAA", 1, "p1", "LIMIT", price),
        plan_slice("e2", "AAA", 2, "p1", slice_type=VWAP_SLICE),
    ], config=LIMITS)
    assert follow["results"][1]["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"


def test_vwap_start_inside_interval_is_accepted():
    out = replay_events(
        [vwap_start("e1", "AAA", 1, "p1", "LIMIT", 105)], config=LIMITS
    )
    assert out["results"][0]["status"] == ACCEPTED


def test_market_vwap_start_is_not_checked():
    out = replay_events([vwap_start("e1", "AAA", 1, "p1", "MARKET")], config=LIMITS)
    assert out["results"][0]["status"] == ACCEPTED


def test_duplicate_execution_plan_takes_priority_over_price_breach():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p1", "LIMIT", 100),
        twap_start("e2", "AAA", 2, "p1", "LIMIT", 200),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_EXECUTION_PLAN"


def test_derived_id_conflict_takes_priority_over_price_breach():
    # An existing order squats on the new plan's first derived id; that
    # DUPLICATE_ORDER_ID must win even though the plan price is out of range.
    out = replay_events([
        add("e1", "AAA", 1, "p9#1", "SELL", "LIMIT", 1, 100),
        twap_start("e2", "AAA", 2, "p9", "LIMIT", 200, total_quantity=2, slice_count=1),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_accepted_plan_slices_still_enforce_via_their_reserved_price():
    # Sanity companion: an in-range LIMIT plan slices and trades exactly as
    # without limits; price limits add no extra slice-time filtering. The
    # 4-quantity plan over 2 slices releases 2 per slice.
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 95),
        twap_start("e2", "AAA", 2, "p1", "LIMIT", 95, total_quantity=4, slice_count=2),
        plan_slice("e3", "AAA", 3, "p1"),
    ], config=LIMITS)
    slice_result = out["results"][2]
    assert slice_result["result"] == "FILLED"
    # The first slice takes the 2 available units as one trade, id 1.
    assert [t["trade_id"] for t in slice_result["trades"]] == [1]


# ---------------------------------------------------------------------------
# Snapshot integration
# ---------------------------------------------------------------------------


def _mixed_stream_part1():
    return [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 3, 500),
        twap_start("e3", "AAA", 2, "p1", "LIMIT", 95),
        add("e4", "AAA", 3, "s2", "SELL", "LIMIT", 2, 101),
        add("e5", "AAA", 4, "wide", "SELL", "LIMIT", 2, 999),   # breach, consumed
    ]


def _mixed_stream_part2():
    return [
        plan_slice("e6", "AAA", 5, "p1"),
        replace("e7", "BBB", 2, "x1", 3, 505),
        add("e8", "AAA", 6, "b1", "BUY", "LIMIT", 6, 101),
        replace("e9", "BBB", 3, "x1", 3, 9999),                 # breach on BBB
    ]


def test_price_limits_are_part_of_the_snapshot_config():
    snap = replay_events(_mixed_stream_part1(), config=LIMITS)["snapshot"]
    assert snap["config"]["price_limits"] == LIMITS["price_limits"]
    assert snap["config_digest"] == EventReplayer(LIMITS).config_digest
    assert len(snap["content_digest"]) == 64


def test_resumed_replay_with_same_limits_matches_one_shot_byte_for_byte():
    part1 = _mixed_stream_part1()
    part2 = _mixed_stream_part2()
    one_shot = replay_events(part1 + part2, config=LIMITS)
    snapshot = replay_events(part1, config=LIMITS)["snapshot"]
    segmented = replay_events(part2, config=LIMITS, snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_resume_with_different_limits_raises_config_mismatch():
    snapshot = replay_events(_mixed_stream_part1(), config=LIMITS)["snapshot"]
    tighter = {"price_limits": {"AAA": {"lower": 90, "upper": 109}}}
    with pytest.raises(SnapshotError) as exc:
        replay_events(_mixed_stream_part2(), config=tighter, snapshot=snapshot)
    assert exc.value.code == CONFIG_MISMATCH


def test_resume_without_limits_against_limited_snapshot_raises_config_mismatch():
    snapshot = replay_events(_mixed_stream_part1(), config=LIMITS)["snapshot"]
    with pytest.raises(SnapshotError) as exc:
        replay_events(_mixed_stream_part2(), snapshot=snapshot)
    assert exc.value.code == CONFIG_MISMATCH


def test_resume_with_limits_against_baseline_snapshot_raises_config_mismatch():
    snapshot = replay_events([])["snapshot"]
    with pytest.raises(SnapshotError) as exc:
        replay_events([], config=LIMITS, snapshot=snapshot)
    assert exc.value.code == CONFIG_MISMATCH


def test_roundtrip_export_restore_preserves_limited_session():
    first = replay_events(_mixed_stream_part1(), config=LIMITS)
    replayer = restore_replayer(first["snapshot"], config=LIMITS)
    assert replayer.config["price_limits"] == LIMITS["price_limits"]
    assert canonical_json(export_snapshot(replayer)) == canonical_json(first["snapshot"])
    # Enforcement is still active after the snapshot boundary.
    out = replay_events(
        [add("e6", "AAA", 5, "wide", "SELL", "LIMIT", 1, 5)],
        config=LIMITS, snapshot=export_snapshot(replayer),
    )
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED


# ---------------------------------------------------------------------------
# CLI: invalid configuration maps to INVALID_REQUEST / exit code 2
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


@pytest.mark.parametrize("bad_config", [
    {"price_limits": []},
    {"price_limits": {"AAA": {"lower": 0, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 5, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": True, "upper": 2}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 2, "x": 3}}},
])
def test_cli_invalid_price_limits_returns_invalid_request_exit_2(bad_config):
    code, out, err = _run_cli({"events": [], "config": bad_config})
    assert code == 2
    assert out["error"]["code"] == "INVALID_REQUEST"
    assert err == ""


def test_cli_invalid_config_with_snapshot_still_invalid_request():
    good = replay_events([], config=LIMITS)["snapshot"]
    code, out, _ = _run_cli({
        "events": [], "snapshot": good,
        "config": {"price_limits": {"AAA": {"lower": 5, "upper": 1}}},
    })
    assert code == 2
    assert out["error"]["code"] == "INVALID_REQUEST"


def test_cli_config_mismatch_keeps_snapshot_error_document():
    good = replay_events([], config=LIMITS)["snapshot"]
    code, out, _ = _run_cli({
        "events": [], "snapshot": good,
        "config": {"price_limits": {"AAA": {"lower": 90, "upper": 111}}},
    })
    assert code == 2
    assert out["error"]["code"] == CONFIG_MISMATCH


def test_cli_end_to_end_enforces_price_limits():
    code, out, _ = _run_cli({
        "events": [
            add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
            add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 2, 200),
        ],
        "config": LIMITS,
    })
    assert code == 0
    assert out["results"][0]["result"] == "RESTING"
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["snapshot"]["config"]["price_limits"] == LIMITS["price_limits"]
