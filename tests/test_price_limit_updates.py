"""Tests for the replay-only intraday PRICE_LIMIT_UPDATE command.

An update fully replaces one security's *active* price interval: the new
bounds constrain every subsequently submitted LIMIT/ICEBERG ADD, REPLACE
and limit TWAP/VWAP START (existing identifier/business precedence kept),
and already-started limit plans are re-checked against the interval active
when each SLICE is released. Narrowing never cancels or moves resting
orders; only newly submitted limit prices are constrained, never trade
prices. The active interval is part of the digest-protected snapshot;
snapshots written before the feature existed initialize it from config.
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    INVALID_EVENT,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    SEQUENCE_GAP,
    SNAPSHOT_CORRUPT,
    EventReplayer,
    SnapshotError,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.event_replay import _digest


LIMITS = {"price_limits": {"AAA": {"lower": 95, "upper": 105}}}
SYMBOL = "AAA"


def add(event_id, sequence, order_id, side, order_type, quantity, price=None, **extra):
    event = {
        "event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
        "type": "ADD", "order_id": order_id, "side": side,
        "order_type": order_type, "quantity": quantity,
    }
    if price is not None:
        event["price"] = price
    event.update(extra)
    return event


def replace(event_id, sequence, order_id, quantity, price, **extra):
    event = {
        "event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
        "type": "REPLACE", "order_id": order_id,
        "quantity": quantity, "price": price,
    }
    event.update(extra)
    return event


def update(event_id, sequence, lower, upper, symbol=SYMBOL, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "PRICE_LIMIT_UPDATE",
        "lower_price": lower, "upper_price": upper,
    }
    event.update(extra)
    return event


def twap_start(event_id, sequence, plan_id, price, order_type="LIMIT",
               side="BUY", total_quantity=4, slice_count=2):
    event = {
        "event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
        "type": "TWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def twap_slice(event_id, sequence, plan_id):
    return {"event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
            "type": "TWAP_SLICE", "plan_id": plan_id}


def vwap_start(event_id, sequence, plan_id, price, order_type="LIMIT"):
    event = {
        "event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
        "type": "VWAP_START", "plan_id": plan_id, "side": "BUY",
        "total_quantity": 6, "volume_weights": [1, 2, 3],
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def vwap_slice(event_id, sequence, plan_id):
    return {"event_id": event_id, "symbol": SYMBOL, "sequence": sequence,
            "type": "VWAP_SLICE", "plan_id": plan_id}


def code(result):
    return result.get("result") or result.get("rejection_code")


def state_for(snapshot, symbol=SYMBOL):
    for entry in snapshot["content"]["symbols"]:
        if entry["symbol"] == symbol:
            return entry["state"]
    raise KeyError(symbol)


# ---------------------------------------------------------------------------
# Acceptance and result shape
# ---------------------------------------------------------------------------


def test_update_is_accepted_and_returns_active_interval():
    out = replay_events([update("u1", 1, 90, 110)])
    result = out["results"][0]
    assert result["status"] == ACCEPTED
    assert result["result"] == PRICE_LIMIT_UPDATED
    assert result["active_price_limits"] == {"lower_price": 90, "upper_price": 110}
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": []}
    assert result["bids"] == []
    assert result["asks"] == []
    # No other result-only field leaks in.
    assert "execution_plan" not in result
    assert "portfolio_analysis" not in result
    assert "rejection_code" not in result


def test_update_echoes_the_unchanged_book():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 3, 100),
        update("u1", 2, 90, 110),
    ])
    result = out["results"][1]
    assert result["result"] == PRICE_LIMIT_UPDATED
    assert result["bids"] == []
    assert result["asks"] == [{"price": 100, "quantity": 3}]
    assert result["book_changes"] == {"bids": [], "asks": []}


def test_same_bounds_update_still_succeeds():
    out = replay_events([
        update("u1", 1, 95, 105),
        update("u2", 2, 95, 105),
    ], config=LIMITS)
    assert [r["result"] for r in out["results"]] == [PRICE_LIMIT_UPDATED] * 2


def test_degenerate_single_price_interval_is_accepted_and_enforced():
    out = replay_events([
        update("u1", 1, 100, 100),
        add("e2", 2, "a", "SELL", "LIMIT", 1, 99),
        add("e3", 3, "b", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(out["results"][2]) == "RESTING"


def test_update_fully_replaces_not_intersects_the_interval():
    out = replay_events([
        update("u1", 1, 90, 110),
        update("u2", 2, 1, 5),
        add("e3", 3, "a", "SELL", "LIMIT", 1, 100),
        add("e4", 4, "b", "SELL", "LIMIT", 1, 3),
    ])
    # 100 was legal under the first interval but is outside the replacement.
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(out["results"][3]) == "RESTING"


def test_update_works_through_stateful_replayer_and_nested_payload():
    replayer = EventReplayer(config=LIMITS)
    results = replayer.submit([
        {"event_id": "u1", "symbol": SYMBOL, "sequence": 1,
         "event": {"event_id": "u1", "type": "PRICE_LIMIT_UPDATE",
                   "lower_price": 90, "upper_price": 110}},
    ])
    assert results[0]["result"] == PRICE_LIMIT_UPDATED
    # The stateful session now enforces the new bounds.
    follow = replayer.submit([add("e2", 2, "a", "SELL", "LIMIT", 1, 108)])
    assert code(follow[0]) == "RESTING"


def test_update_on_unconfigured_symbol_then_constrains_it():
    # No static config at all: the first event on ZZZ constrains it.
    out = replay_events([
        update("u1", 1, 10, 20, symbol="ZZZ"),
        add("e2", 2, "a", "BUY", "LIMIT", 1, 21, symbol="ZZZ"),
        add("e3", 3, "b", "BUY", "LIMIT", 1, 20, symbol="ZZZ"),
    ])
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(out["results"][2]) == "RESTING"


def test_updates_are_per_symbol_independent():
    out = replay_events([
        update("u1", 1, 10, 20, symbol="AAA"),
        add("e2", 1, "a", "SELL", "LIMIT", 1, 100, symbol="BBB"),
        add("e3", 2, "b", "SELL", "LIMIT", 1, 15, symbol="AAA"),
        add("e4", 2, "c", "SELL", "LIMIT", 1, 99, symbol="BBB"),
    ])
    assert code(out["results"][1]) == "RESTING"   # BBB unconstrained
    assert code(out["results"][2]) == "RESTING"   # 15 inside AAA's band
    assert code(out["results"][3]) == "RESTING"   # BBB still unconstrained


# ---------------------------------------------------------------------------
# Schema validation: INVALID_EVENT consumes neither id nor sequence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    {"lower_price": 90},                                  # missing upper
    {"upper_price": 110},                                 # missing lower
    {"lower_price": 90, "upper_price": 110, "x": 1},      # extra field
    {"lower_price": True, "upper_price": 110},            # bool masquerade
    {"lower_price": 90, "upper_price": False},
    {"lower_price": 0, "upper_price": 110},               # non-positive
    {"lower_price": -1, "upper_price": 110},
    {"lower_price": 90, "upper_price": 0},
    {"lower_price": 110, "upper_price": 90},              # inverted
    {"lower_price": "90", "upper_price": 110},            # string
    {"lower_price": 90.0, "upper_price": 110},            # float
    {"lower_price": None, "upper_price": 110},            # null
])
def test_malformed_update_is_invalid_event(payload):
    event = {"event_id": "u1", "symbol": SYMBOL, "sequence": 1,
             "type": "PRICE_LIMIT_UPDATE", **payload}
    out = replay_events([event, update("u2", 1, 1, 2)])
    first = out["results"][0]
    assert first["status"] == REJECTED
    assert first["rejection_code"] == INVALID_EVENT
    # Nothing was consumed: the very next event still uses sequence 1 and a
    # fresh event id, and the malformed update left no active interval.
    assert out["results"][1]["result"] == PRICE_LIMIT_UPDATED
    assert out["results"][1]["active_price_limits"] == {"lower_price": 1, "upper_price": 2}


def test_nested_update_with_extra_wrapper_field_is_invalid():
    event = {
        "event_id": "u1", "symbol": SYMBOL, "sequence": 1,
        "event": {"event_id": "u1", "type": "PRICE_LIMIT_UPDATE",
                  "lower_price": 90, "upper_price": 110},
        "bogus": 1,
    }
    out = replay_events([event])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_invalid_update_reusing_a_seen_id_does_not_conflict():
    # Schema validation precedes the idempotency check, so a structurally
    # invalid retry is INVALID_EVENT (and consumes nothing).
    good = update("u1", 1, 90, 110)
    bad = dict(good, sequence=2)
    del bad["upper_price"]
    out = replay_events([good, bad])
    assert out["results"][0]["result"] == PRICE_LIMIT_UPDATED
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency / sequence semantics
# ---------------------------------------------------------------------------


def test_identical_update_retry_is_a_duplicate():
    event = update("u1", 2, 90, 110)
    out = replay_events([
        add("e0", 1, "s1", "SELL", "LIMIT", 1, 100),
        event,
        dict(event, sequence=9),   # retried delivery carries a stale sequence
    ], config=LIMITS)
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][2]["trades"] == []


def test_same_id_different_content_is_event_id_conflict():
    out = replay_events([
        update("u1", 1, 90, 110),
        update("u1", 2, 1, 2),
    ])
    assert out["results"][1]["rejection_code"] == "EVENT_ID_CONFLICT"


def test_sequence_gap_and_out_of_order_apply_to_updates():
    out = replay_events([
        update("u1", 1, 90, 110),
        update("u2", 3, 1, 2),     # gap
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][1]["expected_sequence"] == 2
    follow = replay_events([
        update("u1", 1, 90, 110),
        update("u2", 1, 1, 2),     # backwards
    ])
    assert follow["results"][1]["rejection_code"] == "OUT_OF_ORDER"
    assert follow["results"][1]["expected_sequence"] == 2


def test_update_event_id_lives_only_in_the_replay_log():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 1, 100),
        update("u2", 2, 90, 110),
    ], config=LIMITS)
    state = state_for(out["snapshot"])
    assert "u2" not in state["engine"]["event_ids"]
    assert "u2" in {entry["event_id"] for entry in state["event_log"]}
    assert "u2" in {e["event_id"] for e in out["snapshot"]["content"]["events"]}


def test_update_whose_id_clashes_with_reserved_child_id_is_duplicate_event_id():
    out = replay_events([
        twap_start("e1", 1, "p1", 100),
        update("p1#1", 2, 90, 110),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_EVENT_ID"
    # The bounds were not changed by the rejected update.
    after = replay_events([
        twap_start("e1", 1, "p1", 100),
        update("p1#1", 2, 90, 110),
        add("e3", 3, "a", "SELL", "LIMIT", 1, 106),
    ], config=LIMITS)
    assert after["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED


# ---------------------------------------------------------------------------
# Enforcement on subsequently submitted limit prices
# ---------------------------------------------------------------------------


def test_widened_interval_allows_formerly_illegal_add():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 1, 100),
        update("u2", 2, 90, 110),
        add("e3", 3, "a", "SELL", "LIMIT", 1, 108),
        add("e4", 4, "b", "SELL", "LIMIT", 1, 89),
    ], config=LIMITS)
    assert code(out["results"][2]) == "RESTING"
    assert out["results"][3]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_add_breach_after_update_consumes_id_and_sequence_but_no_trade_id():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 1, 100),
        update("u2", 2, 101, 110),
        add("e3", 3, "b1", "BUY", "LIMIT", 5, 100),   # would cross, rejected
        add("e4", 4, "b2", "BUY", "LIMIT", 1, 105),
    ])
    rejected = out["results"][2]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    assert rejected["asks"] == [{"price": 100, "quantity": 1}]
    # The legal buy trades first, so trade ids start at 1 despite the breach.
    assert [t["trade_id"] for t in out["results"][3]["trades"]] == [1]


def test_replace_breach_after_update_keeps_order_and_queue_position():
    out = replay_events([
        add("e1", 1, "o1", "BUY", "LIMIT", 2, 100),
        add("e2", 2, "o2", "BUY", "LIMIT", 2, 100),
        update("u3", 3, 95, 104),
        replace("e4", 4, "o1", 2, 105),               # outside the new band
        add("e5", 5, "s1", "SELL", "LIMIT", 2, 100),
    ])
    assert out["results"][3]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    # o1 kept its head-of-queue priority and its original price/quantity.
    assert [t["maker_order_id"] for t in out["results"][4]["trades"]] == ["o1"]


def test_limit_plan_start_after_update_is_checked():
    out = replay_events([
        update("u1", 1, 101, 110),
        twap_start("e2", 2, "p1", 100),               # now out of bounds
        add("e3", 3, "p1#1", "BUY", "LIMIT", 1, 105),  # id never reserved
        twap_slice("e4", 4, "p1"),                    # plan never created
    ])
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert "execution_plan" not in out["results"][1]
    assert code(out["results"][2]) == "RESTING"
    assert out["results"][3]["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"


def test_vwap_limit_start_after_update_is_checked():
    out = replay_events([
        update("u1", 1, 101, 110),
        vwap_start("e2", 2, "v1", 100),
    ])
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_market_orders_and_market_plans_remain_exempt_after_narrowing():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 2, 100),
        update("u2", 2, 101, 110),
        add("e3", 3, "m1", "BUY", "MARKET", 2),
        twap_start("e4", 4, "p1", None, order_type="MARKET", side="SELL"),
        twap_slice("e5", 5, "p1"),
    ])
    assert code(out["results"][2]) == "FILLED"
    assert out["results"][3]["status"] == ACCEPTED
    assert code(out["results"][4]) == "UNFILLED_CANCELLED"


# ---------------------------------------------------------------------------
# Started limit plans are re-checked before every SLICE release
# ---------------------------------------------------------------------------


def test_limit_twap_slice_is_rechecked_against_the_active_interval():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100, total_quantity=4, slice_count=2),
        twap_slice("e3", 3, "p1"),                    # fills slice 1
        update("u4", 4, 101, 110),                    # 100 now out of bounds
        twap_slice("e5", 5, "p1"),
    ])
    rejected = out["results"][4]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    assert "execution_plan" not in rejected

    # The rejected slice moved none of the plan counters and spent no trade
    # id; widening the band lets the *same* slice number release and fill.
    follow = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100, total_quantity=4, slice_count=2),
        twap_slice("e3", 3, "p1"),
        update("u4", 4, 101, 110),
        twap_slice("e5", 5, "p1"),                    # rejected, id occupied
        update("u6", 6, 95, 110),
        twap_slice("e7", 7, "p1"),                    # still slice number 2
        twap_slice("e8", 8, "p1"),                    # closed after completion
    ])
    released = follow["results"][6]
    assert released["execution_plan"]["slice_number"] == 2
    assert released["execution_plan"]["child_order_id"] == "p1#2"
    assert released["execution_plan"]["released_quantity"] == 4
    assert [t["trade_id"] for t in released["trades"]] == [2]
    assert follow["results"][7]["rejection_code"] == "EXECUTION_PLAN_CLOSED"


def test_rejected_slice_occupies_its_event_id():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100),
        update("u3", 3, 101, 110),
        twap_slice("e4", 4, "p1"),                    # rejected
        twap_slice("e4", 4, "p1"),                    # identical retry
    ])
    assert out["results"][3]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][4]["status"] == DUPLICATE


def test_rejected_slice_keeps_the_next_child_id_reserved():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100),
        update("u3", 3, 101, 110),
        twap_slice("e4", 4, "p1"),                    # rejected, no release
        add("e5", 5, "p1#1", "BUY", "LIMIT", 1, 105),  # spent already
        add("e6", 6, "p1#2", "BUY", "LIMIT", 1, 105),  # still reserved
    ])
    assert out["results"][4]["rejection_code"] == "DUPLICATE_ORDER_ID"
    assert out["results"][5]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_limit_vwap_slice_is_rechecked_like_twap():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 6, 100),
        vwap_start("e2", 2, "v1", 100),
        vwap_slice("e3", 3, "v1"),
        update("u4", 4, 101, 110),
        vwap_slice("e5", 5, "v1"),
    ])
    assert out["results"][4]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    state = state_for(out["snapshot"])
    plan = state["plans"][0]
    assert plan["released"] == 1
    assert plan["slice_quantities"] == [2, 2, 2]
    # Only the first child id left the reservation set.
    assert state["engine"]["reserved_order_ids"] == ["v1#2", "v1#3"]


def test_market_plan_slice_is_not_rechecked_after_narrowing():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", None, order_type="MARKET"),
        update("u3", 3, 101, 110),
        twap_slice("e4", 4, "p1"),
    ])
    assert out["results"][3]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Narrowing constrains new limit prices, never resting orders or trade prices
# ---------------------------------------------------------------------------


def test_narrowing_leaves_resting_orders_untouched_in_book():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 3, 100),
        update("u2", 2, 101, 110),
    ])
    result = out["results"][1]
    # The old order is still quoted at 100, outside the new band.
    assert result["asks"] == [{"price": 100, "quantity": 3}]
    assert result["book_changes"] == {"bids": [], "asks": []}


def test_out_of_band_old_order_still_makes_market_against_market_taker():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 3, 100),
        update("u2", 2, 101, 110),
        add("e3", 3, "m1", "BUY", "MARKET", 3),
    ])
    trades = out["results"][2]["trades"]
    assert [t["price"] for t in trades] == [100]
    assert [t["maker_order_id"] for t in trades] == ["s1"]


def test_in_band_new_limit_trades_against_out_of_band_maker_at_maker_price():
    out = replay_events([
        add("e1", 1, "s1", "SELL", "LIMIT", 2, 100),
        update("u2", 2, 95, 105),          # 100 still inside, then narrow:
        update("u3", 3, 101, 110),         # resting s1 at 100 is now outside
        add("e4", 4, "b1", "BUY", "LIMIT", 2, 105),  # new price is inside
    ])
    trades = out["results"][3]["trades"]
    # The trade prints at the maker's out-of-band price; limits bind
    # submission prices, not execution prices.
    assert [(t["price"], t["maker_order_id"]) for t in trades] == [(100, "s1")]


# ---------------------------------------------------------------------------
# Snapshot persistence, digest protection and legacy snapshots
# ---------------------------------------------------------------------------


def test_active_interval_is_in_snapshot_state():
    out = replay_events([update("u1", 1, 90, 110)], config=LIMITS)
    state = state_for(out["snapshot"])
    assert state["active_price_limits"] == {"lower_price": 90, "upper_price": 110}

    plain = replay_events([add("e1", 1, "s1", "SELL", "LIMIT", 1, 100)])
    assert state_for(plain["snapshot"])["active_price_limits"] is None

    static = replay_events([add("e1", 1, "s1", "SELL", "LIMIT", 1, 100)],
                           config=LIMITS)
    # No update yet: the active interval is the static config interval.
    assert state_for(static["snapshot"])["active_price_limits"] == {
        "lower_price": 95, "upper_price": 105,
    }


def test_active_interval_is_protected_by_the_content_digest():
    snapshot = replay_events([update("u1", 1, 90, 110)])["snapshot"]
    tampered = copy.deepcopy(snapshot)
    state_for(tampered)["active_price_limits"]["upper_price"] = 200
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_snapshot_resumes_byte_identically_through_updates_and_slice_breach():
    part1 = [
        add("e1", 1, "s1", "SELL", "LIMIT", 6, 100),
        twap_start("e2", 2, "p1", 100, total_quantity=6, slice_count=3),
        twap_slice("e3", 3, "p1"),
        update("u4", 4, 101, 110),
        twap_slice("e5", 5, "p1"),                        # price breach
    ]
    part2 = [
        update("u6", 6, 95, 110),
        twap_slice("e7", 7, "p1"),
        replace("e8", 8, "s1", 2, 108),
        add("e9", 9, "a", "SELL", "LIMIT", 1, 109),
    ]
    one_shot = replay_events(part1 + part2)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(
        one_shot["snapshot"]
    )
    # Restore/export is idempotent and keeps the active interval.
    replayer = restore_replayer(one_shot["snapshot"])
    assert canonical_json(export_snapshot(replayer)) == canonical_json(
        one_shot["snapshot"]
    )


def test_legacy_snapshot_without_active_limits_initializes_from_config():
    events = [
        add("e1", 1, "s1", "SELL", "LIMIT", 1, 100),
    ]
    snapshot = replay_events(events, config=LIMITS)["snapshot"]
    # Simulate a snapshot written before PRICE_LIMIT_UPDATE existed.
    del state_for(snapshot)["active_price_limits"]
    snapshot["content_digest"] = _digest(
        {key: value for key, value in snapshot.items() if key != "content_digest"}
    )
    replayer = restore_replayer(snapshot, config=LIMITS)
    follow = replayer.submit([
        add("e2", 2, "a", "BUY", "LIMIT", 1, 94),
        add("e3", 3, "b", "BUY", "LIMIT", 1, 100),
    ])
    assert follow[0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(follow[1]) == "FILLED"


def test_legacy_snapshot_without_config_stays_unconstrained():
    snapshot = replay_events([add("e1", 1, "s1", "SELL", "LIMIT", 1, 100)])["snapshot"]
    del state_for(snapshot)["active_price_limits"]
    snapshot["content_digest"] = _digest(
        {key: value for key, value in snapshot.items() if key != "content_digest"}
    )
    replayer = restore_replayer(snapshot)
    follow = replayer.submit([add("e2", 2, "a", "BUY", "LIMIT", 1, 1_000_000)])
    assert code(follow[0]) == "FILLED"


def test_legacy_snapshot_with_unknown_active_field_is_corrupt():
    snapshot = replay_events([update("u1", 1, 1, 2)])["snapshot"]
    state = state_for(snapshot)
    state["active_price_limits"] = {"lower_price": 1, "upper_price": 2, "x": 3}
    snapshot["content_digest"] = _digest(
        {key: value for key, value in snapshot.items() if key != "content_digest"}
    )
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(snapshot)
    assert exc.value.code == SNAPSHOT_CORRUPT


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _run_cli(request_obj):
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    exit_code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    body = stdout.buffer.getvalue().decode("utf-8")
    return exit_code, json.loads(body) if body else None, stderr.getvalue()


def test_cli_accepts_price_limit_update():
    exit_code, doc, _ = _run_cli({
        "events": [
            {"event_id": "u1", "symbol": "AAA", "sequence": 1,
             "type": "PRICE_LIMIT_UPDATE", "lower_price": 1, "upper_price": 2},
        ],
    })
    assert exit_code == 0
    result = doc["results"][0]
    assert result["result"] == PRICE_LIMIT_UPDATED
    assert result["active_price_limits"] == {"lower_price": 1, "upper_price": 2}


def test_cli_rejects_malformed_update_as_invalid_event():
    exit_code, doc, _ = _run_cli({
        "events": [
            {"event_id": "u1", "symbol": "AAA", "sequence": 1,
             "type": "PRICE_LIMIT_UPDATE", "lower_price": 5, "upper_price": 1},
        ],
    })
    assert exit_code == 0
    assert doc["results"][0]["rejection_code"] == INVALID_EVENT
