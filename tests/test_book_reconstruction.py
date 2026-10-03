"""Tests for the replay-only BOOK_RECONSTRUCTION_REPORT point-in-time query.

A BOOK_RECONSTRUCTION_REPORT rebuilds the envelope symbol's resting order
queues exactly as they stood immediately after a committed per-symbol
sequence (zero denotes the empty pre-session book and the session's initial
price-limit interval). It is read-only: it never matches, never releases a
plan, never moves the active price-limit interval or any other trading state.
The single-security JSON Lines entry point and the baseline Engine reject the
type with INVALID_SCHEMA.
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_RECONSTRUCTION_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    TARGET_SEQUENCE_NOT_FOUND,
    EventReplayer,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine import replay as line_replay


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


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def recon(event_id, symbol, sequence, target_sequence, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": BOOK_RECONSTRUCTION_REPORT,
        "target_sequence": target_sequence,
    }
    event.update(extra)
    return event


def plu(event_id, symbol, sequence, lower, upper):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "PRICE_LIMIT_UPDATE",
            "lower_price": lower, "upper_price": upper}


def twap_start(event_id, sequence, plan_id, side, total_quantity, slice_count, price,
               order_type="LIMIT", symbol="AAA"):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "TWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def twap_slice(event_id, sequence, plan_id, symbol="AAA"):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "TWAP_SLICE", "plan_id": plan_id}


def vwap_start(event_id, sequence, plan_id, side, total_quantity, weights, price,
               symbol="AAA"):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "VWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": weights,
        "order_type": "LIMIT", "benchmark_price": 100, "price": price,
    }


def vwap_slice(event_id, sequence, plan_id, symbol="AAA"):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "VWAP_SLICE", "plan_id": plan_id}


def pov_start(event_id, sequence, plan_id, side, total_quantity, participation_bps, price,
              symbol="AAA"):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "POV_START", "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity,
        "participation_bps": participation_bps,
        "order_type": "LIMIT", "benchmark_price": 100, "price": price,
    }


def pov_volume(event_id, sequence, plan_id, increment, symbol="AAA"):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "POV_VOLUME", "plan_id": plan_id,
            "market_volume_increment": increment}


def book_reconstruction(result):
    return result["book_reconstruction"]


def ask_map(report):
    return {level["price"]: level for level in report["ask_queues"]}


def bid_map(report):
    return {level["price"]: level for level in report["bid_queues"]}


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutation", [
    lambda e: e.pop("target_sequence"),
    lambda e: e.update(target_sequence=-1),
    lambda e: e.update(target_sequence=True),
    lambda e: e.update(target_sequence=False),
    lambda e: e.update(target_sequence="1"),
    lambda e: e.update(target_sequence=1.0),
    lambda e: e.update(target_sequence=None),
    lambda e: e.update(target_sequence=[1]),
    lambda e: e.update(unexpected=1),
    lambda e: e.update(target_sequence=1, plan_id="p1"),
])
def test_malformed_query_is_invalid_event_and_consumes_nothing(mutation):
    bad = recon("q1", "AAA", 2, 1)
    mutation(bad)
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        bad,
        # Sequence 2 is still free: the malformed query consumed nothing.
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
    ], config=LIMITS)
    rejected = out["results"][1]
    assert rejected["status"] == REJECTED
    assert rejected["rejection_code"] == INVALID_EVENT
    assert rejected["event_id"] == "q1"
    assert rejected["sequence"] == 2
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    assert "book_reconstruction" not in rejected
    assert out["results"][2]["status"] == ACCEPTED


def test_invalid_query_leaves_event_id_free_for_reuse():
    out = replay_events([
        recon("q1", "AAA", 1, -1),
        recon("q1", "AAA", 1, 0),
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        {"event_id": "q1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT,
                   "target_sequence": 1}},
    ])
    assert out["results"][1]["status"] == ACCEPTED
    assert book_reconstruction(out["results"][1])["target_sequence"] == 1


def test_nested_payload_event_id_mismatch_is_invalid_event():
    out = replay_events([
        {"event_id": "q1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "OTHER", "type": BOOK_RECONSTRUCTION_REPORT,
                   "target_sequence": 0}},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Success response shape
# ---------------------------------------------------------------------------


def test_target_zero_is_the_empty_book_without_limits():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        recon("q1", "AAA", 2, 0),
    ])
    result = out["results"][1]
    assert list(result) == [
        "event_id", "symbol", "sequence", "status", "result", "trades",
        "book_changes", "bids", "asks", "book_reconstruction",
    ]
    assert result["status"] == ACCEPTED
    assert result["result"] == "REPORTED"
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": []}
    # The outer aggregates remain the query-time book.
    assert result["asks"] == [{"price": 100, "quantity": 2}]
    report = book_reconstruction(result)
    assert list(report) == [
        "symbol", "target_sequence", "active_price_limits",
        "bid_queues", "ask_queues",
    ]
    assert report["symbol"] == "AAA"
    assert report["target_sequence"] == 0
    assert report["active_price_limits"] is None
    assert report["bid_queues"] == []
    assert report["ask_queues"] == []


def test_target_zero_reports_the_session_initial_price_limits():
    out = replay_events([
        plu("e1", "AAA", 1, 95, 105),
        recon("q1", "AAA", 2, 0),
        recon("q2", "AAA", 3, 1),
    ], config=LIMITS)
    # Target zero always reports the configured seed, not the updated band.
    assert book_reconstruction(out["results"][1])["active_price_limits"] == {
        "lower_price": 90, "upper_price": 110,
    }
    # Sequence 1 is the update: the narrower band is already active.
    assert book_reconstruction(out["results"][2])["active_price_limits"] == {
        "lower_price": 95, "upper_price": 105,
    }


def test_reconstructed_queues_match_an_independent_prefix_replay():
    prefix = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 99),
        add("e3", "AAA", 3, "s2", "SELL", "LIMIT", 4, 101),
        add("e4", "AAA", 4, "s0", "SELL", "LIMIT", 2, 100),
        add("e5", "AAA", 5, "b0", "BUY", "LIMIT", 1, 98),
    ]
    # Cross-check every target against a standalone replay of the prefix: the
    # level aggregates must agree and queues keep price-time priority.
    for target in range(0, 6):
        out = replay_events(prefix + [recon("q", "AAA", 6, target)])
        report = book_reconstruction(out["results"][5])
        standalone = replay_events(prefix[:target])["results"]
        expected_asks = standalone[-1]["asks"] if standalone else []
        expected_bids = standalone[-1]["bids"] if standalone else []
        assert [
            {"price": level["price"], "quantity": level["visible_quantity"]}
            for level in report["ask_queues"]
        ] == expected_asks, target
        assert [
            {"price": level["price"], "quantity": level["visible_quantity"]}
            for level in report["bid_queues"]
        ] == expected_bids, target


def test_queues_keep_price_time_priority_within_a_level():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 1, 100),
        add("e4", "AAA", 4, "b9", "BUY", "LIMIT", 1, 90),
        recon("q1", "AAA", 5, 4),
        recon("q2", "AAA", 6, 2),
    ])
    full = ask_map(book_reconstruction(out["results"][4]))[100]
    assert [order["order_id"] for order in full["orders"]] == ["s1", "s2", "s3"]
    assert full["visible_quantity"] == 3
    # Bids sort descending; the single bid at 90 is present.
    assert [level["price"] for level in book_reconstruction(out["results"][4])["bid_queues"]] == [90]
    partial = ask_map(book_reconstruction(out["results"][5]))[100]
    assert [order["order_id"] for order in partial["orders"]] == ["s1", "s2"]


def test_order_fields_for_plain_and_iceberg_orders():
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 5, 101),
        # A buyer takes 2 from i1's first 3-slice at 100: 1 visible remains and
        # the hidden reserve is 7; remaining_quantity stays the total leftover.
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
        recon("q1", "AAA", 4, 3),
    ])
    queues = ask_map(book_reconstruction(out["results"][3]))
    iceberg_level = queues[100]
    assert iceberg_level["visible_quantity"] == 1
    assert iceberg_level["orders"] == [{
        "order_id": "i1",
        "order_type": "ICEBERG",
        "remaining_quantity": 8,   # 1 visible + 7 hidden reserve
        "visible_quantity": 1,     # only the current public slice
    }]
    assert queues[101]["orders"] == [{
        "order_id": "s1",
        "order_type": "LIMIT",
        "remaining_quantity": 5,
        "visible_quantity": 5,
    }]


def test_iceberg_replenishment_requeues_at_the_level_tail():
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        # Drains i1's first slice; the replenished slice queues behind s2.
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 3, 100),
        recon("q1", "AAA", 4, 3),
    ])
    level = ask_map(book_reconstruction(out["results"][3]))[100]
    assert [order["order_id"] for order in level["orders"]] == ["s2", "i1"]
    assert level["visible_quantity"] == 8


def test_filled_cancelled_and_replaced_states_do_not_appear():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "gone", "SELL", "LIMIT", 1, 101),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),   # fills s1
        cancel("e4", "AAA", 4, "gone"),                       # cancels gone
        add("e5", "AAA", 5, "r1", "BUY", "LIMIT", 3, 90),
        replace("e6", "AAA", 6, "r1", 2, 91),                 # r1 moves to 91
        recon("q1", "AAA", 7, 6),
        # At the pre-fill point s1 still rested; gone rested at 101.
        recon("q2", "AAA", 8, 1),
        recon("q3", "AAA", 9, 4),
        recon("q4", "AAA", 10, 5),
    ])
    final = book_reconstruction(out["results"][6])
    assert final["ask_queues"] == []
    assert [level["price"] for level in final["bid_queues"]] == [91]
    assert [order["order_id"] for order in final["bid_queues"][0]["orders"]] == ["r1"]

    at_one = book_reconstruction(out["results"][7])
    assert [order["order_id"] for order in ask_map(at_one)[100]["orders"]] == ["s1"]
    after_cancel = book_reconstruction(out["results"][8])
    assert after_cancel["ask_queues"] == []
    at_five = book_reconstruction(out["results"][9])
    assert [level["price"] for level in at_five["bid_queues"]] == [90]


def test_reconstruction_targeting_the_current_sequence_matches_the_live_book():
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 4, 101),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
        recon("q1", "AAA", 4, 3),
    ])
    result = out["results"][3]
    report = book_reconstruction(result)
    assert [
        {"price": level["price"], "quantity": level["visible_quantity"]}
        for level in report["ask_queues"]
    ] == result["asks"]
    assert result["bids"] == []


def test_rejected_events_and_read_only_reports_occupy_sequences_but_not_state():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        cancel("e2", "AAA", 2, "ghost"),          # UNKNOWN_ORDER business reject
        recon("q0", "AAA", 3, 2),                  # earlier read-only report
        add("e3", "AAA", 4, "dup", "SELL", "LIMIT", 1, 100),
        add("e4", "AAA", 5, "dup", "SELL", "LIMIT", 1, 100),  # DUPLICATE_ORDER_ID
        recon("q1", "AAA", 6, 2),
        recon("q2", "AAA", 7, 3),
        recon("q3", "AAA", 8, 5),
    ])
    # Targets 2 and 3 describe the identical book: only sequence 1 moved it.
    for index in (5, 6):
        report = book_reconstruction(out["results"][index])
        assert ask_map(report)[100]["orders"] == [{
            "order_id": "s1",
            "order_type": "LIMIT",
            "remaining_quantity": 2,
            "visible_quantity": 2,
        }]
    # By sequence 5 the accepted "dup" order rests behind s1; the duplicate
    # resubmission at sequence 5 changed nothing further.
    assert [
        order["order_id"]
        for order in ask_map(book_reconstruction(out["results"][7]))[100]["orders"]
    ] == ["s1", "dup"]


# ---------------------------------------------------------------------------
# Plan child orders and PRICE_LIMIT_UPDATE reconstruction
# ---------------------------------------------------------------------------


def test_twap_child_orders_are_reconstructed_like_real_matching():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("e2", 2, "p1", "BUY", 4, 2, 100),
        twap_slice("e3", 3, "p1"),                # p1#1 buys 2 from s1
        recon("q1", "AAA", 4, 3),
        twap_slice("e4", 5, "p1"),                # p1#2 buys the other 2
        recon("q2", "AAA", 6, 5),
    ])
    first = book_reconstruction(out["results"][3])
    assert ask_map(first)[100]["orders"][0]["remaining_quantity"] == 3
    # IOC children never rest: after both 2-unit slices the seller keeps one.
    final = book_reconstruction(out["results"][5])
    assert [order["order_id"] for order in ask_map(final)[100]["orders"]] == ["s1"]
    assert ask_map(final)[100]["orders"][0]["remaining_quantity"] == 1


def test_vwap_slice_schedule_is_rebuilt():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        # total 6 over weights [1, 2, 3] allocates one unit per bucket first and
        # distributes the remainder by quotient, so the slices are [2, 2, 2].
        vwap_start("e2", 2, "v1", "BUY", 6, [1, 2, 3], 100),
        vwap_slice("e3", 3, "v1"),
        recon("q1", "AAA", 4, 3),
        vwap_slice("e4", 5, "v1"),
        recon("q2", "AAA", 6, 5),
        vwap_slice("e5", 7, "v1"),
        recon("q3", "AAA", 8, 7),
    ])
    remaining = [
        ask_map(book_reconstruction(out["results"][i]))[100]["orders"][0][
            "remaining_quantity"
        ]
        for i in (3, 5, 7)
    ]
    assert remaining == [8, 6, 4]


def test_pov_releases_and_rejected_volumes_are_rebuilt():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        pov_start("e2", 2, "pp", "BUY", 50, 5000, 100),
        pov_volume("e3", 3, "pp", 10),          # target 5 -> releases 5
        recon("q1", "AAA", 4, 3),
        plu("e4", "AAA", 5, 101, 110),                 # plan price 100 now outside
        pov_volume("e5", 6, "pp", 100),         # rejected: no volume retained
        recon("q2", "AAA", 7, 6),               # book unchanged
        plu("e6", "AAA", 8, 90, 110),
        pov_volume("e7", 9, "pp", 20),          # M=30 -> target 15 -> releases 10
        recon("q3", "AAA", 10, 9),
    ], config=LIMITS)
    remaining = [
        ask_map(book_reconstruction(out["results"][i]))[100]["orders"][0][
            "remaining_quantity"
        ]
        for i in (3, 6, 9)
    ]
    assert remaining == [95, 95, 85]


def test_price_limit_breaches_are_committed_but_change_nothing():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 120),   # breach
        plu("e3", "AAA", 3, 95, 120),
        add("e4", "AAA", 4, "s3", "SELL", "LIMIT", 1, 120),   # now legal
        recon("q1", "AAA", 5, 2),
        recon("q2", "AAA", 6, 4),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    # At sequence 2 the breached order is absent.
    assert [level["price"] for level in book_reconstruction(out["results"][4])["ask_queues"]] == [100]
    # At sequence 4 it rests.
    assert [level["price"] for level in book_reconstruction(out["results"][5])["ask_queues"]] == [100, 120]


def test_rejected_plan_start_and_slice_create_nothing():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", "BUY", 4, 2, 100),
        twap_start("e3", 3, "p1", "BUY", 4, 2, 100),   # DUPLICATE_EXECUTION_PLAN
        plu("e4", "AAA", 4, 101, 110),                   # 100 outside now
        twap_slice("e5", 5, "p1"),                      # PRICE_LIMIT_EXCEEDED
        recon("q1", "AAA", 6, 5),
        twap_slice("e6", 1, "ghost"),                   # stale sequence, not logged
    ], config=LIMITS)
    assert out["results"][2]["rejection_code"] == "DUPLICATE_EXECUTION_PLAN"
    assert out["results"][4]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    # Nothing ever traded: the full seller quantity is still resting.
    assert ask_map(book_reconstruction(out["results"][5]))[100]["orders"][0][
        "remaining_quantity"
    ] == 4


# ---------------------------------------------------------------------------
# TARGET_SEQUENCE_NOT_FOUND business rejection
# ---------------------------------------------------------------------------


def test_target_ahead_of_last_committed_sequence_is_business_rejected():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        recon("q1", "AAA", 2, 5),
    ])
    rejected = out["results"][1]
    assert list(rejected) == [
        "event_id", "symbol", "sequence", "status", "rejection_code",
        "trades", "book_changes", "bids", "asks",
    ]
    assert rejected["status"] == REJECTED
    assert rejected["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    assert "book_reconstruction" not in rejected
    # The untouched book is echoed.
    assert rejected["asks"] == [{"price": 100, "quantity": 2}]


def test_target_not_found_consumes_event_id_and_advances_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        recon("qX", "AAA", 2, 9),                        # rejected, consumes seq 2
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),  # stale -> OUT_OF_ORDER
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 2, 100),  # expected seq is 3
        recon("qY", "AAA", 4, 2),                        # target 2 is now the rejection itself
    ])
    assert out["results"][1]["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][3]["status"] == ACCEPTED
    # At rejected sequence 2 nothing moved yet: the seller rests in full (b1 at
    # the stale sequence 2 was never processed; b2 only arrives at sequence 3).
    target_two = ask_map(book_reconstruction(out["results"][4]))[100]
    assert [order["order_id"] for order in target_two["orders"]] == ["s1"]
    assert target_two["orders"][0]["remaining_quantity"] == 2


def test_target_not_found_id_is_occupied_globally():
    out = replay_events([
        recon("q1", "AAA", 1, 1),            # unknown symbol positive -> reject
        {"event_id": "q1", "symbol": "BBB", "sequence": 1,
         "type": "ADD", "order_id": "x1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 9},
    ])
    assert out["results"][0]["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


def test_unknown_symbol_target_zero_succeeds_but_positive_rejects():
    out = replay_events([
        recon("q1", "NEW", 1, 0),      # empty book, symbol registers
        recon("q2", "NEW", 2, 3),      # ahead of last committed (1) -> reject
        recon("q3", "NEW", 3, 1),      # target 1 is q1 itself: empty book
        recon("q4", "NEW", 4, 2),      # target 2 is the rejection: empty book
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED, REJECTED, ACCEPTED, ACCEPTED]
    assert out["results"][1]["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    assert out["results"][1]["bids"] == [] and out["results"][1]["asks"] == []
    assert book_reconstruction(out["results"][2])["ask_queues"] == []
    assert book_reconstruction(out["results"][3])["ask_queues"] == []
    # The successful zero-target query really registered the symbol.
    assert len(out["snapshot"]["content"]["symbols"]) == 1
    assert out["snapshot"]["content"]["symbols"][0]["symbol"] == "NEW"


def test_very_first_event_positive_target_rejects_without_creating_state():
    out = replay_events([recon("q1", "NEW", 2, 1)])
    # Sequence gap on an unknown symbol precedes the business check, so the
    # symbol is not created and the event consumes nothing.
    assert out["results"][0]["rejection_code"] == SEQUENCE_GAP
    assert out["snapshot"]["content"]["symbols"] == []


# ---------------------------------------------------------------------------
# Read-only and idempotency guarantees
# ---------------------------------------------------------------------------


def test_query_matches_nothing_and_releases_no_plan():
    replayer = EventReplayer()
    results = replayer.submit([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", "BUY", 4, 2, 100),
        recon("q1", "AAA", 3, 1),
        recon("q2", "AAA", 4, 2),
    ])
    assert all(r["trades"] == [] for r in results[2:])
    snapshot_after_queries = export_snapshot(replayer)
    state = snapshot_after_queries["content"]["symbols"][0]["state"]
    # No trade id was spent, and the plan never released a slice.
    assert state["engine"]["next_trade_id"] == 1
    assert state["plans"][0]["released"] == 0
    # The seller is untouched.
    assert state["engine"]["orders"]["s1"]["remaining"] == 4
    # Releasing the first slice now behaves exactly as without the queries:
    # one 2-unit trade against s1, carrying the session's first trade id.
    [slice_result] = replayer.submit([twap_slice("e3", 5, "p1")])
    assert [t["trade_id"] for t in slice_result["trades"]] == [1]
    assert slice_result["trades"][0]["quantity"] == 2


def test_duplicate_delivery_is_idempotent():
    query = recon("q1", "AAA", 2, 1)
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        query,
        copy.deepcopy(query),                            # exact retry, stale sequence
        add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
    ])
    duplicate = out["results"][2]
    assert duplicate["status"] == DUPLICATE
    assert "book_reconstruction" not in duplicate
    assert duplicate["trades"] == []
    assert duplicate["book_changes"] == {"bids": [], "asks": []}
    # Sequence 3 proceeds normally and the query never matched.
    assert out["results"][3]["status"] == ACCEPTED


def test_duplicate_content_is_compared_after_canonicalization():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        recon("q1", "AAA", 2, 1),
        {"sequence": 2, "target_sequence": 1, "type": BOOK_RECONSTRUCTION_REPORT,
         "event_id": "q1", "symbol": "AAA"},
    ])
    assert out["results"][2]["status"] == DUPLICATE


def test_same_event_id_with_different_target_conflicts():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        recon("q1", "AAA", 2, 1),
        recon("q1", "AAA", 3, 0),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT
    # The conflict consumed neither the sequence slot nor anything else.
    assert replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        recon("q1", "AAA", 2, 1),
        recon("q1", "AAA", 3, 0),
        add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
    ])["results"][3]["status"] == ACCEPTED


def test_outer_aggregates_are_the_query_time_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100),   # empties the asks
        recon("q1", "AAA", 3, 1),
    ])
    result = out["results"][2]
    assert result["asks"] == []
    # But the reconstruction looks back at sequence 1, when the seller rested.
    assert ask_map(book_reconstruction(result))[100]["orders"][0]["order_id"] == "s1"


# ---------------------------------------------------------------------------
# Determinism and snapshot resumption
# ---------------------------------------------------------------------------


def _rich_events():
    return [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 4, 100),
        plu("e3", "BBB", 1, 90, 110),
        add("e4", "BBB", 2, "x1", "BUY", "LIMIT", 2, 95),
        twap_start("e5", 3, "p1", "BUY", 4, 2, 100),
        add("e6", 4, "b1", "BUY", "LIMIT", 3, 100),
        twap_slice("e7", 5, "p1"),
        recon("q1", "AAA", 6, 2),
        recon("q2", "AAA", 7, 6),
        recon("q3", "BBB", 3, 0),
        recon("q4", "AAA", 8, 99),                          # business rejection
    ]


def test_output_is_byte_for_byte_deterministic():
    events = _rich_events()
    first = canonical_json(replay_events(events, config=LIMITS))
    second = canonical_json(replay_events(copy.deepcopy(events), config=LIMITS))
    assert first == second

    def no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float leaked into deterministic output")
        if isinstance(value, dict):
            for item in value.values():
                no_floats(item)
        elif isinstance(value, list):
            for item in value:
                no_floats(item)

    no_floats(json.loads(first))


def test_resumed_replay_with_reconstruction_is_byte_identical():
    events = _rich_events()
    split = 5
    part1, part2 = events[:split], events[split:]
    one_shot = replay_events(events, config=LIMITS)
    snapshot = replay_events(part1, config=LIMITS)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot, config=LIMITS)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][split:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_reconstruction_after_restore_uses_only_the_prefix():
    # Restore after sequence 4, then ask for targets on either side of the
    # snapshot boundary; the pre-boundary history is still available.
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
        add("e4", "AAA", 4, "s3", "SELL", "LIMIT", 2, 102),
    ]
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events([
        recon("q1", "AAA", 5, 1),
        add("e5", "AAA", 6, "b2", "BUY", "LIMIT", 2, 100),
        recon("q2", "AAA", 7, 6),
        recon("q3", "AAA", 8, 2),
    ], snapshot=snapshot)
    at_one = book_reconstruction(out["results"][0])
    assert ask_map(at_one)[100]["orders"][0]["remaining_quantity"] == 3
    at_six = book_reconstruction(out["results"][2])
    # b1 took 1 from s1 in part 1 and b2 takes the remaining 2: s1 is gone.
    assert 100 not in ask_map(at_six)
    assert [level["price"] for level in at_six["ask_queues"]] == [101, 102]
    at_two = book_reconstruction(out["results"][3])
    assert [level["price"] for level in at_two["ask_queues"]] == [100, 101]


def test_reconstruction_roundtrips_through_export_and_restore():
    out = replay_events(_rich_events(), config=LIMITS)
    restored = restore_replayer(copy.deepcopy(out["snapshot"]), config=LIMITS)
    assert canonical_json(export_snapshot(restored, )) == canonical_json(out["snapshot"])
    # Querying the restored session repeats the original answer byte for byte.
    repeated = replay_events([recon("qz", "AAA", 9, 6)],
                             snapshot=export_snapshot(restored), config=LIMITS)
    original = next(
        r for r in out["results"]
        if r.get("event_id") == "q1"
    )
    repeated_body = {k: v for k, v in repeated["results"][0].items()
                     if k not in ("event_id", "symbol", "sequence")}
    original_body = {k: v for k, v in original.items()
                     if k not in ("event_id", "symbol", "sequence")}
    assert canonical_json(repeated_body) == canonical_json(original_body)


# ---------------------------------------------------------------------------
# Baseline entry points reject the type
# ---------------------------------------------------------------------------


def test_single_security_json_lines_entry_rejects_the_type():
    line = json.dumps({"event_id": "e1", "type": BOOK_RECONSTRUCTION_REPORT,
                       "target_sequence": 0})
    stdin = io.TextIOWrapper(io.BytesIO((line + "\n").encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    exit_code = line_replay.replay(stdin, stdout, io.StringIO())
    stdout.flush()
    parsed = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    assert exit_code == 0
    assert parsed["result"] == REJECTED
    assert parsed["reason"] == "INVALID_SCHEMA"


def test_baseline_engine_rejects_the_type():
    from order_book_engine.engine import Engine

    _eid, result, reason, _trades, _stp = Engine().handle_object({
        "event_id": "e1",
        "type": BOOK_RECONSTRUCTION_REPORT,
        "target_sequence": 0,
    })
    assert result == REJECTED
    assert reason == "INVALID_SCHEMA"


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


def test_cli_reconstruction_flow():
    exit_code, doc, stderr = _run_cli({
        "events": [
            add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
            recon("q1", "AAA", 2, 1),
            recon("q2", "AAA", 3, 9),
        ],
        "config": LIMITS,
    })
    assert (exit_code, stderr) == (0, "")
    accepted, rejected = doc["results"][1], doc["results"][2]
    assert accepted["result"] == "REPORTED"
    assert accepted["book_reconstruction"]["ask_queues"][0]["orders"][0][
        "order_id"
    ] == "s1"
    assert rejected["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
