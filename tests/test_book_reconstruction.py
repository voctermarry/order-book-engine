"""Tests for the read-only historical BOOK_RECONSTRUCTION_REPORT event.

The query rebuilds one security's price-time book queues as of a previously
committed sequence: target ``0`` is the empty book with the session-initial
price-limit interval, a positive target names a committed sequence of that
security, and a later target rejects with TARGET_SEQUENCE_NOT_FOUND. The
rebuild runs on a throwaway session, so matching, iceberg replenishment,
replacement queue loss, plan child orders and price-limit updates reproduce
exactly while the live session never matches or releases anything.
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
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PRICE_LIMIT_UPDATE,
    REJECTED,
    SEQUENCE_GAP,
    TARGET_SEQUENCE_NOT_FOUND,
    TWAP_SLICE,
    TWAP_START,
    EventReplayer,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.engine import INVALID_SCHEMA, Engine


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


def recon(event_id, symbol, sequence, target, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": target}
    event.update(extra)
    return event


def limit_update(event_id, symbol, sequence, lower, upper):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": PRICE_LIMIT_UPDATE, "lower_price": lower, "upper_price": upper}


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
               price, benchmark_price=100, order_type="LIMIT"):
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
    return event


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


def reconstruction_of(result):
    return result["book_reconstruction"]


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0, "x": 1},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": True},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": False},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": -1},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 1.0},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": "0"},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": None},
        {"event_id": "q1", "target_sequence": 0},
        {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0.0},
    ],
)
def test_inline_schema_errors_are_invalid_event_and_consume_nothing(payload):
    event = {"symbol": "AAA", "sequence": 1, **payload}
    out = replay_events([event], snapshot_after=None)["results"][0]
    assert out["status"] == REJECTED
    assert out["rejection_code"] == INVALID_EVENT
    assert "book_reconstruction" not in out
    # No symbol state was created and nothing was consumed.
    assert out["bids"] == [] and out["asks"] == []
    follow_up = replay_events(
        [event, recon("q1", "AAA", 1, 0)], snapshot_after=None
    )["results"]
    assert follow_up[1]["status"] == ACCEPTED


def test_nested_form_and_inner_event_id_mismatch():
    good = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q1", "type": BOOK_RECONSTRUCTION_REPORT,
                  "target_sequence": 0},
    }
    assert replay_events([good], snapshot_after=None)["results"][0]["status"] == ACCEPTED
    mismatch = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q2", "type": BOOK_RECONSTRUCTION_REPORT,
                  "target_sequence": 0},
    }
    out = replay_events([mismatch], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


def test_inline_form_rejects_envelope_only_extra_fields():
    event = recon("q1", "AAA", 1, 0, timestamp="t1")
    assert replay_events([event], snapshot_after=None)["results"][0]["status"] == ACCEPTED
    bad = recon("q1", "AAA", 1, 0, bogus=1)
    out = replay_events([bad], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


# ---------------------------------------------------------------------------
# Target zero and unknown securities
# ---------------------------------------------------------------------------


def test_target_zero_on_fresh_symbol_is_empty_book_with_initial_limits():
    out = replay_events(
        [recon("q0", "AAA", 1, 0)],
        config={"price_limits": {"AAA": {"lower": 90, "upper": 110}}},
        snapshot_after=None,
    )["results"][0]
    assert out["status"] == ACCEPTED
    assert out["result"] == "REPORTED"
    assert out["trades"] == []
    assert out["book_changes"] == {"bids": [], "asks": []}
    assert out["bids"] == [] and out["asks"] == []
    rec = reconstruction_of(out)
    assert rec == {
        "symbol": "AAA",
        "target_sequence": 0,
        "active_price_limits": {"lower_price": 90, "upper_price": 110},
        "bid_queues": [],
        "ask_queues": [],
    }


def test_target_zero_without_configured_limits_reports_null_interval():
    out = replay_events([recon("q0", "AAA", 1, 0)], snapshot_after=None)["results"][0]
    assert reconstruction_of(out)["active_price_limits"] is None


def test_unknown_symbol_positive_target_rejects_and_consumes_sequence():
    out = replay_events([recon("q1", "NEW", 1, 1)], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, TARGET_SEQUENCE_NOT_FOUND)
    assert out["bids"] == [] and out["asks"] == []
    assert "book_reconstruction" not in out
    # Like every other committed business rejection, the rejection registered
    # the (still empty) symbol and advanced its sequence: the next event for
    # this symbol must use sequence 2, where a target-0 query succeeds against
    # the empty book.
    replayer = EventReplayer()
    first = replayer.submit([recon("q1", "NEW", 1, 1)])[0]
    assert first["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    retry_at_one = replayer.submit([recon("q2", "NEW", 1, 0)])[0]
    assert retry_at_one["rejection_code"] == OUT_OF_ORDER
    at_two = replayer.submit([recon("q2", "NEW", 2, 0)])[0]
    assert at_two["status"] == ACCEPTED
    assert reconstruction_of(at_two)["ask_queues"] == []
    # The registered book is and stays empty.
    assert replayer.book("NEW") == ([], [])


# ---------------------------------------------------------------------------
# Successful reconstruction content
# ---------------------------------------------------------------------------


def _two_sided_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
        add("a3", "AAA", 3, "b1", "BUY", "LIMIT", 4, 98),
        add("a4", "AAA", 4, "b2", "BUY", "LIMIT", 2, 99),
        add("a5", "AAA", 5, "s3", "SELL", "LIMIT", 7, 100),
    ]


def test_queues_are_sorted_and_orders_keep_time_priority():
    stream = _two_sided_stream()
    out = replay_events(stream + [recon("q", "AAA", 6, 5)], snapshot_after=None)
    rec = reconstruction_of(out["results"][-1])
    assert [level["price"] for level in rec["bid_queues"]] == [99, 98]
    assert [level["price"] for level in rec["ask_queues"]] == [100, 101]
    best_ask = rec["ask_queues"][0]
    assert best_ask["visible_quantity"] == 10
    assert [order["order_id"] for order in best_ask["orders"]] == ["s2", "s3"]
    assert best_ask["orders"][0] == {
        "order_id": "s2", "order_type": "LIMIT",
        "remaining_quantity": 3, "visible_quantity": 3,
    }
    assert [o["order_id"] for o in rec["bid_queues"][1]["orders"]] == ["b1"]


def test_reconstruction_reflects_only_the_target_point():
    stream = _two_sided_stream()
    # At sequence 2 only the two asks exist; bids were added later.
    out = replay_events(stream + [recon("q", "AAA", 6, 2)], snapshot_after=None)
    rec = reconstruction_of(out["results"][-1])
    assert rec["bid_queues"] == []
    assert [level["price"] for level in rec["ask_queues"]] == [100, 101]
    # The outer book fields still describe the current (query-time) book.
    assert out["results"][-1]["bids"] == [
        {"price": 99, "quantity": 2}, {"price": 98, "quantity": 4}
    ]


def test_filled_cancelled_and_replaced_orders_do_not_appear():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("b1", "AAA", 2, "b1", "BUY", "MARKET", 3),       # trades s1 away
        add("s2", "AAA", 3, "s2", "SELL", "LIMIT", 4, 101),
        cancel("c1", "AAA", 4, "s2"),                        # cancels s2
        add("s3", "AAA", 5, "s3", "SELL", "LIMIT", 2, 102),
        replace("r1", "AAA", 6, "s3", 2, 103),               # replaced at new price
        recon("q", "AAA", 7, 3),                             # after fill, before cancel/replace
        recon("q2", "AAA", 8, 6),                            # after everything
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    at_3 = reconstruction_of(results[6])
    ids_at_3 = [o["order_id"] for q in at_3["ask_queues"] for o in q["orders"]]
    assert ids_at_3 == ["s2"]
    at_6 = reconstruction_of(results[7])
    assert [q["price"] for q in at_6["ask_queues"]] == [103]
    ids_at_6 = [o["order_id"] for q in at_6["ask_queues"] for o in q["orders"]]
    assert ids_at_6 == ["s3"]
    order = at_6["ask_queues"][0]["orders"][0]
    assert order["remaining_quantity"] == 2 and order["visible_quantity"] == 2


def test_iceberg_reserve_visible_slice_and_tail_replenishment():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("p1", "AAA", 2, "pl", "SELL", "LIMIT", 3, 100),
        # Buy 5: exhausts the iceberg slice of 4 (it replenishes at the tail
        # behind pl), then takes 1 from pl.
        add("t1", "AAA", 3, "tk", "BUY", "MARKET", 5),
        recon("q", "AAA", 4, 3),
        # Buy another 1: pl leads with 2 left, so pl goes to 1.
        add("t2", "AAA", 5, "tk2", "BUY", "MARKET", 1),
        recon("q2", "AAA", 6, 5),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    at_3 = reconstruction_of(results[3])
    level = at_3["ask_queues"][0]
    assert level["price"] == 100 and level["visible_quantity"] == 6
    assert [o["order_id"] for o in level["orders"]] == ["pl", "ic"]
    plain, ice = level["orders"]
    assert plain == {"order_id": "pl", "order_type": "LIMIT",
                     "remaining_quantity": 2, "visible_quantity": 2}
    # The iceberg still holds 6 units total but only the replenished slice of
    # 4 is public.
    assert ice == {"order_id": "ic", "order_type": "ICEBERG",
                   "remaining_quantity": 6, "visible_quantity": 4}
    at_5 = reconstruction_of(results[5])
    assert [o["order_id"] for o in at_5["ask_queues"][0]["orders"]] == ["pl", "ic"]
    assert at_5["ask_queues"][0]["orders"][0]["visible_quantity"] == 1


def test_partially_consumed_iceberg_slice_is_reported_as_is():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("t1", "AAA", 2, "tk", "BUY", "MARKET", 1),
        recon("q", "AAA", 3, 2),
    ]
    rec = reconstruction_of(replay_events(stream, snapshot_after=None)["results"][-1])
    order = rec["ask_queues"][0]["orders"][0]
    assert order["order_type"] == "ICEBERG"
    assert order["remaining_quantity"] == 9
    # The slice is only partially consumed: no replenishment yet, so the
    # public slice shows 3 and the reserve stays out of the aggregate.
    assert order["visible_quantity"] == 3
    assert rec["ask_queues"][0]["visible_quantity"] == 3


# ---------------------------------------------------------------------------
# Plan child orders and price-limit update timing
# ---------------------------------------------------------------------------


def test_plan_slice_fills_and_cancelled_children_are_rebuilt():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("p1", "AAA", 2, "P", "BUY", 4, 2, price=100),
        twap_slice("p2", "AAA", 3, "P"),   # IOC buy 2, fills against s1
        recon("q", "AAA", 4, 3),
        twap_slice("p3", "AAA", 5, "P"),   # IOC buy 2, no liquidity -> cancelled
        recon("q2", "AAA", 6, 5),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    at_3 = reconstruction_of(results[3])
    # s1 has 3 left; the released IOC child (P#1) is finished and not queued.
    assert at_3["ask_queues"][0]["orders"] == [{
        "order_id": "s1", "order_type": "LIMIT",
        "remaining_quantity": 3, "visible_quantity": 3,
    }]
    at_5 = reconstruction_of(results[5])
    # s1 has 1 left after the two 2-unit slices; both released IOC children
    # (P#1, P#2) are finished and absent from the queues.
    assert at_5["ask_queues"][0]["orders"] == [{
        "order_id": "s1", "order_type": "LIMIT",
        "remaining_quantity": 1, "visible_quantity": 1,
    }]
    assert [o["order_id"] for q in at_5["bid_queues"] for o in q["orders"]] == []


def test_active_price_limits_follow_the_target_point():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        limit_update("u1", "AAA", 2, 95, 105),
        limit_update("u2", "AAA", 3, 97, 103),
        recon("q0", "AAA", 4, 0),
        recon("q1", "AAA", 5, 1),
        recon("q2", "AAA", 6, 2),
        recon("q3", "AAA", 7, 3),
    ]
    results = replay_events(
        stream,
        config={"price_limits": {"AAA": {"lower": 90, "upper": 110}}},
        snapshot_after=None,
    )["results"]
    assert reconstruction_of(results[3])["active_price_limits"] == {
        "lower_price": 90, "upper_price": 110}
    assert reconstruction_of(results[4])["active_price_limits"] == {
        "lower_price": 90, "upper_price": 110}
    assert reconstruction_of(results[5])["active_price_limits"] == {
        "lower_price": 95, "upper_price": 105}
    assert reconstruction_of(results[6])["active_price_limits"] == {
        "lower_price": 97, "upper_price": 103}


def test_targets_on_read_only_and_rejected_sequences_are_state_neutral():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        # An impact report occupies sequence 2 and changes nothing.
        {"event_id": "m1", "symbol": "AAA", "sequence": 2,
         "type": "IMPACT_REPORT", "side": "BUY", "quantity": 1,
         "benchmark_price": 100},
        # A business rejection (unknown cancel) occupies sequence 3.
        cancel("c1", "AAA", 3, "missing"),
        recon("q2", "AAA", 4, 2),
        recon("q3", "AAA", 5, 3),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    for index in (3, 4):
        rec = reconstruction_of(results[index])
        assert [o["order_id"] for q in rec["ask_queues"] for o in q["orders"]] == ["s1"]


def test_pov_volume_releases_are_rebuilt_at_release_time():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        {
            "event_id": "v0", "symbol": "AAA", "sequence": 2,
            "type": "POV_START", "plan_id": "V", "side": "BUY",
            "total_quantity": 5, "participation_bps": 5000,
            "order_type": "LIMIT", "benchmark_price": 100, "price": 100,
        },
        # Market volume 4 at 50% participation -> release 2 (fills 2 of s1).
        {"event_id": "v1", "symbol": "AAA", "sequence": 3,
         "type": "POV_VOLUME", "plan_id": "V", "market_volume_increment": 4},
        recon("q3", "AAA", 4, 3),
        # A zero-release volume event occupies sequence 5 and submits nothing
        # (cumulative volume 5 at 50% still targets 2 released units).
        {"event_id": "v2", "symbol": "AAA", "sequence": 5,
         "type": "POV_VOLUME", "plan_id": "V", "market_volume_increment": 1},
        recon("q5", "AAA", 6, 5),
        # Volume 6 more -> cumulative 11, target 5: release 3 more.
        {"event_id": "v3", "symbol": "AAA", "sequence": 7,
         "type": "POV_VOLUME", "plan_id": "V", "market_volume_increment": 6},
        recon("q7", "AAA", 8, 7),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    at_3 = reconstruction_of(results[3])
    assert at_3["ask_queues"][0]["orders"][0]["remaining_quantity"] == 3
    at_5 = reconstruction_of(results[5])
    assert at_5["ask_queues"][0]["orders"][0]["remaining_quantity"] == 3
    at_7 = reconstruction_of(results[7])
    assert at_7["ask_queues"] == []
    queued_ids = {o["order_id"] for q in at_7["bid_queues"] for o in q["orders"]}
    # The IOC POV children never rest.
    assert queued_ids == set()


def test_reconstruction_query_can_target_an_earlier_query_sequence():
    # Targeting the sequence an earlier BOOK_RECONSTRUCTION_REPORT occupied
    # must rebuild without re-entering the query (no recursion) and show the
    # same state-neutral book.
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        recon("q1", "AAA", 2, 0),
        recon("q2", "AAA", 3, 2),
        recon("q3", "AAA", 4, 3),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    # At target 2 the mutating sequence 1 has rested s1; the sequence-2 query
    # moved nothing.
    assert [
        o["order_id"] for q in reconstruction_of(results[2])["ask_queues"]
        for o in q["orders"]
    ] == ["s1"]
    # Targeting the sequence the immediately preceding query occupied
    # reproduces the same book without re-entering any reconstruction query.
    target_of_query = reconstruction_of(results[3])
    assert [o["order_id"] for q in target_of_query["ask_queues"] for o in q["orders"]] == ["s1"]


def test_reconstruction_reproduces_price_limit_business_rejections():
    # A limit breach at sequence 2 occupies the sequence and rests nothing;
    # rebuilding at that point shows only the sequence-1 book.
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("bad", "AAA", 2, "far", "SELL", "LIMIT", 1, 200),
        recon("q2", "AAA", 3, 2),
    ]
    results = replay_events(
        stream,
        config={"price_limits": {"AAA": {"lower": 90, "upper": 110}}},
        snapshot_after=None,
    )["results"]
    assert results[1]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    rec = reconstruction_of(results[2])
    assert [o["order_id"] for q in rec["ask_queues"] for o in q["orders"]] == ["s1"]


# ---------------------------------------------------------------------------
# Read-only guarantee, ordering and idempotency
# ---------------------------------------------------------------------------


def test_query_does_not_move_any_trading_state():
    stream = _two_sided_stream() + [recon("q", "AAA", 6, 1), recon("q2", "AAA", 7, 2)]
    out = replay_events(stream)["snapshot"]
    # Run the mutating prefix alone and with the queries appended: identical
    # engine content means the queries changed no orders, queues or counters.
    prefix_only = replay_events(_two_sided_stream())["snapshot"]
    prefix_engine = prefix_only["content"]["symbols"][0]["state"]["engine"]
    queried_engine = out["content"]["symbols"][0]["state"]["engine"]
    assert canonical_json(prefix_engine) == canonical_json(queried_engine)


def test_target_not_found_consumes_id_and_sequence_but_no_state():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        recon("bad", "AAA", 2, 5),
        recon("ok", "AAA", 3, 1),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    assert results[1]["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
    assert results[2]["status"] == ACCEPTED
    # The rejected id is now spent: re-delivering it verbatim is a duplicate,
    # and it never entered the engine journal.
    replayer = restore_replayer(replay_events(stream)["snapshot"])
    duplicate = replayer.submit([recon("bad", "AAA", 4, 5)])[0]
    assert duplicate["status"] == DUPLICATE
    assert "book_reconstruction" not in duplicate


def test_duplicate_delivery_is_reported_without_rebuilding():
    stream = _two_sided_stream() + [recon("q", "AAA", 6, 2)]
    original = replay_events(copy.deepcopy(stream))["results"][-1]
    # A retry carries the stale sequence, exactly like other reports.
    retry_event = recon("q", "AAA", 99, 2)
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    retry = replayer.submit([retry_event])[0]
    assert retry["status"] == DUPLICATE
    assert "book_reconstruction" not in retry
    assert retry["trades"] == [] and retry["book_changes"] == {"bids": [], "asks": []}
    assert canonical_json(retry["bids"]) == canonical_json(original["bids"])


def test_event_id_conflict_and_sequence_errors_keep_precedence():
    stream = _two_sided_stream() + [recon("q", "AAA", 6, 2)]
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    # Same id, different content -> conflict without consuming sequence 7.
    conflict = replayer.submit([recon("q", "AAA", 7, 3)])[0]
    assert conflict["status"] == REJECTED
    assert conflict["rejection_code"] == EVENT_ID_CONFLICT
    gap = replayer.submit([recon("qg", "AAA", 9, 2)])[0]
    assert gap["rejection_code"] == SEQUENCE_GAP
    early = replayer.submit([recon("qe", "AAA", 3, 2)])[0]
    assert early["rejection_code"] == OUT_OF_ORDER


def test_query_ids_stay_out_of_the_engine_journal():
    stream = _two_sided_stream() + [
        recon("q1", "AAA", 6, 1),
        recon("q2", "AAA", 7, 2),
    ]
    snapshot = replay_events(stream)["snapshot"]
    engine_ids = set(snapshot["content"]["symbols"][0]["state"]["engine"]["event_ids"])
    assert engine_ids == {"a1", "a2", "a3", "a4", "a5"}


# ---------------------------------------------------------------------------
# Snapshot restoration
# ---------------------------------------------------------------------------


def _rich_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "BBB", 1, "w1", "SELL", "LIMIT", 2, 70),
        iceberg("a3", "AAA", 2, "ic", "SELL", 9, 100, 3),
        add("a4", "AAA", 3, "b1", "BUY", "MARKET", 4),
        limit_update("u1", "AAA", 4, 95, 105),
        add("a5", "BBB", 2, "w2", "BUY", "LIMIT", 1, 69),
        recon("q1", "AAA", 5, 2),
        recon("q2", "AAA", 6, 0),
        recon("q3", "AAA", 7, 4),
        recon("q4", "BBB", 3, 1),
    ]


def test_resumed_replay_matches_one_shot_byte_for_byte():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    snapshot = replay_events(copy.deepcopy(stream[:6]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[6:]), snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][6:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(
        one_shot["snapshot"]
    )


def test_snapshot_roundtrip_and_post_restore_duplicate():
    stream = _rich_stream()
    out = replay_events(copy.deepcopy(stream))
    restored = restore_replayer(copy.deepcopy(out["snapshot"]))
    assert canonical_json(export_snapshot(restored)) == canonical_json(out["snapshot"])
    duplicate = restored.submit([copy.deepcopy(stream[6])])[0]
    assert duplicate["status"] == DUPLICATE
    assert "book_reconstruction" not in duplicate
    fresh = restored.submit([recon("q9", "AAA", 8, 5)])[0]
    assert (fresh["status"], fresh["result"]) == (ACCEPTED, "REPORTED")


def test_snapshot_after_named_query_event():
    stream = _rich_stream()
    out = replay_events(stream, snapshot_after={"symbol": "AAA", "sequence": 5})
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
    # After the marker (q1, AAA sequence 5) the next expected AAA sequence
    # is 6.
    resumed = replay_events(
        [recon("qx", "AAA", 6, 0)], snapshot=out["snapshot"], snapshot_after=None
    )
    assert resumed["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Baseline Engine / JSON Lines rejection and CLI
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_type_as_invalid_schema_without_id_occupancy():
    engine = Engine()
    line = json.dumps(
        {"event_id": "z1", "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0}
    )
    event_id, result, reason, trades = engine.handle_line(line)
    assert event_id == "z1"
    assert result == REJECTED
    assert reason == INVALID_SCHEMA
    assert trades == []
    # A structural rejection spends no event id: the same id is usable.
    follow_up = json.dumps(
        {"event_id": "z1", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "MARKET", "quantity": 1}
    )
    _eid, result2, reason2, _trades = engine.handle_line(follow_up)
    assert reason2 is None


def test_events_cli_supports_reconstruction_end_to_end():
    request_obj = {"events": _two_sided_stream() + [recon("q1", "AAA", 6, 2)]}
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    assert code == 0
    assert stderr.getvalue() == ""
    parsed = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    reported = parsed["results"][-1]
    assert reported["status"] == ACCEPTED
    assert reported["result"] == "REPORTED"
    rec = reported["book_reconstruction"]
    assert rec["target_sequence"] == 2
    assert [level["price"] for level in rec["ask_queues"]] == [100, 101]
    assert parsed["snapshot"]["format_version"] == FORMAT_VERSION


def test_events_cli_returns_target_not_found():
    request_obj = {"events": [recon("q1", "AAA", 1, 3)]}
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    assert code == 0
    parsed = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    result = parsed["results"][0]
    assert result["status"] == REJECTED
    assert result["rejection_code"] == TARGET_SEQUENCE_NOT_FOUND
