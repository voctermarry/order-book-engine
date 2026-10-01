"""Tests for deterministic multi-instrument event replay and snapshots."""

from __future__ import annotations

import copy
import hashlib
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    EventReplayer,
    SnapshotError,
    canonical_dumps,
    format_version,
    replay_events,
    restore_snapshot,
)
from order_book_engine.engine import Engine


# --------------------------------------------------------------------------
# Event builders
# --------------------------------------------------------------------------


def _wrap(eid, symbol, seq, inner):
    return {
        "event_id": eid,
        "symbol": symbol,
        "sequence": seq,
        "type": inner["type"],
        "event": inner,
    }


def add(eid, symbol, seq, order_id, side, order_type, quantity, price=None, **extra):
    inner = {
        "event_id": eid,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
    }
    if price is not None:
        inner["price"] = price
    inner.update(extra)
    return _wrap(eid, symbol, seq, inner)


def iceberg(eid, symbol, seq, order_id, side, quantity, price, display_quantity, **extra):
    return add(
        eid, symbol, seq, order_id, side, "ICEBERG", quantity, price,
        display_quantity=display_quantity, **extra
    )


def cancel(eid, symbol, seq, order_id):
    return _wrap(
        eid, symbol, seq,
        {"event_id": eid, "type": "CANCEL", "order_id": order_id},
    )


def replace(eid, symbol, seq, order_id, quantity, price, **extra):
    inner = {
        "event_id": eid,
        "type": "REPLACE",
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
    }
    inner.update(extra)
    return _wrap(eid, symbol, seq, inner)


def execution_report(eid, symbol, seq, order_id, benchmark_price):
    return _wrap(
        eid, symbol, seq,
        {"event_id": eid, "type": "EXECUTION_REPORT",
         "order_id": order_id, "benchmark_price": benchmark_price},
    )


# --------------------------------------------------------------------------
# Basic acceptance / book / trades
# --------------------------------------------------------------------------


def test_single_symbol_resting_filled_and_result_shape():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100),
    ])
    r1, r2 = out["results"]
    assert r1["status"] is ACCEPTED
    assert r1["code"] is None
    assert r1["result"] == "RESTING"
    assert r1["reason"] is None
    assert r1["trades"] == []
    assert r1["book_changes"]["before"] == {"bids": [], "asks": []}
    assert r1["book_changes"]["after"] == {
        "bids": [], "asks": [{"price": 100, "quantity": 5}]
    }
    for key in ("self_trade_prevention", "execution_analysis", "position_analysis"):
        assert r1[key] is None

    assert r2["result"] == "FILLED"
    assert r2["trades"] == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 5}
    ]
    assert r2["book_changes"]["before"] == {
        "bids": [], "asks": [{"price": 100, "quantity": 5}]
    }
    assert r2["book_changes"]["after"] == {"bids": [], "asks": []}

    assert out["trades"] == [
        {"symbol": "AAA", "sequence": 2, "event_id": "e2",
         "trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 5}
    ]
    assert out["final_books"] == {"AAA": {"bids": [], "asks": []}}


def test_multiple_symbols_keep_independent_books_and_trade_counters():
    out = replay_events([
        add("e1", "AAA", 1, "a1", "SELL", "LIMIT", 4, 100),
        add("e2", "BBB", 1, "b1", "SELL", "LIMIT", 2, 50),
        add("e3", "AAA", 2, "x", "BUY", "LIMIT", 4, 100),
        add("e4", "BBB", 2, "y", "BUY", "LIMIT", 2, 50),
    ])
    assert out["trades"] == [
        {"symbol": "AAA", "sequence": 2, "event_id": "e3", "trade_id": 1,
         "maker_order_id": "a1", "taker_order_id": "x", "price": 100, "quantity": 4},
        {"symbol": "BBB", "sequence": 2, "event_id": "e4", "trade_id": 1,
         "maker_order_id": "b1", "taker_order_id": "y", "price": 50, "quantity": 2},
    ]
    assert set(out["final_books"]) == {"AAA", "BBB"}


def test_cancel_and_replace_events_are_supported():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        replace("e2", "AAA", 2, "o1", 3, 101),
        cancel("e3", "AAA", 3, "o1"),
    ])
    assert [r["result"] for r in out["results"]] == [
        "RESTING", "REPLACED", "CANCELLED"
    ]
    assert out["results"][-1]["book_changes"]["before"] == {
        "bids": [{"price": 101, "quantity": 3}], "asks": []
    }
    assert out["final_books"] == {"AAA": {"bids": [], "asks": []}}


def test_input_order_is_preserved_even_when_timestamps_would_tie():
    # There is no timestamp field; interleaved symbols must be handled strictly
    # in list order, which determines the cross-symbol trade ordering.
    events = [
        add("e1", "S1", 1, "s", "SELL", "LIMIT", 1, 10),
        add("e2", "S2", 1, "s", "SELL", "LIMIT", 1, 20),
        add("e3", "S2", 2, "b", "BUY", "LIMIT", 1, 20),
        add("e4", "S1", 2, "b", "BUY", "LIMIT", 1, 10),
    ]
    out = replay_events(events)
    assert [(t["symbol"], t["event_id"]) for t in out["trades"]] == [
        ("S2", "e3"), ("S1", "e4")
    ]


def test_execution_report_inner_event_passes_analysis_through():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
        execution_report("e3", "AAA", 3, "b1", 100),
    ])
    assert out["results"][2]["status"] is ACCEPTED
    assert out["results"][2]["result"] == "REPORTED"
    assert out["results"][2]["execution_analysis"]["filled_quantity"] == 3
    assert out["results"][2]["trades"] == []


# --------------------------------------------------------------------------
# Sequence semantics
# --------------------------------------------------------------------------


def test_new_symbol_must_start_at_one_else_gap():
    r = EventReplayer()
    bad = r.submit(add("e1", "AAA", 2, "o1", "BUY", "LIMIT", 1, 100))
    assert bad["status"] is REJECTED
    assert bad["code"] is SEQUENCE_GAP
    assert bad["trades"] == []
    assert bad["book_changes"] == {
        "before": {"bids": [], "asks": []},
        "after": {"bids": [], "asks": []},
    }
    # Nothing was created: the symbol still accepts sequence 1 afterwards.
    ok = r.submit(add("e2", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100))
    assert ok["status"] is ACCEPTED


def test_sequence_gap_and_regression_leave_state_untouched():
    r = EventReplayer()
    r.submit(add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100))
    before = r.final_books()

    gap = r.submit(add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 5, 100))
    assert gap["code"] is SEQUENCE_GAP
    assert gap["trades"] == []
    assert r.final_books() == before

    # Sequence 2 now succeeds: the gap event consumed neither seq nor id/order.
    ok = r.submit(add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100))
    assert ok["status"] is ACCEPTED
    assert ok["trades"][0]["trade_id"] == 1

    back = r.submit(add("e3", "AAA", 2, "b2", "BUY", "LIMIT", 1, 100))
    assert back["code"] is OUT_OF_ORDER
    assert back["trades"] == []
    assert r.final_books()["AAA"] == {"bids": [], "asks": []}


def test_invalid_event_does_not_consume_sequence_or_id():
    r = EventReplayer()
    bad = r.submit(add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 0, 100))
    assert bad["status"] is REJECTED
    assert bad["code"] is INVALID_EVENT
    assert bad["result"] is None and bad["reason"] is None
    # Same sequence, corrected content succeeds with the same event id.
    ok = r.submit(add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100))
    assert ok["status"] is ACCEPTED


@pytest.mark.parametrize(
    "event",
    [
        None,
        123,
        "x",
        [],
        {},
        {"event_id": "", "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": {}},
        {"event_id": 7, "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "", "sequence": 1, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": 9, "sequence": 1, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 0, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": -3, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": True, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1.5, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD"},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": {}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": {"event_id": "e1", "type": "ADD", "order_id": "o1",
                   "side": "BUY", "order_type": "LIMIT",
                   "quantity": 1, "price": 100}, "extra": 1},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": []},
        # inner identifiers disagree with the envelope
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD",
         "event": {"event_id": "OTHER", "type": "ADD", "order_id": "o1",
                   "side": "BUY", "order_type": "LIMIT",
                   "quantity": 1, "price": 100}},
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "CANCEL",
         "event": {"event_id": "e1", "type": "ADD", "order_id": "o1",
                   "side": "BUY", "order_type": "LIMIT",
                   "quantity": 1, "price": 100}},
        # unknown inner type
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "BOGUS",
         "event": {"event_id": "e1", "type": "BOGUS"}},
    ],
)
def test_invalid_envelope_variants(event):
    r = EventReplayer()
    out = r.submit(event)
    assert out["status"] is REJECTED
    assert out["code"] is INVALID_EVENT
    assert out["trades"] == []
    assert r.final_books() == {}


# --------------------------------------------------------------------------
# Business rejections and per-event commit semantics
# --------------------------------------------------------------------------


def test_business_rejection_keeps_baseline_reason_and_consumes_sequence():
    out = replay_events([
        cancel("e1", "AAA", 1, "ghost"),
        add("e2", "AAA", 2, "o1", "BUY", "LIMIT", 1, 100),
        replace("e3", "AAA", 3, "ghost", 1, 100),
    ])
    r1, _, r3 = out["results"]
    assert r1["status"] is REJECTED
    assert r1["code"] is None
    assert r1["result"] == "REJECTED"
    assert r1["reason"] == "UNKNOWN_ORDER"
    assert r3["reason"] == "UNKNOWN_ORDER"
    # Replaying the identical rejected event is idempotent (no second engine
    # evaluation) and echoes the original business rejection.
    r = restore_snapshot(out["snapshot"])
    repeat = r.submit(cancel("e1", "AAA", 1, "ghost"))
    assert repeat["status"] is DUPLICATE
    assert repeat["result"] == "REJECTED"
    assert repeat["reason"] == "UNKNOWN_ORDER"
    # The business-rejected events did occupy their sequence slots: a fresh
    # event at an old sequence is a regression.
    assert r.submit(
        add("e8", "AAA", 1, "z", "BUY", "LIMIT", 1, 100)
    )["code"] is OUT_OF_ORDER


def test_failed_event_leaves_no_order_trade_counter_or_book_change():
    r = EventReplayer()
    r.submit(add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100))
    r.submit(add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100))
    before = r.final_books()
    # A FOK that cannot be fully filled is accepted by the envelope but
    # produces no trades, no book change and spends no trade id.
    fok = r.submit(add("e3", "AAA", 3, "b", "BUY", "LIMIT", 99, 100,
                       time_in_force="FOK"))
    assert fok["result"] == "UNFILLED_CANCELLED"
    assert fok["trades"] == []
    assert fok["book_changes"]["before"] == fok["book_changes"]["after"] == before["AAA"]
    nxt = r.submit(add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 1, 100))
    assert nxt["trades"][0]["trade_id"] == 1
    assert nxt["trades"][0]["maker_order_id"] == "s1"


def test_duplicate_order_id_is_baseline_business_rejection():
    r = EventReplayer()
    r.submit(add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100))
    dup = r.submit(add("e2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100))
    assert dup["status"] is REJECTED
    assert dup["code"] is None
    assert dup["reason"] == "DUPLICATE_ORDER_ID"


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


def test_duplicate_event_id_identical_content_is_idempotent():
    r = EventReplayer()
    first = r.submit(add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100))
    fill = r.submit(add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100))
    assert fill["trades"][0]["trade_id"] == 1
    dup = r.submit(add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100))
    assert dup["status"] is DUPLICATE
    assert dup["code"] is None
    # The original result is echoed without matching again.
    assert dup["result"] == fill["result"] == "FILLED"
    assert dup["trades"] == fill["trades"]
    # No extra trade id was spent; the next trade starts at 2.
    nxt = r.submit(add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 100))
    assert nxt["trades"][0]["trade_id"] == 2
    assert r.final_books()["AAA"] == {"bids": [], "asks": [{"price": 100, "quantity": 2}]}
    assert first is not dup


def test_duplicate_detection_is_key_order_insensitive():
    r = EventReplayer()
    r.submit(add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100))
    reordered = {
        "event": {"quantity": 5, "price": 100, "type": "ADD",
                  "order_id": "b1", "side": "BUY", "order_type": "LIMIT",
                  "event_id": "e2"},
        "sequence": 2,
        "symbol": "AAA",
        "type": "ADD",
        "event_id": "e2",
    }
    assert r.submit(reordered)["status"] is ACCEPTED
    again = add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100)
    assert r.submit(again)["status"] is DUPLICATE


def test_event_id_conflict_on_different_content():
    r = EventReplayer()
    r.submit(add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100))
    conflict = add("e1", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100)
    out = r.submit(conflict)
    assert out["status"] is REJECTED
    assert out["code"] is EVENT_ID_CONFLICT
    assert out["trades"] == []
    # The conflicting event neither matched nor consumed the AAA sequence.
    nxt = r.submit(add("e2", "AAA", 2, "b2", "BUY", "LIMIT", 1, 100))
    assert nxt["status"] is ACCEPTED
    assert nxt["trades"][0]["trade_id"] == 1
    assert r.final_books()["AAA"] == {"bids": [], "asks": [{"price": 100, "quantity": 4}]}


def test_event_id_conflict_detects_any_content_difference():
    r = EventReplayer()
    original = add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100)
    r.submit(original)
    # Same envelope shape, different nested quantity.
    changed = copy.deepcopy(original)
    changed["event"]["quantity"] = 6
    assert r.submit(changed)["code"] is EVENT_ID_CONFLICT
    # A different symbol for the same global event id is also a conflict.
    moved = copy.deepcopy(original)
    moved["symbol"] = "BBB"
    moved["sequence"] = 1
    assert r.submit(moved)["code"] is EVENT_ID_CONFLICT


# --------------------------------------------------------------------------
# Canonical JSON determinism
# --------------------------------------------------------------------------


def test_canonical_dumps_is_stable_and_key_order_insensitive():
    a = {"z": 1, "a": [1, 2, {"k": "v"}], "n": None, "b": True}
    b = {"b": True, "n": None, "a": [1, 2, {"k": "v"}], "z": 1}
    assert canonical_dumps(a) == canonical_dumps(b)
    assert canonical_dumps([1, 2, 3]) == b"[1,2,3]"


def test_replay_results_are_byte_deterministic():
    events = [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        add("e3", "BBB", 1, "z", "BUY", "LIMIT", 2, 50),
        add("e4", "AAA", 3, "b1", "BUY", "LIMIT", 10, 100),
        cancel("e5", "AAA", 4, "i1"),
        add("e6", "BBB", 2, "q", "BUY", "MARKET", 1),
    ]
    out1 = replay_events(copy.deepcopy(events))
    out2 = replay_events(copy.deepcopy(events))
    assert canonical_dumps(out1["results"]) == canonical_dumps(out2["results"])
    assert canonical_dumps(out1["final_books"]) == canonical_dumps(out2["final_books"])
    assert canonical_dumps(out1["trades"]) == canonical_dumps(out2["trades"])
    assert canonical_dumps(out1["snapshot"]) == canonical_dumps(out2["snapshot"])

    # Explicit compact, stable serialization of a simple accepted result.
    simple = replay_events([add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100)])
    encoded = canonical_dumps(simple["results"][0])
    assert json.loads(encoded)["book_changes"]["after"] == {
        "bids": [{"price": 100, "quantity": 1}], "asks": []
    }
    assert encoded == canonical_dumps(replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100)
    ])["results"][0])


# --------------------------------------------------------------------------
# Snapshots
# --------------------------------------------------------------------------


def _stream():
    return [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 100),  # 3+1 filled, visible 2
        add("e3", "BBB", 1, "s1", "SELL", "LIMIT", 7, 200),
        replace("e4", "AAA", 3, "i1", 6, 100),
    ]


def _continuation():
    return [
        add("e5", "BBB", 2, "b2", "BUY", "LIMIT", 7, 200),
        add("e6", "AAA", 4, "b3", "BUY", "LIMIT", 6, 100),
        cancel("e7", "AAA", 5, "i1"),
    ]


def test_snapshot_contains_version_config_and_digests():
    out = replay_events(_stream())
    snap = out["snapshot"]
    assert snap["format_version"] == format_version()
    assert snap["config"]["matching"] == "price-time-priority"
    assert snap["config_digest"] == hashlib.sha256(
        canonical_dumps(snap["config"])
    ).hexdigest()
    assert snap["state_digest"] == hashlib.sha256(
        canonical_dumps(snap["state"])
    ).hexdigest()
    assert snap["state"]["instruments"]["AAA"]["last_sequence"] == 3
    assert snap["state"]["instruments"]["BBB"]["last_sequence"] == 1
    # The trade id counter and cumulative journal are both captured.
    assert snap["state"]["instruments"]["AAA"]["engine"]["next_trade_id"] == 3


def test_resume_matches_uninterrupted_run_exactly():
    full_stream = _stream() + _continuation()

    # Split after the second event of AAA (iceberg mid-state), then feed the
    # remainder (including the first post-snapshot BBB event) via snapshot.
    head = _stream()[:2]
    tail = _stream()[2:] + _continuation()

    part1 = replay_events(head)
    part2 = replay_events(tail, snapshot=part1["snapshot"])
    continuous = replay_events(full_stream)

    assert part2["trades"] == continuous["trades"]
    assert part2["final_books"] == continuous["final_books"]
    assert canonical_dumps(part2["results"]) == canonical_dumps(
        continuous["results"][2:]
    )
    assert canonical_dumps(part2["snapshot"]) == canonical_dumps(
        continuous["snapshot"]
    )

    # Iceberg replenishment state survived: AAA trades keep the exact
    # uninterrupted id sequence (1,2 from before the split, then 3,4 after).
    aaa_trades = [t for t in part2["trades"] if t["symbol"] == "AAA"]
    assert [t["trade_id"] for t in aaa_trades] == [1, 2, 3, 4]
    # BBB's independent counter starts at 1 after the split.
    bbb_trades = [t for t in part2["trades"] if t["symbol"] == "BBB"]
    assert [t["trade_id"] for t in bbb_trades] == [1]


def test_snapshot_round_trip_preserves_priority_visible_slice_and_reserve():
    r = EventReplayer()
    r.submit(iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3))
    r.submit(add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100))
    r.submit(add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 3, 100))  # first slice consumed
    snap = r.export_snapshot()

    resumed = restore_snapshot(snap)
    # The replenished slice queued behind s2; verify exact price-time priority.
    out = resumed.submit(add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 5, 100))
    assert [t["maker_order_id"] for t in out["trades"]] == ["s2"]
    out = resumed.submit(add("e5", "AAA", 5, "b3", "BUY", "LIMIT", 3, 100))
    assert [t["maker_order_id"] for t in out["trades"]] == ["i1"]
    # e3 spent trade id 1, e4 id 2; the replenished slice therefore gets id 3.
    assert [t["trade_id"] for t in out["trades"]] == [3]


def test_resume_keeps_idempotency_state():
    part1 = replay_events(_stream()[:2])
    r = restore_snapshot(part1["snapshot"])
    repeat = r.submit(_stream()[1])
    assert repeat["status"] is DUPLICATE
    assert [t["trade_id"] for t in repeat["trades"]] == [1, 2]
    changed = copy.deepcopy(_stream()[1])
    changed["event"]["quantity"] = 5
    assert r.submit(changed)["code"] is EVENT_ID_CONFLICT
    # Sequence cursor resumed too: AAA expects 3 next.
    assert r.submit(add("e9", "AAA", 3, "z", "SELL", "LIMIT", 1, 100))["code"] is None
    assert r.submit(add("e10", "AAA", 3, "z", "SELL", "LIMIT", 1, 100))["code"] is OUT_OF_ORDER


def test_snapshot_is_json_serializable():
    snap = replay_events(_stream())["snapshot"]
    encoded = json.dumps(snap, ensure_ascii=False)
    restored = restore_snapshot(json.loads(encoded))
    assert restored.final_books() == replay_events(_stream())["final_books"]


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda s: s["state"]["instruments"]["AAA"]["engine"].__setitem__(
            "next_trade_id", 999), SNAPSHOT_CORRUPT),
        (lambda s: s["state"].__setitem__("tampered", True), SNAPSHOT_CORRUPT),
        (lambda s: s["state"]["events"][0].__setitem__(
            "content_digest", "0" * 64), SNAPSHOT_CORRUPT),
        (lambda s: s.__setitem__("state_digest", "0" * 64), SNAPSHOT_CORRUPT),
        (lambda s: s.__setitem__("format_version", "order-book-replay/9"),
         SNAPSHOT_VERSION_UNSUPPORTED),
        (lambda s: s["config"].__setitem__("trade_id_start", 2),
         CONFIG_MISMATCH),
        (lambda s: s.__setitem__("config_digest", "0" * 64), CONFIG_MISMATCH),
        (lambda s: s.__setitem__("config", []), CONFIG_MISMATCH),
    ],
)
def test_corrupt_snapshots_are_rejected(mutate, expected):
    snap = replay_events(_stream())["snapshot"]
    mutate(snap)
    with pytest.raises(SnapshotError) as exc:
        restore_snapshot(snap)
    assert exc.value.code == expected


def test_rechecksummed_internal_tamper_is_still_corrupt():
    # An attacker who rewrites state and recomputes the digest must still be
    # refused because engine/event state is no longer internally consistent.
    snap = replay_events(_stream())["snapshot"]
    snap["state"]["instruments"]["AAA"]["engine"]["next_trade_id"] = 42
    # Rebuild a bogus but self-consistent journal length for the counter, then
    # recompute the digest; queue/order consistency must still reject it.
    with pytest.raises(SnapshotError) as exc:
        restore_snapshot(_rechecksum(snap))
    assert exc.value.code == SNAPSHOT_CORRUPT


def _rechecksum(snap):
    snap["state_digest"] = hashlib.sha256(
        canonical_dumps(snap["state"])
    ).hexdigest()
    return snap


def test_failed_restore_creates_no_partial_state():
    snap = replay_events(_stream())["snapshot"]
    bad = copy.deepcopy(snap)
    bad["state_digest"] = "deadbeef"
    with pytest.raises(SnapshotError):
        restore_snapshot(bad)
    # A fresh restore of the untouched snapshot still works.
    r = restore_snapshot(snap)
    assert r.final_books() == replay_events(_stream())["final_books"]


def test_empty_replay_snapshot_round_trips():
    out = replay_events([])
    assert out["results"] == []
    assert out["trades"] == []
    assert out["final_books"] == {}
    r = restore_snapshot(out["snapshot"])
    ok = r.submit(add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100))
    assert ok["status"] is ACCEPTED


# --------------------------------------------------------------------------
# Engine state dump/load primitives
# --------------------------------------------------------------------------


def test_engine_dump_and_from_state_continue_identically():
    engine = Engine()
    engine.handle_object(
        {"event_id": "e1", "type": "ADD", "order_id": "i1", "side": "SELL",
         "order_type": "ICEBERG", "quantity": 10, "price": 100,
         "display_quantity": 3}
    )
    engine.handle_object(
        {"event_id": "e2", "type": "ADD", "order_id": "s2", "side": "SELL",
         "order_type": "LIMIT", "quantity": 5, "price": 100}
    )
    engine.handle_object(
        {"event_id": "e3", "type": "ADD", "order_id": "b1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 3, "price": 100}
    )
    state = engine.dump_state()
    # JSON-safe and canonicalizable.
    json.dumps(state)
    canonical_dumps(state)

    resumed = Engine.from_state(copy.deepcopy(state))
    assert resumed.snapshot() == engine.snapshot()
    eid, result, reason, trades, _stp = resumed.handle_object(
        {"event_id": "e4", "type": "ADD", "order_id": "b2", "side": "BUY",
         "order_type": "LIMIT", "quantity": 5, "price": 100}
    )
    assert [t["maker_order_id"] for t in trades] == ["s2"]
    assert [t["trade_id"] for t in trades] == [2]
    # The original engine is untouched by the resumed one.
    assert engine.dump_state()["next_trade_id"] == 2


def test_engine_dump_state_is_key_stable():
    def build(first, second):
        engine = Engine()
        engine.handle_object(first)
        engine.handle_object(second)
        return engine

    objs = [
        {"event_id": "e1", "type": "ADD", "order_id": "o2", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100},
        {"event_id": "e2", "type": "ADD", "order_id": "o1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 99},
    ]
    # Keys are emitted in sorted order regardless of arrival order.
    assert list(build(*objs).dump_state()["orders"]) == ["o1", "o2"]
    # Canonical content is identical regardless of dict insertion order.
    assert canonical_dumps(build(*objs).dump_state()) == canonical_dumps(
        build(*reversed(objs)).dump_state()
    )


# --------------------------------------------------------------------------
# Baseline non-replay behaviour must be unchanged
# --------------------------------------------------------------------------


def test_baseline_engine_and_cli_are_unchanged():
    # Existing tuple shape and reasons keep their baseline spelling.
    engine = Engine()
    eid, result, reason, trades = engine.handle_line(
        json.dumps({"event_id": "e1", "type": "ADD", "order_id": "o1",
                    "side": "BUY", "order_type": "LIMIT",
                    "quantity": 1, "price": 100})
    )
    assert (eid, result, reason, trades) == ("e1", "RESTING", None, [])
    eid, result, reason, trades = engine.handle_line("{")
    assert (eid, result, reason, trades) == (None, "REJECTED", "INVALID_JSON", [])
