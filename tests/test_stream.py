"""Tests for deterministic multi-symbol event replay and resumable snapshots."""

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
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    EventReplayer,
    SnapshotError,
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


def statuses(out):
    return [(r["event_id"], r["status"], r.get("result") or r.get("rejection_code"))
            for r in out["results"]]


# ---------------------------------------------------------------------------
# Basic multi-symbol replay
# ---------------------------------------------------------------------------


def test_multiple_symbols_keep_independent_books_and_sequences():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "BBB", 1, "x1", "BUY", "LIMIT", 2, 50),
        add("e3", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
        add("e4", "BBB", 2, "y1", "SELL", "MARKET", 2),
    ])
    results = out["results"]
    assert statuses(out) == [
        ("e1", ACCEPTED, "RESTING"),
        ("e2", ACCEPTED, "RESTING"),
        ("e3", ACCEPTED, "FILLED"),
        ("e4", ACCEPTED, "FILLED"),
    ]
    # AAA trade id is 1 within AAA; BBB market trade id is 1 within BBB.
    assert [t["trade_id"] for t in results[2]["trades"]] == [1]
    assert [t["trade_id"] for t in results[3]["trades"]] == [1]
    assert results[2]["asks"] == [{"price": 100, "quantity": 2}]
    assert results[2]["bids"] == []
    assert results[3]["bids"] == []
    assert results[3]["asks"] == []
    assert out["snapshot"] is not None


def test_each_result_is_keyed_by_event_id_symbol_sequence():
    out = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100)])
    result = out["results"][0]
    assert result["event_id"] == "e1"
    assert result["symbol"] == "AAA"
    assert result["sequence"] == 1
    assert result["status"] == ACCEPTED
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": [{"price": 100, "quantity": 5}]}
    assert result["bids"] == []
    assert result["asks"] == [{"price": 100, "quantity": 5}]


def test_book_changes_list_removed_levels_as_zero_quantity():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100),
    ])
    changes = out["results"][1]["book_changes"]
    assert changes["bids"] == []
    assert changes["asks"] == [{"price": 100, "quantity": 0}]


def test_events_interleave_symbols_but_apply_in_input_order():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        add("e3", "BBB", 1, "z1", "SELL", "LIMIT", 1, 9),
        add("e4", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
    ])
    trades = out["results"][3]["trades"]
    assert [(t["maker_order_id"], t["trade_id"]) for t in trades] == [("s1", 1)]


def test_equal_timestamps_never_reorder_input():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1, "timestamp": 1000,
         "type": "ADD", "order_id": "s1", "side": "SELL",
         "order_type": "LIMIT", "quantity": 1, "price": 100},
        {"event_id": "e2", "symbol": "AAA", "sequence": 2, "timestamp": 1000,
         "type": "ADD", "order_id": "s2", "side": "SELL",
         "order_type": "LIMIT", "quantity": 1, "price": 100},
        {"event_id": "e3", "symbol": "AAA", "sequence": 3, "timestamp": 999,
         "type": "ADD", "order_id": "b1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100},
    ])
    # The earlier input order wins even though the last timestamp is smaller.
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["s1"]


# ---------------------------------------------------------------------------
# Sequence rules
# ---------------------------------------------------------------------------


def test_sequence_gap_is_rejected_and_consumes_nothing():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
        add("e3", "AAA", 2, "s3", "SELL", "LIMIT", 1, 100),
        add("e4", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
    ])
    r_gap = out["results"][1]
    assert r_gap["status"] == REJECTED
    assert r_gap["rejection_code"] == SEQUENCE_GAP
    assert r_gap["expected_sequence"] == 2
    assert r_gap["trades"] == []
    assert r_gap["book_changes"] == {"bids": [], "asks": []}
    # The book is echoed unchanged for a known symbol.
    assert r_gap["asks"] == [{"price": 100, "quantity": 1}]
    # The gap event did not occupy sequence 3 or its event id.
    assert out["results"][2]["status"] == ACCEPTED
    assert [t["maker_order_id"] for t in out["results"][3]["trades"]] == ["s1", "s3"]


def test_sequence_gap_on_unknown_symbol_does_not_create_the_symbol():
    out = replay_events([cancel("e1", "NEW", 2, "ghost")])
    assert out["results"][0]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_out_of_order_sequence_is_rejected():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        add("e3", "AAA", 1, "s3", "SELL", "LIMIT", 1, 100),
    ])
    r = out["results"][2]
    assert (r["status"], r["rejection_code"], r["expected_sequence"]) == (
        REJECTED, OUT_OF_ORDER, 3,
    )
    assert r["asks"] == [{"price": 100, "quantity": 2}]


def test_sequence_counters_are_per_symbol():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 1, 50),
        add("e3", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
        add("e4", "BBB", 2, "x2", "SELL", "LIMIT", 1, 50),
    ])
    assert [r["status"] for r in out["results"]] == [ACCEPTED] * 4


def test_sequence_zero_or_negative_is_invalid_event():
    for seq in (0, -1, True, "1", 1.0, None):
        out = replay_events([
            {"event_id": "e1", "symbol": "AAA", "sequence": seq,
             "type": "ADD", "order_id": "s1", "side": "SELL",
             "order_type": "LIMIT", "quantity": 1, "price": 100},
        ])
        assert out["results"][0]["rejection_code"] == INVALID_EVENT, seq


# ---------------------------------------------------------------------------
# INVALID_EVENT envelope and payload validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("event", [
    None,
    42,
    "nope",
    ["x"],
    {},
    {"event_id": "e1", "symbol": "AAA"},                               # missing sequence/type
    {"event_id": "", "symbol": "AAA", "sequence": 1,
     "type": "CANCEL", "order_id": "o1"},                              # empty id
    {"event_id": 9, "symbol": "AAA", "sequence": 1,
     "type": "CANCEL", "order_id": "o1"},                              # non-string id
    {"event_id": "e1", "symbol": "", "sequence": 1,
     "type": "CANCEL", "order_id": "o1"},                              # empty symbol
    {"event_id": "e1", "symbol": 7, "sequence": 1,
     "type": "CANCEL", "order_id": "o1"},                              # non-string symbol
    {"event_id": "e1", "symbol": "AAA", "sequence": 1},                # missing type/payload
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": "BOGUS", "order_id": "o1"},                               # unknown type
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": "DAY_END_RECONCILIATION",
     "expected_trades": [], "expected_accounts": []},                  # query stays baseline-only
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": "ADD", "order_id": "o1", "side": "BUY",
     "order_type": "LIMIT", "quantity": 0, "price": 100},              # illegal value
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "type": "ADD", "order_id": "o1", "side": "BUY",
     "order_type": "LIMIT", "quantity": 1, "price": 100, "x": 1},      # unknown field
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "timestamp": -1, "type": "CANCEL", "order_id": "o1"},             # bad timestamp
    {"event_id": "e1", "symbol": "AAA", "sequence": 1,
     "timestamp": True, "type": "CANCEL", "order_id": "o1"},           # bool timestamp
])
def test_invalid_events_are_rejected(event):
    out = replay_events([event])
    assert out["results"][0]["status"] == REJECTED
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["trades"] == []
    assert out["results"][0]["book_changes"] == {"bids": [], "asks": []}


def test_invalid_event_on_known_symbol_echoes_untouched_book():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        {"event_id": "e2", "symbol": "AAA", "sequence": 2,
         "type": "ADD", "order_id": "s2", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100, "bogus": 1},
    ])
    r = out["results"][1]
    assert r["rejection_code"] == INVALID_EVENT
    assert r["asks"] == [{"price": 100, "quantity": 3}]
    # Neither the event id nor the sequence slot were consumed.
    follow = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
    ])
    assert follow["results"][0]["status"] == ACCEPTED


def test_nested_payload_form_is_supported():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "e1", "type": "ADD", "order_id": "s1",
                   "side": "SELL", "order_type": "LIMIT",
                   "quantity": 1, "price": 100}},
    ])
    assert out["results"][0]["status"] == ACCEPTED


def test_nested_payload_event_id_mismatch_is_invalid_event():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "OTHER", "type": "CANCEL", "order_id": "o1"}},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


def test_failed_event_leaves_no_order_trade_counter_or_book_change():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        # Schema-invalid ADD: nothing must be consumed.
        add("e2", "AAA", 2, "bad", "BUY", "LIMIT", 2, 100, nonsense=1),
        # The first real trade after the failure still takes id 1.
        add("e3", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]
    assert out["results"][2]["asks"] == [{"price": 100, "quantity": 1}]


def test_earlier_successful_events_are_not_rolled_back_by_later_failure():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        cancel("e2", "AAA", 2, "ghost"),            # UNKNOWN_ORDER business reject
        add("e3", "AAA", 99, "b1", "BUY", "LIMIT", 1, 100),  # gap
    ])
    assert out["results"][0]["status"] == ACCEPTED
    assert out["results"][1]["rejection_code"] == "UNKNOWN_ORDER"
    assert out["results"][2]["rejection_code"] == SEQUENCE_GAP
    # The resting sell survived both later failures.
    assert out["results"][2]["asks"] == [{"price": 100, "quantity": 2}]


# ---------------------------------------------------------------------------
# Baseline behaviours preserved (cancel, replace, iceberg, rejection codes)
# ---------------------------------------------------------------------------


def test_cancel_success_and_unknown_order_rejection():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        cancel("e2", "AAA", 2, "s1"),
        cancel("e3", "AAA", 3, "s1"),
    ])
    assert (out["results"][1]["status"], out["results"][1]["result"]) == (
        ACCEPTED, "CANCELLED")
    assert out["results"][1]["asks"] == []
    assert (out["results"][2]["status"], out["results"][2]["rejection_code"]) == (
        REJECTED, "UNKNOWN_ORDER")


def test_replace_uses_baseline_rules_and_loses_priority():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "o2", "BUY", "LIMIT", 2, 100),
        replace("e3", "AAA", 3, "o1", 2, 100),
        add("e4", "AAA", 4, "s1", "SELL", "LIMIT", 2, 100),
    ])
    assert out["results"][2]["result"] == "REPLACED"
    assert [t["maker_order_id"] for t in out["results"][3]["trades"]] == ["o2"]


def test_replace_of_finished_order_keeps_baseline_rejection():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100),
        replace("e3", "AAA", 3, "s1", 1, 100),
    ])
    assert (out["results"][2]["status"], out["results"][2]["rejection_code"]) == (
        REJECTED, "UNKNOWN_ORDER")


def test_duplicate_order_id_rejection_code_is_preserved():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100),
    ])
    r = out["results"][1]
    assert r["status"] == REJECTED
    assert r["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_business_rejection_occupies_its_event_id_like_baseline():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100),  # DUPLICATE_ORDER_ID
        add("e2", "AAA", 3, "o2", "BUY", "LIMIT", 1, 99),    # same event id
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_iceberg_slice_and_reserve_flow_through_event_results():
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 10, 100),
    ])
    trades = out["results"][2]["trades"]
    assert [(t["maker_order_id"], t["quantity"]) for t in trades] == [
        ("i1", 3), ("s2", 5), ("i1", 2),
    ]
    assert [t["trade_id"] for t in trades] == [1, 2, 3]
    assert out["results"][2]["asks"] == [{"price": 100, "quantity": 1}]


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_exact_duplicate_is_idempotent_and_not_rematched():
    first = add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)
    out = replay_events([
        first,
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100),
        # Retried delivery of e1: stale sequence 1, identical content.
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 100),
    ])
    dup = out["results"][2]
    assert dup["status"] == DUPLICATE
    assert dup["trades"] == []
    assert dup["book_changes"] == {"bids": [], "asks": []}
    # The book is the post-e2 book (s1 fully gone), echoed, not rematched.
    assert dup["asks"] == []
    # Sequence 3 still follows the two accepted events: the duplicate did not
    # rematch and did not re-enter the book, so the new buy finds no seller and
    # rests on the bid side.
    assert out["results"][3]["status"] == ACCEPTED
    assert out["results"][3]["result"] == "RESTING"
    assert out["results"][3]["trades"] == []


def test_duplicate_content_is_compared_after_canonicalization():
    # Different textual key order describes identical normalized content.
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        {"sequence": 1, "order_id": "s1", "type": "ADD", "event_id": "e1",
         "symbol": "AAA", "side": "SELL", "order_type": "LIMIT",
         "quantity": 1, "price": 100},
    ])
    assert out["results"][1]["status"] == DUPLICATE


def test_same_event_id_with_different_content_conflicts():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e1", "AAA", 2, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 100),
    ])
    conflict = out["results"][1]
    assert conflict["status"] == REJECTED
    assert conflict["rejection_code"] == EVENT_ID_CONFLICT
    # The conflict consumed neither the sequence slot nor an order.
    assert out["results"][2]["status"] == ACCEPTED


def test_same_event_id_reused_on_another_symbol_conflicts():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e1", "BBB", 1, "x1", "SELL", "LIMIT", 1, 50),
    ])
    assert out["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_replay_output_is_byte_for_byte_deterministic():
    events = [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "BBB", 1, "x1", "BUY", "LIMIT", 4, 50, account_id="acct"),
        add("e3", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        replace("e4", "AAA", 3, "i1", 8, 100),
        cancel("e5", "BBB", 2, "x1"),
        add("e6", "AAA", 9, "b1", "BUY", "LIMIT", 1, 100),
        add("e1", "AAA", 4, "i1", "SELL", "LIMIT", 1, 100),
    ]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
    assert a == b

    def _no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float value leaked into deterministic output")
        if isinstance(value, dict):
            for item in value.values():
                _no_floats(item)
        elif isinstance(value, list):
            for item in value:
                _no_floats(item)

    _no_floats(json.loads(a))


def test_snapshot_export_is_byte_stable_across_calls():
    events = [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100)]
    s1 = replay_events(events)["snapshot"]
    s2 = replay_events(copy.deepcopy(events))["snapshot"]
    assert canonical_json(s1) == canonical_json(s2)


# ---------------------------------------------------------------------------
# Snapshots: export / restore / resume equivalence
# ---------------------------------------------------------------------------


def test_snapshot_document_shape():
    snap = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100)])["snapshot"]
    assert snap["format_version"] == FORMAT_VERSION
    assert isinstance(snap["engine_version"], str)
    assert isinstance(snap["config"], dict)
    assert len(snap["config_digest"]) == 64
    assert len(snap["content_digest"]) == 64
    content = snap["content"]
    assert content["symbols"][0]["symbol"] == "AAA"
    state = content["symbols"][0]["state"]
    assert state["last_sequence"] == 1
    assert state["engine"]["next_trade_id"] == 1
    assert state["engine"]["asks"][0]["order_ids"] == ["s1"]


def _segmented_equivalent_events():
    part1 = [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 4, 100),
        add("e3", "BBB", 1, "x1", "BUY", "LIMIT", 9, 50),
        add("e4", "AAA", 3, "b1", "BUY", "LIMIT", 4, 100),
    ]
    part2 = [
        replace("e5", "AAA", 4, "s2", 2, 101),
        add("e6", "AAA", 5, "b2", "BUY", "LIMIT", 12, 101),
        add("e7", "BBB", 2, "y1", "SELL", "MARKET", 4),
        cancel("e8", "AAA", 6, "i1"),
    ]
    return part1, part2


def test_resumed_replay_matches_uninterrupted_replay_exactly():
    part1, part2 = _segmented_equivalent_events()

    one_shot = replay_events(part1 + part2)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)

    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    # Final snapshots of the two runs are identical documents.
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_trade_ids_continue_per_symbol_after_restore():
    part1, part2 = _segmented_equivalent_events()
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)
    # AAA spent trade id 1 in part1 (b1 vs i1 slice of 3 + 1 more); the next
    # AAA aggressive event continues numbering at 3.
    aggressive = segmented["results"][1]
    assert aggressive["event_id"] == "e6"
    assert [t["trade_id"] for t in aggressive["trades"]] == [3, 4, 5, 6]
    # BBB trade ids are independent: x1 rested in part1, y1 takes id 1.
    bbb_trades = segmented["results"][2]["trades"]
    assert [t["trade_id"] for t in bbb_trades] == [1]


def test_snapshot_preserves_iceberg_slice_reserve_and_priority():
    part1 = [
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 3, 100),  # drains first i1 slice
    ]
    snapshot = replay_events(part1)["snapshot"]
    # First slice exhausted: replenished slice queues behind s2.
    state = snapshot["content"]["symbols"][0]["state"]
    ask_level = state["engine"]["asks"][0]
    assert ask_level["order_ids"] == ["s2", "i1"]
    i1 = state["engine"]["orders"]["i1"]
    assert i1["remaining"] == 7
    assert i1["visible"] == 3
    assert i1["display_quantity"] == 3

    part2 = [add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 7, 100)]
    resumed = replay_events(part2, snapshot=snapshot)["results"][0]
    # s2 (5) first, then the replenished i1 slice (2 of 3).
    assert [(t["maker_order_id"], t["quantity"]) for t in resumed["trades"]] == [
        ("s2", 5), ("i1", 2),
    ]
    assert [t["trade_id"] for t in resumed["trades"]] == [2, 3]


def test_snapshot_preserves_cumulative_trade_journal():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
    ]
    snapshot = replay_events(part1)["snapshot"]
    assert snapshot["content"]["symbols"][0]["state"]["engine"]["trade_log"] == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 2, "event_id": "e2"},
    ]
    restored = restore_replayer(snapshot)
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)


def test_midpoint_snapshot_after_named_event():
    events = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 2, 101),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 4, 101),
    ]
    out = replay_events(events, snapshot_after={"symbol": "AAA", "sequence": 1})
    mid = out["snapshot"]
    assert mid["content"]["symbols"][0]["state"]["last_sequence"] == 1
    assert mid["content"]["symbols"][0]["state"]["engine"]["next_trade_id"] == 1


def test_midpoint_snapshot_marker_must_identify_an_accepted_event():
    with pytest.raises(ValueError):
        replay_events(
            [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
             add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100)],
            snapshot_after={"symbol": "AAA", "sequence": 2},
        )
    with pytest.raises(ValueError):
        replay_events(
            [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100)],
            snapshot_after={"symbol": "AAA", "sequence": 9},
        )


def test_snapshot_can_be_omitted():
    out = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)],
                        snapshot_after=None)
    assert out["snapshot"] is None


def test_empty_stream_snapshot_roundtrips():
    out = replay_events([])
    assert out["results"] == []
    restored = restore_replayer(out["snapshot"])
    follow = [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)]
    r = replay_events(follow, snapshot=export_snapshot(restored))
    assert r["results"][0]["result"] == "RESTING"


# ---------------------------------------------------------------------------
# Snapshot integrity failures
# ---------------------------------------------------------------------------


def _tamper(snapshot, fn):
    broken = copy.deepcopy(snapshot)
    fn(broken)
    return broken


def test_snapshot_digest_mismatch_is_corrupt():
    good = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)])["snapshot"]
    broken = _tamper(good, lambda s: s["content"]["symbols"][0]["state"]
                     .__setitem__("last_sequence", 2))
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_snapshot_unsupported_version():
    good = replay_events([])["snapshot"]
    broken = _tamper(good, lambda s: s.__setitem__("format_version", "event-replay/9"))
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_VERSION_UNSUPPORTED


def test_snapshot_config_mismatch():
    good = replay_events([])["snapshot"]
    custom = {**good["config"], "trade_id_scheme": "global"}
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(good, config=custom)
    assert exc.value.code == CONFIG_MISMATCH


def test_snapshot_with_custom_config_roundtrips():
    config = {"venue": "X", "variant": 1}
    out = replay_events([], config=config)
    assert out["snapshot"]["config"] == config
    restored = restore_replayer(out["snapshot"], config=config)
    assert restored.config == config


def test_failed_restore_creates_no_partial_state():
    good = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)])["snapshot"]
    broken = _tamper(good, lambda s: s["content"]["symbols"][0]["state"]["engine"]
                     .__setitem__("next_trade_id", 42))
    with pytest.raises(SnapshotError):
        restore_replayer(broken)
    # The good snapshot still restores cleanly afterwards: no global damage.
    replayer = restore_replayer(good)
    r = replay_events(
        [add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100)],
        snapshot=export_snapshot(replayer),
    )
    assert [t["trade_id"] for t in r["results"][0]["trades"]] == [1]


@pytest.mark.parametrize("fn", [
    lambda s: s["content"]["symbols"][0]["state"]["engine"]["bid_totals"]
        .__setitem__("101", 5),
    lambda s: s["content"]["symbols"][0]["state"]["engine"]["orders"]["s1"]
        .__setitem__("remaining", 99),
    lambda s: s["content"]["symbols"][0]["state"]["event_log"]
        .append({"event_id": "zz", "content": "{}"}),
    lambda s: s["content"]["events"].__setitem__(
        0, {"event_id": "e1", "symbol": "OTHER", "content": "{}"}),
])
def test_structural_tampering_is_rejected_even_if_digest_recomputed(fn):
    # Simulate a malformed document with a freshly recomputed outer digest:
    # structural cross-validation must still reject it.
    good = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)])["snapshot"]
    broken = copy.deepcopy(good)
    fn(broken)
    broken["content_digest"] = __import__(
        "order_book_engine.event_replay", fromlist=["_digest"]
    )._digest({k: v for k, v in broken.items() if k != "content_digest"})
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(broken)
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_restore_input_is_not_an_object():
    with pytest.raises(SnapshotError) as exc:
        restore_replayer([1, 2, 3])
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_duplicate_after_restore_is_still_idempotent():
    first = add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)
    snapshot = replay_events([first])["snapshot"]
    out = replay_events(
        [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
         add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100)],
        snapshot=snapshot,
    )
    assert out["results"][0]["status"] == DUPLICATE
    # The duplicate did not re-enter anything, but the original resting sell
    # from before the snapshot is still in the book and trades normally.
    assert out["results"][1]["result"] == "FILLED"
    assert [(t["maker_order_id"], t["trade_id"]) for t in out["results"][1]["trades"]] == [
        ("s1", 1),
    ]


# ---------------------------------------------------------------------------
# EventReplayer stateful class
# ---------------------------------------------------------------------------


def test_self_trade_prevention_is_preserved_from_baseline():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="A"),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100, account_id="A"),
    ])
    r = out["results"][1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "SELF_TRADE_PREVENTED"
    assert r["trades"] == []
    assert r["asks"] == [{"price": 100, "quantity": 5}]


def test_failed_fok_consumes_no_trade_id_across_restore():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 100, time_in_force="FOK"),
    ]
    snapshot = replay_events(part1)["snapshot"]
    part2 = [add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 100)]
    resumed = replay_events(part2, snapshot=snapshot)["results"][0]
    assert [t["trade_id"] for t in resumed["trades"]] == [1]


def test_business_rejected_event_id_is_occupied_globally():
    # UNKNOWN_ORDER on AAA occupies e2; reusing e2 on BBB must conflict.
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        cancel("e2", "AAA", 2, "ghost"),
        {"event_id": "e2", "symbol": "BBB", "sequence": 1,
         "type": "ADD", "order_id": "x1", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 9},
    ])
    assert out["results"][1]["rejection_code"] == "UNKNOWN_ORDER"
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_unicode_content_is_normalized_and_byte_stable():
    events = [add("e1", "证券甲", 1, "订单①", "SELL", "LIMIT", 1, 100, account_id="账户★")]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
    assert a == b
    assert "证券甲".encode("utf-8") in a


def test_replay_events_requires_a_list():
    with pytest.raises(TypeError):
        replay_events({"event_id": "e1"})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        EventReplayer(config=[])  # type: ignore[arg-type]


def test_stateful_replayer_accumulates_across_submissions():
    replayer = EventReplayer()
    r1 = replayer.submit([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100)])
    r2 = replayer.submit([add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100)])
    assert r1[0]["result"] == "RESTING"
    assert [t["trade_id"] for t in r2[0]["trades"]] == [1]
    assert replayer.book("AAA") == ([], [])
    assert replayer.book("UNKNOWN") == ([], [])


def test_restore_copies_snapshot_state():
    snapshot = replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)])["snapshot"]
    replayer = restore_replayer(copy.deepcopy(snapshot))
    # Mutating the caller's snapshot document after restore cannot reach the
    # live replayer.
    snapshot["content"]["symbols"][0]["state"]["last_sequence"] = 999
    assert export_snapshot(replayer)["content"]["symbols"][0]["state"]["last_sequence"] == 1


# ---------------------------------------------------------------------------
# Low-level Engine state hooks
# ---------------------------------------------------------------------------


def test_engine_dump_and_load_state_preserves_everything():
    from order_book_engine.engine import Engine

    engine = Engine()
    engine.handle_object_position({
        "event_id": "e1", "type": "ADD", "order_id": "i1", "side": "SELL",
        "order_type": "ICEBERG", "quantity": 10, "price": 100,
        "display_quantity": 3, "account_id": "acct",
    })
    engine.handle_object_position({
        "event_id": "e2", "type": "ADD", "order_id": "b1", "side": "BUY",
        "order_type": "LIMIT", "quantity": 4, "price": 100,
    })

    state = engine.dump_state()
    rebuilt = Engine().load_state(state)
    # Mutating the export must not reach either engine.
    state["orders"]["i1"]["remaining"] = 1
    assert rebuilt.snapshot() == engine.snapshot()
    bids1, asks1 = rebuilt.snapshot()
    assert asks1 == [{"price": 100, "quantity": 2}]

    # The next trade id continues from the journal (two slices were traded).
    _, _, _, trades, *_ = rebuilt.handle_object_position({
        "event_id": "e3", "type": "ADD", "order_id": "b2", "side": "BUY",
        "order_type": "LIMIT", "quantity": 2, "price": 100,
    })
    assert [t["trade_id"] for t in trades] == [3]


# ---------------------------------------------------------------------------
# CLI: events subcommand
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


def test_cli_events_end_to_end_is_deterministic():
    request = {"events": [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
    ]}
    code1, out1, err1 = _run_cli(request)
    code2, out2, err2 = _run_cli(request)
    assert (code1, code2) == (0, 0)
    assert (err1, err2) == ("", "")
    assert canonical_json(out1) == canonical_json(out2)
    assert [r["result"] for r in out1["results"]] == ["RESTING", "FILLED"]
    assert out1["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_resumes_from_snapshot_in_request():
    first = {"events": [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100)]}
    _, out1, _ = _run_cli(first)
    second = {
        "events": [add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100)],
        "snapshot": out1["snapshot"],
    }
    code, out2, _ = _run_cli(second)
    assert code == 0
    assert [t["trade_id"] for t in out2["results"][0]["trades"]] == [1]
    assert out2["results"][0]["asks"] == [{"price": 100, "quantity": 1}]


def test_cli_rejects_malformed_request_document():
    stdin = io.TextIOWrapper(io.BytesIO(b"{not json"))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    parsed = json.loads(stdout.buffer.getvalue())
    assert code == 2
    assert parsed["error"]["code"] == "INVALID_REQUEST"


def test_cli_reports_snapshot_restore_failure():
    good = replay_events([])["snapshot"]
    good["format_version"] = "event-replay/9"
    code, out, _ = _run_cli({"events": [], "snapshot": good})
    assert code == 2
    assert out["error"]["code"] == SNAPSHOT_VERSION_UNSUPPORTED
