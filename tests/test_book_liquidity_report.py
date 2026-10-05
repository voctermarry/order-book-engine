"""Tests for the read-only current-book BOOK_LIQUIDITY_REPORT event.

The query summarizes the envelope security's current public book depth:
``bid_levels``/``ask_levels`` carry at most ``depth`` ordered levels with
per-level and cumulative visible quantities and order counts, plus the
best prices, spread, midpoint (exact half-sum fraction) and imbalance
(exact, unreduced fraction over the returned levels). Iceberg reserve is
never public; an iceberg's current visible fragment counts as one order.
The query never matches or replenishes anything; a first-seen symbol
answers against its empty book.
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_LIQUIDITY_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    REJECTED,
    SEQUENCE_GAP,
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


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def liq(event_id, symbol, sequence, depth, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": BOOK_LIQUIDITY_REPORT, "depth": depth}
    event.update(extra)
    return event


def analysis_of(result):
    return result["liquidity_analysis"]


def _two_sided_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
        add("a3", "AAA", 3, "s3", "SELL", "LIMIT", 2, 100),
        add("a4", "AAA", 4, "b1", "BUY", "LIMIT", 4, 98),
        add("a5", "AAA", 5, "b2", "BUY", "LIMIT", 2, 99),
        add("a6", "AAA", 6, "b3", "BUY", "LIMIT", 1, 99),
    ]


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 1, "x": 1},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": True},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": False},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 0},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": -1},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 1.0},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": "2"},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": None},
        {"event_id": "q1", "depth": 3},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 3.5},
    ],
)
def test_schema_errors_are_invalid_event_and_consume_nothing(payload):
    event = {"symbol": "AAA", "sequence": 1, **payload}
    out = replay_events([event], snapshot_after=None)["results"][0]
    assert out["status"] == REJECTED
    assert out["rejection_code"] == INVALID_EVENT
    assert "liquidity_analysis" not in out
    # No symbol state was created and neither the id nor the sequence moved.
    assert out["bids"] == [] and out["asks"] == []
    follow_up = replay_events(
        [event, liq("q1", "AAA", 1, 3)], snapshot_after=None
    )["results"]
    assert follow_up[1]["status"] == ACCEPTED


def test_nested_form_and_inner_event_id_mismatch():
    good = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 2},
    }
    assert replay_events([good], snapshot_after=None)["results"][0]["status"] == ACCEPTED
    mismatch = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q2", "type": BOOK_LIQUIDITY_REPORT, "depth": 2},
    }
    out = replay_events([mismatch], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


def test_inline_form_rejects_envelope_only_extra_fields():
    assert replay_events(
        [liq("q1", "AAA", 1, 1, timestamp="t1")], snapshot_after=None
    )["results"][0]["status"] == ACCEPTED
    bad = liq("q1", "AAA", 1, 1, bogus=1)
    out = replay_events([bad], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


# ---------------------------------------------------------------------------
# Empty book and first-seen symbols
# ---------------------------------------------------------------------------


def test_fresh_symbol_empty_book_reports_nulls_and_empty_levels():
    out = replay_events([liq("q0", "AAA", 1, 5)], snapshot_after=None)["results"][0]
    assert out["status"] == ACCEPTED
    assert out["result"] == "REPORTED"
    assert out["trades"] == []
    assert out["book_changes"] == {"bids": [], "asks": []}
    assert out["bids"] == [] and out["asks"] == []
    assert analysis_of(out) == {
        "depth": 5,
        "bid_levels": [],
        "ask_levels": [],
        "best_bid": None,
        "best_ask": None,
        "spread": None,
        "midpoint": None,
        "imbalance": None,
    }


def test_fresh_symbol_registers_and_advances_sequence():
    replayer = EventReplayer()
    first = replayer.submit([liq("q1", "NEW", 1, 3)])[0]
    assert first["status"] == ACCEPTED
    assert replayer.book("NEW") == ([], [])
    # The next event for this symbol must use sequence 2.
    second = replayer.submit([liq("q2", "NEW", 1, 3)])[0]
    assert second["rejection_code"] == OUT_OF_ORDER
    at_two = replayer.submit([liq("q2", "NEW", 2, 3)])[0]
    assert at_two["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Successful report content
# ---------------------------------------------------------------------------


def test_levels_are_sorted_capped_and_cumulative():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 2)]
    out = replay_events(stream, snapshot_after=None)["results"][-1]
    analysis = analysis_of(out)
    assert analysis["depth"] == 2
    assert analysis["bid_levels"] == [
        {"price": 99, "visible_quantity": 3,
         "cumulative_visible_quantity": 3, "order_count": 2},
        {"price": 98, "visible_quantity": 4,
         "cumulative_visible_quantity": 7, "order_count": 1},
    ]
    assert analysis["ask_levels"] == [
        {"price": 100, "visible_quantity": 5,
         "cumulative_visible_quantity": 5, "order_count": 2},
        {"price": 101, "visible_quantity": 5,
         "cumulative_visible_quantity": 10, "order_count": 1},
    ]
    assert analysis["best_bid"] == 99
    assert analysis["best_ask"] == 100
    assert analysis["spread"] == 1
    assert analysis["midpoint"] == {"numerator": 199, "denominator": 2}
    # Bids 7, asks 10 across the (non-truncated here) two levels per side.
    assert analysis["imbalance"] == {"numerator": -3, "denominator": 17}


def test_depth_caps_each_side_independently_and_imbalance_uses_returned_levels():
    # Three levels per side; depth 1 must keep only each best level and the
    # imbalance must be computed from those two levels alone (unreduced).
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("s2", "AAA", 2, "s2", "SELL", "LIMIT", 4, 101),
        add("s3", "AAA", 3, "s3", "SELL", "LIMIT", 8, 102),
        add("b1", "AAA", 4, "b1", "BUY", "LIMIT", 3, 99),
        add("b2", "AAA", 5, "b2", "BUY", "LIMIT", 9, 98),
        add("b3", "AAA", 6, "b3", "BUY", "LIMIT", 27, 97),
        liq("q", "AAA", 7, 1),
    ]
    analysis = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    assert [level["price"] for level in analysis["bid_levels"]] == [99]
    assert [level["price"] for level in analysis["ask_levels"]] == [100]
    # (3 - 2) / (3 + 2), deliberately not reduced.
    assert analysis["imbalance"] == {"numerator": 1, "denominator": 5}


def test_depth_beyond_available_levels_reports_every_level():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 100)]
    analysis = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    assert [level["price"] for level in analysis["bid_levels"]] == [99, 98]
    assert [level["price"] for level in analysis["ask_levels"]] == [100, 101]


def test_one_sided_book_keeps_side_best_but_nulls_spread_and_midpoint():
    stream = [
        add("s1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("s2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        liq("q", "AAA", 3, 5),
    ]
    analysis = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    assert analysis["best_bid"] is None
    assert analysis["best_ask"] == 100
    assert analysis["spread"] is None
    assert analysis["midpoint"] is None
    assert analysis["bid_levels"] == []
    assert [level["price"] for level in analysis["ask_levels"]] == [100, 101]
    # Only the ask side contributes; denominator is positive so imbalance
    # is the unreduced (0 - 5) / 5.
    assert analysis["imbalance"] == {"numerator": -5, "denominator": 5}


def test_iceberg_reserve_is_hidden_and_visible_fragment_counts_once():
    # Iceberg 10 @100 peak 4, a plain 3 @100 behind it; a 5-unit market buy
    # exhausts the 4-slice (it replenishes at the level tail behind the
    # plain order) and takes 1 from the plain order.
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("p1", "AAA", 2, "pl", "SELL", "LIMIT", 3, 100),
        add("t1", "AAA", 3, "tk", "BUY", "MARKET", 5),
        liq("q", "AAA", 4, 2),
    ]
    analysis = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    level = analysis["ask_levels"][0]
    assert level == {
        "price": 100,
        # Plain remainder 2 leads; the replenished iceberg slice of 4 is
        # behind it. The iceberg's other 2 reserve units stay invisible.
        "visible_quantity": 6,
        "cumulative_visible_quantity": 6,
        # The current visible fragment counts as exactly one order.
        "order_count": 2,
    }
    assert analysis["ask_levels"][1:] == []
    # Imbalance sees the empty bid side: -6 / 6 unreduced.
    assert analysis["imbalance"] == {"numerator": -6, "denominator": 6}


def test_partially_consumed_iceberg_slice_is_aggregated_as_is():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("t1", "AAA", 2, "tk", "BUY", "MARKET", 1),
        liq("q", "AAA", 3, 1),
    ]
    level = analysis_of(
        replay_events(stream, snapshot_after=None)["results"][-1]
    )["ask_levels"][0]
    assert level["visible_quantity"] == 3
    assert level["cumulative_visible_quantity"] == 3
    assert level["order_count"] == 1


def test_outer_book_fields_echo_the_unchanged_book():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 1)]
    result = replay_events(stream, snapshot_after=None)["results"][-1]
    assert result["bids"] == [
        {"price": 99, "quantity": 3}, {"price": 98, "quantity": 4}
    ]
    assert result["asks"] == [
        {"price": 100, "quantity": 5}, {"price": 101, "quantity": 5}
    ]
    assert result["book_changes"] == {"bids": [], "asks": []}
    assert result["trades"] == []


# ---------------------------------------------------------------------------
# Read-only guarantee, ordering and idempotency
# ---------------------------------------------------------------------------


def test_query_does_not_move_any_trading_state():
    queried = replay_events(
        _two_sided_stream() + [liq("q1", "AAA", 7, 1), liq("q2", "AAA", 8, 9)]
    )["snapshot"]
    prefix_only = replay_events(_two_sided_stream())["snapshot"]
    prefix_engine = prefix_only["content"]["symbols"][0]["state"]["engine"]
    queried_engine = queried["content"]["symbols"][0]["state"]["engine"]
    assert canonical_json(prefix_engine) == canonical_json(queried_engine)


def test_query_ids_stay_out_of_the_engine_journal():
    stream = _two_sided_stream() + [liq("q1", "AAA", 7, 1), liq("q2", "AAA", 8, 2)]
    snapshot = replay_events(stream)["snapshot"]
    engine_ids = set(snapshot["content"]["symbols"][0]["state"]["engine"]["event_ids"])
    assert engine_ids == {"a1", "a2", "a3", "a4", "a5", "a6"}


def test_duplicate_conflict_gap_and_out_of_order_precedence():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 2)]
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    # A verbatim retry with a stale sequence is a duplicate and carries no
    # analysis object.
    retry = replayer.submit([liq("q", "AAA", 99, 2)])[0]
    assert retry["status"] == DUPLICATE
    assert "liquidity_analysis" not in retry
    assert retry["trades"] == [] and retry["book_changes"] == {"bids": [], "asks": []}
    # Same id, changed depth -> conflict, sequence 8 not consumed.
    conflict = replayer.submit([liq("q", "AAA", 8, 3)])[0]
    assert conflict["rejection_code"] == EVENT_ID_CONFLICT
    # Cross-security id reuse is the same conflict.
    cross = replayer.submit([liq("q", "BBB", 1, 2)])[0]
    assert cross["rejection_code"] == EVENT_ID_CONFLICT
    gap = replayer.submit([liq("qg", "AAA", 10, 2)])[0]
    assert gap["rejection_code"] == SEQUENCE_GAP
    early = replayer.submit([liq("qe", "AAA", 3, 2)])[0]
    assert early["rejection_code"] == OUT_OF_ORDER
    # The rejected attempts moved neither the sequence nor the book: the
    # correct next event follows immediately.
    follow = replayer.submit([liq("qf", "AAA", 8, 2)])[0]
    assert follow["status"] == ACCEPTED


def test_duplicate_echoes_the_current_book():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 2)]
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    # Change the resting book after the original query, then retry it: the
    # duplicate response echoes the current book, like every other report.
    replayer.submit([add("n1", "AAA", 8, "s9", "SELL", "LIMIT", 6, 102)])
    current = replayer.submit([liq("probe", "AAA", 9, 2)])[0]
    retry = replayer.submit([liq("q", "AAA", 99, 2)])[0]
    assert retry["status"] == DUPLICATE
    assert "liquidity_analysis" not in retry
    assert canonical_json(retry["bids"]) == canonical_json(current["bids"])
    assert canonical_json(retry["asks"]) == canonical_json(current["asks"])


# ---------------------------------------------------------------------------
# Multi-symbol semantics
# ---------------------------------------------------------------------------


def test_sequence_counters_are_per_symbol():
    stream = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("b1", "BBB", 1, "w1", "SELL", "LIMIT", 4, 70),
        liq("qA", "AAA", 2, 1),
        add("b2", "BBB", 2, "w2", "BUY", "LIMIT", 3, 69),
        liq("qB", "BBB", 3, 2),
        liq("qA2", "AAA", 3, 5),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    assert results[2]["status"] == ACCEPTED
    assert analysis_of(results[2])["ask_levels"] == [{
        "price": 100, "visible_quantity": 2,
        "cumulative_visible_quantity": 2, "order_count": 1}]
    assert results[4]["status"] == ACCEPTED
    assert analysis_of(results[4])["ask_levels"] == [{
        "price": 70, "visible_quantity": 4,
        "cumulative_visible_quantity": 4, "order_count": 1}]
    assert results[5]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Snapshot restoration and deterministic replay
# ---------------------------------------------------------------------------


def _rich_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "BBB", 1, "w1", "SELL", "LIMIT", 2, 70),
        iceberg("a3", "AAA", 2, "ic", "SELL", 9, 100, 3),
        add("a4", "AAA", 3, "b1", "BUY", "MARKET", 4),
        add("a5", "BBB", 2, "w2", "BUY", "LIMIT", 1, 69),
        liq("q1", "AAA", 4, 1),
        liq("q2", "AAA", 5, 10),
        liq("q3", "BBB", 3, 2),
        liq("q4", "AAA", 6, 1),
    ]


def test_resumed_replay_matches_one_shot_byte_for_byte():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    snapshot = replay_events(copy.deepcopy(stream[:5]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[5:]), snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][5:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(
        one_shot["snapshot"]
    )


def test_snapshot_roundtrip_and_post_restore_behaviour():
    stream = _rich_stream()
    out = replay_events(copy.deepcopy(stream))
    restored = restore_replayer(copy.deepcopy(out["snapshot"]))
    assert canonical_json(export_snapshot(restored)) == canonical_json(out["snapshot"])
    # The logged query is a duplicate after restoration.
    duplicate = restored.submit([copy.deepcopy(stream[5])])[0]
    assert duplicate["status"] == DUPLICATE
    assert "liquidity_analysis" not in duplicate
    # Sequence 7 is the next AAA sequence; a fresh query is answered.
    fresh = restored.submit([liq("q9", "AAA", 7, 2)])[0]
    assert (fresh["status"], fresh["result"]) == (ACCEPTED, "REPORTED")
    assert analysis_of(fresh)["depth"] == 2


def test_snapshot_after_named_query_event():
    stream = _rich_stream()
    out = replay_events(stream, snapshot_after={"symbol": "AAA", "sequence": 4})
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
    resumed = replay_events(
        [liq("qx", "AAA", 5, 1)], snapshot=out["snapshot"], snapshot_after=None
    )
    assert resumed["results"][0]["status"] == ACCEPTED


def test_contiguous_and_segmented_runs_are_byte_identical_at_every_cut():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    for cut in range(1, len(stream)):
        snapshot = replay_events(copy.deepcopy(stream[:cut]))["snapshot"]
        segmented = replay_events(copy.deepcopy(stream[cut:]), snapshot=snapshot)
        assert canonical_json(segmented["results"]) == canonical_json(
            one_shot["results"][cut:]
        )
    assert canonical_json(replay_events(copy.deepcopy(stream))["snapshot"]) == (
        canonical_json(one_shot["snapshot"])
    )


# ---------------------------------------------------------------------------
# Baseline Engine / JSON Lines rejection and CLI
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_type_as_invalid_schema_without_id_occupancy():
    engine = Engine()
    line = json.dumps({"event_id": "z1", "type": BOOK_LIQUIDITY_REPORT, "depth": 3})
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


def test_events_cli_supports_liquidity_end_to_end():
    request_obj = {"events": _two_sided_stream() + [liq("q1", "AAA", 7, 1)]}
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
    analysis = reported["liquidity_analysis"]
    assert analysis["depth"] == 1
    assert analysis["bid_levels"][0]["price"] == 99
    assert analysis["ask_levels"][0]["price"] == 100
    assert parsed["snapshot"]["format_version"] == FORMAT_VERSION
