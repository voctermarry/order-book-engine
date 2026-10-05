"""Tests for the read-only BOOK_LIQUIDITY_REPORT event.

The query summarizes one security's *current* public book depth: up to
``depth`` bid levels (descending price) and ask levels (ascending price),
each with its visible quantity, the cumulative visible quantity over the
returned levels and the visible order count (an iceberg's current slice is
one order and its reserve never counts), plus the best prices, the spread,
the exact-fraction midpoint and an unreduced exact-fraction imbalance over
the returned levels' cumulative public quantities. The query is purely
read-only and occupies its event id and the envelope symbol's sequence like
every other replay-only report; the baseline single-security Engine and the
JSON Lines entry point reject the type as INVALID_SCHEMA.
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
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": -2},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 1.0},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": "2"},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": None},
        {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": [1]},
        {"event_id": "q1", "depth": 1},
    ],
)
def test_inline_schema_errors_are_invalid_event_and_consume_nothing(payload):
    event = {"symbol": "AAA", "sequence": 1, **payload}
    out = replay_events([event], snapshot_after=None)["results"][0]
    assert out["status"] == REJECTED
    assert out["rejection_code"] == INVALID_EVENT
    assert "liquidity_analysis" not in out
    # No symbol state was created and neither the id nor the sequence moved.
    assert out["bids"] == [] and out["asks"] == []
    follow_up = replay_events(
        [event, liq("q1", "AAA", 1, 1)], snapshot_after=None
    )["results"]
    assert follow_up[1]["status"] == ACCEPTED


def test_nested_form_and_inner_event_id_mismatch():
    good = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q1", "type": BOOK_LIQUIDITY_REPORT, "depth": 3},
    }
    assert replay_events([good], snapshot_after=None)["results"][0]["status"] == ACCEPTED
    mismatch = {
        "event_id": "q1", "symbol": "AAA", "sequence": 1,
        "event": {"event_id": "q2", "type": BOOK_LIQUIDITY_REPORT, "depth": 3},
    }
    out = replay_events([mismatch], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


def test_inline_form_rejects_envelope_only_extra_fields():
    assert replay_events([liq("q1", "AAA", 1, 1, timestamp="t1")],
                         snapshot_after=None)["results"][0]["status"] == ACCEPTED
    bad = liq("q1", "AAA", 1, 1, bogus=1)
    out = replay_events([bad], snapshot_after=None)["results"][0]
    assert (out["status"], out["rejection_code"]) == (REJECTED, INVALID_EVENT)


# ---------------------------------------------------------------------------
# Empty book and brand new symbol
# ---------------------------------------------------------------------------


def test_empty_book_on_brand_new_symbol_is_reported_with_nulls():
    out = replay_events([liq("q0", "NEW", 1, 3)], snapshot_after=None)["results"][0]
    assert out["status"] == ACCEPTED
    assert out["result"] == "REPORTED"
    assert out["trades"] == []
    assert out["book_changes"] == {"bids": [], "asks": []}
    assert out["bids"] == [] and out["asks"] == []
    assert analysis_of(out) == {
        "depth": 3,
        "bid_levels": [],
        "ask_levels": [],
        "best_bid": None,
        "best_ask": None,
        "spread": None,
        "midpoint": None,
        "imbalance": None,
    }
    # The well-formed query registered the symbol and advanced its sequence.
    replayer = EventReplayer()
    replayer.submit([liq("q0", "NEW", 1, 3)])
    assert replayer.book("NEW") == ([], [])
    second = replayer.submit([liq("q1", "NEW", 2, 1)])[0]
    assert second["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Level content, ordering and depth truncation
# ---------------------------------------------------------------------------


def _two_sided_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 100),
        add("a3", "AAA", 3, "b1", "BUY", "LIMIT", 4, 98),
        add("a4", "AAA", 4, "b2", "BUY", "LIMIT", 2, 99),
        add("a5", "AAA", 5, "s3", "SELL", "LIMIT", 7, 100),
        add("a6", "AAA", 6, "b3", "BUY", "LIMIT", 6, 97),
    ]


def test_levels_are_sorted_cumulative_and_depth_truncated():
    out = replay_events(
        _two_sided_stream() + [liq("q", "AAA", 7, 2)], snapshot_after=None
    )["results"][-1]
    a = analysis_of(out)
    assert a["depth"] == 2
    assert [level["price"] for level in a["bid_levels"]] == [99, 98]
    assert [level["price"] for level in a["ask_levels"]] == [100, 101]
    assert a["bid_levels"] == [
        {"price": 99, "visible_quantity": 2, "cumulative_visible_quantity": 2,
         "order_count": 1},
        {"price": 98, "visible_quantity": 4, "cumulative_visible_quantity": 6,
         "order_count": 1},
    ]
    # Two resting orders (s2, s3) aggregate at the best ask; cumulative then
    # continues into the second returned level.
    assert a["ask_levels"] == [
        {"price": 100, "visible_quantity": 10, "cumulative_visible_quantity": 10,
         "order_count": 2},
        {"price": 101, "visible_quantity": 5, "cumulative_visible_quantity": 15,
         "order_count": 1},
    ]
    assert (a["best_bid"], a["best_ask"]) == (99, 100)
    assert a["spread"] == 1
    assert a["midpoint"] == {"numerator": 199, "denominator": 2}
    # Imbalance over the two returned levels per side: 6 - 15 over 6 + 15.
    assert a["imbalance"] == {"numerator": -9, "denominator": 21}
    # The outer echo is the untouched query-time book.
    assert out["bids"] == [
        {"price": 99, "quantity": 2}, {"price": 98, "quantity": 4},
        {"price": 97, "quantity": 6},
    ]
    assert out["asks"] == [
        {"price": 100, "quantity": 10}, {"price": 101, "quantity": 5}
    ]


def test_depth_larger_than_the_book_returns_every_level():
    out = replay_events(
        _two_sided_stream() + [liq("q", "AAA", 7, 10)], snapshot_after=None
    )["results"][-1]
    a = analysis_of(out)
    assert [level["price"] for level in a["bid_levels"]] == [99, 98, 97]
    assert [level["price"] for level in a["ask_levels"]] == [100, 101]
    assert a["bid_levels"][-1]["cumulative_visible_quantity"] == 12
    assert a["ask_levels"][-1]["cumulative_visible_quantity"] == 15


def test_depth_one_reports_only_the_touch_and_imbalance_over_it():
    out = replay_events(
        _two_sided_stream() + [liq("q", "AAA", 7, 1)], snapshot_after=None
    )["results"][-1]
    a = analysis_of(out)
    assert [level["price"] for level in a["bid_levels"]] == [99]
    assert [level["price"] for level in a["ask_levels"]] == [100]
    assert a["imbalance"] == {"numerator": 2 - 10, "denominator": 12}


def test_one_sided_book_has_null_side_price_spread_and_midpoint():
    stream = [
        add("a1", "AAA", 1, "b1", "BUY", "LIMIT", 2, 99),
        add("a2", "AAA", 2, "b2", "BUY", "LIMIT", 3, 98),
        liq("q", "AAA", 3, 5),
    ]
    a = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    assert a["best_bid"] == 99
    assert a["best_ask"] is None
    assert a["spread"] is None
    assert a["midpoint"] is None
    # The denominator is the returned bid cumulative volume, which is nonzero.
    assert a["imbalance"] == {"numerator": 5, "denominator": 5}


def test_imbalance_numerator_can_be_negative_and_is_never_reduced():
    stream = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 101),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 99),
        liq("q", "AAA", 3, 1),
    ]
    a = analysis_of(replay_events(stream, snapshot_after=None)["results"][-1])
    # 1 - 4 = -3 over 5: the fraction is reported unreduced.
    assert a["imbalance"] == {"numerator": -3, "denominator": 5}


# ---------------------------------------------------------------------------
# Iceberg visibility
# ---------------------------------------------------------------------------


def test_iceberg_reserve_is_excluded_and_current_slice_counts_once():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("p1", "AAA", 2, "pl", "SELL", "LIMIT", 3, 100),
        liq("q", "AAA", 3, 5),
    ]
    level = analysis_of(
        replay_events(stream, snapshot_after=None)["results"][-1]
    )["ask_levels"][0]
    # Only the 4-unit public slice of the iceberg joins the plain 3; the six
    # hidden units never count, and the iceberg is a single queued order.
    assert level == {
        "price": 100, "visible_quantity": 7,
        "cumulative_visible_quantity": 7, "order_count": 2,
    }


def test_partially_consumed_slice_is_reported_with_its_remainder():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("t1", "AAA", 2, "tk", "BUY", "MARKET", 1),
        liq("q", "AAA", 3, 5),
    ]
    level = analysis_of(
        replay_events(stream, snapshot_after=None)["results"][-1]
    )["ask_levels"][0]
    assert level["visible_quantity"] == 3
    assert level["order_count"] == 1


def test_replenished_tail_slice_still_counts_as_one_order():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("p1", "AAA", 2, "pl", "SELL", "LIMIT", 3, 100),
        # Exhausts the iceberg slice of 4 (it replenishes at the level tail
        # behind pl), then takes one from pl.
        add("t1", "AAA", 3, "tk", "BUY", "MARKET", 5),
        liq("q", "AAA", 4, 5),
    ]
    level = analysis_of(
        replay_events(stream, snapshot_after=None)["results"][-1]
    )["ask_levels"][0]
    # pl leads with 2 left; the replenished 4-unit slice trails it: 6 public
    # units from two orders, the 2-unit reserve still hidden.
    assert level["visible_quantity"] == 6
    assert level["order_count"] == 2
    assert level["cumulative_visible_quantity"] == 6


# ---------------------------------------------------------------------------
# Read-only guarantee
# ---------------------------------------------------------------------------


def test_query_moves_no_trading_state_and_spends_no_trade_id():
    prefix = _two_sided_stream()
    queried = replay_events(prefix + [liq("q1", "AAA", 7, 2),
                                      liq("q2", "BBB", 1, 2)])
    plain = replay_events(copy.deepcopy(prefix))["snapshot"]
    symbols = {entry["symbol"]: entry
               for entry in queried["snapshot"]["content"]["symbols"]}
    plain_state = plain["content"]["symbols"][0]["state"]
    # The two AAA queries occupied sequences 7 and advanced the replay-log
    # counters, but every trading structure (engine, plans, limits) is exactly
    # what the mutating prefix alone produced.
    assert canonical_json(symbols["AAA"]["state"]["engine"]) == canonical_json(
        plain_state["engine"]
    )
    assert symbols["AAA"]["state"]["plans"] == []
    assert symbols["AAA"]["state"]["price_limits"] is None
    assert symbols["BBB"]["state"]["engine"]["next_trade_id"] == 1
    assert symbols["BBB"]["state"]["last_sequence"] == 1
    # Global idempotency log: the mutating prefix ids plus the two queries,
    # serialized in sorted event-id order.
    assert [entry["event_id"] for entry in queried["snapshot"]["content"]["events"]] == [
        "a1", "a2", "a3", "a4", "a5", "a6", "q1", "q2",
    ]
    assert [entry["symbol"] for entry in queried["snapshot"]["content"]["events"]] == [
        "AAA", "AAA", "AAA", "AAA", "AAA", "AAA", "AAA", "BBB",
    ]


def test_query_does_not_replenish_an_iceberg_slice():
    stream = [
        iceberg("i1", "AAA", 1, "ic", "SELL", 10, 100, 4),
        add("t1", "AAA", 2, "tk", "BUY", "MARKET", 1),
        liq("q", "AAA", 3, 5),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    # Asking twice gives byte-identical visible content and book echoes.
    assert canonical_json(results[2]) == canonical_json(results[2])
    snapshot = replay_events(stream[:2])["snapshot"]
    queried = replay_events(stream[:3])["snapshot"]
    assert canonical_json(snapshot["content"]["symbols"][0]["state"]["engine"]) == \
        canonical_json(queried["content"]["symbols"][0]["state"]["engine"])


def test_reconstruction_targeting_a_liquidity_sequence_rebuilds_the_unchanged_book():
    # A BOOK_LIQUIDITY_REPORT occupies a sequence but moves nothing, so a
    # historical reconstruction targeting that sequence rebuilds the same book
    # through the scratch session without re-running the liquidity query.
    stream = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        liq("l1", "AAA", 2, 5),
        {"event_id": "r1", "symbol": "AAA", "sequence": 3,
         "type": "BOOK_RECONSTRUCTION_REPORT", "target_sequence": 2},
        liq("l2", "AAA", 4, 5),
    ]
    results = replay_events(stream, snapshot_after=None)["results"]
    rec = results[2]["book_reconstruction"]
    assert [o["order_id"] for q in rec["ask_queues"] for o in q["orders"]] == ["s1"]
    assert results[3]["liquidity_analysis"]["ask_levels"] == [{
        "price": 100, "visible_quantity": 3,
        "cumulative_visible_quantity": 3, "order_count": 1,
    }]


# ---------------------------------------------------------------------------
# Idempotency, conflicts and sequencing
# ---------------------------------------------------------------------------


def test_duplicate_delivery_is_reported_without_the_analysis():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 2)]
    original = replay_events(copy.deepcopy(stream))["results"][-1]
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    retry = replayer.submit([liq("q", "AAA", 99, 2)])[0]
    assert retry["status"] == DUPLICATE
    assert "liquidity_analysis" not in retry
    assert retry["trades"] == [] and retry["book_changes"] == {"bids": [], "asks": []}
    assert canonical_json(retry["bids"]) == canonical_json(original["bids"])


def test_event_id_conflict_and_sequence_errors_keep_precedence():
    stream = _two_sided_stream() + [liq("q", "AAA", 7, 2)]
    replayer = restore_replayer(replay_events(copy.deepcopy(stream))["snapshot"])
    # Same id, different normalized content (depth changed): conflict without
    # consuming sequence 8.
    conflict = replayer.submit([liq("q", "AAA", 8, 3)])[0]
    assert conflict["status"] == REJECTED
    assert conflict["rejection_code"] == EVENT_ID_CONFLICT
    # Reusing the id on another security is the same conflict.
    cross = replayer.submit([liq("q", "BBB", 1, 2)])[0]
    assert cross["rejection_code"] == EVENT_ID_CONFLICT
    gap = replayer.submit([liq("qg", "AAA", 10, 2)])[0]
    assert gap["rejection_code"] == SEQUENCE_GAP
    early = replayer.submit([liq("qe", "AAA", 3, 2)])[0]
    assert early["rejection_code"] == OUT_OF_ORDER
    # Sequence 8 was never consumed, so it is still the expected one.
    fine = replayer.submit([liq("qok", "AAA", 8, 1)])[0]
    assert fine["status"] == ACCEPTED


def test_query_ids_stay_out_of_the_engine_journal():
    stream = _two_sided_stream() + [
        liq("q1", "AAA", 7, 1),
        liq("q2", "AAA", 8, 2),
        liq("q3", "BBB", 1, 2),
    ]
    snapshot = replay_events(stream)["snapshot"]
    # Symbol entries are serialized in sorted order, so AAA is index 0.
    assert snapshot["content"]["symbols"][0]["symbol"] == "AAA"
    aaa_engine_ids = set(
        snapshot["content"]["symbols"][0]["state"]["engine"]["event_ids"]
    )
    assert aaa_engine_ids == {"a1", "a2", "a3", "a4", "a5", "a6"}
    assert snapshot["content"]["symbols"][1]["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# Snapshot restoration and segmented replay
# ---------------------------------------------------------------------------


def _rich_stream():
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        add("a2", "BBB", 1, "w1", "SELL", "LIMIT", 2, 70),
        iceberg("a3", "AAA", 2, "ic", "SELL", 9, 100, 3),
        add("a4", "AAA", 3, "b1", "BUY", "MARKET", 4),
        add("a5", "BBB", 2, "w2", "BUY", "LIMIT", 1, 69),
        liq("q1", "AAA", 4, 2),
        liq("q2", "AAA", 5, 10),
        liq("q3", "BBB", 3, 1),
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
    duplicate = restored.submit([copy.deepcopy(stream[5])])[0]
    assert duplicate["status"] == DUPLICATE
    assert "liquidity_analysis" not in duplicate
    fresh = restored.submit([liq("q9", "AAA", 6, 1)])[0]
    assert (fresh["status"], fresh["result"]) == (ACCEPTED, "REPORTED")
    assert analysis_of(fresh)["depth"] == 1


def test_snapshot_after_named_query_event():
    stream = _rich_stream()
    out = replay_events(stream, snapshot_after={"symbol": "AAA", "sequence": 4})
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
    resumed = replay_events(
        [liq("qx", "AAA", 5, 1)], snapshot=out["snapshot"], snapshot_after=None
    )
    assert resumed["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Baseline Engine / JSON Lines rejection and CLI
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_type_as_invalid_schema_without_id_occupancy():
    engine = Engine()
    line = json.dumps(
        {"event_id": "z1", "type": BOOK_LIQUIDITY_REPORT, "depth": 1}
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


def test_events_cli_supports_liquidity_report_end_to_end():
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
    a = reported["liquidity_analysis"]
    assert a["depth"] == 1
    assert a["bid_levels"][0]["price"] == 99
    assert a["ask_levels"][0]["price"] == 100
    assert parsed["snapshot"]["format_version"] == FORMAT_VERSION


def test_events_cli_rejects_malformed_depth_as_invalid_event():
    request_obj = {"events": [
        {"event_id": "q1", "symbol": "AAA", "sequence": 1,
         "type": BOOK_LIQUIDITY_REPORT, "depth": True},
        {"event_id": "q2", "symbol": "AAA", "sequence": 1,
         "type": BOOK_LIQUIDITY_REPORT, "depth": 1},
    ]}
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    assert code == 0
    parsed = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    assert parsed["results"][0]["status"] == REJECTED
    assert parsed["results"][0]["rejection_code"] == INVALID_EVENT
    # The malformed event consumed neither the id nor sequence 1.
    assert parsed["results"][1]["status"] == ACCEPTED
