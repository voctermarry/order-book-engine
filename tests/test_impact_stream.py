"""Tests for the IMPACT_REPORT what-if query in the multi-symbol stream."""

from __future__ import annotations

import copy
import io
import json

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    IMPACT_REPORT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PRICE_LIMIT_UPDATE,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.engine import Engine


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def add(event_id, symbol, sequence, order_id, side, order_type, quantity,
        price=None, **extra):
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


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price,
            display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG",
               quantity, price, display_quantity=display, **extra)


def impact(event_id, symbol, sequence, side, quantity, benchmark_price,
           **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": IMPACT_REPORT,
        "side": side,
        "quantity": quantity,
        "benchmark_price": benchmark_price,
    }
    event.update(extra)
    return event


def nested_impact(event_id, symbol, sequence, side, quantity, benchmark_price):
    return {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "event": {
            "event_id": event_id,
            "type": IMPACT_REPORT,
            "side": side,
            "quantity": quantity,
            "benchmark_price": benchmark_price,
        },
    }


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity,
               slice_count, price, benchmark_price=None, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": TWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price,
    }
    event.update(extra)
    return event


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


# ---------------------------------------------------------------------------
# Success shape and analysis content
# ---------------------------------------------------------------------------


def test_buy_impact_success_shape_and_analysis():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        impact("q1", "AAA", 3, "BUY", 6, 99),
    ], snapshot_after=None)
    r = out["results"][2]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The untouched book is echoed.
    assert r["bids"] == []
    assert r["asks"] == [
        {"price": 100, "quantity": 5},
        {"price": 101, "quantity": 3},
    ]
    assert r["impact_analysis"] == {
        "side": "BUY",
        "requested_quantity": 6,
        "benchmark_price": 99,
        "executable_quantity": 6,
        "unfilled_quantity": 0,
        "executed_notional": 5 * 100 + 101,
        "best_price": 100,
        "vwap": {"numerator": 601, "denominator": 6},
        "slippage_notional": 7,
        "impact_notional": 1,
        "price_breakdown": [
            {"price": 100, "quantity": 5},
            {"price": 101, "quantity": 1},
        ],
    }


def test_sell_impact_walks_bids_at_maker_prices():
    out = replay_events([
        add("a1", "AAA", 1, "b1", "BUY", "LIMIT", 3, 100),
        add("a2", "AAA", 2, "b2", "BUY", "LIMIT", 4, 99),
        impact("q1", "AAA", 3, "SELL", 5, 101),
    ], snapshot_after=None)
    analysis = out["results"][2]["impact_analysis"]
    assert analysis["price_breakdown"] == [
        {"price": 100, "quantity": 3},
        {"price": 99, "quantity": 2},
    ]
    assert analysis["executable_quantity"] == 5
    assert analysis["unfilled_quantity"] == 0
    assert analysis["best_price"] == 100
    assert analysis["executed_notional"] == 3 * 100 + 2 * 99
    assert analysis["vwap"] == {"numerator": 498, "denominator": 5}
    # Sell mirrors the buy formula: negative means improvement.
    assert analysis["slippage_notional"] == 101 * 5 - 498
    assert analysis["impact_notional"] == 100 * 5 - 498


def test_partial_liquidity_reports_unfilled_remainder():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        impact("q1", "AAA", 3, "BUY", 100, 100),
    ], snapshot_after=None)
    analysis = out["results"][2]["impact_analysis"]
    assert analysis["requested_quantity"] == 100
    assert analysis["executable_quantity"] == 8
    assert analysis["unfilled_quantity"] == 92
    assert analysis["executed_notional"] == 5 * 100 + 3 * 101
    assert analysis["vwap"] == {"numerator": 803, "denominator": 8}
    # Costs are computed over the executable quantity only.
    assert analysis["slippage_notional"] == 803 - 100 * 8
    assert analysis["impact_notional"] == 803 - 100 * 8


def test_empty_book_reports_zeroes_and_nulls():
    out = replay_events([
        impact("q1", "AAA", 1, "BUY", 10, 100),
    ], snapshot_after=None)
    r = out["results"][0]
    # A first-seen symbol reports successfully against its empty book.
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["bids"] == [] and r["asks"] == []
    assert r["impact_analysis"] == {
        "side": "BUY",
        "requested_quantity": 10,
        "benchmark_price": 100,
        "executable_quantity": 0,
        "unfilled_quantity": 10,
        "executed_notional": 0,
        "best_price": None,
        "vwap": None,
        "slippage_notional": 0,
        "impact_notional": 0,
        "price_breakdown": [],
    }


def test_iceberg_replenishment_orders_breakdown_like_real_matching():
    out = replay_events([
        iceberg("a1", "AAA", 1, "ice", "SELL", 30, 100, 5),
        add("a2", "AAA", 2, "s1", "SELL", "LIMIT", 4, 100),
        impact("q1", "AAA", 3, "BUY", 12, 100),
    ], snapshot_after=None)
    analysis = out["results"][2]["impact_analysis"]
    # Visible slice, then the same-price maker ahead of the replenished
    # slice, then the replenished slice itself: one entry per simulated
    # visible-slice fill, exactly the granularity of real trades.
    assert analysis["price_breakdown"] == [
        {"price": 100, "quantity": 5},
        {"price": 100, "quantity": 4},
        {"price": 100, "quantity": 3},
    ]
    assert analysis["executable_quantity"] == 12
    assert analysis["executed_notional"] == 1200


def test_symbols_are_isolated():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("b1", "BBB", 1, "w1", "SELL", "LIMIT", 7, 50),
        impact("q1", "AAA", 2, "BUY", 10, 100),
        impact("q2", "BBB", 2, "BUY", 10, 50),
    ], snapshot_after=None)
    aaa = out["results"][2]["impact_analysis"]
    bbb = out["results"][3]["impact_analysis"]
    assert aaa["executable_quantity"] == 5
    assert aaa["best_price"] == 100
    assert bbb["executable_quantity"] == 7
    assert bbb["best_price"] == 50


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        nested_impact("q1", "AAA", 2, "BUY", 2, 100),
    ], snapshot_after=None)
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][1]["result"] == "REPORTED"
    assert out["results"][1]["impact_analysis"]["executable_quantity"] == 2


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------


def test_schema_rejections_consume_neither_id_nor_sequence():
    bad_events = [
        # Missing benchmark_price.
        {"event_id": "q1", "symbol": "AAA", "sequence": 2,
         "type": IMPACT_REPORT, "side": "BUY", "quantity": 1},
        # Extra field.
        impact("q2", "AAA", 2, "BUY", 1, 100, order_id="o1"),
        # Illegal side.
        impact("q3", "AAA", 2, "HOLD", 1, 100),
        # Non-positive / non-integer quantity and benchmark.
        impact("q4", "AAA", 2, "BUY", 0, 100),
        impact("q5", "AAA", 2, "BUY", -1, 100),
        impact("q6", "AAA", 2, "BUY", True, 100),
        impact("q7", "AAA", 2, "BUY", 1.5, 100),
        impact("q8", "AAA", 2, "BUY", "1", 100),
        impact("q9", "AAA", 2, "BUY", None, 100),
        impact("q10", "AAA", 2, "BUY", 1, 0),
        impact("q11", "AAA", 2, "BUY", 1, False),
        impact("q12", "AAA", 2, "BUY", 1, 100.0),
    ]
    out = replay_events(
        [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100)] + bad_events,
        snapshot_after=None,
    )
    for result in out["results"][1:]:
        assert result["status"] == REJECTED
        assert result["rejection_code"] == INVALID_EVENT
        assert "impact_analysis" not in result
    # Nothing was consumed: sequence 2 and every rejected id are still free.
    follow = replay_events(
        [impact("q1", "AAA", 2, "BUY", 1, 100)],
        snapshot=replay_events(
            [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100)] + bad_events
        )["snapshot"],
        snapshot_after=None,
    )
    assert follow["results"][0]["status"] == ACCEPTED


def test_unknown_type_and_envelope_problems_stay_invalid():
    out = replay_events([
        {"event_id": "q1", "symbol": "AAA", "sequence": 1,
         "type": "IMPACT_REPORT", "side": "BUY", "quantity": 1,
         "benchmark_price": 100, "unexpected": 1},
        {"event_id": "q2", "symbol": "AAA", "sequence": 1,
         "type": "ACCOUNT_REPORT", "account_id": "x", "mark_price": 1},
    ], snapshot_after=None)
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    # ACCOUNT_REPORT stays exclusive to the JSON Lines entry point.
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency, conflicts and ordering
# ---------------------------------------------------------------------------


def test_duplicate_conflict_and_sequence_rules():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        impact("q1", "AAA", 2, "BUY", 1, 100),
        # Identical retry with a stale sequence is a duplicate.
        impact("q1", "AAA", 2, "BUY", 1, 100),
        # Same id, different content or another symbol: conflict.
        impact("q1", "AAA", 3, "BUY", 2, 100),
        impact("q1", "BBB", 1, "BUY", 1, 100),
        # Sequence hole and regression.
        impact("q2", "AAA", 4, "BUY", 1, 100),
        impact("q3", "AAA", 1, "BUY", 1, 100),
        # The freed slot still works.
        impact("q4", "AAA", 3, "SELL", 1, 100),
    ], snapshot_after=None)
    results = out["results"]
    assert results[2]["status"] == DUPLICATE
    assert "impact_analysis" not in results[2]
    assert results[3]["rejection_code"] == EVENT_ID_CONFLICT
    assert results[4]["rejection_code"] == EVENT_ID_CONFLICT
    assert results[5]["rejection_code"] == SEQUENCE_GAP
    assert results[6]["rejection_code"] == OUT_OF_ORDER
    assert results[7]["status"] == ACCEPTED
    assert results[7]["impact_analysis"]["side"] == "SELL"


# ---------------------------------------------------------------------------
# Read-only guarantees
# ---------------------------------------------------------------------------


def test_query_is_read_only_against_snapshot_state():
    stream = [
        iceberg("a1", "AAA", 1, "ice", "SELL", 30, 100, 5),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 12, 100, account_id="fund"),
        twap_start("a3", "AAA", 3, "p1", "BUY", 4, 2, 100),
    ]
    before = replay_events(copy.deepcopy(stream))
    with_query = replay_events(
        copy.deepcopy(stream) + [impact("q1", "AAA", 4, "BUY", 40, 100)]
    )
    snap_before = before["snapshot"]
    snap_after = with_query["snapshot"]
    assert snap_after["format_version"] == FORMAT_VERSION

    def strip_events(snapshot):
        doc = copy.deepcopy(snapshot)
        doc["content"]["events"] = []
        for symbol in doc["content"]["symbols"]:
            symbol["state"]["event_log"] = []
            symbol["state"]["last_sequence"] = 0
        doc["content_digest"] = ""
        return doc

    # The query changed nothing but the replay log's own idempotency records.
    assert canonical_json(strip_events(snap_after)) == canonical_json(
        strip_events(snap_before)
    )

    def next_trade_id(snapshot):
        for symbol in snapshot["content"]["symbols"]:
            if symbol["symbol"] == "AAA":
                return symbol["state"]["engine"]["next_trade_id"]
        raise AssertionError("symbol missing")

    assert next_trade_id(snap_after) == next_trade_id(snap_before)


def test_query_does_not_match_or_consume_trade_ids():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 50, 100),
        impact("q1", "AAA", 2, "BUY", 10, 100),
        add("a3", "AAA", 3, "b1", "BUY", "LIMIT", 4, 100),
    ], snapshot_after=None)
    # The query matched nothing: the full 50 still rests and the following
    # real trade gets trade id 1.
    assert out["results"][1]["asks"] == [{"price": 100, "quantity": 50}]
    assert out["results"][2]["trades"][0]["trade_id"] == 1
    assert out["results"][2]["asks"] == [{"price": 100, "quantity": 46}]


def test_query_ignores_self_trade_prevention_and_price_limits():
    config = {"price_limits": {"AAA": {"lower": 90, "upper": 110}}}
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100,
            account_id="fund"),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 130,
            account_id="fund"),
        {"event_id": "u1", "symbol": "AAA", "sequence": 3,
         "type": PRICE_LIMIT_UPDATE, "lower_price": 95, "upper_price": 105},
        # Anonymous and unrestricted: same-account liquidity is consumed and
        # the active price-limit interval (nor the static one) never blocks.
        impact("q1", "AAA", 4, "BUY", 8, 100),
    ], config=config, snapshot_after=None)
    analysis = out["results"][3]["impact_analysis"]
    assert analysis["executable_quantity"] == 5
    assert analysis["price_breakdown"] == [{"price": 100, "quantity": 5}]


# ---------------------------------------------------------------------------
# Determinism and snapshots
# ---------------------------------------------------------------------------


def _rich_stream():
    return [
        iceberg("a1", "AAA", 1, "ice", "SELL", 30, 100, 5),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 12, 100, account_id="fund"),
        twap_start("a3", "AAA", 3, "p1", "BUY", 4, 2, 100),
        twap_slice("a4", "AAA", 4, "p1"),
        add("b1", "BBB", 1, "w1", "SELL", "LIMIT", 3, 50),
        impact("q1", "AAA", 5, "BUY", 20, 100),
        impact("q2", "AAA", 6, "SELL", 5, 99),
        impact("q3", "BBB", 2, "BUY", 10, 50),
        impact("q4", "CCC", 1, "SELL", 1, 1),
    ]


def test_impact_output_is_byte_for_byte_deterministic():
    a = canonical_json(replay_events(_rich_stream()))
    b = canonical_json(replay_events(copy.deepcopy(_rich_stream())))
    assert a == b
    parsed = json.loads(a)

    def no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float leaked into output")
        if isinstance(value, dict):
            for item in value.values():
                no_floats(item)
        elif isinstance(value, list):
            for item in value:
                no_floats(item)

    no_floats(parsed)


def test_resumed_replay_with_impact_reports_matches_one_shot():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    # Split right after the first query to exercise mid-stream resume.
    snapshot = replay_events(copy.deepcopy(stream[:6]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[6:]), snapshot=snapshot)
    one_results = one_shot["results"]
    assert canonical_json(segmented["results"]) == canonical_json(one_results[6:])
    assert canonical_json(segmented["snapshot"]) == canonical_json(
        one_shot["snapshot"]
    )


def test_duplicate_query_is_still_idempotent_after_restore():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    replayer = restore_replayer(one_shot["snapshot"])
    # Resubmit an already-seen query with its original, now-stale sequence.
    result = replayer.submit([copy.deepcopy(stream[5])])[0]
    assert result["status"] == DUPLICATE
    # And a fresh query keeps working on the restored session.
    fresh = replayer.submit([impact("q9", "AAA", 7, "BUY", 3, 100)])[0]
    assert fresh["status"] == ACCEPTED
    assert fresh["result"] == "REPORTED"


def test_query_roundtrips_through_snapshot():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        impact("q1", "AAA", 2, "BUY", 2, 100),
    ])
    snapshot = out["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [impact("q2", "AAA", 3, "SELL", 1, 100)],
        snapshot=export_snapshot(restored),
        snapshot_after=None,
    )
    assert follow["results"][0]["status"] == ACCEPTED


def test_snapshot_after_named_impact_event():
    out = replay_events(
        _rich_stream(),
        snapshot_after={"symbol": "AAA", "sequence": 6},
    )
    assert out["snapshot"] is not None
    resumed = replay_events(
        [impact("qx", "AAA", 7, "BUY", 1, 100)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert resumed["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Parity with the baseline engine
# ---------------------------------------------------------------------------


def test_baseline_engine_impact_matches_stream_analysis():
    engine = Engine()
    engine.handle_object({
        "event_id": "a1", "type": "ADD", "order_id": "s1", "side": "SELL",
        "order_type": "LIMIT", "quantity": 5, "price": 100,
    })
    engine.handle_object({
        "event_id": "a2", "type": "ADD", "order_id": "s2", "side": "SELL",
        "order_type": "LIMIT", "quantity": 3, "price": 101,
    })
    _eid, result, reason, _trades, impact_analysis = engine._impact_report(
        "q1", {
            "event_id": "q1", "type": "IMPACT_REPORT", "side": "BUY",
            "quantity": 6, "benchmark_price": 99,
        }
    )
    assert (result, reason) == ("REPORTED", None)

    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        impact("q1", "AAA", 3, "BUY", 6, 99),
    ], snapshot_after=None)
    assert out["results"][2]["impact_analysis"] == impact_analysis


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


def test_cli_events_supports_impact_report_end_to_end():
    code, out, err = _run_cli({"events": [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        impact("q1", "AAA", 3, "BUY", 6, 99),
        impact("q2", "AAA", 4, "BUY", 0, 99),
    ]})
    assert code == 0
    assert err == ""
    reported, rejected = out["results"][2], out["results"][3]
    assert reported["status"] == ACCEPTED
    assert reported["result"] == "REPORTED"
    assert reported["impact_analysis"]["executable_quantity"] == 6
    assert rejected["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
