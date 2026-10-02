"""Tests for the per-order EXECUTION_REPORT query in the multi-symbol stream."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EXECUTION_REPORT,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    POV_START,
    POV_VOLUME,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ORDER,
    VWAP_SLICE,
    VWAP_START,
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


def report(event_id, symbol, sequence, order_id, benchmark_price, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": EXECUTION_REPORT,
        "order_id": order_id,
        "benchmark_price": benchmark_price,
    }
    event.update(extra)
    return event


def nested_report(event_id, symbol, sequence, order_id, benchmark_price):
    return {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "event": {
            "event_id": event_id,
            "type": EXECUTION_REPORT,
            "order_id": order_id,
            "benchmark_price": benchmark_price,
        },
    }


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
               price, benchmark_price=None, **extra):
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


def vwap_start(event_id, symbol, sequence, plan_id, side, total_quantity, weights,
               price, benchmark_price=None):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": weights,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price,
    }


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


def pov_start(event_id, symbol, sequence, plan_id, side, total_quantity,
              participation_bps, price, benchmark_price=None):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": POV_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity,
        "participation_bps": participation_bps,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price,
    }


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_VOLUME, "plan_id": plan_id,
            "market_volume_increment": increment}


# ---------------------------------------------------------------------------
# Success shape and analysis content
# ---------------------------------------------------------------------------


def test_report_success_shape_and_analysis():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 101),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 101),
        report("r1", "AAA", 3, "s1", 100),
    ], snapshot_after=None)
    r = out["results"][2]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The untouched book is echoed.
    assert r["bids"] == []
    assert r["asks"] == [{"price": 101, "quantity": 6}]
    assert r["execution_analysis"] == {
        "side": "SELL",
        "current_status": "RESTING",
        "open_quantity": 6,
        "filled_quantity": 4,
        "executed_notional": 404,
        "vwap": {"numerator": 404, "denominator": 4},
        "slippage_notional": -4,
        "trade_attribution": [
            {"trade_id": 1, "role": "MAKER", "counterparty_order_id": "b1",
             "event_id": "a2", "price": 101, "quantity": 4},
        ],
    }


def test_report_without_fills_has_null_vwap_and_empty_attribution():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 2, "o1", 100),
    ], snapshot_after=None)
    analysis = out["results"][1]["execution_analysis"]
    assert analysis["current_status"] == "RESTING"
    assert analysis["open_quantity"] == 5
    assert analysis["filled_quantity"] == 0
    assert analysis["executed_notional"] == 0
    assert analysis["vwap"] is None
    assert analysis["slippage_notional"] == 0
    assert analysis["trade_attribution"] == []


def test_taker_and_maker_roles_are_attributed_in_trade_id_order():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 100),
        add("a2", "AAA", 2, "s1", "SELL", "LIMIT", 2, 100),
        add("a3", "AAA", 3, "s2", "SELL", "LIMIT", 5, 99),
        report("r1", "AAA", 4, "o1", 100),
    ], snapshot_after=None)
    analysis = out["results"][3]["execution_analysis"]
    # o1 first traded as maker against s1, then as taker against s2's
    # replacement... o1 filled 2 as maker; s2 then crosses o1's remainder.
    assert [t["trade_id"] for t in analysis["trade_attribution"]] == [1, 2]
    assert [t["role"] for t in analysis["trade_attribution"]] == ["MAKER", "MAKER"]
    assert analysis["filled_quantity"] == 3
    assert analysis["current_status"] == "FILLED"
    assert analysis["open_quantity"] == 0

    out2 = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
        report("r1", "AAA", 3, "b1", 100),
    ], snapshot_after=None)
    attribution = out2["results"][2]["execution_analysis"]["trade_attribution"]
    assert [t["role"] for t in attribution] == ["TAKER"]


def test_iceberg_open_quantity_includes_reserve():
    out = replay_events([
        iceberg("a1", "AAA", 1, "ice", "SELL", 30, 100, 5),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 12, 100),
        report("r1", "AAA", 3, "ice", 100),
    ], snapshot_after=None)
    analysis = out["results"][2]["execution_analysis"]
    # 12 filled across replenished slices; 18 remain including hidden reserve.
    assert analysis["filled_quantity"] == 12
    assert analysis["open_quantity"] == 18
    assert analysis["current_status"] == "RESTING"
    # Each consumed slice is a separate maker trade under the same id.
    assert [t["quantity"] for t in analysis["trade_attribution"]] == [5, 5, 2]
    assert all(t["role"] == "MAKER" for t in analysis["trade_attribution"])


def test_replaced_order_keeps_id_and_gathers_full_history():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        add("a2", "AAA", 2, "o1", "BUY", "LIMIT", 8, 99),
        replace("a3", "AAA", 3, "o1", 6, 100),
        report("r1", "AAA", 4, "o1", 100),
    ], snapshot_after=None)
    analysis = out["results"][3]["execution_analysis"]
    # The replacement traded 6 as taker under the original order id.
    assert analysis["filled_quantity"] == 6
    assert analysis["open_quantity"] == 0
    assert analysis["current_status"] == "FILLED"
    assert [t["role"] for t in analysis["trade_attribution"]] == ["TAKER"]
    assert analysis["trade_attribution"][0]["event_id"] == "a3"


def test_cancelled_order_stays_queryable():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        cancel("a2", "AAA", 2, "o1"),
        report("r1", "AAA", 3, "o1", 100),
    ], snapshot_after=None)
    analysis = out["results"][2]["execution_analysis"]
    assert analysis["current_status"] == "CANCELLED"
    assert analysis["open_quantity"] == 0


def test_sell_slippage_mirrors_buy_formula():
    out = replay_events([
        add("a1", "AAA", 1, "b1", "BUY", "LIMIT", 5, 99),
        add("a2", "AAA", 2, "s1", "SELL", "LIMIT", 5, 99),
        report("r1", "AAA", 3, "s1", 100),
    ], snapshot_after=None)
    analysis = out["results"][2]["execution_analysis"]
    # Sold at 99 against a 100 benchmark: worse by 5, reported positive.
    assert analysis["executed_notional"] == 495
    assert analysis["slippage_notional"] == 5


# ---------------------------------------------------------------------------
# Plan child orders
# ---------------------------------------------------------------------------


def test_released_twap_child_is_queryable_plan_and_unreleased_ids_are_not():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 20, 100),
        twap_start("a2", "AAA", 2, "p1", "BUY", 12, 2, 100),
        twap_slice("a3", "AAA", 3, "p1"),
        report("r1", "AAA", 4, "p1#1", 100),
        report("r2", "AAA", 5, "p1", 100),
        report("r3", "AAA", 6, "p1#2", 100),
    ], snapshot_after=None)
    r = out["results"][3]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["execution_analysis"]["filled_quantity"] == 6
    assert r["execution_analysis"]["current_status"] == "FILLED"
    # The parent plan id and the not-yet-released derived id are not orders.
    assert out["results"][4]["rejection_code"] == UNKNOWN_ORDER
    assert out["results"][5]["rejection_code"] == UNKNOWN_ORDER


def test_released_vwap_and_pov_children_are_queryable():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 100, 100),
        vwap_start("a2", "AAA", 2, "v1", "BUY", 10, [1, 3], 100),
        vwap_slice("a3", "AAA", 3, "v1"),
        pov_start("a4", "AAA", 4, "p1", "BUY", 10, 5000, 100),
        pov_volume("a5", "AAA", 5, "p1", 8),
        report("r1", "AAA", 6, "v1#1", 100),
        report("r2", "AAA", 7, "p1#1", 100),
        report("r3", "AAA", 8, "p1#2", 100),
    ], snapshot_after=None)
    assert out["results"][5]["execution_analysis"]["filled_quantity"] == 3
    assert out["results"][6]["execution_analysis"]["filled_quantity"] == 4
    # The second POV release has not happened yet.
    assert out["results"][7]["rejection_code"] == UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# Per-symbol isolation
# ---------------------------------------------------------------------------


def test_same_order_id_on_another_symbol_is_not_cross_queried():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "BBB", 1, "o1", 100),
        add("a2", "BBB", 2, "o1", "SELL", "LIMIT", 3, 50),
        report("r2", "BBB", 3, "o1", 50),
        report("r3", "AAA", 2, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][1]["rejection_code"] == UNKNOWN_ORDER
    # Each symbol resolves its own o1.
    assert out["results"][3]["execution_analysis"]["side"] == "SELL"
    assert out["results"][4]["execution_analysis"]["side"] == "BUY"


def test_unknown_symbol_is_unknown_order_and_registers_symbol():
    out = replay_events([
        report("r1", "ZZZ", 1, "o1", 100),
        report("r1", "ZZZ", 1, "o1", 100),
        report("r2", "ZZZ", 2, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][0]["rejection_code"] == UNKNOWN_ORDER
    assert out["results"][0]["bids"] == [] and out["results"][0]["asks"] == []
    # The well-formed query occupied its id and advanced the sequence: the
    # identical retry is a duplicate, and sequence 2 is next.
    assert out["results"][1]["status"] == DUPLICATE
    assert out["results"][2]["rejection_code"] == UNKNOWN_ORDER


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutation", [
    lambda e: e.pop("order_id"),
    lambda e: e.pop("benchmark_price"),
    lambda e: e.pop("type"),
    lambda e: e.update(order_id=""),
    lambda e: e.update(order_id=7),
    lambda e: e.update(order_id=None),
    lambda e: e.update(benchmark_price=True),
    lambda e: e.update(benchmark_price=0),
    lambda e: e.update(benchmark_price=-3),
    lambda e: e.update(benchmark_price=1.5),
    lambda e: e.update(benchmark_price="100"),
    lambda e: e.update(benchmark_price=None),
    lambda e: e.update(extra_field=1),
])
def test_malformed_report_events_are_invalid(mutation):
    event = report("r1", "AAA", 2, "o1", 100)
    mutation(event)
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        event,
    ], snapshot_after=None)
    assert out["results"][1]["status"] == REJECTED
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


def test_invalid_event_consumes_neither_id_nor_sequence():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 2, "", 100),          # invalid: empty order id
        report("r1", "AAA", 2, "o1", 100),        # same id, now well formed
    ], snapshot_after=None)
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    assert out["results"][2]["result"] == "REPORTED"


def test_invalid_report_on_unknown_symbol_does_not_create_it():
    out = replay_events([
        report("r1", "ZZZ", 1, "", 100),
        report("r2", "ZZZ", 1, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    # The invalid event registered nothing: sequence 1 is still expected.
    assert out["results"][1]["rejection_code"] == UNKNOWN_ORDER
    assert out["results"][1]["sequence"] == 1


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        nested_report("r1", "AAA", 2, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][1]["result"] == "REPORTED"


def test_nested_payload_event_id_mismatch_is_invalid():
    event = nested_report("r1", "AAA", 2, "o1", 100)
    event["event"]["event_id"] = "other"
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        event,
    ], snapshot_after=None)
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency, conflicts and ordering
# ---------------------------------------------------------------------------


def test_unknown_order_consumes_id_and_sequence():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 2, "gone", 100),
        report("r1", "AAA", 3, "gone", 100),
        report("r2", "AAA", 3, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][1]["rejection_code"] == UNKNOWN_ORDER
    # The identical retry is recognized as a duplicate with a stale sequence.
    assert out["results"][2]["status"] == DUPLICATE
    # The rejection advanced the symbol sequence to 2.
    assert out["results"][3]["status"] == ACCEPTED


def test_same_report_id_with_different_content_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 2, "o1", 100),
        report("r1", "AAA", 3, "o1", 101),
        report("r1", "BBB", 1, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT
    # Same id on another symbol conflicts as well.
    assert out["results"][3]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_business_checks():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 3, "o1", 100),
        report("r2", "AAA", 1, "o1", 100),
        report("r3", "AAA", 2, "o1", 100),
    ], snapshot_after=None)
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    # Neither occupied its slot: sequence 2 still works.
    assert out["results"][3]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Read-only guarantees
# ---------------------------------------------------------------------------


def test_report_is_read_only():
    stream = [
        iceberg("a1", "AAA", 1, "ice", "SELL", 30, 100, 5),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 12, 100),
        twap_start("a3", "AAA", 3, "p1", "BUY", 4, 2, 100),
    ]
    before = replay_events(copy.deepcopy(stream))
    with_report = replay_events(
        copy.deepcopy(stream) + [report("r1", "AAA", 4, "ice", 100)]
    )
    # The query changed nothing: the snapshot after the report equals the
    # snapshot before it, modulo the replay log's own idempotency records.
    snap_before = before["snapshot"]
    snap_after = with_report["snapshot"]
    assert snap_after["format_version"] == FORMAT_VERSION

    def strip_events(snapshot):
        doc = copy.deepcopy(snapshot)
        doc["content"]["events"] = []
        for symbol in doc["content"]["symbols"]:
            symbol["state"]["event_log"] = []
            symbol["state"]["last_sequence"] = 0
        doc["content_digest"] = ""
        return doc

    assert canonical_json(strip_events(snap_after)) == canonical_json(
        strip_events(snap_before)
    )
    # In particular the next trade id did not move.
    def next_trade_id(snapshot):
        for symbol in snapshot["content"]["symbols"]:
            if symbol["symbol"] == "AAA":
                return symbol["state"]["engine"]["next_trade_id"]
        raise AssertionError("symbol missing")

    assert next_trade_id(snap_after) == next_trade_id(snap_before)


def test_report_does_not_release_plan_slices_or_trade_ids():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 50, 100),
        twap_start("a2", "AAA", 2, "p1", "BUY", 4, 2, 100),
        report("r1", "AAA", 3, "p1", 100),
        report("r2", "AAA", 4, "p1#1", 100),
        twap_slice("a5", "AAA", 5, "p1"),
    ], snapshot_after=None)
    assert out["results"][2]["rejection_code"] == UNKNOWN_ORDER
    assert out["results"][3]["rejection_code"] == UNKNOWN_ORDER
    # The slice released afterwards still gets trade id 1 for its first trade.
    assert out["results"][4]["trades"][0]["trade_id"] == 1


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
        report("r1", "AAA", 5, "ice", 100),
        report("r2", "AAA", 6, "p1#1", 100),
        report("r3", "BBB", 2, "w1", 50),
        report("r4", "AAA", 7, "gone", 100),
        report("r5", "CCC", 1, "ice", 100),
    ]


def test_report_output_is_byte_for_byte_deterministic():
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


def test_resumed_replay_with_reports_matches_one_shot():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    # Split right after the first report to exercise mid-stream resume.
    snapshot = replay_events(copy.deepcopy(stream[:6]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[6:]), snapshot=snapshot)
    one_results = one_shot["results"]
    assert canonical_json(segmented["results"]) == canonical_json(one_results[6:])
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_duplicate_report_is_still_idempotent_after_restore():
    stream = _rich_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    replayer = restore_replayer(one_shot["snapshot"])
    # Resubmit an already-seen report with its original, now-stale sequence.
    result = replayer.submit([copy.deepcopy(stream[5])])[0]
    assert result["status"] == DUPLICATE
    # And a fresh report keeps working on the restored session.
    fresh = replayer.submit([report("r9", "AAA", 8, "b1", 100)])[0]
    assert fresh["status"] == ACCEPTED
    assert fresh["result"] == "REPORTED"


def test_business_rejected_report_roundtrips_through_snapshot():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 5, 100),
        report("r1", "AAA", 2, "gone", 100),
    ])
    snapshot = out["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [report("r2", "AAA", 3, "o1", 100)],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == ACCEPTED


def test_snapshot_after_named_report_event():
    out = replay_events(
        _rich_stream(),
        snapshot_after={"symbol": "AAA", "sequence": 6},
    )
    assert out["snapshot"] is not None
    resumed = replay_events(
        [report("rx", "AAA", 7, "p1#1", 100)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert resumed["results"][0]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Baseline entry points are unchanged
# ---------------------------------------------------------------------------


def test_baseline_engine_report_matches_stream_analysis():
    engine = Engine()
    engine.handle_object({
        "event_id": "a1", "type": "ADD", "order_id": "s1", "side": "SELL",
        "order_type": "LIMIT", "quantity": 10, "price": 101,
    })
    engine.handle_object({
        "event_id": "a2", "type": "ADD", "order_id": "b1", "side": "BUY",
        "order_type": "LIMIT", "quantity": 4, "price": 101,
    })
    _eid, result, reason, _trades, _stp, analysis = engine.handle_object_extended({
        "event_id": "r1", "type": "EXECUTION_REPORT",
        "order_id": "s1", "benchmark_price": 100,
    })
    assert (result, reason) == ("REPORTED", None)

    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 101),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 101),
        report("r1", "AAA", 3, "s1", 100),
    ], snapshot_after=None)
    assert out["results"][2]["execution_analysis"] == analysis


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


def test_cli_events_supports_execution_report_end_to_end():
    code, out, err = _run_cli({"events": [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 101),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 4, 101),
        report("r1", "AAA", 3, "s1", 100),
        report("r2", "AAA", 4, "gone", 100),
    ]})
    assert code == 0
    assert err == ""
    reported, rejected = out["results"][2], out["results"][3]
    assert reported["status"] == ACCEPTED
    assert reported["result"] == "REPORTED"
    assert reported["execution_analysis"]["filled_quantity"] == 4
    assert rejected["rejection_code"] == UNKNOWN_ORDER
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
