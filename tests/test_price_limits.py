"""Tests for optional static per-security price limits (``price_limits``).

The block is part of the replay session ``config`` and bounds the price of
LIMIT/ICEBERG ADDs, REPLACEs and LIMIT TWAP/VWAP plans against a closed
``[lower, upper]`` interval per security. MARKET events are exempt. A breach
is a committed ``PRICE_LIMIT_EXCEEDED`` business rejection decided after the
envelope/idempotency/sequence/identifier checks but before any matching or
state change.
"""

from __future__ import annotations

import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    CONFIG_MISMATCH,
    DUPLICATE,
    PRICE_LIMIT_EXCEEDED,
    REJECTED,
    EventReplayer,
    SnapshotError,
    VWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli


LIMITS = {"price_limits": {"AAA": {"lower": 95, "upper": 105}}}


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


def iceberg(event_id, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, "AAA", sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def replace(event_id, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": "AAA", "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def twap_start(event_id, sequence, plan_id, price, order_type="LIMIT",
               side="BUY", total_quantity=4, slice_count=2):
    event = {
        "event_id": event_id, "symbol": "AAA", "sequence": sequence,
        "type": "TWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def twap_slice(event_id, sequence, plan_id):
    return {"event_id": event_id, "symbol": "AAA", "sequence": sequence,
            "type": "TWAP_SLICE", "plan_id": plan_id}


def vwap_start(event_id, sequence, plan_id, price, order_type="LIMIT"):
    event = {
        "event_id": event_id, "symbol": "AAA", "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": "BUY",
        "total_quantity": 6, "volume_weights": [1, 2, 3],
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def code(result):
    return result.get("result") or result.get("rejection_code")


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"price_limits": []},
    {"price_limits": None},
    {"price_limits": {"AAA": []}},
    {"price_limits": {"AAA": {"lower": 1}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 5, "mid": 3}}},
    {"price_limits": {"AAA": {"lower": 0, "upper": 5}}},
    {"price_limits": {"AAA": {"lower": -1, "upper": 5}}},
    {"price_limits": {"AAA": {"lower": 6, "upper": 5}}},
    {"price_limits": {"AAA": {"lower": True, "upper": 5}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": False}}},
    {"price_limits": {"AAA": {"lower": "1", "upper": 5}}},
    {"price_limits": {"AAA": {"lower": 1.0, "upper": 5}}},
    {"price_limits": {"": {"lower": 1, "upper": 5}}},
    {"price_limits": {7: {"lower": 1, "upper": 5}}},
])
def test_invalid_price_limits_raise_value_error(bad):
    with pytest.raises(ValueError):
        replay_events([], config=bad)
    with pytest.raises(ValueError):
        EventReplayer(config=bad)


@pytest.mark.parametrize("good", [
    None,
    {},
    {"price_limits": {}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 1}}},
    {"price_limits": {"AAA": {"lower": 1, "upper": 5},
                      "BBB": {"lower": 100, "upper": 200}}},
])
def test_absent_empty_or_valid_limits_are_accepted(good):
    # ZZZ is bounded by none of the configurations above.
    out = replay_events([add("e1", "ZZZ", 1, "s1", "SELL", "LIMIT", 1, 10000)],
                        config=good)
    assert out["results"][0]["status"] == ACCEPTED


def test_invalid_config_raises_before_events_are_processed():
    # The malformed config must fail even though the stream is empty or would
    # itself be rejected.
    with pytest.raises(ValueError):
        replay_events([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)],
                      config={"price_limits": {"AAA": {"lower": 2, "upper": 1}}})


# ---------------------------------------------------------------------------
# ADD enforcement
# ---------------------------------------------------------------------------


def test_in_bounds_add_is_accepted_at_each_boundary():
    out = replay_events([
        add("lo", "AAA", 1, "l1", "SELL", "LIMIT", 1, 95),
        add("hi", "AAA", 2, "h1", "SELL", "LIMIT", 1, 105),
        add("mid", "AAA", 3, "m1", "SELL", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert [code(r) for r in out["results"]] == ["RESTING"] * 3


def test_add_out_of_bounds_is_committed_price_limit_rejection():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 94),
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 3, 106),
    ], config=LIMITS)
    for index in (1, 2):
        result = out["results"][index]
        assert result["status"] == REJECTED
        assert result["rejection_code"] == PRICE_LIMIT_EXCEEDED
        assert result["trades"] == []
        assert result["book_changes"] == {"bids": [], "asks": []}
        # The untouched pre-event book is echoed.
        assert result["asks"] == [{"price": 100, "quantity": 3}]


def test_price_rejection_consumes_event_id_and_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 1),   # rejected
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert out["results"][2]["status"] == ACCEPTED
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]
    # Reusing e2 with different content is an id conflict; identical content
    # is an idempotent duplicate: the id really was occupied.
    conflict = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 1),
        add("e2", "AAA", 2, "x", "BUY", "LIMIT", 1, 2),
    ], config=LIMITS)
    assert conflict["results"][2]["rejection_code"] == "EVENT_ID_CONFLICT"


def test_price_rejection_spends_no_trade_id():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        # Would cross at 100 but for its illegal price: nothing trades.
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 5, 1),
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert out["results"][1]["trades"] == []
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]
    assert out["results"][2]["asks"] == []


def test_iceberg_add_is_checked_against_the_limits():
    rejected = replay_events([iceberg("i0", 1, "i1", "SELL", 10, 200, 3)],
                             config=LIMITS)["results"][0]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    accepted = replay_events([iceberg("i0", 1, "i1", "SELL", 10, 100, 3)],
                             config=LIMITS)["results"][0]
    assert code(accepted) == "RESTING"


def test_market_add_is_exempt_from_limits():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "m1", "BUY", "MARKET", 3),
        add("e3", "AAA", 3, "m2", "SELL", "MARKET", 1, time_in_force="IOC"),
    ], config=LIMITS)
    assert code(out["results"][1]) == "FILLED"
    assert code(out["results"][2]) == "UNFILLED_CANCELLED"


def test_unconfigured_symbol_keeps_baseline_behaviour():
    out = replay_events([
        add("e1", "BBB", 1, "s1", "SELL", "LIMIT", 1, 10_000),
        add("e2", "AAA", 1, "s2", "SELL", "LIMIT", 1, 106),
    ], config=LIMITS)
    assert code(out["results"][0]) == "RESTING"
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_absent_price_limits_leaves_baseline_unchanged():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 1_000_000),
    ])
    assert code(out["results"][0]) == "RESTING"


# ---------------------------------------------------------------------------
# REPLACE enforcement
# ---------------------------------------------------------------------------


def test_replace_out_of_bounds_is_rejected_and_keeps_order_and_priority():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "o2", "BUY", "LIMIT", 2, 100),
        replace("e3", 3, "o1", 2, 106),
        add("e4", "AAA", 4, "s1", "SELL", "LIMIT", 2, 100),
    ], config=LIMITS)
    rejected = out["results"][2]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    # o1 kept its price, quantity and head-of-queue priority.
    assert [t["maker_order_id"] for t in out["results"][3]["trades"]] == ["o1"]

    # The same replace is honoured once it stays inside the band.
    follow = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        replace("e2", 2, "o1", 2, 105),
        add("e3", "AAA", 3, "s1", "SELL", "LIMIT", 2, 105),
    ], config=LIMITS)
    assert code(follow["results"][1]) == "REPLACED"
    assert code(follow["results"][2]) == "FILLED"


def test_replace_below_lower_bound_is_rejected():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        replace("e2", 2, "o1", 2, 94),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_iceberg_replace_is_checked():
    out = replay_events([
        iceberg("e1", 1, "i1", "SELL", 10, 100, 3),
        replace("e2", 2, "i1", 8, 106, display_quantity=3),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 2, 100),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    # The original iceberg slice still trades at 100 and keeps replenishing.
    assert [t["maker_order_id"] for t in out["results"][2]["trades"]] == ["i1"]


# ---------------------------------------------------------------------------
# Precedence of pre-existing rejection codes
# ---------------------------------------------------------------------------


def test_duplicate_order_id_takes_precedence_over_price_breach():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "o1", "BUY", "LIMIT", 1, 1_000),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_unknown_order_replace_takes_precedence_over_price_breach():
    out = replay_events([
        replace("e1", 1, "ghost", 1, 1_000),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == "UNKNOWN_ORDER"


def test_duplicate_execution_plan_takes_precedence_over_price_breach():
    out = replay_events([
        twap_start("e1", 1, "p1", 100),
        twap_start("e2", 2, "p1", 1_000),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_EXECUTION_PLAN"


def test_derived_id_clash_takes_precedence_over_price_breach():
    out = replay_events([
        add("e0", "AAA", 1, "p2#1", "SELL", "LIMIT", 1, 100),
        twap_start("e2", 2, "p2", 1_000),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_duplicate_event_id_and_sequence_checks_precede_price_breach():
    well_formed = add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)
    out = replay_events([
        well_formed,
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),   # exact duplicate
        add("e3", "AAA", 9, "x", "BUY", "LIMIT", 1, 1),       # sequence gap
    ], config=LIMITS)
    assert out["results"][1]["status"] == DUPLICATE
    assert out["results"][2]["rejection_code"] == "SEQUENCE_GAP"


# ---------------------------------------------------------------------------
# TWAP / VWAP plan enforcement
# ---------------------------------------------------------------------------


def test_limit_plan_out_of_bounds_creates_no_plan_and_reserves_no_ids():
    out = replay_events([
        twap_start("e1", 1, "p1", 90),
        add("e2", "AAA", 2, "p1#1", "BUY", "LIMIT", 1, 100),
        twap_slice("e3", 3, "p1"),
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert "execution_plan" not in out["results"][0]
    # No reservation was made: p1#1 is a free order id, and the plan is unknown.
    assert code(out["results"][1]) == "RESTING"
    assert out["results"][2]["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"


def test_limit_plan_id_can_be_restarted_after_a_price_rejection():
    out = replay_events([
        twap_start("e1", 1, "p1", 90),
        twap_start("e2", 2, "p1", 100),
        twap_slice("e3", 3, "p1"),
    ], config=LIMITS)
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][2]["execution_plan"]["slice_number"] == 1


def test_limit_plan_at_boundary_is_accepted_and_slices_inside_band():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 95),
        twap_start("e2", 2, "p1", 95),
        twap_slice("e3", 3, "p1"),
    ], config=LIMITS)
    assert out["results"][1]["status"] == ACCEPTED
    assert code(out["results"][2]) == "FILLED"


def test_market_plan_is_exempt_from_limits():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        twap_start("e2", 2, "p1", None, order_type="MARKET", side="SELL"),
        twap_slice("e3", 3, "p1"),
    ], config=LIMITS)
    assert out["results"][1]["status"] == ACCEPTED
    assert code(out["results"][2]) == "UNFILLED_CANCELLED"


def test_vwap_limit_plan_is_checked():
    rejected = replay_events([vwap_start("e1", 1, "v1", 500)], config=LIMITS)
    assert rejected["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    accepted = replay_events([vwap_start("e1", 1, "v1", 105)], config=LIMITS)
    assert accepted["results"][0]["status"] == ACCEPTED


def test_vwap_market_plan_is_exempt():
    out = replay_events([vwap_start("e1", 1, "v1", None, order_type="MARKET")],
                        config=LIMITS)
    assert out["results"][0]["status"] == ACCEPTED


def test_rejected_plan_command_consumes_sequence_but_moves_nothing():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        twap_start("e2", 2, "p1", 90),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]


# ---------------------------------------------------------------------------
# Deterministic serialization, config digest and snapshots
# ---------------------------------------------------------------------------


def test_price_limits_are_part_of_session_config_and_digests():
    out = replay_events([], config=LIMITS)
    snapshot = out["snapshot"]
    assert snapshot["config"]["price_limits"] == LIMITS["price_limits"]
    assert isinstance(snapshot["config_digest"], str)
    assert len(snapshot["config_digest"]) == 64
    # The digest must actually depend on the bounds.
    other = replay_events([], config={"price_limits": {
        "AAA": {"lower": 95, "upper": 106}}})["snapshot"]
    assert snapshot["config_digest"] != other["config_digest"]


def test_same_config_resumes_byte_identically_through_price_rejections():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 1),      # price rejection
        twap_start("e3", 3, "p1", 90),                        # price rejection
    ]
    part2 = [
        add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 3, 100),
        twap_start("e5", 5, "p1", 100),
        twap_slice("e6", 6, "p1"),
        replace("e7", 7, "s1", 2, 105),
    ]
    one_shot = replay_events(part1 + part2, config=LIMITS)
    snapshot = replay_events(part1, config=LIMITS)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot, config=LIMITS)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_rejected_baseline_event_roundtrips_in_snapshot_journal():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 1),
    ]
    snapshot = replay_events(part1, config=LIMITS)["snapshot"]
    replayer = restore_replayer(snapshot, config=LIMITS)
    assert canonical_json(export_snapshot(replayer)) == canonical_json(snapshot)
    # The rejected id is still known globally after restore.
    duplicate = replay_events(
        [add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 1)],
        snapshot=export_snapshot(replayer), config=LIMITS,
    )
    assert duplicate["results"][0]["status"] == DUPLICATE


def test_different_limits_on_restore_raise_config_mismatch():
    snapshot = replay_events([], config=LIMITS)["snapshot"]
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(snapshot, config={"price_limits": {
            "AAA": {"lower": 90, "upper": 110}}})
    assert exc.value.code == CONFIG_MISMATCH


def test_snapshot_without_price_limits_differs_from_limited_config():
    baseline = replay_events([])["snapshot"]
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(baseline, config=LIMITS)
    assert exc.value.code == CONFIG_MISMATCH
    # The reverse direction is a mismatch too, not a corruption.
    limited = replay_events([], config=LIMITS)["snapshot"]
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(limited)
    assert exc.value.code == CONFIG_MISMATCH


def test_malformed_caller_config_raises_value_error_before_snapshot_checks():
    snapshot = replay_events([], config=LIMITS)["snapshot"]
    with pytest.raises(ValueError):
        restore_replayer(snapshot, config={"price_limits": 7})
    with pytest.raises(ValueError):
        replay_events([], snapshot=snapshot,
                      config={"price_limits": {"AAA": {"lower": 2, "upper": 1}}})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _run_cli(request_obj):
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    body = stdout.buffer.getvalue().decode("utf-8")
    return code, json.loads(body) if body else None, stderr.getvalue()


def test_cli_invalid_price_limits_return_invalid_request_exit_2():
    code, doc, err = _run_cli({
        "events": [],
        "config": {"price_limits": {"AAA": {"lower": 9, "upper": 1}}},
    })
    assert code == 2
    assert err == ""
    assert doc["error"]["code"] == "INVALID_REQUEST"


def test_cli_price_breach_is_a_normal_rejected_result():
    code, doc, _ = _run_cli({
        "events": [
            {"event_id": "e1", "symbol": "AAA", "sequence": 1, "type": "ADD",
             "order_id": "o1", "side": "BUY", "order_type": "LIMIT",
             "quantity": 1, "price": 50},
        ],
        "config": LIMITS,
    })
    assert code == 0
    result = doc["results"][0]
    assert result["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert result["trades"] == []
