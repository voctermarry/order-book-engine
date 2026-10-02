"""Tests for the intraday PRICE_LIMIT_UPDATE adjustment event.

A PRICE_LIMIT_UPDATE replaces one security's active price-limit interval
(seeded from the static ``price_limits`` config block) for all subsequently
submitted limit prices: LIMIT/ICEBERG ADDs, REPLACEs, LIMIT TWAP/VWAP starts
and — re-checked at release time — every slice of an already started LIMIT
plan. MARKET orders and plans are exempt. The command itself is replay-only,
never moves the book and is part of the snapshot state; snapshots written
before the feature existed restore the interval from the configuration.
"""

from __future__ import annotations

import copy
import hashlib
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    INVALID_EVENT,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    SNAPSHOT_CORRUPT,
    SnapshotError,
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


def replace(event_id, sequence, order_id, quantity, price, symbol="AAA"):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "REPLACE", "order_id": order_id,
            "quantity": quantity, "price": price}


def update(event_id, sequence, lower, upper, symbol="AAA", **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": PRICE_LIMIT_UPDATE,
             "lower_price": lower, "upper_price": upper}
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


def twap_report(event_id, sequence, plan_id):
    return {"event_id": event_id, "symbol": "AAA", "sequence": sequence,
            "type": "TWAP_REPORT", "plan_id": plan_id}


def vwap_start(event_id, sequence, plan_id, price, order_type="LIMIT"):
    event = {
        "event_id": event_id, "symbol": "AAA", "sequence": sequence,
        "type": "VWAP_START", "plan_id": plan_id, "side": "BUY",
        "total_quantity": 6, "volume_weights": [1, 2, 3],
        "order_type": order_type, "benchmark_price": 100,
    }
    if order_type == "LIMIT":
        event["price"] = price
    return event


def vwap_slice(event_id, sequence, plan_id):
    return {"event_id": event_id, "symbol": "AAA", "sequence": sequence,
            "type": "VWAP_SLICE", "plan_id": plan_id}


def code(result):
    return result.get("result") or result.get("rejection_code")


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutation", [
    lambda e: e.pop("lower_price"),
    lambda e: e.pop("upper_price"),
    lambda e: e.update(lower_price=True),
    lambda e: e.update(upper_price=False),
    lambda e: e.update(lower_price=0),
    lambda e: e.update(upper_price=-3),
    lambda e: e.update(lower_price="95"),
    lambda e: e.update(upper_price=105.0),
    lambda e: e.update(lower_price=106, upper_price=105),   # inverted
    lambda e: e.update(mid_price=100),                      # extra field
])
def test_malformed_update_is_invalid_event_and_consumes_nothing(mutation):
    bad = update("e2", 2, 95, 105)
    mutation(bad)
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        bad,
        # The sequence stayed at 2 and the static band still applies.
        add("e3", "AAA", 2, "b1", "BUY", "LIMIT", 1, 94),
    ], config=LIMITS)
    rejected = out["results"][1]
    assert rejected["status"] == REJECTED
    assert rejected["rejection_code"] == INVALID_EVENT
    assert rejected["event_id"] == "e2"
    assert rejected["sequence"] == 2
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    # The invalid event occupied nothing: e3 at sequence 2 is dispatched and
    # still checked against the *static* band (94 < 95).
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_invalid_update_leaves_event_id_free_for_reuse():
    bad = update("e2", 1, 0, 105)
    out = replay_events([
        bad,
        update("e2", 1, 90, 110),   # same id and sequence, now well formed
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][1]["status"] == ACCEPTED
    assert out["results"][1]["result"] == PRICE_LIMIT_UPDATED


def test_unknown_type_and_envelope_rules_still_apply():
    out = replay_events([
        {"event_id": "e1", "symbol": "AAA", "sequence": 1,
         "type": "PRICE_LIMIT_UPDATE", "lower_price": 1},        # missing field
        {"event_id": "e2", "symbol": "AAA", "sequence": 1,
         "type": "PRICE_LIMIT_UPDATE", "lower_price": 90,
         "upper_price": 110, "price": 100},                      # stray field
    ], config=LIMITS)
    assert all(r["rejection_code"] == INVALID_EVENT for r in out["results"])


# ---------------------------------------------------------------------------
# Acceptance shape
# ---------------------------------------------------------------------------


def test_accepted_update_replaces_interval_and_echoes_it():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        update("e2", 2, 98, 102),
    ], config=LIMITS)
    result = out["results"][1]
    assert list(result) == [
        "event_id", "symbol", "sequence", "status", "result", "trades",
        "book_changes", "bids", "asks", "active_price_limits",
    ]
    assert result["status"] == ACCEPTED
    assert result["result"] == PRICE_LIMIT_UPDATED
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": []}
    # The book is echoed untouched.
    assert result["asks"] == [{"price": 100, "quantity": 2}]
    assert result["bids"] == []
    assert result["active_price_limits"] == {"lower_price": 98, "upper_price": 102}


def test_update_without_static_config_creates_the_interval():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 10_000),
        update("e2", 2, 90, 110),
        add("e3", "AAA", 3, "s2", "SELL", "LIMIT", 1, 10_000),
        add("e4", "AAA", 4, "s3", "SELL", "LIMIT", 1, 100),
    ])
    assert code(out["results"][0]) == "RESTING"
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(out["results"][3]) == "RESTING"


def test_same_bounds_update_still_succeeds():
    out = replay_events([
        update("e1", 1, 95, 105),
        update("e2", 2, 95, 105),
    ], config=LIMITS)
    assert [r["result"] for r in out["results"]] == [
        PRICE_LIMIT_UPDATED, PRICE_LIMIT_UPDATED,
    ]


def test_update_is_per_security():
    out = replay_events([
        update("e1", 1, 90, 110, symbol="AAA"),
        add("e2", "BBB", 1, "s1", "SELL", "LIMIT", 1, 10_000),
        add("e3", "AAA", 2, "s2", "SELL", "LIMIT", 1, 10_000),
    ])
    assert code(out["results"][1]) == "RESTING"      # BBB unlimited
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_update_spends_no_trade_id():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
        update("e2", 2, 90, 110),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
    ], config=LIMITS)
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]


# ---------------------------------------------------------------------------
# Enforcement of the adjusted interval
# ---------------------------------------------------------------------------


def test_narrowed_band_gates_new_adds_but_not_resting_orders():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 103),
        add("e2", "AAA", 2, "o2", "BUY", "LIMIT", 1, 103),
        update("e3", 3, 98, 102),
        add("e4", "AAA", 4, "b1", "BUY", "LIMIT", 1, 104),    # now outside
        add("e5", "AAA", 5, "s1", "SELL", "LIMIT", 1, 98),    # taker, in band
    ], config=LIMITS)
    assert out["results"][3]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    trades = out["results"][4]["trades"]
    # The resting 103 bids predate the narrowing: they keep their queue
    # priority and the head still trades as maker at its own price — the
    # limit is on submitted prices, not on trade prices.
    assert [(t["maker_order_id"], t["price"]) for t in trades] == [("o1", 103)]
    # o2 is still resting at the out-of-band level.
    assert out["results"][4]["bids"] == [{"price": 103, "quantity": 1}]


def test_widened_band_admits_previously_illegal_prices():
    out = replay_events([
        add("e1", "AAA", 1, "b1", "BUY", "LIMIT", 1, 90),     # static breach
        update("e2", 2, 85, 105),
        add("e3", "AAA", 3, "b2", "BUY", "LIMIT", 1, 90),     # now legal
    ], config=LIMITS)
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert code(out["results"][2]) == "RESTING"


def test_replace_is_checked_against_the_adjusted_band():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100),
        add("e2", "AAA", 2, "o2", "BUY", "LIMIT", 2, 100),
        update("e3", 3, 98, 102),
        replace("e4", 4, "o1", 2, 104),                       # outside new band
        add("e5", "AAA", 5, "s1", "SELL", "LIMIT", 2, 100),
    ], config=LIMITS)
    rejected = out["results"][3]
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    # o1 kept its price, quantity and head-of-queue priority.
    assert [t["maker_order_id"] for t in out["results"][4]["trades"]] == ["o1"]


def test_market_add_stays_exempt_after_update():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),
        update("e2", 2, 99, 101),
        add("e3", "AAA", 3, "m1", "BUY", "MARKET", 3),
    ], config=LIMITS)
    assert code(out["results"][2]) == "FILLED"


def test_plan_start_uses_the_adjusted_band():
    out = replay_events([
        update("e1", 1, 90, 98),
        twap_start("e2", 2, "p1", 100),                       # outside new band
        vwap_start("e3", 3, "v1", 95),                        # inside
        twap_start("e4", 4, "p2", None, order_type="MARKET"), # exempt
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][2]["status"] == ACCEPTED
    assert out["results"][3]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Slice-time re-check of started LIMIT plans
# ---------------------------------------------------------------------------


def test_limit_plan_slices_are_rechecked_against_the_current_band():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100),
        update("e3", 3, 103, 110),                            # 100 now outside
        twap_slice("e4", 4, "p1"),                            # rejected
        twap_report("e5", 5, "p1"),
        update("e6", 6, 95, 105),                             # band restored
        twap_slice("e7", 7, "p1"),                            # now releases
    ], config=LIMITS)
    rejected = out["results"][3]
    assert rejected["status"] == REJECTED
    assert rejected["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert rejected["trades"] == []
    assert rejected["book_changes"] == {"bids": [], "asks": []}
    assert "execution_plan" not in rejected
    # The rejection consumed its id and sequence but advanced nothing: the
    # plan still has every slice left and no cumulative counters moved.
    plan = out["results"][4]["execution_plan"]
    assert plan["status"] == "ACTIVE"
    assert plan["released_quantity"] == 0
    assert plan["filled_quantity"] == 0
    assert plan["remaining_slices"] == 2
    # The slice released after re-widening is still slice number 1 and the
    # first trade id was not spent by the rejected attempt.
    released = out["results"][6]
    assert released["execution_plan"]["slice_number"] == 1
    assert released["execution_plan"]["child_order_id"] == "p1#1"
    assert [t["trade_id"] for t in released["trades"]] == [1]


def test_rejected_slice_keeps_derived_ids_reserved():
    out = replay_events([
        twap_start("e1", 1, "p1", 100),
        update("e2", 2, 103, 110),
        twap_slice("e3", 3, "p1"),                            # rejected
        add("e4", "AAA", 4, "p1#2", "BUY", "LIMIT", 1, 105),  # still reserved
    ], config=LIMITS)
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert out["results"][3]["rejection_code"] == "DUPLICATE_ORDER_ID"


def test_vwap_limit_plan_slice_is_rechecked():
    out = replay_events([
        vwap_start("e1", 1, "v1", 100),
        update("e2", 2, 101, 110),
        vwap_slice("e3", 3, "v1"),
    ], config=LIMITS)
    assert out["results"][2]["rejection_code"] == PRICE_LIMIT_EXCEEDED


def test_market_plan_slices_ignore_the_band():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        twap_start("e2", 2, "p1", None, order_type="MARKET", side="SELL"),
        update("e3", 3, 1, 2),                                # absurdly narrow
        twap_slice("e4", 4, "p1"),
    ], config=LIMITS)
    assert out["results"][3]["status"] == ACCEPTED
    assert code(out["results"][3]) == "UNFILLED_CANCELLED"


def test_slice_recheck_uses_interval_active_at_release_time():
    # The band narrows and re-widens between slices; each release is judged
    # against the interval in force at that moment.
    out = replay_events([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e1", 2, "p1", 100),
        twap_slice("e2", 3, "p1"),                            # in static band
        update("e3", 4, 103, 110),
        twap_slice("e4", 5, "p1"),                            # rejected
        update("e5", 6, 95, 105),
        twap_slice("e6", 7, "p1"),                            # accepted
    ], config=LIMITS)
    assert out["results"][2]["execution_plan"]["slice_number"] == 1
    assert out["results"][4]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    final = out["results"][6]["execution_plan"]
    assert final["slice_number"] == 2
    assert final["status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# Idempotency and ordering
# ---------------------------------------------------------------------------


def test_duplicate_update_is_idempotent():
    out = replay_events([
        update("e1", 1, 90, 110),
        update("e1", 1, 90, 110),                             # exact retry
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 108),
    ], config=LIMITS)
    assert out["results"][1]["status"] == DUPLICATE
    assert "active_price_limits" not in out["results"][1]
    # The first update really committed: 108 is inside the widened band.
    assert code(out["results"][2]) == "RESTING"


def test_conflicting_update_id_is_a_conflict():
    out = replay_events([
        update("e1", 1, 90, 110),
        update("e1", 2, 80, 120),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "EVENT_ID_CONFLICT"


def test_update_sequence_errors_use_existing_codes():
    out = replay_events([
        update("e1", 1, 90, 110),
        update("e2", 3, 80, 120),                             # gap
        update("e3", 1, 80, 120),                             # stale
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "SEQUENCE_GAP"
    assert out["results"][1]["expected_sequence"] == 2
    assert out["results"][2]["rejection_code"] == "OUT_OF_ORDER"


def test_update_id_clashing_a_derived_child_id_is_duplicate_event_id():
    out = replay_events([
        twap_start("e1", 1, "p1", 100),
        update("p1#1", 2, 90, 110),
    ], config=LIMITS)
    assert out["results"][1]["rejection_code"] == "DUPLICATE_EVENT_ID"


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


def _symbol_state(snapshot, symbol):
    for entry in snapshot["content"]["symbols"]:
        if entry["symbol"] == symbol:
            return entry["state"]
    raise KeyError(symbol)


def test_active_interval_is_part_of_the_snapshot():
    snapshot = replay_events([
        update("e1", 1, 90, 110),
    ], config=LIMITS)["snapshot"]
    assert _symbol_state(snapshot, "AAA")["price_limits"] == {
        "lower": 90, "upper": 110,
    }
    # A security that never saw an update carries its configured interval.
    plain = replay_events(
        [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)], config=LIMITS,
    )["snapshot"]
    assert _symbol_state(plain, "AAA")["price_limits"] == {
        "lower": 95, "upper": 105,
    }


def test_resumed_run_with_updates_is_byte_identical_to_one_shot():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        twap_start("e2", 2, "p1", 100),
        update("e3", 3, 103, 110),
        twap_slice("e4", 4, "p1"),                            # rejected
    ]
    part2 = [
        update("e5", 5, 95, 105),
        twap_slice("e6", 6, "p1"),
        add("e7", "AAA", 7, "b1", "BUY", "LIMIT", 1, 100),
        update("e8", 8, 1, 200, symbol="BBB"),
        add("e9", "BBB", 1, "s2", "SELL", "LIMIT", 1, 150),
    ]
    one_shot = replay_events(part1 + part2, config=LIMITS)
    snapshot = replay_events(part1, config=LIMITS)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot, config=LIMITS)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_update_event_roundtrips_as_replay_only_id():
    snapshot = replay_events([
        update("e1", 1, 90, 110),
    ], config=LIMITS)["snapshot"]
    replayer = restore_replayer(snapshot, config=LIMITS)
    assert canonical_json(export_snapshot(replayer)) == canonical_json(snapshot)
    # The id is still known globally after restore.
    duplicate = replay_events(
        [update("e1", 1, 90, 110)],
        snapshot=export_snapshot(replayer), config=LIMITS,
    )
    assert duplicate["results"][0]["status"] == DUPLICATE


def test_tampered_snapshot_interval_breaks_the_digest():
    snapshot = replay_events([
        update("e1", 1, 90, 110),
    ], config=LIMITS)["snapshot"]
    tampered = copy.deepcopy(snapshot)
    _symbol_state(tampered, "AAA")["price_limits"] = {"lower": 1, "upper": 2}
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, config=LIMITS)
    assert exc.value.code == SNAPSHOT_CORRUPT


def _to_legacy_snapshot(snapshot):
    """Rewrite a snapshot to the pre-PRICE_LIMIT_UPDATE symbol-state shape."""
    legacy = copy.deepcopy(snapshot)
    for entry in legacy["content"]["symbols"]:
        entry["state"].pop("price_limits")
    legacy.pop("content_digest")
    legacy["content_digest"] = hashlib.sha256(
        canonical_json(legacy)
    ).hexdigest()
    return legacy


def test_legacy_snapshot_seeds_interval_from_config():
    part1 = [add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100)]
    legacy = _to_legacy_snapshot(replay_events(part1, config=LIMITS)["snapshot"])
    out = replay_events(
        [add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 94)],
        snapshot=legacy, config=LIMITS,
    )
    # The configured static band applies again after restoration.
    assert out["results"][0]["rejection_code"] == PRICE_LIMIT_EXCEEDED
    # Re-exporting the restored session matches the continuous run's
    # snapshot byte for byte.
    continuous = replay_events(part1, config=LIMITS)
    assert canonical_json(out["snapshot"]) == canonical_json(
        replay_events(part1 + [
            add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 94),
        ], config=LIMITS)["snapshot"]
    )
    assert canonical_json(export_snapshot(restore_replayer(legacy, config=LIMITS))) == (
        canonical_json(continuous["snapshot"])
    )


def test_legacy_snapshot_without_config_limits_stays_unlimited():
    legacy = _to_legacy_snapshot(replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100),
    ])["snapshot"])
    out = replay_events(
        [add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 1, 10_000)],
        snapshot=legacy,
    )
    assert code(out["results"][0]) == "RESTING"


def test_malformed_snapshot_interval_is_corrupt():
    snapshot = replay_events([
        update("e1", 1, 90, 110),
    ], config=LIMITS)["snapshot"]

    def with_interval(value):
        doc = copy.deepcopy(snapshot)
        state = _symbol_state(doc, "AAA")
        if value is ...:
            state["price_limits"] = {"lower": 90}              # missing bound
        else:
            state["price_limits"] = value
        doc.pop("content_digest")
        doc["content_digest"] = hashlib.sha256(canonical_json(doc)).hexdigest()
        return doc

    for bad in ({"lower": 90}, {"lower": 90, "upper": 110, "mid": 1},
                {"lower": True, "upper": 110}, {"lower": 0, "upper": 110},
                {"lower": 120, "upper": 110}, "unlimited", 7):
        with pytest.raises(SnapshotError) as exc:
            restore_replayer(with_interval(bad), config=LIMITS)
        assert exc.value.code == SNAPSHOT_CORRUPT


# ---------------------------------------------------------------------------
# Unaffected surfaces
# ---------------------------------------------------------------------------


def test_other_results_never_carry_active_price_limits():
    out = replay_events([
        update("e1", 1, 90, 110),
        add("e2", "AAA", 2, "s1", "SELL", "LIMIT", 1, 100),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
        add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 1, 1),      # rejected
    ], config=LIMITS)
    for result in out["results"][1:]:
        assert "active_price_limits" not in result


def test_single_security_json_lines_entry_rejects_the_type():
    from order_book_engine import replay as line_replay

    stdin = io.TextIOWrapper(io.BytesIO(
        b'{"event_id": "e1", "type": "PRICE_LIMIT_UPDATE", '
        b'"lower_price": 90, "upper_price": 110}\n'
    ))
    stdout = io.TextIOWrapper(io.BytesIO())
    assert line_replay.replay(stdin, stdout, io.StringIO()) == 0
    stdout.flush()
    line = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    assert line["result"] == "REJECTED"
    assert line["reason"] == "INVALID_SCHEMA"


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


def test_cli_update_flow():
    exit_code, doc, _ = _run_cli({
        "events": [
            {"event_id": "e1", "symbol": "AAA", "sequence": 1,
             "type": "PRICE_LIMIT_UPDATE", "lower_price": 90, "upper_price": 110},
            {"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": "ADD",
             "order_id": "s1", "side": "SELL", "order_type": "LIMIT",
             "quantity": 1, "price": 108},
        ],
        "config": LIMITS,
    })
    assert exit_code == 0
    first, second = doc["results"]
    assert first["result"] == PRICE_LIMIT_UPDATED
    assert first["active_price_limits"] == {"lower_price": 90, "upper_price": 110}
    assert second["status"] == ACCEPTED
