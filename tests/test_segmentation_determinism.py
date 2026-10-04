"""Segmentation determinism regression tests for the multi-symbol replay layer.

The contract under test is *any legal segmentation of an event stream yields
exactly the same outcome as the uninterrupted run*:

* :func:`replay_events` and the stateful :class:`EventReplayer` commit every
  structurally valid event individually (accepted or business-rejected);
* :func:`export_snapshot` / :func:`restore_replayer` resume at every
  exportable boundary;
* the ``order-book-engine events`` command line entry point produces the same
  canonical JSON document, exit code and (empty) standard error as the Python
  entry point.

The fixed stream interleaves two securities and keeps every state kind a
snapshot has to preserve live at several cut points:

* a resting plain limit order;
* an iceberg order still holding hidden reserve, with a replenished slice
  queued at its price-level tail and a partially consumed slice;
* an open TWAP, an open VWAP and an open POV plan, each partially executed;
* an active price-limit interval already replaced by PRICE_LIMIT_UPDATE;

and it interleaves read-only reports (EXECUTION_REPORT, IMPACT_REPORT,
PORTFOLIO_REPORT, SESSION_RECONCILIATION, PLAN_TCA_REPORT,
BOOK_RECONSTRUCTION_REPORT) and structurally valid business rejections, so
sequences that occupy an id/sequence while moving no trading state are
recovery boundaries too.

Every comparison is a :func:`canonical_json` byte comparison of full response
documents, snapshots and report analyses; trades, queues and diff arrays are
compared in production order and never sorted or normalized first.
"""

from __future__ import annotations

import copy
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
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

REPO_ROOT = Path(__file__).resolve().parents[1]

SYMBOL_A = "AAA"
SYMBOL_B = "BBB"
CONFIG = {"price_limits": {
    SYMBOL_A: {"lower": 90, "upper": 110},
    SYMBOL_B: {"lower": 80, "upper": 120},
}}


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def add(event_id, symbol, sequence, order_id, side, order_type, quantity, price=None, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "ADD", "order_id": order_id, "side": side,
        "order_type": order_type, "quantity": quantity,
    }
    if price is not None:
        event["price"] = price
    event.update(extra)
    return event


def limit(event_id, symbol, sequence, order_id, side, quantity, price, **extra):
    return add(event_id, symbol, sequence, order_id, side, "LIMIT", quantity, price, **extra)


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def limit_update(event_id, symbol, sequence, lower, upper):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "PRICE_LIMIT_UPDATE", "lower_price": lower, "upper_price": upper}


def twap_start(event_id, symbol, sequence, plan_id, side, total, slices, price,
               benchmark=100, account_id=None):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "TWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total, "slice_count": slices,
        "order_type": "LIMIT", "benchmark_price": benchmark, "price": price,
    }
    if account_id is not None:
        event["account_id"] = account_id
    return event


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "TWAP_SLICE", "plan_id": plan_id}


def vwap_start(event_id, symbol, sequence, plan_id, side, total, weights, price,
               benchmark=100, account_id=None):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "VWAP_START", "plan_id": plan_id, "side": side,
        "total_quantity": total, "volume_weights": weights,
        "order_type": "LIMIT", "benchmark_price": benchmark, "price": price,
    }
    if account_id is not None:
        event["account_id"] = account_id
    return event


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "VWAP_SLICE", "plan_id": plan_id}


def pov_start(event_id, symbol, sequence, plan_id, side, total, bps, price,
              benchmark=100, account_id=None):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": "POV_START", "plan_id": plan_id, "side": side,
        "total_quantity": total, "participation_bps": bps,
        "order_type": "LIMIT", "benchmark_price": benchmark, "price": price,
    }
    if account_id is not None:
        event["account_id"] = account_id
    return event


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "POV_VOLUME", "plan_id": plan_id,
            "market_volume_increment": increment}


def execution_report(event_id, symbol, sequence, order_id, benchmark=100):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "EXECUTION_REPORT", "order_id": order_id,
            "benchmark_price": benchmark}


def impact_report(event_id, symbol, sequence, side, quantity, benchmark=100):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "IMPACT_REPORT", "side": side, "quantity": quantity,
            "benchmark_price": benchmark}


def portfolio_report(event_id, symbol, sequence, account_id, mark_prices):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "PORTFOLIO_REPORT", "account_id": account_id,
            "mark_prices": mark_prices}


def session_reconciliation(event_id, symbol, sequence, expected_trades, expected_accounts):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "SESSION_RECONCILIATION", "expected_trades": expected_trades,
            "expected_accounts": expected_accounts}


def plan_tca_report(event_id, symbol, sequence, plan_id, mark_price):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "PLAN_TCA_REPORT", "plan_id": plan_id, "mark_price": mark_price}


def book_reconstruction(event_id, symbol, sequence, target):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "BOOK_RECONSTRUCTION_REPORT", "target_sequence": target}


# ---------------------------------------------------------------------------
# The fixed interleaved stream
# ---------------------------------------------------------------------------
#
# AAA static band 90..110, replaced at sequence 4 by 95..105:
#   1 a01 ICEBERG SELL ia 10 @100 peak 3          (queues first)
#   2 a02 LIMIT  SELL sa 4 @100                   (queues behind ia)
#   3 a03 MARKET BUY 4  -> ia 3 (slice exhausts,
#       replenishes to level tail behind sa), sa 1; queue [sa(3), ia(3)]
#   4 a04 PRICE_LIMIT_UPDATE 95..105
#   5 a05 TWAP_START TA BUY 6 / 3 slices of 2 @100 (account alpha)
#   6 a06 TWAP_SLICE TA#1 buy 2 -> sa 2           (sa 3 -> 1)
#   7 a07 VWAP_START VA SELL 4 weights [1,1,2] @102 (slices 1,1,2)
#   8 a08 VWAP_SLICE VA#1 sell 1 @102: no bid, IOC cancelled
#   9 a09 POV_START PA BUY total 6 @5000bps @100
#  10 a10 POV_VOLUME +4 (target 2) PA#1 buy 2 -> sa 1, ia 1
#       (ia visible slice 3 -> 2, partially consumed, no replenish)
#  11 r11 EXECUTION_REPORT(ia)                    (read-only)
#  12 a12 LIMIT SELL sa2 1 @103                   (rests)
#  13 b13 CANCEL ghost                            UNKNOWN_ORDER (committed)
#  14 a14 TWAP_SLICE TA#2 buy 2 -> ia visible 2 exactly exhausted,
#       replenishes slice 3 at level tail         (ia remaining 4)
#  15 a15 LIMIT SELL @120                         PRICE_LIMIT_EXCEEDED
#  16 q16 PLAN_TCA_REPORT(TA, 101)                (read-only)
#  17 a17 VWAP_SLICE VA#2 sell 1 @102: IOC cancelled (VA still open)
#  18 q18 BOOK_RECONSTRUCTION_REPORT target 14
#  19 a19 POV_VOLUME +4 (cum 8 -> target 4) PA#2 buy 2 -> ia 2
#       (ia visible 3 -> 1; PA stays ACTIVE: 4 of 6 released)
#  20 q20 BOOK_RECONSTRUCTION_REPORT target 6
#  21 a21 LIMIT BUY ba 3 @99                       (rests)
#  22 a22 VWAP_SLICE VA#3 sell 2 @102: IOC cancelled, VA COMPLETES
#  23 q23 SESSION_RECONCILIATION([], []) -> BREAKS_FOUND (read-only)
#  24 q24 IMPACT_REPORT SELL 2                    (read-only)
#  25 a25 CANCEL sa2
#
# BBB static band 80..120, replaced at sequence 4 by 85..95:
#   1 b01 LIMIT SELL sb 5 @90 (account beta)
#   2 b02 MARKET BUY 2 -> sb 2 (sb 5 -> 3)
#   3 b03 LIMIT SELL sb2 3 @91
#   4 b04 PRICE_LIMIT_UPDATE 85..95
#   5 q05 PORTFOLIO_REPORT(beta, {BBB: 90})       (read-only, accepted)
#   6 q06 PORTFOLIO_REPORT(ghost) -> UNKNOWN_ACCOUNT (committed reject)
#   7 b07 LIMIT BUY bb 2 @90 -> sb 2 (sb 3 -> 1, bb fully filled)
#   8 b08 EXECUTION_REPORT(sb2)                   (read-only)
#   9 q09 IMPACT_REPORT BUY 4                     (read-only)
#  10 b10 LIMIT SELL sb3 1 @88                    (rests inside new band)
#
# AAA trade ids: 1 ia 3, 2 sa 1 (a03); 3 sa 2 (a06); 4 sa 1, 5 ia 1 (a10);
#                6 ia 2 (a14); 7 ia 2 (a19).
# BBB trade ids: 1 sb 2 (b02); 2 sb 2 (b07).


def build_stream():
    return [
        # --- AAA: iceberg first, plain order behind it, band tightened -----
        iceberg("a01", SYMBOL_A, 1, "ia", "SELL", 10, 100, 3),
        limit("a02", SYMBOL_A, 2, "sa", "SELL", 4, 100),
        add("a03", SYMBOL_A, 3, "ma", "BUY", "MARKET", 4),
        limit_update("a04", SYMBOL_A, 4, 95, 105),

        # --- BBB interleaved: plain resting order, trades, band move -------
        limit("b01", SYMBOL_B, 1, "sb", "SELL", 5, 90, account_id="beta"),
        add("b02", SYMBOL_B, 2, "mb", "BUY", "MARKET", 2),

        # --- AAA: three plans, each later left partially executed/open -----
        twap_start("a05", SYMBOL_A, 5, "TA", "BUY", 6, 3, 100, account_id="alpha"),
        twap_slice("a06", SYMBOL_A, 6, "TA"),
        vwap_start("a07", SYMBOL_A, 7, "VA", "SELL", 4, [1, 1, 2], 102),
        vwap_slice("a08", SYMBOL_A, 8, "VA"),
        pov_start("a09", SYMBOL_A, 9, "PA", "BUY", 6, 5000, 100),
        pov_volume("a10", SYMBOL_A, 10, "PA", 4),

        # --- BBB: second seller, new band, accepted + rejected portfolio ---
        limit("b03", SYMBOL_B, 3, "sb2", "SELL", 3, 91),
        limit_update("b04", SYMBOL_B, 4, 85, 95),
        portfolio_report("q05", SYMBOL_B, 5, "beta", {SYMBOL_B: 90}),
        portfolio_report("q06", SYMBOL_B, 6, "ghost", {SYMBOL_B: 90}),

        # --- AAA: read-only report then resting order and a valid reject ---
        execution_report("r11", SYMBOL_A, 11, "ia"),
        limit("a12", SYMBOL_A, 12, "sa2", "SELL", 1, 103),
        cancel("b13", SYMBOL_A, 13, "ghost"),

        # --- AAA: second TWAP slice exhausts ia's partially consumed slice -
        twap_slice("a14", SYMBOL_A, 14, "TA"),
        limit("a15", SYMBOL_A, 15, "xa", "SELL", 1, 120),
        plan_tca_report("q16", SYMBOL_A, 16, "TA", 101),
        vwap_slice("a17", SYMBOL_A, 17, "VA"),
        book_reconstruction("q18", SYMBOL_A, 18, 14),

        # --- BBB: aggressive limit trades with the resting seller ----------
        limit("b07", SYMBOL_B, 7, "bb", "BUY", 2, 90),
        execution_report("b08", SYMBOL_B, 8, "sb2"),
        impact_report("q09", SYMBOL_B, 9, "BUY", 4),

        # --- AAA: second POV release (plan remains open, iceberg reserve) --
        pov_volume("a19", SYMBOL_A, 19, "PA", 4),
        book_reconstruction("q20", SYMBOL_A, 20, 6),
        limit("a21", SYMBOL_A, 21, "ba", "BUY", 3, 99),
        vwap_slice("a22", SYMBOL_A, 22, "VA"),
        session_reconciliation("q23", SYMBOL_A, 23, [], []),
        impact_report("q24", SYMBOL_A, 24, "SELL", 2),
        cancel("a25", SYMBOL_A, 25, "sa2"),

        # --- BBB last: a resting sell inside the updated band -------------
        limit("b10", SYMBOL_B, 10, "sb3", "SELL", 1, 88),
    ]


# ---------------------------------------------------------------------------
# Baseline fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stream():
    return build_stream()


@pytest.fixture(scope="module")
def baseline(stream):
    out = replay_events(copy.deepcopy(stream), config=CONFIG)
    assert len(out["results"]) == len(stream)
    return out


@pytest.fixture(scope="module")
def accepted_boundaries(baseline):
    """Cut sizes k (1-based) where stream[:k] ends on an ACCEPTED event."""
    return [
        k for k, result in enumerate(baseline["results"], start=1)
        if result["status"] == ACCEPTED
    ]


def _last_for_symbol(results, symbol):
    return next(r for r in reversed(results) if r["symbol"] == symbol)


def _plans_by_id(snapshot, symbol):
    states = {entry["symbol"]: entry["state"] for entry in snapshot["content"]["symbols"]}
    return {plan["plan_id"]: plan for plan in states[symbol]["plans"]}


# ---------------------------------------------------------------------------
# Baseline sanity: the stream really covers every required state kind
# ---------------------------------------------------------------------------


def test_baseline_has_exactly_the_three_documented_business_rejections(baseline):
    rejected = [
        (r["event_id"], r["rejection_code"])
        for r in baseline["results"] if r["status"] == REJECTED
    ]
    assert rejected == [
        ("q06", "UNKNOWN_ACCOUNT"),
        ("b13", "UNKNOWN_ORDER"),
        ("a15", "PRICE_LIMIT_EXCEEDED"),
    ]
    assert all(r["status"] != DUPLICATE for r in baseline["results"])


def test_baseline_trades_occur_in_exact_id_and_order(baseline):
    aaa = [
        (t["trade_id"], t["maker_order_id"], t["taker_order_id"], t["quantity"])
        for r in baseline["results"] if r["symbol"] == SYMBOL_A
        for t in r["trades"]
    ]
    assert aaa == [
        (1, "ia", "ma", 3),
        (2, "sa", "ma", 1),
        (3, "sa", "TA#1", 2),
        (4, "sa", "PA#1", 1),
        (5, "ia", "PA#1", 1),
        (6, "ia", "TA#2", 2),
        (7, "ia", "PA#2", 2),
    ]
    bbb = [
        (t["trade_id"], t["maker_order_id"], t["taker_order_id"], t["quantity"])
        for r in baseline["results"] if r["symbol"] == SYMBOL_B
        for t in r["trades"]
    ]
    assert bbb == [
        (1, "sb", "mb", 2),
        (2, "sb", "bb", 2),
    ]


def test_baseline_final_books_keep_resting_limit_and_iceberg_reserve(baseline):
    aaa = _last_for_symbol(baseline["results"], SYMBOL_A)
    # ia: 8 of 10 filled -> remaining 2 = 1 visible + 1 still hidden.
    assert aaa["asks"] == [{"price": 100, "quantity": 1}]
    assert aaa["bids"] == [{"price": 99, "quantity": 3}]
    bbb = _last_for_symbol(baseline["results"], SYMBOL_B)
    assert bbb["asks"] == [
        {"price": 88, "quantity": 1},
        {"price": 90, "quantity": 1},
        {"price": 91, "quantity": 3},
    ]
    assert bbb["bids"] == []


def test_baseline_final_snapshot_plans_capture_open_and_completed_states(baseline):
    plans = _plans_by_id(baseline["snapshot"], SYMBOL_A)

    ta = plans["TA"]
    assert ta["status"] == "ACTIVE"
    assert ta["slice_quantities"] == [2, 2, 2]
    assert (ta["released"], ta["released_quantity"], ta["filled_quantity"]) == (2, 4, 4)

    # The VWAP plan only completes on its third (final) slice; mid-stream
    # boundaries below capture it ACTIVE.
    va = plans["VA"]
    assert va["status"] == "COMPLETED" and va["algorithm"] == "VWAP"
    assert va["slice_quantities"] == [1, 1, 2]
    assert va["released"] == 3 and va["filled_quantity"] == 0

    pa = plans["PA"]
    assert pa["status"] == "ACTIVE" and pa["algorithm"] == "POV"
    assert pa["total_quantity"] == 6
    assert (pa["released"], pa["released_quantity"], pa["filled_quantity"]) == (2, 4, 4)
    assert pa["market_volume"] == 8


def test_mid_stream_snapshot_has_all_three_plans_open_and_iceberg_reserve(stream):
    # After a17: TA open (2 of 3 slices), VA open (2 of 3 releases), PA open
    # (4 of 6 released) and ia still holds hidden reserve.
    cut = next(i for i, e in enumerate(stream) if e["event_id"] == "a17") + 1
    snapshot = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)["snapshot"]
    plans = _plans_by_id(snapshot, SYMBOL_A)
    assert {p: plans[p]["status"] for p in ("TA", "VA", "PA")} == {
        "TA": "ACTIVE", "VA": "ACTIVE", "PA": "ACTIVE",
    }
    va = plans["VA"]
    assert va["released"] == 2 and va["slice_quantities"] == [1, 1, 2]
    states = {e["symbol"]: e["state"] for e in snapshot["content"]["symbols"]}
    ia = states[SYMBOL_A]["engine"]["orders"]["ia"]
    assert ia["status"] == "RESTING" and ia["remaining"] == 4
    assert ia["visible"] == 3 and ia["remaining"] - ia["visible"] > 0
    # The active interval at this cut is the intraday-replaced one.
    assert states[SYMBOL_A]["price_limits"] == {"lower": 95, "upper": 105}


def test_baseline_active_price_limits_were_replaced(baseline):
    states = {e["symbol"]: e["state"] for e in baseline["snapshot"]["content"]["symbols"]}
    assert states[SYMBOL_A]["price_limits"] == {"lower": 95, "upper": 105}
    assert states[SYMBOL_B]["price_limits"] == {"lower": 85, "upper": 95}


# ---------------------------------------------------------------------------
# replay_events segmentation: every accepted boundary, byte-identical
# ---------------------------------------------------------------------------


def test_segmented_replay_matches_uninterrupted_at_every_boundary(
    stream, baseline, accepted_boundaries
):
    for k in accepted_boundaries:
        prefix_out = replay_events(copy.deepcopy(stream[:k]), config=CONFIG)
        resumed = replay_events(
            copy.deepcopy(stream[k:]), config=CONFIG, snapshot=prefix_out["snapshot"]
        )
        # Full suffix response documents compared as canonical bytes: every
        # per-event response, trade id/ordering, bids/asks, plan summary and
        # embedded report is included, arrays in production order.
        assert canonical_json(resumed["results"]) == canonical_json(
            baseline["results"][k:]
        ), f"suffix results diverge at boundary {k}"
        assert canonical_json(resumed["snapshot"]) == canonical_json(
            baseline["snapshot"]
        ), f"final snapshot diverges at boundary {k}"

        # The boundary snapshot exported via segmentation must also equal the
        # snapshot the uninterrupted run exports at exactly that marker.
        marker = {"symbol": baseline["results"][k - 1]["symbol"],
                  "sequence": baseline["results"][k - 1]["sequence"]}
        marked = replay_events(
            copy.deepcopy(stream), config=CONFIG, snapshot_after=marker
        )
        assert canonical_json(marked["snapshot"]) == canonical_json(
            prefix_out["snapshot"]
        ), f"boundary snapshot diverges at {marker}"


def test_boundary_set_covers_every_required_state_kind(baseline, accepted_boundaries):
    position = {r["event_id"]: k for k, r in enumerate(baseline["results"], start=1)}
    # Cuts with a resting plain order, iceberg with hidden reserve and each
    # open partially-executed plan must all exist.
    for event_id in ("a06", "a14", "a17", "a19"):
        assert position[event_id] in accepted_boundaries
    # Read-only occupied sequences / valid business rejections sit directly
    # between exportable boundaries (the following ACCEPTED event resumes).
    assert position["r11"] + 1 in accepted_boundaries
    assert position["b13"] + 1 in accepted_boundaries
    assert position["a15"] + 1 in accepted_boundaries
    assert position["q06"] + 1 in accepted_boundaries
    # The two historical queries are themselves accepted boundaries.
    assert position["q18"] in accepted_boundaries
    assert position["q20"] in accepted_boundaries


# ---------------------------------------------------------------------------
# EventReplayer incremental commits and manual export/restore segmentation
# ---------------------------------------------------------------------------


def test_three_way_segmentation_reproduces_the_uninterrupted_run(stream, baseline):
    # An arbitrary three-way split, including a cut directly after an
    # occupied-but-read-only report sequence (q18 itself is accepted).
    cuts = [
        next(i for i, e in enumerate(stream) if e["event_id"] == "a10") + 1,
        next(i for i, e in enumerate(stream) if e["event_id"] == "q18") + 1,
    ]
    c1, c2 = sorted(cuts)
    first = replay_events(copy.deepcopy(stream[:c1]), config=CONFIG)
    second = replay_events(
        copy.deepcopy(stream[c1:c2]), config=CONFIG, snapshot=first["snapshot"]
    )
    third = replay_events(
        copy.deepcopy(stream[c2:]), config=CONFIG, snapshot=second["snapshot"]
    )
    assert canonical_json(second["results"]) == canonical_json(
        baseline["results"][c1:c2]
    )
    assert canonical_json(third["results"]) == canonical_json(
        baseline["results"][c2:]
    )
    assert canonical_json(third["snapshot"]) == canonical_json(baseline["snapshot"])


def test_stateful_replayer_one_event_commits_match_one_shot(stream, baseline):
    replayer = EventReplayer(CONFIG)
    for index, event in enumerate(stream):
        [result] = replayer.submit([copy.deepcopy(event)])
        assert canonical_json(result) == canonical_json(baseline["results"][index]), index
    assert canonical_json(export_snapshot(replayer)) == canonical_json(baseline["snapshot"])


def test_stateful_replayer_uneven_chunking_matches_one_shot(stream, baseline):
    replayer = EventReplayer(CONFIG)
    chunks = [stream[0:1], stream[1:3], stream[3:10], stream[10:17], stream[17:26], stream[26:]]
    seen: list[dict] = []
    for chunk in chunks:
        seen.extend(replayer.submit(copy.deepcopy(chunk)))
    assert canonical_json(seen) == canonical_json(baseline["results"])
    assert canonical_json(export_snapshot(replayer)) == canonical_json(baseline["snapshot"])


def test_manual_export_restore_roundtrip_at_every_boundary(
    stream, baseline, accepted_boundaries
):
    for k in accepted_boundaries:
        first = EventReplayer(CONFIG)
        first.submit(copy.deepcopy(stream[:k]))
        snapshot = export_snapshot(first)
        restored = restore_replayer(copy.deepcopy(snapshot), config=CONFIG)
        # Restore + re-export reproduces the snapshot document byte-for-byte.
        assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot), k
        suffix = restored.submit(copy.deepcopy(stream[k:]))
        assert canonical_json(suffix) == canonical_json(baseline["results"][k:]), k
        assert canonical_json(export_snapshot(restored)) == canonical_json(
            baseline["snapshot"]
        ), k


def test_restored_replayer_keeps_per_symbol_books_independent(stream, baseline):
    # Split in the middle of the early interleaving: 4 AAA events, 2 BBB.
    cut = next(i for i, e in enumerate(stream) if e["event_id"] == "b02") + 1
    prefix_out = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)
    replayer = restore_replayer(prefix_out["snapshot"], config=CONFIG)
    suffix = replayer.submit(copy.deepcopy(stream[cut:]))
    assert canonical_json(suffix) == canonical_json(baseline["results"][cut:])
    for symbol in (SYMBOL_A, SYMBOL_B):
        final = _last_for_symbol(baseline["results"], symbol)
        assert canonical_json(replayer.book(symbol)) == canonical_json(
            (final["bids"], final["asks"])
        )


# ---------------------------------------------------------------------------
# Read-only report content across segmentation
# ---------------------------------------------------------------------------


REPORT_KEYS = (
    "execution_analysis", "impact_analysis", "portfolio_analysis",
    "reconciliation", "plan_tca_analysis", "book_reconstruction",
)


def test_every_read_only_report_is_byte_identical_when_resumed_before_it(
    stream, baseline
):
    report_positions = [
        i for i, r in enumerate(baseline["results"])
        if any(key in r for key in REPORT_KEYS) and r["status"] == ACCEPTED
    ]
    # Every report kind above appears at least once in the stream.
    covered = {
        key for i in report_positions for key in REPORT_KEYS
        if key in baseline["results"][i]
    }
    assert set(REPORT_KEYS) <= covered

    for pos in report_positions:
        cut = pos  # restore immediately before the query event
        snapshot = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)["snapshot"]
        out = replay_events(copy.deepcopy(stream[cut:]), config=CONFIG, snapshot=snapshot)
        assert canonical_json(out["results"][0]) == canonical_json(
            baseline["results"][pos]
        ), f"report at position {pos} diverges after restore"


def test_book_reconstruction_after_restore_pins_queues_iceberg_and_band(stream, baseline):
    # Both embedded historical queries answered from a restored session must
    # reproduce the uninterrupted answers byte-for-byte.
    for event_id in ("q18", "q20"):
        pos = next(i for i, e in enumerate(stream) if e["event_id"] == event_id)
        snapshot = replay_events(copy.deepcopy(stream[:pos]), config=CONFIG)["snapshot"]
        out = replay_events(
            copy.deepcopy(stream[pos:]), config=CONFIG, snapshot=snapshot
        )
        assert canonical_json(out["results"][0]) == canonical_json(
            baseline["results"][pos]
        )

    # Target 14: sa gone; ia's partially-consumed visible slice (2) was fully
    # taken by TA#2 and replenished as a fresh peak-3 slice at the tail; the
    # resting sa2 @103 from sequence 12 is present at its own level; the active
    # interval is the intraday-replaced one.
    at_14 = baseline["results"][
        next(i for i, r in enumerate(baseline["results"]) if r["event_id"] == "q18")
    ]["book_reconstruction"]
    assert at_14["target_sequence"] == 14
    assert at_14["active_price_limits"] == {"lower_price": 95, "upper_price": 105}
    assert [level["price"] for level in at_14["ask_queues"]] == [100, 103]
    level = at_14["ask_queues"][0]
    assert level["visible_quantity"] == 3
    assert [o["order_id"] for o in level["orders"]] == ["ia"]
    assert level["orders"][0] == {
        "order_id": "ia", "order_type": "ICEBERG",
        "remaining_quantity": 4, "visible_quantity": 3,
    }
    assert at_14["ask_queues"][1]["orders"] == [{
        "order_id": "sa2", "order_type": "LIMIT",
        "remaining_quantity": 1, "visible_quantity": 1,
    }]

    # Target 6: TA#1 just consumed 2 from sa (3 -> 1); ia's replenished slice
    # queues behind sa; the band is already the updated 95..105.
    at_6 = baseline["results"][
        next(i for i, r in enumerate(baseline["results"]) if r["event_id"] == "q20")
    ]["book_reconstruction"]
    assert at_6["target_sequence"] == 6
    assert at_6["active_price_limits"] == {"lower_price": 95, "upper_price": 105}
    (level6,) = at_6["ask_queues"]
    assert level6["price"] == 100 and level6["visible_quantity"] == 4
    assert [o["order_id"] for o in level6["orders"]] == ["sa", "ia"]
    sa_view, ia_view = level6["orders"]
    assert sa_view == {"order_id": "sa", "order_type": "LIMIT",
                       "remaining_quantity": 1, "visible_quantity": 1}
    assert ia_view == {"order_id": "ia", "order_type": "ICEBERG",
                       "remaining_quantity": 7, "visible_quantity": 3}

    # A target strictly before the PRICE_LIMIT_UPDATE (target 3), issued after
    # restoring an arbitrary later prefix, reports the *static* interval while
    # the queue order and iceberg visible quantity reproduce the target point.
    pos = next(i for i, e in enumerate(stream) if e["event_id"] == "q18")
    snapshot = replay_events(copy.deepcopy(stream[:pos]), config=CONFIG)["snapshot"]
    probe = book_reconstruction("probe", SYMBOL_A, 18, 3)
    out = replay_events([probe], config=CONFIG, snapshot=snapshot)
    rec = out["results"][0]["book_reconstruction"]
    assert rec["active_price_limits"] == {"lower_price": 90, "upper_price": 110}
    (level3,) = rec["ask_queues"]
    assert [o["order_id"] for o in level3["orders"]] == ["sa", "ia"]
    sa3, ia3 = level3["orders"]
    assert (sa3["remaining_quantity"], sa3["visible_quantity"]) == (3, 3)
    assert (ia3["remaining_quantity"], ia3["visible_quantity"]) == (7, 3)
    assert rec["bid_queues"] == []


# ---------------------------------------------------------------------------
# Envelope precedence and idempotency after restore
# ---------------------------------------------------------------------------


def _restore_after_event(stream, event_id):
    cut = next(i for i, e in enumerate(stream) if e["event_id"] == event_id) + 1
    out = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)
    return restore_replayer(out["snapshot"], config=CONFIG), cut


def _aaa_trade_ids(replayer):
    snap = export_snapshot(replayer)
    for entry in snap["content"]["symbols"]:
        if entry["symbol"] == SYMBOL_A:
            return [t["trade_id"] for t in entry["state"]["engine"]["trade_log"]]
    return []


def test_duplicate_conflict_gap_and_reorder_after_restore_move_nothing(
    stream, baseline
):
    replayer, _ = _restore_after_event(stream, "a14")

    def submit(event):
        return replayer.submit([copy.deepcopy(event)])[0]

    trades_before = _aaa_trade_ids(replayer)
    book_before = canonical_json(replayer.book(SYMBOL_A))

    # Same event_id with identical canonical content: DUPLICATE, despite the
    # retry carrying its stale original sequence (a06 was AAA sequence 6).
    a06 = next(e for e in stream if e["event_id"] == "a06")
    dup = submit(a06)
    assert dup["status"] == DUPLICATE
    assert dup["trades"] == [] and dup["book_changes"] == {"bids": [], "asks": []}

    # Same id, different content: EVENT_ID_CONFLICT.
    conflict = submit(twap_slice("a06", SYMBOL_A, 15, "OTHER"))
    assert conflict["status"] == REJECTED
    assert conflict["rejection_code"] == EVENT_ID_CONFLICT
    assert conflict["trades"] == [] and conflict["book_changes"] == {"bids": [], "asks": []}

    # Sequence hole: next expected is 15, send 17.
    gap = submit(limit("gx", SYMBOL_A, 17, "gx", "SELL", 1, 100))
    assert gap["status"] == REJECTED and gap["rejection_code"] == SEQUENCE_GAP
    assert gap["expected_sequence"] == 15
    assert gap["trades"] == []

    # Backwards sequence.
    early = submit(limit("gy", SYMBOL_A, 1, "gy", "SELL", 1, 100))
    assert early["status"] == REJECTED and early["rejection_code"] == OUT_OF_ORDER
    assert early["expected_sequence"] == 15
    assert early["trades"] == []

    # None of the four responses added a trade or moved the book.
    assert _aaa_trade_ids(replayer) == trades_before
    assert canonical_json(replayer.book(SYMBOL_A)) == book_before

    # The legitimate sequence-15 event then gets its uninterrupted result:
    # a15 is PRICE_LIMIT_EXCEEDED, proving no id/sequence slot was consumed.
    a15_index = next(i for i, e in enumerate(stream) if e["event_id"] == "a15")
    next_result = submit(stream[a15_index])
    assert canonical_json(next_result) == canonical_json(baseline["results"][a15_index])
    assert next_result["trades"] == []
    assert _aaa_trade_ids(replayer) == trades_before


def test_invalid_event_consumes_neither_id_nor_sequence_after_restore(stream):
    replayer, _ = _restore_after_event(stream, "a12")  # AAA last sequence 12
    invalid_events = [
        {"event_id": "bad1", "symbol": SYMBOL_A, "sequence": 13,
         "type": "ADD", "order_id": "x", "side": "BUY",
         "order_type": "LIMIT", "quantity": 1, "price": 100, "bogus": True},
        {"event_id": "", "symbol": SYMBOL_A, "sequence": 13,
         "type": "CANCEL", "order_id": "x"},
        {"event_id": "bad3", "symbol": SYMBOL_A, "sequence": 13,
         "type": "NO_SUCH_TYPE"},
        {"event_id": "bad4", "symbol": SYMBOL_A, "sequence": 13,
         "type": "ADD", "order_id": "x", "side": "SIDE",
         "order_type": "LIMIT", "quantity": 1, "price": 100},
    ]
    book_before = canonical_json(replayer.book(SYMBOL_A))
    for event in invalid_events:
        result = replayer.submit([copy.deepcopy(event)])[0]
        assert result["status"] == REJECTED
        assert result["rejection_code"] == INVALID_EVENT
        assert result["trades"] == []
    assert canonical_json(replayer.book(SYMBOL_A)) == book_before

    # The invalid ids are still free and sequence 13 still unoccupied: reuse
    # one id on a well-formed order, then occupy 14 with a different event.
    reuse = replayer.submit([limit("bad1", SYMBOL_A, 13, "reuse", "SELL", 1, 102)])[0]
    assert reuse["status"] == ACCEPTED and reuse["result"] == "RESTING"
    follow = replayer.submit([execution_report("q13x", SYMBOL_A, 14, "ia")])[0]
    assert follow["status"] == ACCEPTED and follow["result"] == "REPORTED"


def test_invalid_events_around_a_segmentation_boundary_keep_suffix_identical(
    stream, baseline
):
    # Split right before b13 (AAA sequence 13, an UNKNOWN_ORDER business
    # rejection). On the resumed side, deliver structurally invalid events
    # first; they occupy nothing, so the canonical suffix — including the
    # committed business rejection and every later legal event — must match the
    # uninterrupted run byte-for-byte.
    cut = next(i for i, e in enumerate(stream) if e["event_id"] == "b13")
    snapshot = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)["snapshot"]
    replayer = restore_replayer(snapshot, config=CONFIG)
    invalid_pack = [
        {"event_id": "inv1", "symbol": SYMBOL_A, "sequence": 13,
         "type": "ADD", "order_id": "x", "side": "BUY",
         "order_type": "LIMIT", "quantity": -1, "price": 100},
        {"event_id": "inv2", "symbol": SYMBOL_A, "sequence": 13,
         "type": "NO_SUCH_TYPE", "order_id": ""},
        {"event_id": "inv3", "symbol": SYMBOL_A, "sequence": 13,
         "type": "TWAP_SLICE", "plan_id": 7},
    ]
    invalid_results = replayer.submit(copy.deepcopy(invalid_pack))
    assert {r["rejection_code"] for r in invalid_results} == {INVALID_EVENT}
    suffix = replayer.submit(copy.deepcopy(stream[cut:]))
    assert canonical_json(suffix) == canonical_json(baseline["results"][cut:])
    assert canonical_json(export_snapshot(replayer)) == canonical_json(
        baseline["snapshot"]
    )


def test_business_rejections_occupy_id_and_sequence_across_restore(stream):
    # Restore at the very end; the three committed rejections are all logged.
    replayer, _ = _restore_after_event(stream, "b10")

    # Verbatim re-delivery of each structurally valid business rejection is a
    # DUPLICATE and moves nothing.
    dup_cancel = replayer.submit([cancel("b13", SYMBOL_A, 99, "ghost")])[0]
    assert dup_cancel["status"] == DUPLICATE and dup_cancel["trades"] == []
    dup_limit = replayer.submit([limit("a15", SYMBOL_A, 99, "xa", "SELL", 1, 120)])[0]
    assert dup_limit["status"] == DUPLICATE and dup_limit["trades"] == []
    dup_account = replayer.submit([
        portfolio_report("q06", SYMBOL_B, 99, "ghost", {SYMBOL_B: 90})
    ])[0]
    assert dup_account["status"] == DUPLICATE and dup_account["trades"] == []

    # The next per-symbol sequences continue exactly (AAA 26, BBB 11).
    next_a = replayer.submit([limit("na1", SYMBOL_A, 26, "na", "SELL", 1, 100)])[0]
    assert next_a["status"] == ACCEPTED and next_a["result"] == "RESTING"
    next_b = replayer.submit([execution_report("nb1", SYMBOL_B, 11, "sb")])[0]
    assert next_b["status"] == ACCEPTED and next_b["result"] == "REPORTED"


def test_same_id_different_content_precedence_survives_segmentation(stream, baseline):
    # Conflict check precedes sequencing: deliver an id clash with a gapped
    # sequence and confirm EVENT_ID_CONFLICT wins, before and after a split.
    cut = next(i for i, e in enumerate(stream) if e["event_id"] == "a10") + 1
    snapshot = replay_events(copy.deepcopy(stream[:cut]), config=CONFIG)["snapshot"]
    replayer = restore_replayer(snapshot, config=CONFIG)
    # a05 (TWAP_START) reused with different content at a gapped AAA seq 99.
    clash = replayer.submit([
        twap_start("a05", SYMBOL_A, 99, "OTHER", "BUY", 2, 1, 100)
    ])[0]
    assert clash["rejection_code"] == EVENT_ID_CONFLICT
    # Sequence 11 is still the next accepted slot (r11 in the baseline).
    r11_index = next(i for i, e in enumerate(stream) if e["event_id"] == "r11")
    resumed = replayer.submit([stream[r11_index]])[0]
    assert canonical_json(resumed) == canonical_json(baseline["results"][r11_index])


# ---------------------------------------------------------------------------
# CLI segmentation parity with the Python entry point
# ---------------------------------------------------------------------------


def _serve(request_obj):
    text = json.dumps(request_obj, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    return code, stdout.buffer.getvalue(), stderr.getvalue()


@pytest.mark.parametrize("cut_event", ["a06", "a14", "a17", "a19", "q20", "a25", "b10"])
def test_cli_segmented_documents_match_python_entry_byte_for_byte(
    stream, baseline, cut_event
):
    k = next(i for i, e in enumerate(stream) if e["event_id"] == cut_event) + 1
    marker = {"symbol": baseline["results"][k - 1]["symbol"],
              "sequence": baseline["results"][k - 1]["sequence"]}

    code1, raw1, err1 = _serve({
        "events": copy.deepcopy(stream[:k]),
        "config": CONFIG,
        "snapshot_after": marker,
    })
    assert code1 == 0 and err1 == ""
    py_first = replay_events(
        copy.deepcopy(stream[:k]), config=CONFIG, snapshot_after=marker
    )
    assert raw1 == canonical_json(py_first) + b"\n"

    code2, raw2, err2 = _serve({
        "events": copy.deepcopy(stream[k:]),
        "config": CONFIG,
        "snapshot": json.loads(raw1.decode("utf-8"))["snapshot"],
    })
    assert code2 == 0 and err2 == ""
    py_second = replay_events(
        copy.deepcopy(stream[k:]), config=CONFIG, snapshot=py_first["snapshot"]
    )
    assert raw2 == canonical_json(py_second) + b"\n"

    second_doc = json.loads(raw2.decode("utf-8"))
    assert canonical_json(second_doc["results"]) == canonical_json(
        baseline["results"][k:]
    )
    assert canonical_json(second_doc["snapshot"]) == canonical_json(
        baseline["snapshot"]
    )


def test_cli_full_document_matches_python_byte_for_byte(stream):
    code, raw, err = _serve({"events": copy.deepcopy(stream), "config": CONFIG})
    assert code == 0 and err == ""
    expected = canonical_json(
        replay_events(copy.deepcopy(stream), config=CONFIG)
    ) + b"\n"
    assert raw == expected


def _run_cli_subprocess(request_text, hash_seed):
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hash_seed)
    code = (
        "from order_book_engine.cli import main; "
        "import sys; sys.exit(main(['events']))"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        input=request_text.encode("utf-8"),
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def test_real_cli_subprocess_segmentation_exit_code_stderr_and_bytes(stream):
    k = next(i for i, e in enumerate(stream) if e["event_id"] == "a19") + 1
    first_request = json.dumps(
        {"events": stream[:k], "config": CONFIG}, ensure_ascii=False
    )
    runs = {seed: _run_cli_subprocess(first_request, seed) for seed in (0, 1, 7, 12345)}
    reference = runs[0].stdout
    for seed, completed in runs.items():
        assert completed.returncode == 0, completed.stderr.decode("utf-8")
        assert completed.stderr == b"", seed
        assert completed.stdout == reference, seed

    snapshot = json.loads(reference.decode("utf-8"))["snapshot"]
    second_request = json.dumps(
        {"events": stream[k:], "config": CONFIG, "snapshot": snapshot},
        ensure_ascii=False,
    )
    resumed = _run_cli_subprocess(second_request, 0)
    assert resumed.returncode == 0 and resumed.stderr == b""

    resumed_doc = json.loads(resumed.stdout.decode("utf-8"))
    assert canonical_json(resumed_doc["results"]) == canonical_json(
        replay_events(copy.deepcopy(stream), config=CONFIG)["results"][k:]
    )
    full_segmented = _run_cli_subprocess(
        json.dumps({"events": stream, "config": CONFIG}, ensure_ascii=False), 0
    )
    full_doc = json.loads(full_segmented.stdout.decode("utf-8"))
    assert canonical_json(resumed_doc["snapshot"]) == canonical_json(full_doc["snapshot"])
