"""Deterministic regression tests for segmentation-invariant multi-symbol replay.

The contract under test is one invariant: *every legal segmentation of one
fixed event stream produces exactly the same observations as an uninterrupted
run*. Nothing here adds a product interface, an event type or an output field;
the tests only drive the documented public surface

* :func:`order_book_engine.replay_events`,
* :class:`order_book_engine.EventReplayer` incremental :meth:`submit`,
* :func:`order_book_engine.export_snapshot` /
  :func:`order_book_engine.restore_replayer`,
* the read-only historical ``BOOK_RECONSTRUCTION_REPORT`` query, and
* the ``order-book-engine events`` command line entry point
  (:func:`order_book_engine.event_cli.serve_events`).

The fixed stream interleaves two securities and deliberately visits every
state kind a snapshot boundary must preserve:

* a resting plain limit order on each book;
* an iceberg order whose visible slice is repeatedly consumed and
  tail-replenished while a hidden reserve survives to the end;
* a TWAP, a VWAP and a POV plan, all started and partly released (the VWAP
  and the POV plans are left ACTIVE with open quantity, so their unreleased
  derived child ids stay reserved; the TWAP completes late in the stream and
  is ACTIVE across the earlier boundaries);
* an accepted ``PRICE_LIMIT_UPDATE`` that has replaced the active interval,
  so a later limit outside the new band is business-rejected;
* read-only reports of every kind (execution, impact, portfolio,
  portfolio-stress, whole-session reconciliation, plan TCA, TWAP/VWAP plan
  reports and historical book reconstruction), which occupy an id and a
  sequence but move no trading state;
* a structurally legal business rejection (a price-limit breach), which
  occupies both the id and the sequence.

For every exportable prefix boundary the tests obtain the uninterrupted
baseline result first, then restore a replayer from the boundary snapshot and
submit the remaining events. The continued per-event responses (including
trade ids and their order, queues, book diffs, plan summaries and report
content) and the final snapshot are compared **byte for byte** through
:func:`canonical_json`; trades, queues and break arrays are never sorted or
otherwise normalized, so a reordering cannot be hidden. Historical book
reconstructions issued after recovery are compared, again byte for byte,
with the identical probes run against an uninterrupted session — queue order,
iceberg visible quantities and the price-limit interval as of the target
included, both from the boundary state itself and from the final state when
the recovery continued through the rest of the stream.

A dedicated test pins the envelope precedence and idempotency table across a
recovery boundary: duplicate content stays ``DUPLICATE``, the same id with
different content stays ``EVENT_ID_CONFLICT``, a sequence hole stays
``SEQUENCE_GAP`` and a backwards sequence stays ``OUT_OF_ORDER``; none of
these responses adds a trade or moves the book. Structurally invalid
``INVALID_EVENT`` input keeps not consuming the event id or the sequence,
while a structurally legal business rejection keeps consuming both, and the
legal events that follow either one compare identically across the boundary.
Finally, segmented ``events`` command-line requests emit the same canonical
JSON, exit code and empty stderr as the Python entry point, under several
``PYTHONHASHSEED`` values.
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
    BOOK_RECONSTRUCTION_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EventReplayer,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PLAN_TCA_REPORT,
    PORTFOLIO_REPORT,
    PRICE_LIMIT_UPDATE,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    POV_START,
    POV_VOLUME,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli
from order_book_engine.engine import EXECUTION_REPORT, IMPACT_REPORT


# ---------------------------------------------------------------------------
# Fixed matching configuration and event builders
# ---------------------------------------------------------------------------

CONFIG = {"price_limits": {"AAA": {"lower": 90, "upper": 120}}}
AAA = "AAA"
BBB = "BBB"


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


def limit(event_id, symbol, sequence, order_id, side, quantity, price, **extra):
    return add(event_id, symbol, sequence, order_id, side, "LIMIT", quantity, price, **extra)


def iceberg(event_id, symbol, sequence, order_id, quantity, price, display):
    return add(event_id, symbol, sequence, order_id, "SELL", "ICEBERG",
               quantity, price, display_quantity=display)


def market(event_id, symbol, sequence, order_id, side, quantity):
    return add(event_id, symbol, sequence, order_id, side, "MARKET", quantity)


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def report(event_id, symbol, sequence, order_id, benchmark_price):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": EXECUTION_REPORT, "order_id": order_id,
            "benchmark_price": benchmark_price}


def impact(event_id, symbol, sequence, side, quantity, benchmark_price):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": IMPACT_REPORT, "side": side, "quantity": quantity,
            "benchmark_price": benchmark_price}


def portfolio(event_id, symbol, sequence, account_id, mark_prices):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": PORTFOLIO_REPORT, "account_id": account_id,
            "mark_prices": mark_prices}


def portfolio_stress(event_id, symbol, sequence, account_id, mark_prices, scenarios):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "PORTFOLIO_STRESS_REPORT", "account_id": account_id,
            "mark_prices": mark_prices, "scenarios": scenarios}


def reconciliation(event_id, symbol, sequence, expected_trades, expected_accounts):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": SESSION_RECONCILIATION, "expected_trades": expected_trades,
            "expected_accounts": expected_accounts}


def plan_tca(event_id, symbol, sequence, plan_id, mark_price):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": PLAN_TCA_REPORT, "plan_id": plan_id, "mark_price": mark_price}


def reconstruction(event_id, symbol, sequence, target):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": target}


def limit_update(event_id, symbol, sequence, lower, upper):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": PRICE_LIMIT_UPDATE, "lower_price": lower, "upper_price": upper}


def twap_start(event_id, symbol, sequence, plan_id, total_quantity, slice_count, price):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_START, "plan_id": plan_id, "side": "BUY",
            "total_quantity": total_quantity, "slice_count": slice_count,
            "order_type": "LIMIT", "benchmark_price": 100, "price": price}


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


def twap_report(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_REPORT, "plan_id": plan_id}


def vwap_start(event_id, symbol, sequence, plan_id, weights, price, account_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_START, "plan_id": plan_id, "side": "BUY",
            "total_quantity": 10, "volume_weights": weights,
            "order_type": "LIMIT", "benchmark_price": 70, "price": price,
            "account_id": account_id}


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


def vwap_report(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_REPORT, "plan_id": plan_id}


def pov_start(event_id, symbol, sequence, plan_id, total_quantity, participation_bps, account_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_START, "plan_id": plan_id, "side": "BUY",
            "total_quantity": total_quantity,
            "participation_bps": participation_bps,
            "order_type": "MARKET", "benchmark_price": 70,
            "account_id": account_id}


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_VOLUME, "plan_id": plan_id,
            "market_volume_increment": increment}


# ---------------------------------------------------------------------------
# The fixed interleaved multi-symbol stream
# ---------------------------------------------------------------------------
#
# Per-symbol sequences advance independently; the global order interchanges
# AAA and BBB events throughout. Index -> event:
#
#   0 a1   AAA 1  sell limit 5 @100 (account alpha)
#   1 a2   BBB 1  sell limit 10 @70 (account delta)
#   2 i1   AAA 2  sell iceberg 10 @100, visible 3 (hidden reserve survives)
#   3 a4   BBB 2  sell limit 4 @71 (never touched)
#   4 a5   AAA 3  sell limit 3 @100
#   5 t1   AAA 4  market buy 7 -> sweeps a1 (5 @100, trade 1) then 2 of the
#                 iceberg's slice @100 (trade 2); one public slice unit left
#   6 a7   BBB 3  buy limit 2 @68 (resting bid, never filled)
#   7 u1   AAA 5  PRICE_LIMIT_UPDATE -> active band [95, 110]
#   8 w1   BBB 4  VWAP_START V: buy 10 over weights 5/3/1/1 (slices
#                 4/3/2/1), LIMIT @70, account gamma
#   9 P    AAA 6  TWAP_START P: buy 6 in 3 LIMIT slices @100
#  10 v6   BBB 5  VWAP_SLICE -> V#1 IOC buy 4 @70, trades w1 for 4 (BBB t1)
#  11 p2   AAA 7  TWAP_SLICE -> P#1 IOC buy 2 @100; the iceberg's last
#                 visible unit trades first (AAA t3) and exhausts the slice,
#                 so it replenishes 3 at the level tail behind a5; a5 gives 1
#                 (AAA t4) and has 2 left
#  12 qP   AAA 8  read-only TWAP_REPORT (P ACTIVE, 1/3 released)
#  13 m1   AAA 9  read-only IMPACT_REPORT
#  14 x1   BBB 6  POV_START X: buy 10 at 50% participation, MARKET, account
#                 beta
#  15 e1   AAA 10 read-only EXECUTION_REPORT for the partly filled a5
#  16 x8   BBB 7  POV_VOLUME +8 -> target 4: X#1 market buy 4, trades w1 for
#                 4 (BBB t2); X stays ACTIVE with 6 open
#  17 bad  AAA 11 sell LIMIT 1 @150 -> PRICE_LIMIT_EXCEEDED; a legal business
#                 rejection that occupies the id and AAA sequence 11
#  18 qV   BBB 8  read-only VWAP_REPORT (V ACTIVE after one slice)
#  19 f1   AAA 12 read-only PORTFOLIO_REPORT for alpha (marks exactly {AAA})
#  20 x0   BBB 9  POV_VOLUME +1: cumulative 9 at 50% still targets 4, so a
#                 zero-release response that occupies id/sequence
#  21 brA  AAA 13 read-only BOOK_RECONSTRUCTION targeting AAA sequence 1
#  22 sr1  BBB 10 read-only SESSION_RECONCILIATION naming every trade and
#                 account of the WHOLE session so far -> RECONCILED
#  23 tcaP AAA 14 read-only PLAN_TCA_REPORT for P
#  24 r1   AAA 15 sell limit 3 @100 (joins behind the replenished iceberg)
#  25 p3   AAA 16 TWAP_SLICE -> P#2 buy 2 takes a5's remaining 2 (AAA t5)
#  26 brB  BBB 11 read-only BOOK_RECONSTRUCTION targeting BBB sequence 0
#  27 p4   AAA 17 TWAP_SLICE -> P#3 buy 2 takes 2 of the iceberg's 3-lot
#                 slice (AAA t6); P is now COMPLETED; one visible unit left
#  28 a9   AAA 18 limit buy 2 @100 -> the iceberg's last visible unit (AAA
#                 t7) exhausts the slice and replenishes 3 at the tail behind
#                 r1; r1 gives 1 (AAA t8) with 2 left
#  29 v12  BBB 12 VWAP_SLICE -> V#2 IOC buy 3 @70: w1 has exactly 2 left, so
#                 they trade (BBB t3) and one unit is cancelled; V stays
#                 ACTIVE (7/10 released, two slices reserved)
#  30 end  AAA 19 sell limit 2 @103 -> rests inside the updated [95, 110]
#                 band; the book at 100 is r1(2) ahead of the iceberg
#                 (3 visible, 4 total = 1 still hidden)
#  31 f2   AAA 20 read-only PORTFOLIO_STRESS_REPORT for alpha


def _fixed_stream():
    return [
        # -- initial books --------------------------------------------------
        limit("a1", AAA, 1, "a1", "SELL", 5, 100, account_id="alpha"),
        limit("a2", BBB, 1, "w1", "SELL", 10, 70, account_id="delta"),
        iceberg("i1", AAA, 2, "ic", 10, 100, 3),
        limit("a4", BBB, 2, "s71", "SELL", 4, 71),
        limit("a5", AAA, 3, "a5", "SELL", 3, 100),
        market("t1", AAA, 4, "m7", "BUY", 7),
        limit("a7", BBB, 3, "b68", "BUY", 2, 68),
        # -- active interval replaced before the plans start trading --------
        limit_update("u1", AAA, 5, 95, 110),
        vwap_start("w1", BBB, 4, "V", [5, 3, 1, 1], 70, "gamma"),
        twap_start("P", AAA, 6, "P", 6, 3, price=100),
        vwap_slice("v6", BBB, 5, "V"),
        twap_slice("p2", AAA, 7, "P"),
        # -- read-only reports interleaved with the plan lifecycle ---------
        twap_report("qP", AAA, 8, "P"),
        impact("m1", AAA, 9, "BUY", 2, 100),
        pov_start("x1", BBB, 6, "X", 10, 5000, "beta"),
        report("e1", AAA, 10, "a5", 100),
        pov_volume("x8", BBB, 7, "X", 8),
        # -- a structurally legal business rejection occupies id+sequence --
        limit("bad", AAA, 11, "farcry", "SELL", 1, 150),
        vwap_report("qV", BBB, 8, "V"),
        portfolio("f1", AAA, 12, "alpha", {AAA: 100}),
        pov_volume("x0", BBB, 9, "X", 1),
        reconstruction("brA", AAA, 13, 1),
        reconciliation(
            "sr1", BBB, 10,
            expected_trades=[
                {"symbol": AAA, "trade_id": 1, "maker_order_id": "a1",
                 "taker_order_id": "m7", "price": 100, "quantity": 5},
                {"symbol": AAA, "trade_id": 2, "maker_order_id": "ic",
                 "taker_order_id": "m7", "price": 100, "quantity": 2},
                {"symbol": AAA, "trade_id": 3, "maker_order_id": "ic",
                 "taker_order_id": "P#1", "price": 100, "quantity": 1},
                {"symbol": AAA, "trade_id": 4, "maker_order_id": "a5",
                 "taker_order_id": "P#1", "price": 100, "quantity": 1},
                {"symbol": BBB, "trade_id": 1, "maker_order_id": "w1",
                 "taker_order_id": "V#1", "price": 70, "quantity": 4},
                {"symbol": BBB, "trade_id": 2, "maker_order_id": "w1",
                 "taker_order_id": "X#1", "price": 70, "quantity": 4},
            ],
            expected_accounts=[
                {"symbol": AAA, "account_id": "alpha",
                 "net_position": -5, "cash_balance": 500},
                {"symbol": BBB, "account_id": "beta",
                 "net_position": 4, "cash_balance": -280},
                {"symbol": BBB, "account_id": "delta",
                 "net_position": -8, "cash_balance": 560},
                {"symbol": BBB, "account_id": "gamma",
                 "net_position": 4, "cash_balance": -280},
            ],
        ),
        plan_tca("tcaP", AAA, 14, "P", 102),
        limit("r1", AAA, 15, "r1", "SELL", 3, 100),
        twap_slice("p3", AAA, 16, "P"),
        reconstruction("brB", BBB, 11, 0),
        twap_slice("p4", AAA, 17, "P"),
        limit("a9", AAA, 18, "b101", "BUY", 2, 100),
        vwap_slice("v12", BBB, 12, "V"),
        limit("end", AAA, 19, "s103", "SELL", 2, 103),
        portfolio_stress(
            "f2", AAA, 20, "alpha", {AAA: 100},
            [{"name": "down", "prices": {AAA: 95}}],
        ),
    ]


STREAM = _fixed_stream()


def _uninterrupted():
    """The one-shot reference: full response list and final session snapshot."""
    out = replay_events(copy.deepcopy(STREAM), config=CONFIG)
    return out["results"], out["snapshot"]


BASELINE_RESULTS, BASELINE_SNAPSHOT = _uninterrupted()


def _by_id(results):
    return {result["event_id"]: result for result in results}


def replayer_book(snapshot, symbol):
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    return restored.book(symbol)


# ---------------------------------------------------------------------------
# Sanity anchors on the fixed stream: these pin the facts the byte-for-byte
# comparisons below rely on (real trades, a real iceberg reserve, open plans,
# a replaced price-limit band and business rejections inside the stream).
# ---------------------------------------------------------------------------


def test_fixed_stream_has_the_anchor_states_the_suite_depends_on():
    by_id = _by_id(BASELINE_RESULTS)

    # The opening market sweep crosses the best (100) level in queue order.
    assert by_id["t1"]["status"] == ACCEPTED
    assert [(t["maker_order_id"], t["price"], t["quantity"])
            for t in by_id["t1"]["trades"]] == [("a1", 100, 5), ("ic", 100, 2)]
    assert [t["trade_id"] for t in by_id["t1"]["trades"]] == [1, 2]

    # P#1 meets the iceberg's last visible unit first; the exhausted slice
    # replenishes at the tail behind a5, which then gives one unit.
    p2 = by_id["p2"]
    assert [(t["maker_order_id"], t["trade_id"], t["quantity"])
            for t in p2["trades"]] == [("ic", 3, 1), ("a5", 4, 1)]
    assert p2["execution_plan"]["status"] == "ACTIVE"
    assert (p2["execution_plan"]["released_quantity"],
            p2["execution_plan"]["filled_quantity"],
            p2["execution_plan"]["remaining_slices"]) == (2, 2, 2)

    # The read-only execution probe mid-stream sees a5 partly filled.
    e1 = by_id["e1"]["execution_analysis"]
    assert (e1["current_status"], e1["filled_quantity"], e1["open_quantity"]) == (
        "RESTING", 1, 2)

    # P#2 then finishes a5 (trade 5); P#3 takes 2 of the replenished slice
    # (trade 6) and completes the TWAP.
    assert [(t["maker_order_id"], t["trade_id"], t["quantity"])
            for t in by_id["p3"]["trades"]] == [("a5", 5, 2)]
    assert [(t["maker_order_id"], t["trade_id"], t["quantity"])
            for t in by_id["p4"]["trades"]] == [("ic", 6, 2)]
    assert by_id["p4"]["execution_plan"]["status"] == "COMPLETED"

    # The final buy meets the replenished-tail ordering: iceberg first at 100,
    # then r1. Trade ids stay consecutive.
    assert [(t["maker_order_id"], t["trade_id"], t["quantity"])
            for t in by_id["a9"]["trades"]] == [("ic", 7, 1), ("r1", 8, 1)]
    assert by_id["end"]["asks"] == [
        {"price": 100, "quantity": 5}, {"price": 103, "quantity": 2}]

    # BBB: trade ids 1 (VWAP), 2 (POV), 3 (VWAP) stay per-symbol consecutive
    # across plan kinds; the POV plan remains open and the zero-release event
    # submitted no child.
    assert [(t["taker_order_id"], t["trade_id"])
            for t in by_id["v6"]["trades"]] == [("V#1", 1)]
    assert [(t["taker_order_id"], t["trade_id"])
            for t in by_id["x8"]["trades"]] == [("X#1", 2)]
    assert [(t["maker_order_id"], t["trade_id"], t["quantity"])
            for t in by_id["v12"]["trades"]] == [("w1", 3, 2)]
    assert by_id["v12"]["result"] == "PARTIALLY_FILLED_CANCELLED"
    assert by_id["v12"]["execution_plan"]["status"] == "ACTIVE"
    assert by_id["x0"]["trades"] == []
    assert by_id["x0"]["execution_plan"]["child_order_id"] is None
    assert by_id["x0"]["execution_plan"]["status"] == "ACTIVE"

    # The price-limit breach is a committed business rejection that rests
    # nothing and spends no trade id.
    assert by_id["bad"]["status"] == REJECTED
    assert by_id["bad"]["rejection_code"] == "PRICE_LIMIT_EXCEEDED"
    assert by_id["bad"]["trades"] == []
    assert by_id["bad"]["book_changes"] == {"bids": [], "asks": []}

    # Read-only reports are accepted, carry their analysis and moved nothing.
    assert by_id["m1"]["result"] == "REPORTED" and by_id["m1"]["trades"] == []
    assert by_id["qP"]["execution_plan"]["status"] == "ACTIVE"
    assert by_id["qV"]["execution_plan"]["algorithm"] == "VWAP"
    assert by_id["f1"]["result"] == "REPORTED"
    position = by_id["f1"]["portfolio_analysis"]["positions"][0]
    assert (position["symbol"], position["net_position"],
            position["cash_balance"]) == (AAA, -5, 500)
    assert by_id["sr1"]["result"] == "RECONCILED"
    assert by_id["sr1"]["reconciliation"] == {"trade_breaks": [],
                                              "account_breaks": []}
    tca = by_id["tcaP"]["plan_tca_analysis"]
    assert (tca["plan_id"], tca["status"], tca["algorithm"]) == (
        "P", "ACTIVE", "TWAP")
    assert by_id["brA"]["book_reconstruction"]["target_sequence"] == 1
    stress = by_id["f2"]["portfolio_stress_analysis"]
    assert stress["worst_scenario"] == "down"
    assert stress["scenarios"][0]["total_pnl_change"] == 25

    # The resting plain order and the iceberg's hidden reserve both survive.
    assert replayer_book(BASELINE_SNAPSHOT, AAA)[1] == [
        {"price": 100, "quantity": 5}, {"price": 103, "quantity": 2}]
    assert replayer_book(BASELINE_SNAPSHOT, BBB)[0] == [{"price": 68, "quantity": 2}]
    assert replayer_book(BASELINE_SNAPSHOT, BBB)[1] == [{"price": 71, "quantity": 4}]


# ---------------------------------------------------------------------------
# replay_events: every exportable prefix boundary segments without divergence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("boundary", range(len(STREAM) + 1))
def test_replay_events_segmentation_matches_uninterrupted_byte_for_byte(boundary):
    prefix = copy.deepcopy(STREAM[:boundary])
    suffix = copy.deepcopy(STREAM[boundary:])

    snapshot = replay_events(prefix, config=CONFIG)["snapshot"]
    segmented = replay_events(suffix, config=CONFIG, snapshot=snapshot)

    # Every continued per-event response, in order: status, rejection codes,
    # trades with ids and ordering, book diffs, bids/asks, plan summaries and
    # every report's analysis. Canonical bytes tolerate no reordering.
    assert canonical_json(segmented["results"]) == canonical_json(
        BASELINE_RESULTS[boundary:]
    ), f"responses diverge after boundary {boundary}"
    # The final session snapshot is byte-identical too.
    assert canonical_json(segmented["snapshot"]) == canonical_json(BASELINE_SNAPSHOT), (
        f"final snapshot diverges after boundary {boundary}"
    )


def test_trade_ids_and_order_are_continuous_through_every_boundary():
    # An order-sensitive, un-normalized check on its own: the (symbol, trade
    # id) sequence observed after a continuation must be the exact
    # uninterrupted tail, with nothing sorted or deduplicated.
    for boundary in range(len(STREAM) + 1):
        snapshot = replay_events(copy.deepcopy(STREAM[:boundary]),
                                 config=CONFIG)["snapshot"]
        segmented = replay_events(copy.deepcopy(STREAM[boundary:]),
                                  config=CONFIG, snapshot=snapshot)
        tail_ids = [
            (result["symbol"], t["trade_id"])
            for result in segmented["results"]
            for t in result["trades"]
        ]
        # The exact uninterrupted tail at this boundary, nothing normalized.
        expected_tail = [
            (result["symbol"], t["trade_id"])
            for result in BASELINE_RESULTS[boundary:]
            for t in result["trades"]
        ]
        assert tail_ids == expected_tail, boundary


# ---------------------------------------------------------------------------
# EventReplayer incremental submit + export_snapshot/restore_replayer at the
# boundaries specifically named in the contract (resting limit, iceberg with
# hidden reserve, each partly executed open plan, replaced active interval,
# read-only report and legal business rejection).
# ---------------------------------------------------------------------------

NAMED_BOUNDARIES = {
    "after_resting_limits": 1,
    "after_iceberg_partial_consumption": 6,
    "after_price_limit_update": 8,
    "after_vwap_start": 9,
    "after_first_twap_slice_open": 12,
    "after_read_only_twap_report": 13,
    "after_impact_report": 14,
    "after_pov_release_open": 17,
    "after_price_limit_business_rejection": 18,
    "after_vwap_report": 19,
    "after_zero_pov_release": 21,
    "after_book_reconstruction": 22,
    "after_session_reconciliation": 23,
    "after_replenished_iceberg": 29,
    "after_vwap_partial_fill_open": 30,
    "end": len(STREAM),
}


@pytest.mark.parametrize(
    "boundary", sorted(NAMED_BOUNDARIES.values()),
    ids=sorted(NAMED_BOUNDARIES, key=lambda k: NAMED_BOUNDARIES[k]),
)
def test_event_replayer_restore_through_exported_snapshot(boundary):
    # Uninterrupted session driven incrementally through the public class.
    prefix_replayer = EventReplayer(copy.deepcopy(CONFIG))
    prefix_results = prefix_replayer.submit(copy.deepcopy(STREAM[:boundary]))

    # The exported snapshot restores to an equivalent session without replay.
    snapshot = export_snapshot(prefix_replayer)
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))

    # Export is stable across a restore round trip.
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)

    suffix_results = restored.submit(copy.deepcopy(STREAM[boundary:]))
    assert canonical_json(prefix_results + suffix_results) == canonical_json(
        BASELINE_RESULTS
    )
    assert canonical_json(export_snapshot(restored)) == canonical_json(BASELINE_SNAPSHOT)

    # Restore idempotence: restoring the re-exported (final) snapshot changes
    # nothing.
    restored_again = restore_replayer(export_snapshot(restored), copy.deepcopy(CONFIG))
    assert canonical_json(export_snapshot(restored_again)) == canonical_json(
        BASELINE_SNAPSHOT)


# ---------------------------------------------------------------------------
# Historical BOOK_RECONSTRUCTION_REPORT after recovery
# ---------------------------------------------------------------------------


def _last_sequences(results):
    last = {}
    for result in results:
        last[result["symbol"]] = result["sequence"]
    return last.get(AAA, 0), last.get(BBB, 0)


def _reconstruction_probes(last_aaa, last_bbb):
    """Sweep every committed target (plus 0 and one future target) per symbol.

    Envelope sequences follow strictly per symbol. The same probe list is
    handed to the uninterrupted and the recovered session, so the responses
    are directly byte-comparable.
    """
    probes = []
    sequence = last_aaa + 1
    for target in range(0, last_aaa + 1):
        probes.append(reconstruction(f"zpA{sequence}", AAA, sequence, target))
        sequence += 1
    probes.append(reconstruction(f"zpA{sequence}", AAA, sequence, last_aaa + 1))
    sequence = last_bbb + 1
    for target in range(0, last_bbb + 1):
        probes.append(reconstruction(f"zpB{sequence}", BBB, sequence, target))
        sequence += 1
    probes.append(reconstruction(f"zpB{sequence}", BBB, sequence, last_bbb + 1))
    return probes


@pytest.mark.parametrize("boundary", range(len(STREAM) + 1))
def test_book_reconstruction_at_boundary_state_matches_uninterrupted(boundary):
    # Both sessions are stopped exactly at the boundary: one uninterrupted,
    # one restored solely from the boundary snapshot (suffix not submitted).
    last_aaa, last_bbb = _last_sequences(BASELINE_RESULTS[:boundary])
    probes = _reconstruction_probes(last_aaa, last_bbb)

    uninterrupted = EventReplayer(copy.deepcopy(CONFIG))
    uninterrupted.submit(copy.deepcopy(STREAM[:boundary]))
    reference = uninterrupted.submit(copy.deepcopy(probes))

    snapshot = replay_events(copy.deepcopy(STREAM[:boundary]), config=CONFIG)["snapshot"]
    recovered = restore_replayer(snapshot, copy.deepcopy(CONFIG))
    actual = recovered.submit(copy.deepcopy(probes))

    assert canonical_json(actual) == canonical_json(reference), (
        f"historical reconstruction at boundary {boundary} diverges"
    )


RECOVERY_VIEW_BOUNDARIES = [0, 6, 8, 12, 17, 23, 27, 30, len(STREAM)]


@pytest.mark.parametrize("boundary", RECOVERY_VIEW_BOUNDARIES)
def test_book_reconstruction_after_full_continuation_matches_uninterrupted(boundary):
    # Both sessions end in the identical final state — one uninterrupted, one
    # recovered at the boundary and continued. From there they reconstruct the
    # sequences immediately before and after the cut, proving a recovered
    # session reads its own committed history identically.
    last_aaa, last_bbb = _last_sequences(BASELINE_RESULTS)
    probes = _reconstruction_probes(last_aaa, last_bbb)

    uninterrupted = EventReplayer(copy.deepcopy(CONFIG))
    uninterrupted.submit(copy.deepcopy(STREAM))
    reference = uninterrupted.submit(copy.deepcopy(probes))

    snapshot = replay_events(copy.deepcopy(STREAM[:boundary]), config=CONFIG)["snapshot"]
    recovered = restore_replayer(snapshot, copy.deepcopy(CONFIG))
    recovered.submit(copy.deepcopy(STREAM[boundary:]))
    actual = recovered.submit(copy.deepcopy(probes))

    assert canonical_json(actual) == canonical_json(reference), (
        f"post-continuation reconstruction diverges for boundary {boundary}"
    )


def test_recovered_reconstruction_pins_queue_order_iceberg_view_and_band():
    # Explicit content anchors; the byte comparisons above prove these same
    # values hold in the uninterrupted session.
    recovered = restore_replayer(copy.deepcopy(BASELINE_SNAPSHOT), copy.deepcopy(CONFIG))
    last_aaa = next(
        symbol_entry["state"]["last_sequence"]
        for symbol_entry in BASELINE_SNAPSHOT["content"]["symbols"]
        if symbol_entry["symbol"] == AAA
    )
    probe_sequence = last_aaa + 1

    def ask_view(target):
        nonlocal probe_sequence
        out = recovered.submit(
            [reconstruction(f"zq{probe_sequence}", AAA, probe_sequence, target)]
        )[0]
        probe_sequence += 1
        assert out["status"] == ACCEPTED
        return out["book_reconstruction"]

    # Sequence 4 (right after the market sweep): a1 is gone; the iceberg has
    # 8 units total but only one public slice unit, and it leads a5 at 100.
    at_4 = ask_view(4)
    assert at_4["active_price_limits"] == {"lower_price": 90, "upper_price": 120}
    level_100 = [q for q in at_4["ask_queues"] if q["price"] == 100][0]
    assert level_100["visible_quantity"] == 4
    assert [o["order_id"] for o in level_100["orders"]] == ["ic", "a5"]
    assert level_100["orders"][0] == {
        "order_id": "ic", "order_type": "ICEBERG",
        "remaining_quantity": 8, "visible_quantity": 1}

    # Sequence 6 is after PRICE_LIMIT_UPDATE: the historical interval at the
    # target follows the update; the plan start moved no queues.
    assert ask_view(6)["active_price_limits"] == {
        "lower_price": 95, "upper_price": 110}

    # Sequence 7 (after P#1): the exhausted iceberg slice replenished at the
    # tail, so a5's remaining 2 now lead the iceberg's fresh 3-lot slice.
    at_7 = ask_view(7)
    level_100 = [q for q in at_7["ask_queues"] if q["price"] == 100][0]
    assert [o["order_id"] for o in level_100["orders"]] == ["a5", "ic"]
    assert [o["visible_quantity"] for o in level_100["orders"]] == [2, 3]
    assert level_100["orders"][1]["remaining_quantity"] == 7

    # Final point: r1 leads the twice-replenished iceberg (4 total left, a
    # 3-lot slice visible, one still hidden) at 100, s103 rests at 103.
    at_20 = ask_view(20)
    assert [o["order_id"] for q in at_20["ask_queues"] for o in q["orders"]] == [
        "r1", "ic", "s103"]
    level_100 = [q for q in at_20["ask_queues"] if q["price"] == 100][0]
    assert [(o["order_id"], o["remaining_quantity"], o["visible_quantity"])
            for o in level_100["orders"]] == [("r1", 2, 2), ("ic", 4, 3)]

    # Target 0: the empty book and the session-initial configured band.
    at_0 = ask_view(0)
    assert at_0["bid_queues"] == [] and at_0["ask_queues"] == []
    assert at_0["active_price_limits"] == {"lower_price": 90, "upper_price": 120}


def test_recovered_reconstruction_on_bbb_pins_queues_and_target_zero():
    recovered = restore_replayer(copy.deepcopy(BASELINE_SNAPSHOT), copy.deepcopy(CONFIG))
    last_bbb = next(
        symbol_entry["state"]["last_sequence"]
        for symbol_entry in BASELINE_SNAPSHOT["content"]["symbols"]
        if symbol_entry["symbol"] == BBB
    )
    probe_sequence_bbb = last_bbb + 1

    def bbb_view(target):
        nonlocal probe_sequence_bbb
        out = recovered.submit(
            [reconstruction(f"zbq{probe_sequence_bbb}", BBB, probe_sequence_bbb, target)]
        )[0]
        probe_sequence_bbb += 1
        assert out["status"] == ACCEPTED
        return out["book_reconstruction"]

    # BBB has no configured limits: the historical interval is null at every
    # target, and target 0 is the empty book.
    assert bbb_view(0) == {"symbol": BBB, "target_sequence": 0,
                          "active_price_limits": None,
                          "bid_queues": [], "ask_queues": []}
    # At the end w1 is fully gone: only the untouched seller at 71 rests on
    # the ask side and the bid at 68 survives; plan child orders never rest.
    final = bbb_view(last_bbb)
    assert [[o["order_id"] for o in q["orders"]] for q in final["ask_queues"]] == [
        ["s71"]]
    assert final["ask_queues"][0]["visible_quantity"] == 4
    assert [[o["order_id"] for o in q["orders"]] for q in final["bid_queues"]] == [
        ["b68"]]
    assert final["active_price_limits"] is None

    # A target beyond this session's current committed sequence is a business
    # rejection carrying the untouched current book echo (the probes above
    # each occupied a sequence themselves, so the next free target is beyond).
    rejected = recovered.submit(
        [reconstruction("zfuture", BBB, probe_sequence_bbb, probe_sequence_bbb)])[0]
    assert rejected["status"] == REJECTED
    assert rejected["rejection_code"] == "TARGET_SEQUENCE_NOT_FOUND"
    assert rejected["bids"] == [{"price": 68, "quantity": 2}]


# ---------------------------------------------------------------------------
# Envelope precedence and idempotency records across a recovery boundary
# ---------------------------------------------------------------------------


def _precedence_probes():
    """Fixed probes run identically against an uninterrupted and recovered run.

    AAA ends at sequence 20, so the next expected sequence is 21.
    """
    yield copy.deepcopy(STREAM[11])   # p2 (AAA seq 7) verbatim -> DUPLICATE
    yield copy.deepcopy(STREAM[17])   # bad (business-rejected at 11) -> DUPLICATE
    # Same id "a1", different normalized content -> EVENT_ID_CONFLICT.
    yield cancel("a1", AAA, 21, "nobody")
    # Hole: 22 while 21 expected -> SEQUENCE_GAP.
    yield cancel("zgap", AAA, 22, "nobody")
    # Backwards: 5 while 21 expected -> OUT_OF_ORDER.
    yield cancel("zooo", AAA, 5, "nobody")
    # The legal 21 event then applies: a read-only impact report.
    yield impact("zlegit21", AAA, 21, "BUY", 1, 100)
    # A backwards delivery after the commit is still rejected...
    yield cancel("zooo2", AAA, 1, "nobody")
    # ...and sequence 22 then commits normally.
    yield report("zlegit22", AAA, 22, "a5", 100)


def test_envelope_precedence_and_idempotency_survive_recovery():
    probes = list(_precedence_probes())

    uninterrupted = EventReplayer(copy.deepcopy(CONFIG))
    uninterrupted.submit(copy.deepcopy(STREAM))
    reference = uninterrupted.submit(copy.deepcopy(probes))

    recovered = restore_replayer(copy.deepcopy(BASELINE_SNAPSHOT), copy.deepcopy(CONFIG))
    actual = recovered.submit(copy.deepcopy(probes))

    assert [r["status"] for r in reference] == [
        DUPLICATE, DUPLICATE, REJECTED, REJECTED, REJECTED,
        ACCEPTED, REJECTED, ACCEPTED,
    ]
    assert [r.get("rejection_code") for r in reference] == [
        None, None, EVENT_ID_CONFLICT, SEQUENCE_GAP, OUT_OF_ORDER,
        None, OUT_OF_ORDER, None,
    ]
    # The gap/backwards envelopes name the expected next sequence.
    assert reference[3]["expected_sequence"] == 21
    assert reference[4]["expected_sequence"] == 21
    assert reference[6]["expected_sequence"] == 22
    # Byte-identical envelopes, including echoed books and expected_sequence.
    assert canonical_json(actual) == canonical_json(reference)

    # None of the error responses creates a trade or moves the book: the two
    # sessions still export byte-identical final snapshots after the probes.
    assert canonical_json(export_snapshot(recovered)) == canonical_json(
        export_snapshot(uninterrupted)
    )

    # Explicit no-mutation check on the recovered session around each error.
    checker = restore_replayer(copy.deepcopy(BASELINE_SNAPSHOT), copy.deepcopy(CONFIG))
    for probe in probes[:5] + [probes[6]]:
        before = canonical_json(checker.book(AAA))
        result = checker.submit([copy.deepcopy(probe)])[0]
        assert result["trades"] == []
        assert result["book_changes"] == {"bids": [], "asks": []}
        assert canonical_json(checker.book(AAA)) == before


# ---------------------------------------------------------------------------
# INVALID_EVENT occupancy vs business-rejection occupancy, across a boundary
# ---------------------------------------------------------------------------


def test_invalid_event_consumes_nothing_but_business_rejection_consumes_both():
    # Prefix length 17 ends right before the stream's business rejection
    # (bad is index 17): AAA is at sequence 10.
    boundary = 17

    def fresh_session():
        replayer = EventReplayer(copy.deepcopy(CONFIG))
        replayer.submit(copy.deepcopy(STREAM[:boundary]))
        return replayer

    invalid_event = {
        "event_id": "structX", "symbol": AAA, "sequence": 11,
        "type": "ADD", "order_id": "ox", "side": "BUY",
        "order_type": "LIMIT", "quantity": "not-an-int", "price": 100,
    }
    # The same id and sequence become legal immediately afterwards: the
    # structural error occupied neither.
    reused_event = limit("structX", AAA, 11, "ox", "BUY", 1, 99)

    uninterrupted = fresh_session()
    reference_pair = uninterrupted.submit(
        [copy.deepcopy(invalid_event), copy.deepcopy(reused_event)])
    assert [r["status"] for r in reference_pair] == [REJECTED, ACCEPTED]
    assert reference_pair[0]["rejection_code"] == INVALID_EVENT
    assert reference_pair[1]["result"] == "RESTING"

    snapshot = replay_events(copy.deepcopy(STREAM[:boundary]), config=CONFIG)["snapshot"]
    recovered = restore_replayer(snapshot, copy.deepcopy(CONFIG))
    segmented_pair = recovered.submit(
        [copy.deepcopy(invalid_event), copy.deepcopy(reused_event)])
    # Uninterrupted vs segmented observations are byte-identical, and the
    # legal event that follows the invalid one rests at the same queue spot.
    assert canonical_json(segmented_pair) == canonical_json(reference_pair)

    # The stream's structurally legal rejection (bad @ AAA seq 11) did
    # occupy both: replaying it verbatim after segmentation is a DUPLICATE,
    # and the next AAA event must use sequence 12 exactly as uninterrupted.
    prefix_with_rejection = replay_events(
        copy.deepcopy(STREAM[:18]), config=CONFIG)["snapshot"]
    tail = [
        copy.deepcopy(STREAM[17]),                          # bad verbatim
        cancel("late11", AAA, 11, "x"),                     # stale -> OUT_OF_ORDER
        impact("after12", AAA, 12, "BUY", 1, 100),          # legal seq 12
    ]

    recovered = restore_replayer(prefix_with_rejection, copy.deepcopy(CONFIG))
    actual = recovered.submit(copy.deepcopy(tail))
    assert [r["status"] for r in actual] == [DUPLICATE, REJECTED, ACCEPTED]
    assert actual[1]["rejection_code"] == OUT_OF_ORDER
    assert actual[1]["expected_sequence"] == 12

    uninterrupted = EventReplayer(copy.deepcopy(CONFIG))
    uninterrupted.submit(copy.deepcopy(STREAM[:18]))
    reference = uninterrupted.submit(copy.deepcopy(tail))
    assert canonical_json(actual) == canonical_json(reference)


# ---------------------------------------------------------------------------
# events command line entry point: segmented requests match the Python entry
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]


def _serve_document(document):
    """Run the in-process CLI implementation over one JSON document."""
    text = json.dumps(document, ensure_ascii=False)
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = event_cli.serve_events(stdin, stdout, stderr)
    stdout.flush()
    return code, stdout.buffer.getvalue(), stderr.getvalue()


def _run_events_subprocess(document, hash_seed):
    text = json.dumps(document, ensure_ascii=False)
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hash_seed)
    code_text = (
        "from order_book_engine.cli import main; "
        "import sys; sys.exit(main(['events']))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code_text],
        input=text.encode("utf-8"),
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


@pytest.mark.parametrize("boundary", [0, 8, 14, 19, 25, len(STREAM)])
def test_events_cli_segmented_requests_match_python_entry(boundary):
    prefix = copy.deepcopy(STREAM[:boundary])
    suffix = copy.deepcopy(STREAM[boundary:])

    # Python entry point references.
    first = replay_events(prefix, config=CONFIG)
    continued = replay_events(suffix, config=CONFIG, snapshot=first["snapshot"])
    one_shot = replay_events(copy.deepcopy(STREAM), config=CONFIG)

    expected_first = canonical_json(first) + b"\n"
    expected_continued = canonical_json(continued) + b"\n"
    expected_one_shot = canonical_json(one_shot) + b"\n"

    # In-process CLI: canonical JSON, exit code 0 and empty stderr.
    code, out, err = _serve_document({"events": prefix, "config": CONFIG})
    assert (code, err) == (0, "")
    assert out == expected_first
    parsed_first = json.loads(out.decode("utf-8"))

    code, out, err = _serve_document(
        {"events": suffix, "config": CONFIG, "snapshot": parsed_first["snapshot"]})
    assert (code, err) == (0, "")
    assert out == expected_continued
    assert canonical_json(json.loads(out.decode("utf-8"))["results"]) == canonical_json(
        one_shot["results"][boundary:])

    code, out, err = _serve_document(
        {"events": copy.deepcopy(STREAM), "config": CONFIG})
    assert (code, err) == (0, "")
    assert out == expected_one_shot

    # Real subprocess under several hash seeds: same bytes, code, empty stderr.
    first_outputs = {}
    for seed in (0, 1, 7, 12345):
        returncode, stdout, stderr = _run_events_subprocess(
            {"events": prefix, "config": CONFIG}, seed)
        assert returncode == 0 and stderr == b""
        first_outputs[seed] = stdout
    assert set(first_outputs.values()) == {expected_first}

    returncode, stdout, stderr = _run_events_subprocess(
        {"events": suffix, "config": CONFIG, "snapshot": first["snapshot"]}, 7)
    assert returncode == 0 and stderr == b""
    assert stdout == expected_continued

    returncode, stdout, stderr = _run_events_subprocess(
        {"events": copy.deepcopy(STREAM), "config": CONFIG}, 12345)
    assert returncode == 0 and stderr == b""
    assert stdout == expected_one_shot


def test_events_cli_snapshot_after_marker_round_trip_segment():
    # The snapshot_after marker itself must produce a segment boundary that
    # resumes byte-identically through both the Python entry and the CLI.
    marked = replay_events(
        copy.deepcopy(STREAM), config=CONFIG,
        snapshot_after={"symbol": BBB, "sequence": 7})
    assert marked["snapshot"] is not None
    marker_snapshot = marked["snapshot"]

    boundary = next(
        index for index, event in enumerate(STREAM)
        if event["symbol"] == BBB and event["sequence"] == 7
    ) + 1
    continued = replay_events(
        copy.deepcopy(STREAM[boundary:]), config=CONFIG, snapshot=marker_snapshot)
    assert canonical_json(continued["results"]) == canonical_json(
        BASELINE_RESULTS[boundary:])

    code, out, err = _serve_document(
        {"events": copy.deepcopy(STREAM[boundary:]), "config": CONFIG,
         "snapshot": marker_snapshot})
    assert (code, err) == (0, "")
    assert out == canonical_json(continued) + b"\n"
