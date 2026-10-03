"""Deterministic regression tests driven by fixed order-event sequences.

Scope (see README "replay 输入输出约定"): only the public baseline entries are
exercised — ADD / CANCEL / REPLACE for submission and EXECUTION_REPORT plus the
post-event ``bids``/``asks`` book projection for observation. Every assertion
uses documented fields, order types (LIMIT / MARKET), time-in-force values
(GTC / IOC / FOK), result names and rejection reasons. No internal container,
private helper or undocumented invalid input is pinned as a contract.

Each scenario is checked three ways:

1. Exact per-event expectations: result, the ordered new trades (including
   library-generated ``trade_id`` values) and the full book after the event
   (best prices and visible quantities at every level).
2. A continuous run versus prefix-replay runs: at every event boundary a
   fresh equivalent book is created by replaying the existing events from the
   start (the baseline exposes no state-snapshot import, so no production
   interface is added), then both paths are compared on book, per-order
   status/fills and the cumulative ordered trade stream.
3. Repeated execution: the same sequence is replayed several times through
   the engine and through the ``replay`` JSON Lines CLI; all public results,
   including trade ids and trade order, are byte-for-byte identical.

The tests never read the clock, use randomness, threads, dict-iteration
order (expectations are explicit lists) or the filesystem/process path.
"""

from __future__ import annotations

import io
import json

import pytest

from order_book_engine import replay
from order_book_engine.engine import Engine

# Result and reason names are imported from the public engine module so a
# contract rename fails loudly instead of silently weakening an assertion.
from order_book_engine.engine import (
    CANCELLED,
    DUPLICATE_EVENT_ID,
    DUPLICATE_ORDER_ID,
    FILLED,
    PARTIALLY_FILLED_CANCELLED,
    PARTIALLY_FILLED_RESTING,
    REJECTED,
    REPLACED,
    RESTING,
    UNFILLED_CANCELLED,
    UNKNOWN_ORDER,
)


# --------------------------------------------------------------------------
# Public event builders (only fields the README allows for each event type)
# --------------------------------------------------------------------------

def add(event_id, order_id, side, quantity, price=None, time_in_force=None):
    obj = {
        "event_id": event_id,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "quantity": quantity,
    }
    if price is not None:
        obj["order_type"] = "LIMIT"
        obj["price"] = price
    else:
        obj["order_type"] = "MARKET"
    if time_in_force is not None:
        obj["time_in_force"] = time_in_force
    return obj


def cancel(event_id, order_id):
    return {"event_id": event_id, "type": "CANCEL", "order_id": order_id}


def replace(event_id, order_id, quantity, price):
    return {
        "event_id": event_id,
        "type": "REPLACE",
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
    }


def execution_report(event_id, order_id, benchmark_price=100):
    return {
        "event_id": event_id,
        "type": "EXECUTION_REPORT",
        "order_id": order_id,
        "benchmark_price": benchmark_price,
    }


def trade(trade_id, maker, taker, price, quantity):
    return {
        "trade_id": trade_id,
        "maker_order_id": maker,
        "taker_order_id": taker,
        "price": price,
        "quantity": quantity,
    }


def levels(*pairs):
    """A public book side as a list of {"price", "quantity"} levels."""
    return [{"price": price, "quantity": qty} for price, qty in pairs]


# --------------------------------------------------------------------------
# Fixed scenarios. Each row is (event, expected result, expected new trades,
# expected bids after, expected asks after).
# --------------------------------------------------------------------------

# A. Same-price orders fill strictly in arrival (price-time) order; the last
#    buyer's unfilled remainder enters the book.
SCENARIO_FIFO = [
    (add("e1", "s1", "SELL", 3, 100), RESTING, [], levels(), levels((100, 3))),
    (add("e2", "s2", "SELL", 3, 100), RESTING, [], levels(), levels((100, 6))),
    (add("e3", "s3", "SELL", 2, 100), RESTING, [], levels(), levels((100, 8))),
    (
        add("e4", "b1", "BUY", 5, 100),
        FILLED,
        [trade(1, "s1", "b1", 100, 3), trade(2, "s2", "b1", 100, 2)],
        levels(),
        levels((100, 3)),
    ),
    (
        add("e5", "b2", "BUY", 5, 100),
        PARTIALLY_FILLED_RESTING,
        [trade(3, "s2", "b2", 100, 1), trade(4, "s3", "b2", 100, 2)],
        levels((100, 2)),
        levels(),
    ),
]

# B. A sweep across several price levels with partial fills inside a level;
#    the taker remainder rests, and is later hit by an arriving seller.
SCENARIO_SWEEP = [
    (add("e1", "s1", "SELL", 2, 100), RESTING, [], levels(), levels((100, 2))),
    (add("e2", "s2", "SELL", 4, 101), RESTING, [], levels(),
     levels((100, 2), (101, 4))),
    (add("e3", "s3", "SELL", 1, 101), RESTING, [], levels(),
     levels((100, 2), (101, 5))),
    (
        add("e4", "b1", "BUY", 8, 102),
        PARTIALLY_FILLED_RESTING,
        [
            trade(1, "s1", "b1", 100, 2),
            trade(2, "s2", "b1", 101, 4),
            trade(3, "s3", "b1", 101, 1),
        ],
        levels((102, 1)),
        levels(),
    ),
    (
        add("e5", "s4", "SELL", 3, 102),
        PARTIALLY_FILLED_RESTING,
        [trade(4, "b1", "s4", 102, 1)],
        levels(),
        levels((102, 2)),
    ),
]

# C. Cancel of an unfilled order and cancel after a partial fill, followed by
#    every rejection the README pins a unique result to. None of the rejected
#    events may move the book, an order status or the trade set / id counter.
SCENARIO_CANCEL_AND_REJECT = [
    (add("e1", "s0", "SELL", 2, 105), RESTING, [], levels(), levels((105, 2))),
    (add("e2", "s1", "SELL", 5, 100), RESTING, [],
     levels(), levels((100, 5), (105, 2))),
    (add("e3", "s2", "SELL", 3, 100), RESTING, [],
     levels(), levels((100, 8), (105, 2))),
    # Unfilled remainder of a never-touched order is cancelled.
    (cancel("e4", "s0"), CANCELLED, [], levels(), levels((100, 8))),
    # Partial fill of s1/s2; s2 keeps a 2-unit remainder.
    (
        add("e5", "b1", "BUY", 6, 100),
        FILLED,
        [trade(1, "s1", "b1", 100, 5), trade(2, "s2", "b1", 100, 1)],
        levels(),
        levels((100, 2)),
    ),
    # Cancel of an already partially filled order.
    (cancel("e6", "s2"), CANCELLED, [], levels(), levels()),
    # --- Documented rejections; the book stays empty for all of them. ---
    (cancel("e7", "s2"), REJECTED, [], levels(), levels()),       # repeated cancel
    (cancel("e8", "ghost"), REJECTED, [], levels(), levels()),    # unknown order
    (replace("e9", "s2", 1, 100), REJECTED, [], levels(), levels()),
    (replace("e10", "s1", 1, 100), REJECTED, [], levels(), levels()),  # filled target
    (add("e1", "s9", "BUY", 1, 100), REJECTED, [], levels(), levels()),  # dup event id
    (add("e11", "s1", "BUY", 1, 100), REJECTED, [], levels(), levels()),  # dup order id
    (execution_report("e12", "ghost"), REJECTED, [], levels(), levels()),
    # The next real fill must take trade_id 3: rejected events spent no id.
    (add("e13", "s3", "SELL", 2, 100), RESTING, [], levels(), levels((100, 2))),
    (
        add("e14", "b3", "BUY", 2),
        FILLED,
        [trade(3, "s3", "b3", 100, 2)],
        levels(),
        levels(),
    ),
]
# Reason expectations for the rejected rows above, keyed by event id.
REJECT_REASONS = {
    "e7": UNKNOWN_ORDER,
    "e8": UNKNOWN_ORDER,
    "e9": UNKNOWN_ORDER,
    "e10": UNKNOWN_ORDER,
    "e1": DUPLICATE_EVENT_ID,
    "e11": DUPLICATE_ORDER_ID,
    "e12": UNKNOWN_ORDER,
}

# D. REPLACE loses queue priority even with unchanged parameters, can cross the
#    spread as an arriving order, and can be applied after a partial fill.
SCENARIO_REPLACE = [
    (add("e1", "s1", "SELL", 5, 100), RESTING, [], levels(), levels((100, 5))),
    (add("e2", "s2", "SELL", 5, 100), RESTING, [], levels(), levels((100, 10))),
    # s1 rejoins at the tail of the 100 level behind s2.
    (replace("e3", "s1", 5, 100), REPLACED, [], levels(), levels((100, 10))),
    (
        add("e4", "b1", "BUY", 5, 100),
        FILLED,
        [trade(1, "s2", "b1", 100, 5)],
        levels(),
        levels((100, 5)),
    ),
    (
        add("e5", "b2", "BUY", 5, 100),
        FILLED,
        [trade(2, "s1", "b2", 100, 5)],
        levels(),
        levels(),
    ),
    (add("e6", "b3", "BUY", 3, 99), RESTING, [], levels((99, 3)), levels()),
    (add("e7", "s3", "SELL", 5, 101), RESTING, [],
     levels((99, 3)), levels((101, 5))),
    # Replacement crosses the spread: trades 3 at the resting buyer's price 99
    # and re-rests the 2-unit remainder at 99.
    (
        replace("e8", "s3", 5, 99),
        PARTIALLY_FILLED_RESTING,
        [trade(3, "b3", "s3", 99, 3)],
        levels(),
        levels((99, 2)),
    ),
    (add("e9", "s4", "SELL", 4, 98), RESTING, [],
     levels(), levels((98, 4), (99, 2))),
    (
        add("e10", "b4", "BUY", 6, 98),
        PARTIALLY_FILLED_RESTING,
        [trade(4, "s4", "b4", 98, 4)],
        levels((98, 2)),
        levels((99, 2)),
    ),
    # Replace a partially filled resting order (s3: 3 filled, 2 open @99):
    # the 99 remainder leaves first, the new sell matches the 98 bid for 2 and
    # a 2-unit remainder rests at 98.
    (
        replace("e11", "s3", 4, 98),
        PARTIALLY_FILLED_RESTING,
        [trade(5, "b4", "s3", 98, 2)],
        levels(),
        levels((98, 2)),
    ),
]

# E. Immediate (IOC / MARKET) and all-or-none (FOK) public constraints.
SCENARIO_TIF = [
    (add("e1", "s1", "SELL", 2, 100), RESTING, [], levels(), levels((100, 2))),
    (add("e2", "s2", "SELL", 3, 101), RESTING, [],
     levels(), levels((100, 2), (101, 3))),
    # IOC buy limited at 100 takes only s1; leftover is cancelled, never booked.
    (
        add("e3", "b1", "BUY", 5, 100, time_in_force="IOC"),
        PARTIALLY_FILLED_CANCELLED,
        [trade(1, "s1", "b1", 100, 2)],
        levels(),
        levels((101, 3)),
    ),
    # Market order sweeps s2 and has its leftover cancelled.
    (
        add("e4", "b2", "BUY", 5),
        PARTIALLY_FILLED_CANCELLED,
        [trade(2, "s2", "b2", 101, 3)],
        levels(),
        levels(),
    ),
    (add("e5", "s3", "SELL", 2, 100), RESTING, [], levels(), levels((100, 2))),
    # FOK short of liquidity: nothing trades and the book is untouched.
    (add("e6", "b3", "BUY", 5, 100, time_in_force="FOK"),
     UNFILLED_CANCELLED, [], levels(), levels((100, 2))),
    # FOK whose liquidity exists only outside its limit is likewise atomic.
    (add("e7", "b4", "BUY", 2, 99, time_in_force="FOK"),
     UNFILLED_CANCELLED, [], levels(), levels((100, 2))),
    (add("e8", "s4", "SELL", 1, 101), RESTING, [],
     levels(), levels((100, 2), (101, 1))),
    # FOK succeeds across levels in one event; ids 3/4 prove the two failed FOK
    # events spent no trade identifiers.
    (
        add("e9", "b5", "BUY", 3, 101, time_in_force="FOK"),
        FILLED,
        [trade(3, "s3", "b5", 100, 2), trade(4, "s4", "b5", 101, 1)],
        levels(),
        levels(),
    ),
    # Market order against an empty book.
    (add("e10", "b6", "SELL", 1), UNFILLED_CANCELLED, [], levels(), levels()),
    (add("e11", "s5", "SELL", 1, 100), RESTING, [], levels(), levels((100, 1))),
    # Fully filled IOC reports FILLED.
    (
        add("e12", "b7", "BUY", 1, 100, time_in_force="IOC"),
        FILLED,
        [trade(5, "s5", "b7", 100, 1)],
        levels(),
        levels(),
    ),
]

SCENARIOS = {
    "fifo_same_price": SCENARIO_FIFO,
    "multi_level_sweep": SCENARIO_SWEEP,
    "cancel_and_documented_rejects": SCENARIO_CANCEL_AND_REJECT,
    "replace_priority": SCENARIO_REPLACE,
    "immediate_and_full_quantity": SCENARIO_TIF,
}


# --------------------------------------------------------------------------
# Harness: run a sequence through public entries and capture observables
# --------------------------------------------------------------------------

def _send(engine, obj):
    """Process one event object via the public JSON Lines entry semantics."""
    return engine.handle_line_extended(json.dumps(obj, ensure_ascii=False))


def _observe(engine, boundary, order_ids):
    """Public state at a boundary: book plus a per-order execution query.

    Observation query ids live in a reserved ``q-<boundary>-`` namespace that
    never appears in a scenario's business events, so they cannot perturb any
    business result, trade id or book level.
    """
    bids, asks = engine.snapshot()
    reports = {}
    for order_id in order_ids:
        _eid, result, reason, _trades, _stp, analysis = _send(
            engine, execution_report(f"q-{boundary}-{order_id}", order_id)
        )
        reports[order_id] = (result, reason, analysis)
    return {"bids": bids, "asks": asks, "reports": reports}


def run_continuous(rows):
    """Process every business event once, capturing state at each boundary."""
    engine = Engine()
    outputs = []
    boundaries = []
    order_ids_seen = []
    trade_stream = []
    for index, (event, _result, _trades, _bids, _asks) in enumerate(rows, start=1):
        eid, result, reason, new_trades, stp, _analysis = _send(engine, event)
        # Scenarios use no account_id, so self-trade prevention never appears.
        assert stp is None
        outputs.append({
            "event_id": eid,
            "result": result,
            "reason": reason,
            "trades": new_trades,
        })
        trade_stream.extend(new_trades)
        if event["type"] == "ADD" and result != REJECTED and event["order_id"] not in order_ids_seen:
            order_ids_seen.append(event["order_id"])
        state = _observe(engine, index, order_ids_seen)
        state["trade_stream"] = list(trade_stream)
        boundaries.append(state)
    return outputs, boundaries, order_ids_seen


def run_prefix(rows, cutoff, order_ids_at_boundary):
    """Recreate an equivalent book by replaying the prefix from the start."""
    engine = Engine()
    outputs = []
    trade_stream = []
    for event, _result, _trades, _bids, _asks in rows[:cutoff]:
        eid, result, reason, new_trades, _stp, _analysis = _send(engine, event)
        outputs.append({
            "event_id": eid,
            "result": result,
            "reason": reason,
            "trades": new_trades,
        })
        trade_stream.extend(new_trades)
    state = _observe(engine, cutoff, order_ids_at_boundary)
    state["trade_stream"] = trade_stream
    return outputs, state


# --------------------------------------------------------------------------
# 1. Exact per-event expectations
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", list(SCENARIOS))
def test_every_event_matches_documented_result_trades_and_book(name):
    rows = SCENARIOS[name]
    outputs, boundaries, _ids = run_continuous(rows)
    for index, (row, output, state) in enumerate(
        zip(rows, outputs, boundaries), start=1
    ):
        _event, exp_result, exp_trades, exp_bids, exp_asks = row
        assert output["result"] == exp_result, (name, index, output)
        # Trades are compared as an ordered list: order, ids, prices, quantities.
        assert output["trades"] == exp_trades, (name, index, output["trades"])
        assert state["bids"] == exp_bids, (name, index, state["bids"])
        assert state["asks"] == exp_asks, (name, index, state["asks"])
        assert output["reason"] is None or output["result"] == REJECTED


def test_documented_rejection_reasons_are_the_unique_contract():
    rows = SCENARIO_CANCEL_AND_REJECT
    outputs, _boundaries, _ids = run_continuous(rows)
    reasons = {
        output["event_id"]: output["reason"]
        for output in outputs
        if output["result"] == REJECTED
    }
    assert reasons == REJECT_REASONS


def test_rejected_events_change_nothing_observable():
    rows = SCENARIO_CANCEL_AND_REJECT
    outputs, boundaries, order_ids = run_continuous(rows)
    # Rejections occupy event indices 6..12 (event ids e7..e12, zero-based 6..12).
    for index in range(6, 13):
        assert outputs[index]["result"] == REJECTED
        assert outputs[index]["trades"] == []
        # The book immediately before and after the rejected event is identical.
        assert boundaries[index]["bids"] == boundaries[index - 1]["bids"]
        assert boundaries[index]["asks"] == boundaries[index - 1]["asks"]
        # The cumulative trade set and every order's status/fills are frozen.
        assert (
            boundaries[index]["trade_stream"]
            == boundaries[index - 1]["trade_stream"]
        )
        assert boundaries[index]["reports"] == boundaries[index - 1]["reports"]


def test_failed_fok_and_rejects_do_not_consume_trade_ids():
    # Explicitly pins the README rule: failed FOK (scenario E, e6/e7) and all
    # rejected events (scenario C) leave the next trade id untouched.
    outputs_e, _, _ = run_continuous(SCENARIO_TIF)
    assert [t["trade_id"] for t in outputs_e[8]["trades"]] == [3, 4]
    outputs_c, _, _ = run_continuous(SCENARIO_CANCEL_AND_REJECT)
    assert outputs_c[14]["trades"] == [trade(3, "s3", "b3", 100, 2)]


# --------------------------------------------------------------------------
# 2. Continuous processing versus replay-from-scratch at every boundary
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", list(SCENARIOS))
def test_prefix_replay_matches_continuous_run_at_every_boundary(name):
    rows = SCENARIOS[name]
    outputs, boundaries, _ids = run_continuous(rows)
    for cutoff in range(1, len(rows) + 1):
        # Only orders introduced by an accepted ADD at this boundary are
        # publicly known; a rejected ADD's id is not a queryable order.
        known = _accepted_order_ids(rows[:cutoff], outputs[:cutoff])
        prefix_outputs, prefix_state = run_prefix(rows, cutoff, known)
        continuous_outputs = outputs[:cutoff]
        continuous_state = boundaries[cutoff - 1]

        # Same per-event results and same ordered trade increments.
        assert prefix_outputs == continuous_outputs, (name, cutoff)
        # Same book (best prices and visible quantities at every level).
        assert prefix_state["bids"] == continuous_state["bids"]
        assert prefix_state["asks"] == continuous_state["asks"]
        # Same per-order statuses, remainders and fill attributions.
        assert prefix_state["reports"] == continuous_state["reports"]
        # Same cumulative trade set, in the same order, with the same ids.
        assert prefix_state["trade_stream"] == continuous_state["trade_stream"]


def test_recreated_book_after_partial_fill_then_cancel_and_replace():
    # The boundary combination explicitly called out: partial fill followed by
    # cancel (scenario C) and by replace (scenario D, s3 twice replaced).
    c_outputs, c_boundaries, _ = run_continuous(SCENARIO_CANCEL_AND_REJECT)
    # After e5 (index 4) s2 is partially filled; after e6 (index 5) cancelled.
    s2_partial = c_boundaries[4]["reports"]["s2"][2]
    assert s2_partial["current_status"] == RESTING
    assert (s2_partial["filled_quantity"], s2_partial["open_quantity"]) == (1, 2)
    s2_cancelled = c_boundaries[5]["reports"]["s2"][2]
    assert s2_cancelled["current_status"] == CANCELLED
    assert (s2_cancelled["filled_quantity"], s2_cancelled["open_quantity"]) == (1, 0)
    for cutoff in (5, 6):
        _out, state = run_prefix(
            SCENARIO_CANCEL_AND_REJECT, cutoff, ["s0", "s1", "s2", "b1"]
        )
        assert state["reports"] == c_boundaries[cutoff - 1]["reports"]
        assert state["trade_stream"] == c_boundaries[cutoff - 1]["trade_stream"]

    d_outputs, d_boundaries, _ = run_continuous(SCENARIO_REPLACE)
    # s3 after first replace (boundary 8): 3 filled, 2 open.
    r1 = d_boundaries[7]["reports"]["s3"][2]
    assert r1["current_status"] == RESTING
    assert (r1["filled_quantity"], r1["open_quantity"]) == (3, 2)
    # s3 after second replace (boundary 11): 5 filled cumulatively, 2 open.
    r2 = d_boundaries[10]["reports"]["s3"][2]
    assert r2["current_status"] == RESTING
    assert (r2["filled_quantity"], r2["open_quantity"]) == (5, 2)
    for cutoff in (8, 11):
        known = ["s1", "s2", "b1", "b2", "b3", "s3"] + (
            ["s4", "b4"] if cutoff == 11 else []
        )
        _out, state = run_prefix(SCENARIO_REPLACE, cutoff, known)
        assert state["reports"] == d_boundaries[cutoff - 1]["reports"]
        assert state["trade_stream"] == d_boundaries[cutoff - 1]["trade_stream"]
        assert state["asks"] == d_boundaries[cutoff - 1]["asks"]


# --------------------------------------------------------------------------
# 3. Repeat determinism, including byte-level JSON Lines CLI determinism
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", list(SCENARIOS))
def test_repeated_engine_runs_are_identical(name):
    rows = SCENARIOS[name]
    first_outputs, first_boundaries, ids = run_continuous(rows)
    for _ in range(3):
        outputs, boundaries, _ = run_continuous(rows)
        assert outputs == first_outputs
        assert boundaries == first_boundaries


def _run_cli(rows):
    text = "\n".join(
        json.dumps(event, ensure_ascii=False) for event, *_ in rows
    ) + "\n"
    stdin = io.TextIOWrapper(io.BytesIO(text.encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = replay.replay(stdin, stdout, stderr)
    stdout.flush()
    return code, stdout.buffer.getvalue(), stderr.getvalue()


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_cli_output_is_byte_identical_across_repeats(name):
    rows = SCENARIOS[name]
    code, out1, err = _run_cli(rows)
    assert (code, err) == (0, "")
    records1 = [json.loads(line) for line in out1.decode("utf-8").splitlines()]
    for _ in range(3):
        code, out_n, err_n = _run_cli(rows)
        assert (code, err_n) == (0, "")
        assert out_n == out1  # byte-for-byte: key order, integers, no floats

    # CLI records must agree with the engine-level public observations.
    outputs, boundaries, _ids = run_continuous(rows)
    assert len(records1) == len(rows)
    for record, output, state, row in zip(records1, outputs, boundaries, rows):
        _event, _result, _trades, exp_bids, exp_asks = row
        assert record["event_id"] == output["event_id"]
        assert record["result"] == output["result"]
        assert record["trades"] == output["trades"]
        assert record["bids"] == state["bids"] == exp_bids
        assert record["asks"] == state["asks"] == exp_asks


# --------------------------------------------------------------------------
# Quantity conservation across submitted / traded / cancelled / resting
# --------------------------------------------------------------------------

def _accepted_order_ids(rows, outputs):
    ids = []
    for row, output in zip(rows, outputs):
        event = row[0]
        if event["type"] == "ADD" and output["result"] != REJECTED:
            if event["order_id"] not in ids:
                ids.append(event["order_id"])
    return ids


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_quantity_conservation_inputs_equal_trades_plus_open_plus_cancelled(name):
    rows = SCENARIOS[name]
    outputs, boundaries, _ids = run_continuous(rows)
    order_ids = _accepted_order_ids(rows, outputs)

    introduced = {oid: 0 for oid in order_ids}
    removed_outside_trades = {oid: 0 for oid in order_ids}

    for index, (row, output) in enumerate(zip(rows, outputs)):
        event = row[0]
        result, new_trades = output["result"], output["trades"]
        if result == REJECTED:
            continue
        if event["type"] in ("ADD", "REPLACE"):
            # A REPLACE's quantity is a brand-new remaining total; the replaced
            # remainder is measured separately below.
            introduced[event["order_id"]] += event["quantity"]
        if event["type"] == "REPLACE":
            prior = boundaries[index - 1]["reports"][event["order_id"]][2]
            removed_outside_trades[event["order_id"]] += prior["open_quantity"]
        if event["type"] == "CANCEL" and result == CANCELLED:
            prior = boundaries[index - 1]["reports"][event["order_id"]][2]
            removed_outside_trades[event["order_id"]] += prior["open_quantity"]
        if result == UNFILLED_CANCELLED:
            removed_outside_trades[event["order_id"]] += event["quantity"]
        elif result == PARTIALLY_FILLED_CANCELLED:
            traded = sum(t["quantity"] for t in new_trades)
            removed_outside_trades[event["order_id"]] += event["quantity"] - traded

    final_reports = boundaries[-1]["reports"]
    total_traded_qty = sum(t["quantity"] for t in boundaries[-1]["trade_stream"])

    sum_filled = 0
    sum_open = 0
    for oid in order_ids:
        result, reason, analysis = final_reports[oid]
        assert (result, reason) == ("REPORTED", None)
        filled = analysis["filled_quantity"]
        open_qty = analysis["open_quantity"]
        # Per-order identity: every introduced unit is filled, resting or
        # explicitly removed (IOC/FOK/market leftover, cancel, replaced away).
        assert (
            filled + open_qty + removed_outside_trades[oid] == introduced[oid]
        ), (name, oid, filled, open_qty, removed_outside_trades[oid], introduced[oid])
        sum_filled += filled
        sum_open += open_qty

    # Every trade fills exactly one maker and one taker unit.
    assert sum_filled == 2 * total_traded_qty
    # Resting remainders equal the visible book totals (no icebergs in scope).
    visible = sum(level["quantity"] for level in boundaries[-1]["bids"]) + sum(
        level["quantity"] for level in boundaries[-1]["asks"]
    )
    assert sum_open == visible


# --------------------------------------------------------------------------
# Tie-break stability when several orders share a price (arrival order wins)
# --------------------------------------------------------------------------

def test_same_price_tie_break_is_stable_across_recreation():
    # Three sellers share price 100; an exact-sequence buy must take them in
    # arrival order both continuously and after recreation at that boundary.
    rows = SCENARIO_FIFO
    outputs, boundaries, _ = run_continuous(rows)
    makers = [t["maker_order_id"] for t in outputs[3]["trades"]] + [
        t["maker_order_id"] for t in outputs[4]["trades"]
    ]
    assert makers == ["s1", "s2", "s2", "s3"]
    prices = [t["price"] for t in outputs[3]["trades"] + outputs[4]["trades"]]
    assert prices == [100, 100, 100, 100]

    _out, state = run_prefix(rows, 4, ["s1", "s2", "s3", "b1"])
    assert [t["maker_order_id"] for t in state["trade_stream"]] == ["s1", "s2"]
    assert state["trade_stream"][-1]["quantity"] == 2
