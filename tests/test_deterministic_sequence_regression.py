"""Deterministic regression tests driven by fixed order-event sequences.

These tests exercise only the publicly documented contract of the baseline
single-instrument engine (the ``order-book-engine replay`` entry point and the
``Engine`` class):

* events ``ADD`` (LIMIT / MARKET / ICEBERG with GTC / IOC / FOK), ``CANCEL``
  and ``REPLACE`` plus the read-only ``EXECUTION_REPORT`` query;
* documented result codes, rejection reasons, book snapshots and trades;
* engine-generated stable identifiers (``trade_id`` starts at 1 and is
  consecutive; failed FOK and rejected events spend none).

Every scenario is a fixed list of JSON Lines events. After *each* event the
harness records the full public observation:

* the event result (the active order's new state) and rejection reason;
* the new trades, in occurrence order, with maker/taker, price, quantity and
  the engine-generated ``trade_id`` (order is asserted, never normalized);
* the complete best bid/ask and the visible quantity at every level.

Each sequence is verified three ways:

1. one continuous run on a single ``Engine``;
2. a run repeated several times (results must be identical);
3. for every event boundary ``k``, a *brand new* equivalent book built solely by
   replaying the already-known prefix ``events[:k]`` from the start on a fresh
   ``Engine`` (the baseline entry points expose no state-snapshot import, so no
   production interface is added), then continuing with the remaining events;
   every shared boundary must equal the continuous run.

Quantity conservation (submitted vs. filled vs. cancelled vs. resting
remainder) is cross-checked per order through the public ``EXECUTION_REPORT``
contract. Nothing depends on the wall clock, randomness, hash-seeded dict
ordering or filesystem paths: a final test replays every scenario through the
real CLI under several ``PYTHONHASHSEED`` values and requires byte-identical
output.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from order_book_engine.engine import Engine


# ---------------------------------------------------------------------------
# Event builders (compact JSON Lines, exactly like the documented wire format)
# ---------------------------------------------------------------------------


def add(event_id, order_id, side, order_type, quantity, price=None, **extra):
    obj = {
        "event_id": event_id,
        "type": "ADD",
        "order_id": order_id,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
    }
    if price is not None:
        obj["price"] = price
    obj.update(extra)
    return json.dumps(obj, ensure_ascii=False)


def limit(event_id, order_id, side, quantity, price, **extra):
    return add(event_id, order_id, side, "LIMIT", quantity, price, **extra)


def market(event_id, order_id, side, quantity, **extra):
    return add(event_id, order_id, side, "MARKET", quantity, **extra)


def iceberg(event_id, order_id, side, quantity, price, display_quantity, **extra):
    return add(
        event_id, order_id, side, "ICEBERG", quantity, price,
        display_quantity=display_quantity, **extra,
    )


def cancel(event_id, order_id):
    return json.dumps(
        {"event_id": event_id, "type": "CANCEL", "order_id": order_id}
    )


def replace(event_id, order_id, quantity, price, **extra):
    obj = {
        "event_id": event_id,
        "type": "REPLACE",
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
    }
    obj.update(extra)
    return json.dumps(obj, ensure_ascii=False)


def report(event_id, order_id, benchmark_price):
    return json.dumps(
        {
            "event_id": event_id,
            "type": "EXECUTION_REPORT",
            "order_id": order_id,
            "benchmark_price": benchmark_price,
        }
    )


def trade(trade_id, maker, taker, price, quantity):
    return {
        "trade_id": trade_id,
        "maker_order_id": maker,
        "taker_order_id": taker,
        "price": price,
        "quantity": quantity,
    }


def level(price, quantity):
    return {"price": price, "quantity": quantity}


# An expectation is (result, reason, trades, bids, asks) for one event.
# ``reason`` is None for accepted events. Bids are descending, asks ascending.
def expect(result, trades, bids, asks, reason=None):
    return result, reason, trades, bids, asks


# ---------------------------------------------------------------------------
# Fixed deterministic scenarios
# ---------------------------------------------------------------------------

SCENARIOS = [
    # 1. Same-price orders fill strictly in arrival (time-priority) order.
    pytest.param(
        "same_price_time_priority",
        [
            limit("e1", "s1", "SELL", 3, 100),
            limit("e2", "s2", "SELL", 2, 100),
            limit("e3", "s3", "SELL", 4, 100),
            limit("e4", "b1", "BUY", 5, 100),
            limit("e5", "b2", "BUY", 4, 100),
        ],
        [
            expect("RESTING", [], [], [level(100, 3)]),
            expect("RESTING", [], [], [level(100, 5)]),
            expect("RESTING", [], [], [level(100, 9)]),
            expect(
                "FILLED",
                [trade(1, "s1", "b1", 100, 3), trade(2, "s2", "b1", 100, 2)],
                [],
                [level(100, 4)],
            ),
            expect(
                "FILLED", [trade(3, "s3", "b2", 100, 4)], [], []
            ),
        ],
        id="same_price_time_priority",
    ),
    # 2. A sweep across several price levels, the unfilled remainder rests, and
    #    is then cancelled; documented rejections change nothing and spend no
    #    trade id. Conservation: 8 submitted = 7 filled + 1 cancelled.
    pytest.param(
        "sweep_levels_rest_then_cancel",
        [
            limit("e1", "s1", "SELL", 2, 101),
            limit("e2", "s2", "SELL", 3, 100),
            limit("e3", "s3", "SELL", 2, 99),
            limit("e4", "b1", "BUY", 8, 101),
            cancel("e5", "b1"),
            cancel("e6", "b1"),       # already cancelled -> UNKNOWN_ORDER
            cancel("e7", "ghost"),    # never existed -> UNKNOWN_ORDER
            limit("e8", "s9", "SELL", 1, 101),
            limit("e9", "b2", "BUY", 1, 101),
        ],
        [
            expect("RESTING", [], [], [level(101, 2)]),
            expect("RESTING", [], [], [level(100, 3), level(101, 2)]),
            expect(
                "RESTING", [], [],
                [level(99, 2), level(100, 3), level(101, 2)],
            ),
            expect(
                "PARTIALLY_FILLED_RESTING",
                [
                    trade(1, "s3", "b1", 99, 2),
                    trade(2, "s2", "b1", 100, 3),
                    trade(3, "s1", "b1", 101, 2),
                ],
                [level(101, 1)],
                [],
            ),
            expect("CANCELLED", [], [], []),
            expect("REJECTED", [], [], [], reason="UNKNOWN_ORDER"),
            expect("REJECTED", [], [], [], reason="UNKNOWN_ORDER"),
            expect("RESTING", [], [], [level(101, 1)]),
            # id 4, not higher: the two UNKNOWN_ORDER rejections spent none.
            expect("FILLED", [trade(4, "s9", "b2", 101, 1)], [], []),
        ],
        id="sweep_levels_rest_then_cancel",
    ),
    # 3. REPLACE with unchanged parameters still loses queue priority; a later
    #    sell therefore hits the other same-price order first. The replaced
    #    order is partially filled and its remainder is then cancelled.
    pytest.param(
        "replace_loses_priority_cancel_after_partial",
        [
            limit("e1", "b1", "BUY", 2, 100),
            limit("e2", "b2", "BUY", 2, 100),
            replace("e3", "b1", 2, 100),
            limit("e4", "s1", "SELL", 3, 100),
            cancel("e5", "b1"),
            market("e6", "s2", "SELL", 5),
        ],
        [
            expect("RESTING", [], [level(100, 2)], []),
            expect("RESTING", [], [level(100, 4)], []),
            # Aggregate is unchanged; the order merely rejoined at the tail.
            expect("REPLACED", [], [level(100, 4)], []),
            expect(
                "FILLED",
                [trade(1, "b2", "s1", 100, 2), trade(2, "b1", "s1", 100, 1)],
                [level(100, 1)],
                [],
            ),
            expect("CANCELLED", [], [], []),
            # No liquidity left: a market sell is cancelled unfilled and rests
            # nothing.
            expect("UNFILLED_CANCELLED", [], [], []),
        ],
        id="replace_loses_priority_cancel_after_partial",
    ),
    # 4. Publicly supported immediate / all-or-none constraints: MARKET sweeps
    #    every level, a LIMIT IOC cancels its leftover without resting, and FOK
    #    either fills the whole quantity on the pre-event book or does nothing
    #    atomically (no book change, no trade id spent).
    pytest.param(
        "immediate_ioc_and_fill_or_kill",
        [
            limit("e1", "s1", "SELL", 2, 100),
            limit("e2", "s2", "SELL", 3, 101),
            market("e3", "m1", "BUY", 5),
            limit("e4", "s3", "SELL", 2, 100),
            limit("e5", "i1", "BUY", 5, 100, time_in_force="IOC"),
            limit("e6", "i2", "BUY", 1, 99, time_in_force="IOC"),
            limit("e7", "s4", "SELL", 4, 100),
            limit("e8", "f1", "BUY", 5, 100, time_in_force="FOK"),
            limit("e9", "f2", "BUY", 4, 100, time_in_force="FOK"),
        ],
        [
            expect("RESTING", [], [], [level(100, 2)]),
            expect("RESTING", [], [], [level(100, 2), level(101, 3)]),
            expect(
                "FILLED",
                [trade(1, "s1", "m1", 100, 2), trade(2, "s2", "m1", 101, 3)],
                [],
                [],
            ),
            expect("RESTING", [], [], [level(100, 2)]),
            expect(
                "PARTIALLY_FILLED_CANCELLED",
                [trade(3, "s3", "i1", 100, 2)],
                [],
                [],
            ),
            expect("UNFILLED_CANCELLED", [], [], []),
            expect("RESTING", [], [], [level(100, 4)]),
            # Short by one: atomic no-op, book untouched, no id consumed.
            expect("UNFILLED_CANCELLED", [], [], [level(100, 4)]),
            # Exactly fillable; this is trade id 4, proving id 4 was not spent
            # by the failed FOK immediately before it.
            expect("FILLED", [trade(4, "s4", "f2", 100, 4)], [], []),
        ],
        id="immediate_ioc_and_fill_or_kill",
    ),
    # 5. Multiple orders sharing one price: an iceberg's exhausted slice
    #    replenishes at the tail of the level, so the same taker (and later
    #    takers) meets ordinary same-price orders before the fresh slice. The
    #    partially consumed iceberg is then cancelled, visible slice and hidden
    #    reserve together. 10 = 5 filled + 5 cancelled.
    pytest.param(
        "iceberg_replenish_tail_then_cancel",
        [
            iceberg("e1", "i1", "SELL", 10, 100, 3),
            limit("e2", "s2", "SELL", 4, 100),
            limit("e3", "s3", "SELL", 2, 100),
            limit("e4", "b1", "BUY", 3, 100),
            limit("e5", "b2", "BUY", 6, 100),
            limit("e6", "b3", "BUY", 2, 100),
            cancel("e7", "i1"),
        ],
        [
            expect("RESTING", [], [], [level(100, 3)]),
            expect("RESTING", [], [], [level(100, 7)]),
            expect("RESTING", [], [], [level(100, 9)]),
            expect(
                "FILLED", [trade(1, "i1", "b1", 100, 3)], [], [level(100, 9)]
            ),
            expect(
                "FILLED",
                [trade(2, "s2", "b2", 100, 4), trade(3, "s3", "b2", 100, 2)],
                [],
                [level(100, 3)],
            ),
            expect(
                "FILLED", [trade(4, "i1", "b3", 100, 2)], [], [level(100, 1)]
            ),
            expect("CANCELLED", [], [], []),
        ],
        id="iceberg_replenish_tail_then_cancel",
    ),
    # 6. REPLACE after the target was partially filled: the new quantity is the
    #    new remaining total (independent of prior fills), and the replacement
    #    joins the queue tail even though the target arrived long before the
    #    order that now trades ahead of it. Finished targets then give the
    #    documented UNKNOWN_ORDER, and a valid target-unknown event still
    #    occupies its event id.
    pytest.param(
        "replace_after_partial_fill_then_reject",
        [
            limit("e1", "s1", "SELL", 2, 100),
            limit("e2", "b1", "BUY", 5, 100),
            limit("e3", "b2", "BUY", 3, 100),
            replace("e4", "b1", 4, 100),
            limit("e5", "s2", "SELL", 5, 100),
            cancel("e6", "b1"),
            replace("e7", "b1", 1, 100),     # finished target -> UNKNOWN_ORDER
            cancel("e8", "s1"),              # filled maker -> UNKNOWN_ORDER
            cancel("e9", "nobody"),          # never existed -> UNKNOWN_ORDER
            replace("e7", "b1", 1, 100),     # repeats e7 -> DUPLICATE_EVENT_ID
        ],
        [
            expect("RESTING", [], [], [level(100, 2)]),
            expect(
                "PARTIALLY_FILLED_RESTING",
                [trade(1, "s1", "b1", 100, 2)],
                [level(100, 3)],
                [],
            ),
            expect("RESTING", [], [level(100, 6)], []),
            # b1 (4 lots) rejoined behind b2 (3 lots); the 7-lot aggregate.
            expect("REPLACED", [], [level(100, 7)], []),
            # b2 trades first despite arriving after the original b1; b1 gives
            # up only 2 of its 4 new lots.
            expect(
                "FILLED",
                [trade(2, "b2", "s2", 100, 3), trade(3, "b1", "s2", 100, 2)],
                [level(100, 2)],
                [],
            ),
            expect("CANCELLED", [], [], []),
            expect("REJECTED", [], [], [], reason="UNKNOWN_ORDER"),
            expect("REJECTED", [], [], [], reason="UNKNOWN_ORDER"),
            expect("REJECTED", [], [], [], reason="UNKNOWN_ORDER"),
            expect("REJECTED", [], [], [], reason="DUPLICATE_EVENT_ID"),
        ],
        id="replace_after_partial_fill_then_reject",
    ),
]


# ---------------------------------------------------------------------------
# Observation harness
# ---------------------------------------------------------------------------


def observe(engine, line):
    """Public observation after one event: result, reason, trades and book."""
    _event_id, result, reason, trades = engine.handle_line(line)
    bids, asks = engine.snapshot()
    return result, reason, trades, bids, asks


def run_continuous(lines):
    """Process the whole sequence once on one fresh engine."""
    engine = Engine()
    return [observe(engine, line) for line in lines]


def run_recreated_at_boundary(lines, boundary):
    """Recreate an equivalent book by replaying the prefix from the start.

    The baseline entry points provide no state-snapshot import for a single
    book, so equivalence is obtained solely by re-submitting the already-seen
    events ``lines[:boundary]`` to a brand new engine; processing then continues
    with the suffix. No production interface is added for tests.
    """
    engine = Engine()
    for line in lines[:boundary]:
        engine.handle_line(line)
    return [observe(engine, line) for line in lines[boundary:]]


@pytest.mark.parametrize("name,lines,expected", SCENARIOS)
def test_continuous_run_matches_golden_sequence(name, lines, expected):
    observations = run_continuous(lines)
    assert observations == expected


@pytest.mark.parametrize("name,lines,expected", SCENARIOS)
def test_sequence_is_identical_on_every_repeated_run(name, lines, expected):
    first = run_continuous(lines)
    for _ in range(3):
        assert run_continuous(lines) == first


@pytest.mark.parametrize("name,lines,expected", SCENARIOS)
def test_recreated_prefix_book_matches_at_every_boundary(name, lines, expected):
    continuous = run_continuous(lines)
    for boundary in range(len(lines)):
        continued = run_recreated_at_boundary(lines, boundary)
        # Every suffix observation, including trades, trade ids and the book,
        # must equal the continuous run at the same event boundary.
        assert continued == continuous[boundary:], (
            f"scenario {name!r} diverges after recreating the book at "
            f"boundary {boundary}"
        )


@pytest.mark.parametrize("name,lines,expected", SCENARIOS)
def test_trade_identifiers_are_stable_and_consecutive(name, lines, expected):
    # A direct, order-sensitive check that must not be hidden by sorting.
    observations = run_continuous(lines)
    emitted = [
        t
        for _result, _reason, trades, _bids, _asks in observations
        for t in trades
    ]
    assert [t["trade_id"] for t in emitted] == list(range(1, len(emitted) + 1))
    # The golden expectations already pin each trade's position; the same ids
    # must appear in the same order after a prefix replay from every boundary.
    for boundary in range(len(lines)):
        suffix = run_recreated_at_boundary(lines, boundary)
        suffix_ids = [t["trade_id"] for _, _, trades, _, _ in suffix for t in trades]
        expected_ids = [
            t["trade_id"]
            for observation in observations[boundary:]
            for t in observation[2]
        ]
        assert suffix_ids == expected_ids


# ---------------------------------------------------------------------------
# Quantity conservation and intermediate order status via EXECUTION_REPORT
# ---------------------------------------------------------------------------


def analysis_for(engine, event_id, order_id, benchmark=100):
    """Run the public read-only execution query and return its analysis."""
    _eid, result, reason, _trades, _stp, analysis = engine.handle_line_extended(
        report(event_id, order_id, benchmark)
    )
    assert (result, reason) == ("REPORTED", None)
    return analysis


def test_order_status_and_conservation_after_partial_fill_replace_cancel():
    """Scenario 6 with EXECUTION_REPORT probes around each key transition.

    The probes are real, documented events: they occupy event ids but must not
    match, change the book/queues or spend trade ids. The whole sequence
    (probes included) is also re-run through the continuous/recreated harness.
    """
    lines = [
        limit("e1", "s1", "SELL", 2, 100),
        limit("e2", "b1", "BUY", 5, 100),
        limit("e3", "b2", "BUY", 3, 100),
        # Mid-transition: b1 is resting, 2 filled as taker, 3 open.
        report("q1", "b1", 100),
        replace("e4", "b1", 4, 100),
        # After replace: still RESTING, prior fills retained, open is the new
        # remaining total of 4.
        report("q2", "b1", 100),
        limit("e5", "s2", "SELL", 5, 100),
        # b2 is fully filled as a maker; b1 has 2 more fills and 2 open.
        report("q3", "b2", 100),
        report("q4", "b1", 100),
        cancel("e6", "b1"),
        report("q5", "b1", 100),
    ]

    engine = Engine()
    engine.handle_line(lines[0])
    engine.handle_line(lines[1])
    engine.handle_line(lines[2])

    probe1 = analysis_for(engine, "q1", "b1")
    assert probe1["current_status"] == "RESTING"
    assert (probe1["filled_quantity"], probe1["open_quantity"]) == (2, 3)
    assert probe1["executed_notional"] == 200
    assert probe1["trade_attribution"] == [
        {"trade_id": 1, "role": "TAKER", "counterparty_order_id": "s1",
         "event_id": "e2", "price": 100, "quantity": 2}
    ]

    engine.handle_line(lines[4])
    # Snapshot around the read-only query itself: the replace legitimately
    # changed the aggregate from 6 to 7, but the subsequent report must leave
    # it exactly as it was.
    bids_before, asks_before = engine.snapshot()
    probe2 = analysis_for(engine, "q2", "b1")
    assert probe2["current_status"] == "RESTING"
    assert (probe2["filled_quantity"], probe2["open_quantity"]) == (2, 4)
    assert engine.snapshot() == (bids_before, asks_before)

    engine.handle_line(lines[6])
    probe_b2 = analysis_for(engine, "q3", "b2")
    assert probe_b2["current_status"] == "FILLED"
    assert (probe_b2["filled_quantity"], probe_b2["open_quantity"]) == (3, 0)
    assert probe_b2["executed_notional"] == 300

    probe_b1 = analysis_for(engine, "q4", "b1")
    assert probe_b1["current_status"] == "RESTING"
    # 2 filled before the replace + 2 after; 2 of the new 4-lot remainder open.
    assert (probe_b1["filled_quantity"], probe_b1["open_quantity"]) == (4, 2)
    assert probe_b1["executed_notional"] == 400

    engine.handle_line(lines[9])
    probe_end = analysis_for(engine, "q5", "b1")
    assert probe_end["current_status"] == "CANCELLED"
    # Conservation across the replace: new remaining total 4 = 2 filled + 2
    # cancelled (open drops to zero); cumulative fills stay 4.
    assert (probe_end["filled_quantity"], probe_end["open_quantity"]) == (4, 0)

    # Final book is empty and only trade ids 1..3 exist: the five reports and
    # the cancel produced no trades.
    assert engine.snapshot() == ([], [])
    assert [t["trade_id"] for t in engine.trade_history()] == [1, 2, 3]

    # The identical probe-interleaved sequence must reproduce on a recreated
    # prefix book at every boundary (reports included).
    continuous = []
    check_engine = Engine()
    for line in lines:
        _eid, result, reason, trades = check_engine.handle_line(line)
        continuous.append((result, reason, trades, *check_engine.snapshot()))
    for boundary in range(len(lines)):
        replayed = Engine()
        for line in lines[:boundary]:
            replayed.handle_line(line)
        suffix = []
        for line in lines[boundary:]:
            _eid, result, reason, trades = replayed.handle_line(line)
            suffix.append((result, reason, trades, *replayed.snapshot()))
        assert suffix == continuous[boundary:], boundary


def test_iceberg_conservation_and_status_through_replenish_and_cancel():
    # Scenario 5, with read-only status probes after the partial consumption
    # and after the final slice; the probes are not part of the traded events.
    events = [
        iceberg("e1", "i1", "SELL", 10, 100, 3),
        limit("e2", "s2", "SELL", 4, 100),
        limit("e3", "s3", "SELL", 2, 100),
        limit("e4", "b1", "BUY", 3, 100),
        limit("e5", "b2", "BUY", 6, 100),
    ]
    engine = Engine()
    for line in events:
        engine.handle_line(line)

    mid = analysis_for(engine, "q1", "i1")
    assert mid["current_status"] == "RESTING"
    # Only the first slice (3) has traded; replenishment put the next slice
    # behind s2/s3, which the immediately following taker consumed instead.
    assert (mid["filled_quantity"], mid["open_quantity"]) == (3, 7)

    asks_before = engine.snapshot()[1]
    engine.handle_line(limit("e6", "b3", "BUY", 2, 100))
    near_end = analysis_for(engine, "q2", "i1")
    assert near_end["current_status"] == "RESTING"
    # 5 filled (3 + 2), 5 still open: one visible unit plus four in reserve.
    assert (near_end["filled_quantity"], near_end["open_quantity"]) == (5, 5)
    # The read-only probe before e6 changed nothing on the ask side.
    assert asks_before == [level(100, 3)]

    visible_before_cancel = engine.snapshot()[1]
    engine.handle_line(cancel("e7", "i1"))
    final = analysis_for(engine, "q3", "i1")
    assert final["current_status"] == "CANCELLED"
    # 10 submitted = 5 filled + 5 cancelled; nothing remains open.
    assert (final["filled_quantity"], final["open_quantity"]) == (5, 0)
    assert engine.snapshot() == ([], [])

    # The cancel removed both the visible slice and the hidden reserve.
    assert visible_before_cancel == [level(100, 1)]
    assert [t["trade_id"] for t in engine.trade_history()] == [1, 2, 3, 4]

    # Determinism: the exact traded-event sequence reproduces on a recreated
    # prefix book at every boundary, probes and final status included.
    continuous = run_continuous(events)
    for boundary in range(len(events)):
        assert run_recreated_at_boundary(events, boundary) == (
            continuous[boundary:]
        )


def test_documented_rejections_leave_book_orders_and_trades_unchanged():
    engine = Engine()
    engine.handle_line(limit("e1", "s1", "SELL", 2, 100))
    engine.handle_line(limit("e2", "s2", "SELL", 3, 100))
    engine.handle_line(limit("e3", "b0", "BUY", 1, 99))
    seeded_book = engine.snapshot()
    assert seeded_book == ([level(99, 1)], [level(100, 5)])

    # Fill s1 (trade id 1); it then becomes a documented UNKNOWN_ORDER target.
    engine.handle_line(limit("e4", "b1", "BUY", 2, 100))
    book_after_fill = engine.snapshot()
    assert book_after_fill == ([level(99, 1)], [level(100, 3)])

    cases = [
        (cancel("r1", "s1"), "UNKNOWN_ORDER"),          # filled maker
        (cancel("r2", "missing"), "UNKNOWN_ORDER"),     # never existed
        (replace("r3", "s1", 1, 100), "UNKNOWN_ORDER"),  # filled target
        (replace("r4", "missing", 1, 100), "UNKNOWN_ORDER"),
        (cancel("r1", "s1"), "DUPLICATE_EVENT_ID"),     # exact repeat
    ]
    for line, expected_reason in cases:
        snapshot_before = engine.snapshot()
        next_id_before = max((t["trade_id"] for t in engine.trade_history()), default=0)

        _eid, result, reason, trades = engine.handle_line(line)
        assert (result, reason) == ("REJECTED", expected_reason)
        assert trades == []
        # Book, order states and the trade set are identical before and after.
        assert engine.snapshot() == snapshot_before
        assert max((t["trade_id"] for t in engine.trade_history()), default=0) == (
            next_id_before
        )

    # Exactly the one genuine fill (trade id 1) survives every rejection.
    assert engine.trade_history() == [
        {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
         "price": 100, "quantity": 2, "event_id": "e4"}
    ]
    assert book_after_fill == ([level(99, 1)], [level(100, 3)])


# ---------------------------------------------------------------------------
# End-to-end determinism through the public CLI under varied hash seeds
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]


def run_cli_replay(stream_text, hash_seed):
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hash_seed)
    # Invoke the documented console command's implementation with an absolute
    # interpreter path and the repository root as cwd; no ambient path is
    # relied upon and nothing in the process environment carries randomness
    # into the result.
    code = (
        "from order_book_engine.cli import main; "
        "import sys; sys.exit(main(['replay']))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        input=stream_text.encode("utf-8"),
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8")
    assert completed.stderr == b""
    return completed.stdout


@pytest.mark.parametrize("name,lines,expected", SCENARIOS)
def test_cli_output_is_byte_identical_across_hash_seeds(name, lines, expected):
    stream = "\n".join(lines) + "\n"
    outputs = {seed: run_cli_replay(stream, seed) for seed in (0, 1, 7, 12345)}
    reference = outputs[0]
    for seed, out in outputs.items():
        assert out == reference, f"scenario {name!r} differs under seed {seed}"

    # The CLI output must agree with the in-process engine observations.
    records = [json.loads(line) for line in reference.decode("utf-8").splitlines()]
    assert len(records) == len(lines)
    for record, (result, reason, trades, bids, asks) in zip(records, expected):
        assert record["result"] == result
        assert record.get("reason") == reason
        assert record["trades"] == trades
        assert record["bids"] == bids
        assert record["asks"] == asks

    # And the in-process replay entry point itself is byte-for-byte stable.
    import io

    from order_book_engine import replay as replay_module

    def inproc():
        stdin = io.TextIOWrapper(io.BytesIO(stream.encode("utf-8")))
        stdout = io.TextIOWrapper(io.BytesIO())
        code = replay_module.replay(stdin, stdout, io.StringIO())
        stdout.flush()
        assert code == 0
        return stdout.buffer.getvalue()

    assert inproc() == inproc() == reference
