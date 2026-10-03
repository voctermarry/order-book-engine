"""Tests for the read-only PLAN_TCA_REPORT query in the multi-symbol stream.

The query completes one TWAP/VWAP/POV plan's implementation-shortfall
analysis at a caller-supplied assessment price without matching, releasing
slices or changing any state.
"""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PLAN_ACTIVE,
    PLAN_CANCELLED,
    PLAN_COMPLETED,
    PLAN_TCA_REPORT,
    POV_START,
    POV_VOLUME,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_CANCEL,
    TWAP_START,
    TWAP_SLICE,
    UNKNOWN_EXECUTION_PLAN,
    VWAP_START,
    VWAP_SLICE,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine.event_replay import (
    ALGORITHM_POV,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
)
from order_book_engine import event_cli
from order_book_engine.engine import INVALID_SCHEMA, Engine
from order_book_engine.replay import replay as replay_jsonl


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


def twap_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
    order_type, benchmark_price, price=None, account_id=None,
):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": TWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": order_type, "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    elif price is not None:
        event["price"] = price
    if account_id is not None:
        event["account_id"] = account_id
    return event


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


def twap_cancel(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_CANCEL, "plan_id": plan_id}


def vwap_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, weights,
    order_type, benchmark_price, price=None,
):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": weights,
        "order_type": order_type, "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    elif price is not None:
        event["price"] = price
    return event


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


def pov_start(
    event_id, symbol, sequence, plan_id, side, total_quantity, participation_bps,
    order_type, benchmark_price, price=None,
):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": POV_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity,
        "participation_bps": participation_bps,
        "order_type": order_type, "benchmark_price": benchmark_price,
    }
    if order_type == "LIMIT":
        event["price"] = price
    elif price is not None:
        event["price"] = price
    return event


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_VOLUME, "plan_id": plan_id,
            "market_volume_increment": increment}


def tca(event_id, symbol, sequence, plan_id, mark_price, **extra):
    event = {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": PLAN_TCA_REPORT, "plan_id": plan_id, "mark_price": mark_price,
    }
    event.update(extra)
    return event


def nested_tca(event_id, symbol, sequence, plan_id, mark_price):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "event": {
            "event_id": event_id, "type": PLAN_TCA_REPORT,
            "plan_id": plan_id, "mark_price": mark_price,
        },
    }


def buy_twap_with_one_fill():
    """SELL 3 @102; BUY TWAP 5 over 2 LIMIT @102 (benchmark 100); slice once.

    The single slice fills 3 at 102: filled 3, notional 306, plan stays
    ACTIVE with 2 units unreleased.
    """
    return [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 102),
        twap_start("p1e", "AAA", 2, "p1", "BUY", 5, 2, "LIMIT", 100, price=102),
        twap_slice("sl1", "AAA", 3, "p1"),
    ]


# ---------------------------------------------------------------------------
# Success shape and formulas
# ---------------------------------------------------------------------------


def test_tca_success_shape_for_active_buy_twap():
    out = replay_events(
        buy_twap_with_one_fill() + [tca("q1", "AAA", 4, "p1", 103)],
        snapshot_after=None,
    )
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The slice consumed the three sell units, so the book is empty; the
    # query itself leaves it that way.
    assert r["bids"] == []
    assert r["asks"] == []
    assert set(r) == {
        "event_id", "symbol", "sequence", "status", "result",
        "trades", "book_changes", "bids", "asks", "plan_tca_analysis",
    }
    assert r["plan_tca_analysis"] == {
        "plan_id": "p1",
        "algorithm": ALGORITHM_TWAP,
        "side": "BUY",
        "status": PLAN_ACTIVE,
        "benchmark_price": 100,
        "total_quantity": 5,
        "executed_notional": 306,
        "vwap": {"numerator": 306, "denominator": 3},
        "mark_price": 103,
        "opportunity_quantity": 2,
        # 306 - 100*3 = 6
        "execution_slippage_notional": 6,
        # (103 - 100) * 2 = 6
        "opportunity_cost_notional": 6,
        "implementation_shortfall_notional": 12,
    }


def test_tca_buy_slippage_matches_plan_summary():
    out = replay_events(
        buy_twap_with_one_fill() + [tca("q1", "AAA", 4, "p1", 103)],
        snapshot_after=None,
    )
    plan_summary = out["results"][2]["execution_plan"]
    analysis = out["results"][3]["plan_tca_analysis"]
    assert analysis["execution_slippage_notional"] == plan_summary["slippage_notional"]
    assert analysis["vwap"] == plan_summary["vwap"]
    assert analysis["executed_notional"] == plan_summary["executed_notional"]


def test_tca_sell_formulas_are_mirrored():
    events = [
        add("a0", "AAA", 1, "b1", "BUY", "LIMIT", 3, 98),
        twap_start("p1e", "AAA", 2, "p1", "SELL", 5, 2, "LIMIT", 100, price=98),
        twap_slice("sl1", "AAA", 3, "p1"),
    ]
    out = replay_events(events + [tca("q1", "AAA", 4, "p1", 97)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis == {
        "plan_id": "p1",
        "algorithm": ALGORITHM_TWAP,
        "side": "SELL",
        "status": PLAN_ACTIVE,
        "benchmark_price": 100,
        "total_quantity": 5,
        "executed_notional": 294,
        "vwap": {"numerator": 294, "denominator": 3},
        "mark_price": 97,
        "opportunity_quantity": 2,
        # raw 294 - 100*3 = -6; a sell negates it -> 6
        "execution_slippage_notional": 6,
        # raw (97 - 100) * 2 = -6; a sell negates it -> 6
        "opportunity_cost_notional": 6,
        "implementation_shortfall_notional": 12,
    }


def test_tca_negative_values_mean_improvement():
    # Buy plan with no fills at all: zero slippage; mark below benchmark makes
    # the unfilled quantity cheaper to buy later -> negative opportunity cost.
    events = [
        twap_start("p1e", "AAA", 1, "p1", "BUY", 4, 2, "MARKET", 100),
    ]
    out = replay_events(events + [tca("q1", "AAA", 2, "p1", 97)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["vwap"] is None
    assert analysis["executed_notional"] == 0
    assert analysis["opportunity_quantity"] == 4
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == (97 - 100) * 4
    assert analysis["opportunity_cost_notional"] == -12
    assert analysis["implementation_shortfall_notional"] == -12


def test_tca_completed_plan_has_zero_opportunity_quantity():
    events = [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 2, 101),
        twap_start("p1e", "AAA", 2, "p1", "BUY", 2, 1, "LIMIT", 100, price=101),
        twap_slice("sl1", "AAA", 3, "p1"),
    ]
    assert replay_events(events, snapshot_after=None)["results"][2][
        "execution_plan"]["status"] == PLAN_COMPLETED
    out = replay_events(events + [tca("q1", "AAA", 4, "p1", 120)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["status"] == PLAN_COMPLETED
    assert analysis["opportunity_quantity"] == 0
    assert analysis["opportunity_cost_notional"] == 0
    assert analysis["execution_slippage_notional"] == 202 - 100 * 2
    assert analysis["implementation_shortfall_notional"] == 2


def test_tca_cancelled_plan_is_still_queryable():
    events = [
        twap_start("p1e", "AAA", 1, "p1", "BUY", 4, 2, "MARKET", 100),
        twap_cancel("c1", "AAA", 2, "p1"),
    ]
    out = replay_events(events + [tca("q1", "AAA", 3, "p1", 103)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["status"] == PLAN_CANCELLED
    assert analysis["total_quantity"] == 4
    assert analysis["opportunity_quantity"] == 4
    assert analysis["executed_notional"] == 0
    assert analysis["vwap"] is None
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 3 * 4
    assert analysis["implementation_shortfall_notional"] == 12


def test_tca_vwap_plan_reports_vwap_algorithm_and_metrics():
    # Weights [1, 3] over 4 units allocate [2, 2]; the first bucket fills 2.
    events = [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 100),
        vwap_start("p1e", "AAA", 2, "p1", "BUY", 4, [1, 3], "LIMIT", 100, price=100),
        vwap_slice("sl1", "AAA", 3, "p1"),
    ]
    out = replay_events(events + [tca("q1", "AAA", 4, "p1", 104)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["algorithm"] == ALGORITHM_VWAP
    assert analysis["status"] == PLAN_ACTIVE
    assert analysis["total_quantity"] == 4
    assert analysis["executed_notional"] == 200
    assert analysis["vwap"] == {"numerator": 200, "denominator": 2}
    assert analysis["opportunity_quantity"] == 2
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 8
    assert analysis["implementation_shortfall_notional"] == 8


def test_tca_pov_plan_reports_pov_algorithm_and_metrics():
    # 50% participation against a 4-unit market increment releases 2 units.
    events = [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100),
        pov_start("p1e", "AAA", 2, "p1", "BUY", 10, 5000, "LIMIT", 100, price=100),
        pov_volume("v1", "AAA", 3, "p1", 4),
    ]
    out = replay_events(events + [tca("q1", "AAA", 4, "p1", 102)],
                        snapshot_after=None)
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["algorithm"] == ALGORITHM_POV
    assert analysis["total_quantity"] == 10
    assert analysis["executed_notional"] == 200
    assert analysis["vwap"] == {"numerator": 200, "denominator": 2}
    assert analysis["opportunity_quantity"] == 8
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 16
    assert analysis["implementation_shortfall_notional"] == 16


def test_tca_echoes_the_untouched_book():
    events = [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 102),
        add("a1", "AAA", 2, "s2", "SELL", "LIMIT", 3, 103),
        twap_start("p1e", "AAA", 3, "p1", "BUY", 2, 1, "MARKET", 100),
    ]
    out = replay_events(events + [tca("q1", "AAA", 4, "p1", 110)],
                        snapshot_after=None)
    r = out["results"][-1]
    assert r["bids"] == []
    assert r["asks"] == [
        {"price": 102, "quantity": 5},
        {"price": 103, "quantity": 3},
    ]
    assert r["book_changes"] == {"bids": [], "asks": []}


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutation",
    [
        dict(mark_price=True),          # booleans are not integers
        dict(mark_price=False),
        dict(mark_price=0),
        dict(mark_price=-1),
        dict(mark_price=103.0),         # no floats
        dict(mark_price="103"),
        dict(mark_price=None),
        dict(plan_id=""),               # empty plan id
        dict(plan_id=7),                # non-string plan id
        dict(plan_id=None),
        dict(extra=1),                  # an extra field
    ],
)
def test_malformed_tca_events_are_invalid(mutation):
    event = tca("q1", "AAA", 4, "p1", 103)
    event.update(mutation)
    out = replay_events(
        buy_twap_with_one_fill() + [event],
        snapshot_after=None,
    )
    r = out["results"][-1]
    assert r["status"] == REJECTED
    assert r["rejection_code"] == INVALID_EVENT
    assert "plan_tca_analysis" not in r


def test_missing_tca_fields_are_invalid():
    base = {"event_id": "q1", "symbol": "AAA", "sequence": 4,
            "type": PLAN_TCA_REPORT, "plan_id": "p1", "mark_price": 103}
    for dropped in ("plan_id", "mark_price", "type", "event_id"):
        event = {key: value for key, value in base.items() if key != dropped}
        out = replay_events(
            buy_twap_with_one_fill() + [event], snapshot_after=None
        )
        assert out["results"][-1]["rejection_code"] == INVALID_EVENT, dropped


def test_invalid_tca_consumes_neither_id_nor_sequence():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 4, "p1", True),   # invalid: boolean price
            tca("q1", "AAA", 4, "p1", 103),    # same id, now well formed
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["rejection_code"] == INVALID_EVENT
    assert out["results"][4]["status"] == ACCEPTED
    assert out["results"][4]["result"] == "REPORTED"


def test_invalid_tca_on_unknown_symbol_does_not_create_it():
    out = replay_events([
        tca("q1", "ZZZ", 1, "p1", True),       # structurally invalid
        tca("q2", "ZZZ", 1, "p1", 103),        # well formed: plan unknown
    ], snapshot_after=None)
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    # Nothing was consumed: sequence 1 is still expected on a fresh symbol.
    assert out["results"][1]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][1]["sequence"] == 1
    assert out["results"][1]["bids"] == []


def test_nested_tca_payload_form_is_supported():
    out = replay_events(
        buy_twap_with_one_fill() + [nested_tca("q1", "AAA", 4, "p1", 103)],
        snapshot_after=None,
    )
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["plan_tca_analysis"]["mark_price"] == 103


def test_nested_tca_event_id_mismatch_is_invalid():
    event = nested_tca("q1", "AAA", 4, "p1", 103)
    event["event"]["event_id"] = "other"
    out = replay_events(
        buy_twap_with_one_fill() + [event], snapshot_after=None
    )
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Business rejection: unknown / cross-security plans
# ---------------------------------------------------------------------------


def test_unknown_plan_consumes_id_and_sequence():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 4, "gone", 103),
            tca("q1", "AAA", 5, "gone", 103),   # identical retry, stale seq
            tca("q2", "AAA", 5, "p1", 103),
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    # The identical retry is a duplicate even though its sequence is stale.
    assert out["results"][4]["status"] == DUPLICATE
    # The rejection advanced the symbol sequence to 4.
    assert out["results"][5]["status"] == ACCEPTED
    assert "plan_tca_analysis" not in out["results"][3]


def test_plan_existing_only_on_another_symbol_is_unknown():
    out = replay_events(
        buy_twap_with_one_fill()
        + [tca("q1", "BBB", 1, "p1", 103)],
    )
    r = out["results"][-1]
    assert r["symbol"] == "BBB"
    assert r["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert r["bids"] == [] and r["asks"] == []
    # The well-formed query created BBB and advanced its own sequence.
    assert out["snapshot"]["content"]["symbols"][1]["symbol"] == "BBB"
    assert out["snapshot"]["content"]["symbols"][1]["state"]["last_sequence"] == 1


# ---------------------------------------------------------------------------
# Idempotency, conflicts and ordering
# ---------------------------------------------------------------------------


def test_identical_tca_retry_is_a_duplicate_with_current_book():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 4, "p1", 103),
            tca("q1", "AAA", 4, "p1", 103),
        ],
        snapshot_after=None,
    )
    first, second = out["results"][3], out["results"][4]
    assert second["status"] == DUPLICATE
    assert second["trades"] == []
    assert second["book_changes"] == {"bids": [], "asks": []}
    assert second["bids"] == first["bids"]
    assert second["asks"] == first["asks"]
    assert "plan_tca_analysis" not in second


def test_same_tca_id_with_different_mark_conflicts():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 4, "p1", 103),
            tca("q1", "AAA", 5, "p1", 104),
        ],
        snapshot_after=None,
    )
    assert out["results"][4]["rejection_code"] == EVENT_ID_CONFLICT


def test_same_tca_id_on_another_symbol_conflicts():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 4, "p1", 103),
            tca("q1", "BBB", 1, "p1", 103),
        ],
        snapshot_after=None,
    )
    # The same accepted id reused on another security is a conflict even
    # though BBB has no plan named p1.
    assert out["results"][4]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_errors_precede_the_unknown_plan_check():
    out = replay_events(
        buy_twap_with_one_fill()
        + [
            tca("q1", "AAA", 5, "gone", 103),   # gap: expected 4
            tca("q2", "AAA", 3, "gone", 103),   # backwards
            tca("q3", "AAA", 4, "gone", 103),
        ],
        snapshot_after=None,
    )
    assert out["results"][3]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][3]["expected_sequence"] == 4
    assert out["results"][4]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][4]["expected_sequence"] == 4
    # Ordering failures consume nothing; sequence 4 then resolves as the
    # business rejection for the missing plan.
    assert out["results"][5]["rejection_code"] == UNKNOWN_EXECUTION_PLAN


# ---------------------------------------------------------------------------
# Read-only invariants
# ---------------------------------------------------------------------------


def test_tca_is_read_only_across_engine_plan_and_price_limits():
    config = {"price_limits": {"AAA": {"lower": 90, "upper": 110}}}
    prefix = buy_twap_with_one_fill()
    before = replay_events(prefix, config=config)["snapshot"]
    after = replay_events(
        [tca("q1", "AAA", 4, "p1", 103), tca("q2", "AAA", 5, "p1", 999)],
        config=config,
        snapshot=before,
    )["snapshot"]

    state_before = before["content"]["symbols"][0]["state"]
    state_after = after["content"]["symbols"][0]["state"]
    # Everything matching-related is byte-identical; only the sequence and
    # the per-symbol idempotency log grow.
    assert state_after["engine"] == state_before["engine"]
    assert state_after["plans"] == state_before["plans"]
    assert state_after["price_limits"] == state_before["price_limits"]
    assert state_after["last_sequence"] == 5
    assert len(state_after["event_log"]) == len(state_before["event_log"]) + 2


def test_tca_does_not_release_slices_or_spend_trade_ids():
    events = [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 10, 102),
        twap_start("p1e", "AAA", 2, "p1", "BUY", 5, 2, "LIMIT", 100, price=102),
        twap_slice("sl1", "AAA", 3, "p1"),
        tca("q1", "AAA", 4, "p1", 103),
        tca("q2", "AAA", 5, "p1", 103),
        twap_slice("sl2", "AAA", 6, "p1"),
    ]
    out = replay_events(events, snapshot_after=None)
    # The two queries spent no trade ids: the second slice still produces
    # trade id 2 continuing the per-symbol sequence from the first slice.
    second_slice = out["results"][5]
    assert [trade["trade_id"] for trade in second_slice["trades"]] == [2]
    assert second_slice["execution_plan"]["slice_number"] == 2


# ---------------------------------------------------------------------------
# Determinism and snapshot resumption
# ---------------------------------------------------------------------------


def _rich_tca_stream():
    return [
        add("a0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 102),
        twap_start("p1e", "AAA", 2, "p1", "BUY", 5, 2, "LIMIT", 100, price=102),
        twap_slice("sl1", "AAA", 3, "p1"),
        tca("q1", "AAA", 4, "p1", 103),
        add("a1", "BBB", 1, "b1", "BUY", "LIMIT", 3, 98),
        vwap_start("w1e", "BBB", 2, "w1", "SELL", 4, [1, 3], "LIMIT", 100, price=98),
        vwap_slice("wsl1", "BBB", 3, "w1"),
        tca("q2", "BBB", 4, "w1", 97),
        tca("q3", "AAA", 5, "p1", 104),
        tca("q4", "BBB", 5, "missing", 50),
    ]


def test_tca_output_is_byte_for_byte_deterministic():
    a = canonical_json(replay_events(_rich_tca_stream()))
    b = canonical_json(replay_events(copy.deepcopy(_rich_tca_stream())))
    assert a == b

    def no_floats(value):
        if isinstance(value, float):
            raise AssertionError("float leaked into output")
        if isinstance(value, dict):
            for item in value.values():
                no_floats(item)
        elif isinstance(value, list):
            for item in value:
                no_floats(item)

    no_floats(json.loads(a))


def test_resumed_replay_with_tca_queries_matches_one_shot():
    stream = _rich_tca_stream()
    one_shot = replay_events(copy.deepcopy(stream))
    # Split right after AAA's first TCA query (index 3) to resume mid-stream.
    snapshot = replay_events(copy.deepcopy(stream[:4]))["snapshot"]
    segmented = replay_events(copy.deepcopy(stream[4:]), snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(one_shot["results"][4:])
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_duplicate_tca_is_still_idempotent_after_restore():
    one_shot = replay_events(_rich_tca_stream())
    replayer = restore_replayer(one_shot["snapshot"])
    result = replayer.submit([copy.deepcopy(_rich_tca_stream()[3])])[0]
    assert result["status"] == DUPLICATE
    fresh = replayer.submit([tca("q9", "AAA", 6, "p1", 101)])[0]
    assert fresh["status"] == ACCEPTED
    assert fresh["result"] == "REPORTED"
    assert fresh["plan_tca_analysis"]["mark_price"] == 101


def test_business_rejected_tca_roundtrips_through_snapshot():
    out = replay_events(
        buy_twap_with_one_fill() + [tca("q1", "AAA", 4, "gone", 103)]
    )
    snapshot = out["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [tca("q1", "AAA", 5, "gone", 103), tca("q2", "AAA", 5, "p1", 103)],
        snapshot=export_snapshot(restored),
        snapshot_after=None,
    )
    # The rejection is remembered; the next fresh query works at sequence 5.
    assert follow["results"][0]["status"] == DUPLICATE
    assert follow["results"][1]["status"] == ACCEPTED


def test_snapshot_after_named_tca_event():
    out = replay_events(
        _rich_tca_stream(),
        snapshot_after={"symbol": "BBB", "sequence": 4},
    )
    assert out["snapshot"] is not None
    resumed = replay_events(
        [tca("qx", "BBB", 5, "w1", 96)],
        snapshot=out["snapshot"],
        snapshot_after=None,
    )
    assert resumed["results"][0]["status"] == ACCEPTED
    assert resumed["results"][0]["plan_tca_analysis"]["mark_price"] == 96


# ---------------------------------------------------------------------------
# The single-security entry points never accept the event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_plan_tca_report():
    engine = Engine()
    _eid, result, reason, _trades, _stp, analysis = engine.handle_object_extended({
        "event_id": "q1", "type": PLAN_TCA_REPORT,
        "plan_id": "p1", "mark_price": 103,
    })
    assert result == REJECTED
    assert reason == INVALID_SCHEMA
    assert analysis is None


def test_json_lines_replay_rejects_plan_tca_report():
    line = json.dumps({
        "event_id": "q1", "type": PLAN_TCA_REPORT,
        "plan_id": "p1", "mark_price": 103,
    })
    stdin = io.TextIOWrapper(io.BytesIO((line + "\n").encode("utf-8")))
    stdout = io.TextIOWrapper(io.BytesIO())
    stderr = io.StringIO()
    code = replay_jsonl(stdin, stdout, stderr)
    assert code == 0
    out = json.loads(stdout.buffer.getvalue().decode("utf-8"))
    assert out["result"] == REJECTED
    assert out["reason"] == INVALID_SCHEMA
    assert "plan_tca_analysis" not in out


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


def test_cli_events_supports_plan_tca_report_end_to_end():
    code, out, err = _run_cli({"events": _rich_tca_stream()})
    assert code == 0
    assert err == ""
    accepted = out["results"][3]
    rejected = out["results"][-1]
    assert accepted["status"] == ACCEPTED
    assert accepted["result"] == "REPORTED"
    assert accepted["plan_tca_analysis"]["plan_id"] == "p1"
    assert rejected["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
