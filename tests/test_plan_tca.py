"""Tests for the read-only per-plan PLAN_TCA_REPORT event.

The query prices a TWAP/VWAP/POV plan's implementation shortfall against a
caller-supplied evaluation mark: it never matches, never releases a slice
and never changes any plan, order, account, book, trade id or price limit.
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
    POV_CANCEL,
    POV_START,
    POV_VOLUME,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_CANCEL,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_EXECUTION_PLAN,
    VWAP_SLICE,
    VWAP_START,
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


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
               order_type, benchmark_price, price=None, account_id=None):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": TWAP_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "slice_count": slice_count,
        "order_type": order_type,
        "benchmark_price": benchmark_price,
    }
    if price is not None:
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


def vwap_start(event_id, symbol, sequence, plan_id, side, total_quantity, weights,
               order_type, benchmark_price, price=None):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": VWAP_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "volume_weights": weights,
        "order_type": order_type,
        "benchmark_price": benchmark_price,
    }
    if price is not None:
        event["price"] = price
    return event


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


def pov_start(event_id, symbol, sequence, plan_id, side, total_quantity, bps,
              benchmark_price):
    return {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": POV_START,
        "plan_id": plan_id,
        "side": side,
        "total_quantity": total_quantity,
        "participation_bps": bps,
        "order_type": "MARKET",
        "benchmark_price": benchmark_price,
    }


def pov_volume(event_id, symbol, sequence, plan_id, increment):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_VOLUME, "plan_id": plan_id,
            "market_volume_increment": increment}


def pov_cancel(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": POV_CANCEL, "plan_id": plan_id}


def tca(event_id, symbol, sequence, plan_id, mark_price, **extra):
    payload = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": PLAN_TCA_REPORT,
        "plan_id": plan_id,
        "mark_price": mark_price,
    }
    payload.update(extra)
    return payload


def symbol_state(response_or_snapshot, symbol):
    snapshot = response_or_snapshot.get("snapshot", response_or_snapshot)
    for entry in snapshot["content"]["symbols"]:
        if entry["symbol"] == symbol:
            return entry["state"]
    raise KeyError(symbol)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def buy_twap_stream():
    """AAA: a seller rests 3@101; a 5-unit TWAP buy releases its first [3] slice.

    Plan state after the stream: ACTIVE, total 5, filled 3, notional 303,
    benchmark 100.
    """
    return [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 101),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
    ]


def sell_twap_stream():
    """AAA: a buyer rests 3@99; a 5-unit TWAP sell releases its first [3] slice.

    Plan state after the stream: ACTIVE, total 5, filled 3, notional 297,
    benchmark 100.
    """
    return [
        add("e0", "AAA", 1, "b1", "BUY", "LIMIT", 3, 99),
        twap_start("e1", "AAA", 2, "p", "SELL", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
    ]


# ---------------------------------------------------------------------------
# Success shape and arithmetic
# ---------------------------------------------------------------------------


def test_buy_plan_tca_report_shape_and_arithmetic():
    out = replay_events(buy_twap_stream() + [tca("q1", "AAA", 4, "p", 103)])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The seller's level was consumed by the slice earlier; the query itself
    # changes nothing.
    assert r["bids"] == [] and r["asks"] == []
    analysis = r["plan_tca_analysis"]
    assert set(analysis) == {
        "plan_id", "algorithm", "side", "status", "benchmark_price",
        "total_quantity", "executed_notional", "vwap", "mark_price",
        "opportunity_quantity", "execution_slippage_notional",
        "opportunity_cost_notional", "implementation_shortfall_notional",
    }
    assert analysis["plan_id"] == "p"
    assert analysis["algorithm"] == "TWAP"
    assert analysis["side"] == "BUY"
    assert analysis["status"] == PLAN_ACTIVE
    assert analysis["benchmark_price"] == 100
    assert analysis["total_quantity"] == 5
    assert analysis["executed_notional"] == 303
    assert analysis["vwap"] == {"numerator": 303, "denominator": 3}
    assert analysis["mark_price"] == 103
    # 5 - 3 = 2.
    assert analysis["opportunity_quantity"] == 2
    # 303 - 100*3 = 3.
    assert analysis["execution_slippage_notional"] == 3
    # (103 - 100) * 2 = 6.
    assert analysis["opportunity_cost_notional"] == 6
    # 3 + 6 = 9.
    assert analysis["implementation_shortfall_notional"] == 9
    # No other result kind carries the object.
    assert "execution_plan" not in r
    assert "execution_analysis" not in r


def test_sell_plan_formulas_are_negated():
    out = replay_events(sell_twap_stream() + [tca("q1", "AAA", 4, "p", 97)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["side"] == "SELL"
    assert analysis["executed_notional"] == 297
    assert analysis["vwap"] == {"numerator": 297, "denominator": 3}
    assert analysis["opportunity_quantity"] == 2
    # -(297 - 100*3) = 3.
    assert analysis["execution_slippage_notional"] == 3
    # -((97 - 100) * 2) = 6.
    assert analysis["opportunity_cost_notional"] == 6
    assert analysis["implementation_shortfall_notional"] == 9


def test_sell_opportunity_cost_negative_when_mark_rises():
    out = replay_events(sell_twap_stream() + [tca("q1", "AAA", 4, "p", 103)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    # A sell benefits from a rising mark: -((103-100)*2) = -6.
    assert analysis["opportunity_cost_notional"] == -6
    # Slippage still +3: sold at 99, below the 100 benchmark.
    assert analysis["execution_slippage_notional"] == 3
    assert analysis["implementation_shortfall_notional"] == -3


def test_buy_slippage_negative_means_improvement():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 99),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 4, "p", 98)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    # 3*99 - 100*3 = -3: buying below benchmark is an improvement.
    assert analysis["execution_slippage_notional"] == -3
    # (98 - 100) * 2 = -4: prices falling is good for the unfilled balance.
    assert analysis["opportunity_cost_notional"] == -4
    assert analysis["implementation_shortfall_notional"] == -7


def test_plan_without_fills_reports_null_vwap_and_zero_slippage():
    stream = [
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 2, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 3, "p", 103)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["status"] == PLAN_ACTIVE
    assert analysis["executed_notional"] == 0
    assert analysis["vwap"] is None
    assert analysis["opportunity_quantity"] == 5
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 15
    assert analysis["implementation_shortfall_notional"] == 15


def test_query_before_any_slice_uses_full_total_as_opportunity():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q1", "AAA", 2, "p", 100),
    ])
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["opportunity_quantity"] == 5
    assert analysis["vwap"] is None
    assert analysis["opportunity_cost_notional"] == 0


def test_completed_plan_has_zero_opportunity_quantity():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 1, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 4, "p", 103)])
    plan = out["results"][2]["execution_plan"]
    assert plan["status"] == PLAN_COMPLETED
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["status"] == PLAN_COMPLETED
    assert analysis["opportunity_quantity"] == 0
    assert analysis["opportunity_cost_notional"] == 0
    assert analysis["execution_slippage_notional"] == 5
    assert analysis["implementation_shortfall_notional"] == 5


def test_cancelled_plan_is_queryable_and_keeps_unfilled_opportunity():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 101),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
        twap_cancel("e3", "AAA", 4, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 5, "p", 103)])
    assert out["results"][3]["execution_plan"]["status"] == PLAN_CANCELLED
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["status"] == PLAN_CANCELLED
    # The cancelled 2 units are still part of the unfilled opportunity.
    assert analysis["opportunity_quantity"] == 2
    assert analysis["execution_slippage_notional"] == 3
    assert analysis["opportunity_cost_notional"] == 6
    assert analysis["implementation_shortfall_notional"] == 9


def test_vwap_plan_reports_vwap_algorithm_and_schedule_notional():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 5, 101),
        vwap_start("e1", "AAA", 2, "p", "BUY", 10, [1, 1], "MARKET", 100),
        vwap_slice("e2", "AAA", 3, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 4, "p", 102)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["algorithm"] == "VWAP"
    assert analysis["total_quantity"] == 10
    assert analysis["executed_notional"] == 505
    assert analysis["vwap"] == {"numerator": 505, "denominator": 5}
    assert analysis["opportunity_quantity"] == 5
    assert analysis["execution_slippage_notional"] == 5
    assert analysis["opportunity_cost_notional"] == 10
    assert analysis["implementation_shortfall_notional"] == 15


def test_pov_plan_reports_pov_algorithm_and_explicit_total():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 6, 100),
        pov_start("e1", "AAA", 2, "p", "BUY", 10, 5000, 100),
        pov_volume("e2", "AAA", 3, "p", 10),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 4, "p", 102)])
    plan = out["results"][2]["execution_plan"]
    assert plan["algorithm"] == "POV"
    assert plan["filled_quantity"] == 5
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["algorithm"] == "POV"
    assert analysis["total_quantity"] == 10
    assert analysis["executed_notional"] == 500
    assert analysis["vwap"] == {"numerator": 500, "denominator": 5}
    assert analysis["opportunity_quantity"] == 5
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 10
    assert analysis["implementation_shortfall_notional"] == 10


def test_pov_cancelled_plan_is_queryable():
    stream = [
        pov_start("e1", "AAA", 1, "p", "SELL", 8, 2500, 100),
        pov_cancel("e2", "AAA", 2, "p"),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 3, "p", 97)])
    analysis = out["results"][-1]["plan_tca_analysis"]
    assert analysis["algorithm"] == "POV"
    assert analysis["status"] == PLAN_CANCELLED
    assert analysis["side"] == "SELL"
    assert analysis["opportunity_quantity"] == 8
    assert analysis["execution_slippage_notional"] == 0
    assert analysis["opportunity_cost_notional"] == 24
    assert analysis["implementation_shortfall_notional"] == 24


def test_repeated_queries_with_different_marks_are_both_reported():
    out = replay_events(buy_twap_stream() + [
        tca("q1", "AAA", 4, "p", 103),
        tca("q2", "AAA", 5, "p", 110),
    ])
    first = out["results"][-2]["plan_tca_analysis"]
    second = out["results"][-1]["plan_tca_analysis"]
    assert first["mark_price"] == 103
    assert first["implementation_shortfall_notional"] == 9
    assert second["mark_price"] == 110
    # (110 - 100) * 2 = 20 opportunity cost; 3 + 20 = 23.
    assert second["opportunity_cost_notional"] == 20
    assert second["implementation_shortfall_notional"] == 23
    # The plan figures the two queries share stay identical.
    for key in ("plan_id", "algorithm", "side", "status", "benchmark_price",
                "total_quantity", "executed_notional", "vwap",
                "opportunity_quantity", "execution_slippage_notional"):
        assert first[key] == second[key]


# ---------------------------------------------------------------------------
# Business rejection: UNKNOWN_EXECUTION_PLAN
# ---------------------------------------------------------------------------


def test_unknown_plan_is_business_rejection_that_consumes_id_and_sequence():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q1", "AAA", 2, "missing", 103),
        tca("q2", "AAA", 3, "p", 103),
    ])
    rejection = out["results"][1]
    assert rejection["status"] == REJECTED
    assert rejection["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert rejection["trades"] == []
    assert rejection["book_changes"] == {"bids": [], "asks": []}
    assert "plan_tca_analysis" not in rejection
    # Sequence 2 was consumed: the next event arrives at 3.
    assert out["results"][2]["status"] == ACCEPTED
    state = symbol_state(out, "AAA")
    assert state["last_sequence"] == 3
    assert "q1" in {e["event_id"] for e in state["event_log"]}
    # A replay-only id never reaches the engine journal.
    assert "q1" not in state["engine"]["event_ids"]


def test_plan_existing_only_on_another_symbol_is_unknown():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q1", "BBB", 1, "p", 103),
    ])
    rejection = out["results"][1]
    assert rejection["symbol"] == "BBB"
    assert rejection["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert rejection["bids"] == [] and rejection["asks"] == []
    # The well-formed event registers its fresh envelope symbol.
    bbb = symbol_state(out, "BBB")
    assert bbb["last_sequence"] == 1
    assert bbb["engine"]["event_ids"] == []
    assert "q1" in {e["event_id"] for e in bbb["event_log"]}
    # The AAA plan is untouched and still queryable on AAA.
    assert replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q0", "AAA", 2, "p", 103),
    ])["results"][-1]["status"] == ACCEPTED


def test_unknown_plan_on_brand_new_symbol_then_a_real_plan_event_follows():
    out = replay_events([
        tca("q1", "ZZZ", 1, "p", 103),
        twap_start("e1", "ZZZ", 2, "p", "BUY", 4, 1, "MARKET", 100),
        tca("q2", "ZZZ", 3, "p", 102),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
    assert out["results"][2]["status"] == ACCEPTED
    assert out["results"][2]["plan_tca_analysis"]["opportunity_quantity"] == 4


# ---------------------------------------------------------------------------
# Structural validation: INVALID_EVENT consumes nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mutation", [
    lambda e: e.pop("plan_id"),                                  # missing
    lambda e: e.pop("mark_price"),                               # missing
    lambda e: e.update({"unexpected": 1}),                       # extra
    lambda e: e.update({"plan_id": ""}),                         # empty plan id
    lambda e: e.update({"plan_id": 7}),                          # non-string plan id
    lambda e: e.update({"mark_price": 0}),                       # zero
    lambda e: e.update({"mark_price": -1}),                      # negative
    lambda e: e.update({"mark_price": True}),                    # boolean
    lambda e: e.update({"mark_price": 1.5}),                     # float
    lambda e: e.update({"mark_price": "103"}),                   # string
    lambda e: e.update({"mark_price": None}),                    # null
    lambda e: e.update({"type": "TWAP_REPORT"}),                 # wrong payload type
])
def test_malformed_tca_events_are_invalid(mutation):
    event = tca("q1", "AAA", 2, "p", 103)
    mutation(event)
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        event,
    ])
    result = out["results"][-1]
    assert result["rejection_code"] == INVALID_EVENT
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": []}


def test_invalid_event_consumes_neither_id_nor_sequence():
    stream = [twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100)]
    bad = tca("q1", "AAA", 2, "p", True)
    out = replay_events(stream + [
        bad,
        tca("q1", "AAA", 2, "p", 103),     # same id and sequence now free
    ])
    assert out["results"][-2]["rejection_code"] == INVALID_EVENT
    accepted = out["results"][-1]
    assert accepted["status"] == ACCEPTED
    assert accepted["event_id"] == "q1"
    assert accepted["sequence"] == 2


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([tca("q1", "ZZZ", 1, "p", 0)])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events(buy_twap_stream() + [{
        "event_id": "q1",
        "symbol": "AAA",
        "sequence": 4,
        "event": {
            "event_id": "q1",
            "type": PLAN_TCA_REPORT,
            "plan_id": "p",
            "mark_price": 103,
        },
    }])
    assert out["results"][-1]["status"] == ACCEPTED
    assert out["results"][-1]["plan_tca_analysis"]["mark_price"] == 103


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events(buy_twap_stream() + [{
        "event_id": "q1",
        "symbol": "AAA",
        "sequence": 4,
        "event": {
            "event_id": "OTHER",
            "type": PLAN_TCA_REPORT,
            "plan_id": "p",
            "mark_price": 103,
        },
    }])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT


def test_inline_envelope_may_not_carry_unknown_fields():
    event = tca("q1", "AAA", 2, "p", 103)
    event["benchmark_price"] = 100
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        event,
    ])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency and sequencing reuse the existing semantics
# ---------------------------------------------------------------------------


def test_identical_query_replay_is_duplicate_even_with_stale_sequence():
    query = tca("q1", "AAA", 4, "p", 103)
    out = replay_events(buy_twap_stream() + [
        query,
        tca("q1", "AAA", 4, "p", 103),                 # retried delivery
        add("a9", "AAA", 5, "b9", "BUY", "LIMIT", 1, 90),
    ])
    duplicate = out["results"][-2]
    assert duplicate["status"] == DUPLICATE
    assert duplicate["trades"] == []
    assert duplicate["book_changes"] == {"bids": [], "asks": []}
    assert "plan_tca_analysis" not in duplicate
    assert out["results"][-1]["result"] == "RESTING"


def test_same_query_id_with_different_mark_conflicts():
    out = replay_events(buy_twap_stream() + [
        tca("q1", "AAA", 4, "p", 103),
        tca("q1", "AAA", 5, "p", 104),                 # different content
    ])
    assert out["results"][-1]["rejection_code"] == EVENT_ID_CONFLICT


def test_same_query_id_on_another_symbol_conflicts():
    out = replay_events([
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q1", "AAA", 2, "p", 103),
        tca("q1", "BBB", 1, "p", 103),
    ])
    assert out["results"][-1]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_the_plan_lookup():
    stream = [twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100)]
    gapped = replay_events(stream + [
        tca("q1", "AAA", 3, "p", 103),                 # gap, plan exists
        tca("q2", "AAA", 1, "p", 103),                 # backwards
    ])
    assert gapped["results"][-2]["rejection_code"] == SEQUENCE_GAP
    assert gapped["results"][-2]["expected_sequence"] == 2
    assert gapped["results"][-1]["rejection_code"] == OUT_OF_ORDER
    assert gapped["results"][-1]["expected_sequence"] == 2
    # Neither event consumed sequence 2: a clean query follows immediately.
    fixed = replay_events(stream + [
        tca("q3", "AAA", 2, "p", 103),
    ])
    assert fixed["results"][-1]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Read-only behaviour
# ---------------------------------------------------------------------------


def test_query_is_read_only_across_the_whole_snapshot():
    stream = buy_twap_stream()
    before = replay_events(stream)["snapshot"]
    after = replay_events(stream + [tca("qX", "AAA", 4, "p", 103)])["snapshot"]
    assert [s["symbol"] for s in before["content"]["symbols"]] == [
        s["symbol"] for s in after["content"]["symbols"]
    ]
    for sb, sa in zip(before["content"]["symbols"], after["content"]["symbols"]):
        assert canonical_json(sb["state"]["engine"]) == canonical_json(sa["state"]["engine"])
        assert canonical_json(sb["state"]["plans"]) == canonical_json(sa["state"]["plans"])
        assert sb["state"]["price_limits"] == sa["state"]["price_limits"]
    aaa_after = symbol_state(after, "AAA")
    assert "qX" in {e["event_id"] for e in aaa_after["event_log"]}
    assert "qX" not in aaa_after["engine"]["event_ids"]
    assert aaa_after["last_sequence"] == 4


def test_query_does_not_release_a_slice_or_spend_a_trade_id():
    stream = buy_twap_stream()
    control = replay_events(stream + [
        add("a9", "AAA", 4, "s2", "SELL", "LIMIT", 2, 102),
        twap_slice("e3", "AAA", 5, "p"),
    ])
    with_query = replay_events(stream + [
        tca("q1", "AAA", 4, "p", 103),
        add("a9", "AAA", 5, "s2", "SELL", "LIMIT", 2, 102),
        twap_slice("e3", "AAA", 6, "p"),
    ])
    # The second slice produces trade id 2 in both runs (the query spent
    # nothing) and fills the same 2 units at 102.
    assert control["results"][-1]["trades"] == with_query["results"][-1]["trades"]
    control_plan = control["results"][-1]["execution_plan"]
    queried_plan = with_query["results"][-1]["execution_plan"]
    assert queried_plan["released_quantity"] == control_plan["released_quantity"]
    assert queried_plan["filled_quantity"] == control_plan["filled_quantity"]
    assert queried_plan["executed_notional"] == control_plan["executed_notional"]


def test_query_does_not_move_active_price_limits():
    config = {"price_limits": {"AAA": {"lower": 90, "upper": 110}}}
    out = replay_events(
        buy_twap_stream() + [tca("q1", "AAA", 4, "p", 999)],
        config=config,
    )
    assert out["results"][-1]["status"] == ACCEPTED
    assert symbol_state(out, "AAA")["price_limits"] == {"lower": 90, "upper": 110}


def test_query_echoes_untouched_resting_book():
    stream = [
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 4, 101),
        add("e1", "AAA", 2, "b0", "BUY", "LIMIT", 2, 95),
        twap_start("e2", "AAA", 3, "p", "BUY", 5, 2, "MARKET", 100),
    ]
    out = replay_events(stream + [tca("q1", "AAA", 4, "p", 103)])
    r = out["results"][-1]
    assert r["bids"] == [{"price": 95, "quantity": 2}]
    assert r["asks"] == [{"price": 101, "quantity": 4}]


# ---------------------------------------------------------------------------
# Determinism, snapshot export and restoration
# ---------------------------------------------------------------------------


def test_output_is_byte_for_byte_deterministic():
    events_a = buy_twap_stream() + [tca("q1", "AAA", 4, "p", 103)]
    events_b = copy.deepcopy(events_a)
    assert canonical_json(replay_events(events_a)) == canonical_json(replay_events(events_b))


def test_restored_results_are_byte_identical_to_continuous_replay():
    tail = [
        tca("q1", "AAA", 4, "p", 103),
        tca("bad", "AAA", 5, "p", True),                    # invalid: no consume
        tca("q2", "AAA", 5, "p", 97),                       # sell-side style mark
        tca("q3", "AAA", 6, "nope", 100),                   # unknown plan
        twap_slice("e3", "AAA", 7, "p"),
        tca("q4", "AAA", 8, "p", 104),
    ]
    one_shot = replay_events(buy_twap_stream() + tail)
    snapshot = replay_events(buy_twap_stream())["snapshot"]
    resumed = replay_events(copy.deepcopy(tail), snapshot=snapshot)
    assert canonical_json(resumed["results"]) == canonical_json(
        one_shot["results"][len(buy_twap_stream()):]
    )
    assert canonical_json(resumed["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_query_roundtrips_through_snapshot_and_stays_idempotent():
    snapshot = replay_events(
        buy_twap_stream() + [tca("q1", "AAA", 4, "p", 103)]
    )["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [tca("q1", "AAA", 4, "p", 103)],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


def test_snapshot_after_named_tca_event():
    out = replay_events(
        buy_twap_stream() + [tca("q1", "AAA", 4, "p", 103)],
        snapshot_after={"symbol": "AAA", "sequence": 4},
    )
    aaa = symbol_state(out, "AAA")
    assert aaa["last_sequence"] == 4
    assert "q1" in {e["event_id"] for e in aaa["event_log"]}


# ---------------------------------------------------------------------------
# Stateful EventReplayer
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_plan_tca_report():
    replayer = EventReplayer()
    replayer.submit([
        add("e0", "AAA", 1, "s1", "SELL", "LIMIT", 3, 101),
        twap_start("e1", "AAA", 2, "p", "BUY", 5, 2, "MARKET", 100),
        twap_slice("e2", "AAA", 3, "p"),
    ])
    r = replayer.submit([tca("q1", "AAA", 4, "p", 103)])
    assert r[0]["status"] == ACCEPTED
    assert r[0]["result"] == "REPORTED"
    assert r[0]["plan_tca_analysis"]["implementation_shortfall_notional"] == 9
    assert replayer.book("AAA") == ([], [])


# ---------------------------------------------------------------------------
# The baseline single-security entry point keeps rejecting the new event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_plan_tca_report_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1",
        "type": PLAN_TCA_REPORT,
        "plan_id": "p",
        "mark_price": 103,
    }
    result = engine.handle_object_position(query)
    assert result[1] == REJECTED
    assert result[2] == INVALID_SCHEMA
    # Structural rejection occupies no id: a second attempt is identical.
    assert engine.handle_object_position(dict(query))[2] == INVALID_SCHEMA


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
    code, out, err = _run_cli({"events": buy_twap_stream() + [
        tca("q1", "AAA", 4, "p", 103)
    ]})
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["plan_tca_analysis"]["implementation_shortfall_notional"] == 9
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_reports_unknown_plan():
    code, out, err = _run_cli({"events": [
        twap_start("e1", "AAA", 1, "p", "BUY", 5, 2, "MARKET", 100),
        tca("q1", "AAA", 2, "ghost", 103),
    ]})
    assert code == 0
    assert err == ""
    assert out["results"][-1]["rejection_code"] == UNKNOWN_EXECUTION_PLAN
