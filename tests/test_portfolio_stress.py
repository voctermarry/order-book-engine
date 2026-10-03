"""Tests for the cross-security PORTFOLIO_STRESS_REPORT event in the multi-symbol stream."""

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
    MARK_PRICE_MISMATCH,
    OUT_OF_ORDER,
    PORTFOLIO_STRESS_REPORT,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ACCOUNT,
    canonical_json,
    replay_events,
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


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def stress(event_id, symbol, sequence, account_id, mark_prices, scenarios, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": PORTFOLIO_STRESS_REPORT,
        "account_id": account_id,
        "mark_prices": mark_prices,
        "scenarios": scenarios,
    }
    event.update(extra)
    return event


def twap_start(event_id, symbol, sequence, plan_id, side, total_quantity, slice_count,
               price, account_id, benchmark_price=None):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": TWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "slice_count": slice_count,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price, "account_id": account_id,
    }


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": TWAP_SLICE, "plan_id": plan_id}


# ---------------------------------------------------------------------------
# Fixture: maker / taker / replace / iceberg activity across three securities
# ---------------------------------------------------------------------------


def _rich_stream():
    """fund's activity, with anonymous counterparties:

    AAA: fund sells 2@100 as maker, the remaining 4 are REPLACED to 101 and
    sold to an anonymous market buyer: sells 6, notional 604.
    BBB: fund's iceberg buys 4@50 as taker, then makes the last 1@50: buys 5,
    notional 250.
    CCC: fund's only order is cancelled: known, no trades.
    """
    return [
        # AAA sequence 1..4
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 6, 100, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
        replace("a3", "AAA", 3, "s1", 4, 101),
        add("a4", "AAA", 4, "b2", "BUY", "MARKET", 4),
        # BBB sequence 1..3
        add("b1", "BBB", 1, "x1", "SELL", "LIMIT", 4, 50),
        iceberg("b2", "BBB", 2, "i1", "BUY", 5, 50, 2, account_id="fund"),
        add("b3", "BBB", 3, "z1", "SELL", "MARKET", 1),
        # CCC sequence 1..2
        add("c1", "CCC", 1, "o1", "BUY", "LIMIT", 3, 10, account_id="fund"),
        cancel("c2", "CCC", 2, "o1"),
    ]


_MARKS = {"AAA": 110, "BBB": 40, "CCC": 7}
_SCENARIOS = [
    {"name": "up", "prices": {"AAA": 120, "BBB": 50, "CCC": 7}},
    {"name": "down", "prices": {"AAA": 100, "BBB": 30, "CCC": 7}},
]


def _expected_rich_analysis():
    # Positions: AAA net -6 cash +604; BBB net +5 cash -250; CCC flat.
    # Baseline: cash 354; mtm 354 - 660 + 200 = -106; exposure 660 + 200 = 860.
    return {
        "account_id": "fund",
        "baseline_mark_to_market_pnl": -106,
        "baseline_risk_exposure": 860,
        "scenarios": [
            {
                "name": "up",
                "positions": [
                    {"symbol": "AAA", "shocked_price": 120,
                     "net_position": -6, "pnl_change": -60},
                    {"symbol": "BBB", "shocked_price": 50,
                     "net_position": 5, "pnl_change": 50},
                    {"symbol": "CCC", "shocked_price": 7,
                     "net_position": 0, "pnl_change": 0},
                ],
                "stressed_mark_to_market_pnl": -116,
                "risk_exposure": 970,
                "total_pnl_change": -10,
            },
            {
                "name": "down",
                "positions": [
                    {"symbol": "AAA", "shocked_price": 100,
                     "net_position": -6, "pnl_change": 60},
                    {"symbol": "BBB", "shocked_price": 30,
                     "net_position": 5, "pnl_change": -50},
                    {"symbol": "CCC", "shocked_price": 7,
                     "net_position": 0, "pnl_change": 0},
                ],
                "stressed_mark_to_market_pnl": -96,
                "risk_exposure": 750,
                "total_pnl_change": 10,
            },
        ],
        "worst_scenario": "up",
    }


# ---------------------------------------------------------------------------
# Success path and result shape
# ---------------------------------------------------------------------------


def test_stress_report_success_shape_and_analysis():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", dict(_MARKS), copy.deepcopy(_SCENARIOS))
    ]
    out = replay_events(events)
    r = out["results"][-1]
    assert r["event_id"] == "r1"
    assert r["symbol"] == "AAA"
    assert r["sequence"] == 5
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["bids"] == []
    assert r["asks"] == []
    assert "execution_plan" not in r
    assert "rejection_code" not in r
    assert r["portfolio_stress_analysis"] == _expected_rich_analysis()


def test_stress_report_echoes_envelope_symbol_book():
    events = _rich_stream() + [
        add("a5", "AAA", 5, "s9", "SELL", "LIMIT", 3, 123),
        stress("r1", "AAA", 6, "fund", dict(_MARKS), copy.deepcopy(_SCENARIOS)),
    ]
    r = replay_events(events)["results"][-1]
    assert r["asks"] == [{"price": 123, "quantity": 3}]
    assert r["bids"] == []


def test_scenarios_keep_request_order_and_positions_are_symbol_sorted():
    scenarios = [
        {"name": "zeta", "prices": {"CCC": 7, "BBB": 50, "AAA": 120}},
        {"name": "alpha", "prices": {"BBB": 30, "AAA": 100, "CCC": 7}},
    ]
    events = _rich_stream() + [
        stress("r1", "BBB", 4, "fund", dict(_MARKS), scenarios)
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    assert [s["name"] for s in analysis["scenarios"]] == ["zeta", "alpha"]
    for scenario in analysis["scenarios"]:
        assert [p["symbol"] for p in scenario["positions"]] == ["AAA", "BBB", "CCC"]
        assert all(
            set(p) == {"symbol", "shocked_price", "net_position", "pnl_change"}
            for p in scenario["positions"]
        )
        assert set(scenario) == {
            "name", "positions", "stressed_mark_to_market_pnl",
            "risk_exposure", "total_pnl_change",
        }
        # The stressed mark-to-market always equals baseline plus the change.
        assert scenario["stressed_mark_to_market_pnl"] == (
            analysis["baseline_mark_to_market_pnl"] + scenario["total_pnl_change"]
        )
    assert set(analysis) == {
        "account_id", "baseline_mark_to_market_pnl", "baseline_risk_exposure",
        "scenarios", "worst_scenario",
    }


def test_worst_scenario_tie_breaks_by_name():
    # Two flat scenarios tie at zero change: the lexicographically smaller
    # name wins even when it arrives later.
    scenarios = [
        {"name": "beta", "prices": dict(_MARKS)},
        {"name": "alpha", "prices": dict(_MARKS)},
    ]
    events = _rich_stream() + [stress("r1", "AAA", 5, "fund", dict(_MARKS), scenarios)]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    assert analysis["worst_scenario"] == "alpha"
    assert all(s["total_pnl_change"] == 0 for s in analysis["scenarios"])


def test_stress_report_is_read_only():
    events = _rich_stream()
    before = replay_events(events)["snapshot"]
    with_report = replay_events(
        events + [stress("r1", "AAA", 5, "fund", dict(_MARKS),
                         copy.deepcopy(_SCENARIOS))]
    )["snapshot"]

    def engine_states(snapshot):
        return {s["symbol"]: s["state"]["engine"]
                for s in snapshot["content"]["symbols"]}
    before_states = engine_states(before)
    after_states = engine_states(with_report)
    assert set(before_states) == set(after_states)
    for sym in before_states:
        assert canonical_json(before_states[sym]) == canonical_json(after_states[sym])
    # The report id lives solely in the replay log, not in AAA's engine journal.
    assert "r1" not in after_states["AAA"]["event_ids"]
    aaa_state = [s for s in with_report["content"]["symbols"]
                 if s["symbol"] == "AAA"][0]["state"]
    assert aaa_state["last_sequence"] == 5


def test_report_does_not_consume_trade_ids_on_a_trading_symbol():
    # BBB spent trade ids 1-2 in the fixture; further trades keep numbering 3.
    snap = replay_events(
        _rich_stream() + [stress("r0", "BBB", 4, "fund", dict(_MARKS),
                                 copy.deepcopy(_SCENARIOS))]
    )["snapshot"]
    out = replay_events(
        [add("b4", "BBB", 5, "x2", "SELL", "LIMIT", 1, 9),
         add("b5", "BBB", 6, "b9", "BUY", "LIMIT", 1, 9)],
        snapshot=snap,
    )
    assert [t["trade_id"] for t in out["results"][1]["trades"]] == [3]


# ---------------------------------------------------------------------------
# Account knownness and plan child fills
# ---------------------------------------------------------------------------


def test_account_known_through_plan_start_without_any_slice():
    out = replay_events([
        twap_start("d1", "DDD", 1, "p1", "BUY", 4, 2, 70, "tp"),
        stress("r1", "DDD", 2, "tp", {"DDD": 70},
               [{"name": "s", "prices": {"DDD": 80}}]),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    scenario = r["portfolio_stress_analysis"]["scenarios"][0]
    assert scenario["positions"] == [
        {"symbol": "DDD", "shocked_price": 80, "net_position": 0, "pnl_change": 0}
    ]
    assert scenario["stressed_mark_to_market_pnl"] == 0


def test_released_plan_child_fills_are_stressed():
    out = replay_events([
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "BUY", 3, 3, 70, "tp"),
        twap_slice("d3", "DDD", 3, "p1"),
        stress("r1", "DDD", 4, "tp", {"DDD": 70},
               [{"name": "s", "prices": {"DDD": 90}}]),
    ])
    analysis = out["results"][-1]["portfolio_stress_analysis"]
    # The first slice released exactly 1 unit bought at 70.
    assert analysis["baseline_mark_to_market_pnl"] == -70 + 70
    scenario = analysis["scenarios"][0]
    assert scenario["positions"] == [
        {"symbol": "DDD", "shocked_price": 90, "net_position": 1, "pnl_change": 20}
    ]
    assert scenario["stressed_mark_to_market_pnl"] == -70 + 90
    assert scenario["risk_exposure"] == 90


def test_account_from_rejected_add_is_unknown():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("a2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100, account_id="ghost"),
        stress("r1", "AAA", 3, "ghost", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ACCOUNT)


def test_report_can_be_first_event_on_its_envelope_symbol():
    out = replay_events([
        add("d1", "DDD", 1, "o1", "BUY", "LIMIT", 2, 70, account_id="fund"),
        stress("r1", "EEE", 1, "fund", {"DDD": 70},
               [{"name": "s", "prices": {"DDD": 70}}]),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["bids"] == [] and r["asks"] == []
    eee = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "EEE"][0]
    assert eee["state"]["last_sequence"] == 1
    assert eee["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# UNKNOWN_ACCOUNT
# ---------------------------------------------------------------------------


def test_unknown_account_rejects_and_consumes_id_and_sequence():
    prefix = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ]
    out = replay_events(prefix)
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ACCOUNT)
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert "portfolio_stress_analysis" not in r
    # Sequence 2 was consumed: another event at sequence 2 is out of order.
    stale = replay_events(prefix + [
        stress("rX", "AAA", 2, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert stale["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events(prefix + [
        stress("r2", "AAA", 3, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


def test_unknown_account_report_id_is_occupied():
    base = [stress("r1", "AAA", 1, "gone", {"AAA": 100},
                   [{"name": "s", "prices": {"AAA": 100}}])]
    out = replay_events(base + [
        stress("r1", "AAA", 1, "gone", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert out["results"][1]["status"] == DUPLICATE
    out2 = replay_events(base + [
        stress("r1", "AAA", 2, "gone", {"AAA": 101},
               [{"name": "s", "prices": {"AAA": 101}}]),
    ])
    assert out2["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


def test_unknown_account_takes_precedence_over_mark_mismatch():
    out = replay_events([
        stress("r1", "AAA", 1, "gone", {"AAA": 100, "BBB": 50},
               [{"name": "s", "prices": {"AAA": 1}}]),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT


# ---------------------------------------------------------------------------
# MARK_PRICE_MISMATCH
# ---------------------------------------------------------------------------


def test_mark_price_mismatch_cases():
    base = _rich_stream()
    full = dict(_MARKS)
    cases = [
        {"AAA": 110, "BBB": 40},          # missing CCC
        {**full, "DDD": 9},               # extra DDD
        {"DDD": 9},                       # wrong-only
        {},                               # empty map
    ]
    for marks in cases:
        out = replay_events(base + [
            stress("r1", "AAA", 5, "fund", marks, copy.deepcopy(_SCENARIOS))
        ])
        r = out["results"][-1]
        assert (r["status"], r["rejection_code"]) == (REJECTED, MARK_PRICE_MISMATCH), marks
        assert r["trades"] == []
        assert "portfolio_stress_analysis" not in r


def test_scenario_price_mismatch_cases():
    base = _rich_stream()
    bad_prices = [
        {"AAA": 110, "BBB": 40},                  # missing CCC
        {"AAA": 110, "BBB": 40, "CCC": 7, "DDD": 9},  # extra DDD
        {},                                       # empty map
    ]
    for prices in bad_prices:
        out = replay_events(base + [
            stress("r1", "AAA", 5, "fund", dict(_MARKS),
                   [{"name": "s", "prices": prices}])
        ])
        r = out["results"][-1]
        assert (r["status"], r["rejection_code"]) == (REJECTED, MARK_PRICE_MISMATCH), prices


def test_mismatch_consumes_id_and_sequence():
    prefix = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"BBB": 50},
               [{"name": "s", "prices": {"BBB": 50}}]),
    ]
    out = replay_events(prefix + [
        stress("r2", "AAA", 2, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert out["results"][1]["rejection_code"] == MARK_PRICE_MISMATCH
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events(prefix + [
        stress("r3", "AAA", 3, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 100}}]),
    ])
    assert follow["results"][2]["status"] == ACCEPTED
    assert out["results"][1]["asks"] == [{"price": 100, "quantity": 2}]


# ---------------------------------------------------------------------------
# INVALID_EVENT: structural validation consumes nothing
# ---------------------------------------------------------------------------


def _ok_scenarios():
    return [{"name": "s", "prices": {"AAA": 100}}]


@pytest.mark.parametrize("bad", [
    {"account_id": "fund", "mark_prices": {"AAA": 100},
     "scenarios": _ok_scenarios()},                                     # missing event_id/type
    {"event_id": "r1", "account_id": "fund", "mark_prices": {"AAA": 100},
     "scenarios": _ok_scenarios()},                                     # missing type
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
     "mark_prices": {"AAA": 100}, "scenarios": _ok_scenarios()},        # missing account_id
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "scenarios": _ok_scenarios()},                                     # missing mark_prices
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}},                                      # missing scenarios
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "scenarios": _ok_scenarios(),
     "order_id": "o1"},                                                 # extra field
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "",
     "mark_prices": {"AAA": 100}, "scenarios": _ok_scenarios()},        # empty account id
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": 7,
     "mark_prices": {"AAA": 100}, "scenarios": _ok_scenarios()},        # non-string account
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": None, "scenarios": _ok_scenarios()},                # null map
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": True}, "scenarios": _ok_scenarios()},       # bool mark
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 0}, "scenarios": _ok_scenarios()},          # zero mark
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 1.5}, "scenarios": _ok_scenarios()},        # float mark
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"": 100}, "scenarios": _ok_scenarios()},           # empty symbol key
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "scenarios": None},                   # null scenarios
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "scenarios": []},                     # empty scenarios
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "scenarios": "up"},                   # non-list scenarios
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "scenarios": ["up"]},                 # non-object scenario
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s"}]},                                     # missing prices
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"prices": {"AAA": 100}}]},                          # missing name
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": {"AAA": 100}, "x": 1}]},     # extra scenario field
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "", "prices": {"AAA": 100}}]},              # empty name
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": 7, "prices": {"AAA": 100}}]},               # non-string name
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": {"AAA": 100}},
                   {"name": "s", "prices": {"AAA": 90}}]},              # duplicate name
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": None}]},                     # null prices
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": {"AAA": -1}}]},              # negative price
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": {"AAA": "100"}}]},           # string price
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [{"name": "s", "prices": {"AAA": False}}]},           # bool price
])
def test_malformed_stress_events_are_invalid(bad):
    event = {"symbol": "AAA", "sequence": 1, **bad}
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        event,
    ]) if "event_id" in bad else replay_events([event])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT, bad
    assert out["results"][-1]["trades"] == []


def test_invalid_event_consumes_neither_id_nor_sequence():
    base = [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund")]
    bad = stress("r1", "AAA", 2, "fund", {"AAA": 100}, [])
    ok = stress("r2", "AAA", 2, "fund", {"AAA": 100}, _ok_scenarios())
    out = replay_events(base + [bad, ok])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    retry = replay_events(base + [bad, stress("r1", "AAA", 2, "fund", {"AAA": 100},
                                              _ok_scenarios())])
    assert retry["results"][1]["rejection_code"] == INVALID_EVENT
    assert retry["results"][2]["status"] == ACCEPTED


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([stress("r1", "EEE", 1, "fund", {"EEE": 0}, _ok_scenarios())])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100},
                   "scenarios": _ok_scenarios()}},
    ])
    assert out["results"][1]["status"] == ACCEPTED


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "OTHER", "type": PORTFOLIO_STRESS_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100},
                   "scenarios": _ok_scenarios()}},
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Envelope, idempotency and ordering precedence
# ---------------------------------------------------------------------------


def test_identical_report_replay_is_duplicate():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"AAA": 100}, _ok_scenarios()),
        stress("r1", "AAA", 2, "fund", {"AAA": 100}, _ok_scenarios()),
        add("a2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][2]["trades"] == []
    assert out["results"][3]["result"] == "FILLED"


def test_same_report_id_with_different_content_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"AAA": 100}, _ok_scenarios()),
        stress("r1", "AAA", 3, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 101}}]),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_business_checks():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, _ok_scenarios()),
        stress("r2", "AAA", 1, "gone", {"AAA": 100}, _ok_scenarios()),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, _ok_scenarios()),
        stress("r3", "AAA", 2, "fund", {"AAA": 100}, _ok_scenarios()),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_stress_report_output_is_byte_for_byte_deterministic():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", {"CCC": 7, "AAA": 110, "BBB": 40},
               copy.deepcopy(_SCENARIOS)),
    ]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
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


# ---------------------------------------------------------------------------
# Snapshot export / restore
# ---------------------------------------------------------------------------


def test_resumed_replay_with_stress_reports_matches_one_shot():
    stream = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", dict(_MARKS), copy.deepcopy(_SCENARIOS)),
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "SELL", 2, 2, 70, "fund"),
        twap_slice("d3", "DDD", 3, "p1"),
        stress("r2", "CCC", 3, "fund",
               {"AAA": 105, "BBB": 42, "CCC": 8, "DDD": 66},
               [{"name": "s", "prices": {"AAA": 100, "BBB": 40, "CCC": 7, "DDD": 70}}]),
        stress("r3", "ZZZ", 1, "gone", {"AAA": 1},
               [{"name": "s", "prices": {"AAA": 1}}]),          # business reject
        stress("r4", "AAA", 6, "fund", {"AAA": 1, "BBB": 2},
               [{"name": "s", "prices": {"AAA": 1, "BBB": 2}}]),  # mismatch
    ]
    part1 = stream[:4]
    one_shot = replay_events(stream)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(stream[4:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][4:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_stress_report_works_after_restore_and_snapshot_format_is_unchanged():
    snapshot = replay_events(_rich_stream())["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    out = replay_events(
        [stress("r1", "BBB", 4, "fund", dict(_MARKS), copy.deepcopy(_SCENARIOS))],
        snapshot=snapshot,
    )
    assert out["results"][0]["portfolio_stress_analysis"] == _expected_rich_analysis()


def test_business_rejected_report_roundtrips_through_snapshot():
    from order_book_engine import export_snapshot, restore_replayer
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100}, _ok_scenarios()),
    ])
    snapshot = out["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [stress("r1", "AAA", 2, "gone", {"AAA": 100}, _ok_scenarios())],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


# ---------------------------------------------------------------------------
# EventReplayer stateful session
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_stress_reports():
    from order_book_engine import EventReplayer
    replayer = EventReplayer()
    r1 = replayer.submit([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100, account_id="fund")
    ])
    assert r1[0]["result"] == "RESTING"
    r2 = replayer.submit([
        stress("r1", "AAA", 2, "fund", {"AAA": 100},
               [{"name": "s", "prices": {"AAA": 120}}])
    ])
    assert r2[0]["status"] == ACCEPTED
    analysis = r2[0]["portfolio_stress_analysis"]
    assert analysis["baseline_mark_to_market_pnl"] == 0
    assert analysis["scenarios"][0]["positions"][0]["net_position"] == 0


# ---------------------------------------------------------------------------
# Baseline entry points do not accept the event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_stress_report_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1", "type": PORTFOLIO_STRESS_REPORT,
        "account_id": "fund", "mark_prices": {"AAA": 100},
        "scenarios": [{"name": "s", "prices": {"AAA": 100}}],
    }
    result = engine.handle_object_position(query)
    assert result[1] == "REJECTED"
    assert result[2] == INVALID_SCHEMA
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


def test_cli_events_supports_stress_report_end_to_end():
    request = {"events": _rich_stream() + [
        stress("r1", "AAA", 5, "fund", dict(_MARKS), copy.deepcopy(_SCENARIOS))
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["portfolio_stress_analysis"] == _expected_rich_analysis()
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_stress_business_rejections_roundtrip():
    code, out, _ = _run_cli({"events": [
        stress("r0", "AAA", 1, "gone", {"AAA": 100}, _ok_scenarios()),
        add("a1", "BBB", 1, "o1", "BUY", "LIMIT", 1, 10, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"AAA": 1, "BBB": 2},
               [{"name": "s", "prices": {"AAA": 1, "BBB": 2}}]),
    ]})
    assert code == 0
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT
    assert out["results"][2]["rejection_code"] == MARK_PRICE_MISMATCH
