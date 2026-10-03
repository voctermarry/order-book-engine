"""Tests for the cross-security PORTFOLIO_STRESS_REPORT event."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_RECONSTRUCTION_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    INVALID_EVENT,
    MARK_PRICE_MISMATCH,
    OUT_OF_ORDER,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ACCOUNT,
    VWAP_SLICE,
    VWAP_START,
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


def scenario(name, prices):
    return {"name": name, "prices": prices}


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


def vwap_start(event_id, symbol, sequence, plan_id, side, total_quantity, weights,
               price, account_id, benchmark_price=None):
    return {
        "event_id": event_id, "symbol": symbol, "sequence": sequence,
        "type": VWAP_START, "plan_id": plan_id, "side": side,
        "total_quantity": total_quantity, "volume_weights": weights,
        "order_type": "LIMIT",
        "benchmark_price": benchmark_price if benchmark_price is not None else price,
        "price": price, "account_id": account_id,
    }


def vwap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": VWAP_SLICE, "plan_id": plan_id}


# ---------------------------------------------------------------------------
# Fixture: maker / taker / replace / iceberg activity across three securities
# (identical stream to the plain portfolio tests)
# ---------------------------------------------------------------------------


def _rich_stream():
    """fund's activity, with anonymous counterparties:

    AAA: fund sells 2@100 as maker (b1 anonymous), the remaining 4 are
    REPLACED to 101 and sold to an anonymous market buyer: sells 6, notional
    2*100 + 4*101 = 604; net_position -6, cash_balance 604.
    BBB: fund's iceberg buys 4@50 as taker against x1, then makes the last
    1@50 against an anonymous market seller: buys 5, notional 250;
    net_position +5, cash_balance -250.
    CCC: fund's only order is cancelled: known, no trades.
    """
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 6, 100, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
        replace("a3", "AAA", 3, "s1", 4, 101),
        add("a4", "AAA", 4, "b2", "BUY", "MARKET", 4),
        add("b1", "BBB", 1, "x1", "SELL", "LIMIT", 4, 50),
        iceberg("b2", "BBB", 2, "i1", "BUY", 5, 50, 2, account_id="fund"),
        add("b3", "BBB", 3, "z1", "SELL", "MARKET", 1),
        add("c1", "CCC", 1, "o1", "BUY", "LIMIT", 3, 10, account_id="fund"),
        cancel("c2", "CCC", 2, "o1"),
    ]


BASE_MARKS = {"AAA": 110, "BBB": 40, "CCC": 7}


def _scenarios():
    return [
        scenario("crash", {"AAA": 100, "BBB": 30, "CCC": 7}),
        scenario("boom", {"AAA": 120, "BBB": 50, "CCC": 7}),
        scenario("flat", {"AAA": 110, "BBB": 40, "CCC": 7}),
    ]


# Baseline: AAA pnl = 604 - 6*110 = -56; BBB pnl = -250 + 5*40 = -50; CCC 0.
# baseline mark-to-market pnl = -106, baseline exposure = 660 + 200 = 860.
def _expected_analysis():
    return {
        "account_id": "fund",
        "baseline": {
            "positions": [
                {
                    "symbol": "AAA",
                    "mark_price": 110,
                    "net_position": -6,
                    "cash_balance": 604,
                    "position_market_value": -660,
                    "mark_to_market_pnl": -56,
                },
                {
                    "symbol": "BBB",
                    "mark_price": 40,
                    "net_position": 5,
                    "cash_balance": -250,
                    "position_market_value": 200,
                    "mark_to_market_pnl": -50,
                },
                {
                    "symbol": "CCC",
                    "mark_price": 7,
                    "net_position": 0,
                    "cash_balance": 0,
                    "position_market_value": 0,
                    "mark_to_market_pnl": 0,
                },
            ],
            "mark_to_market_pnl": -106,
            "risk_exposure": 860,
        },
        "scenarios": [
            {
                "name": "crash",
                "positions": [
                    {"symbol": "AAA", "shocked_price": 100,
                     "net_position": -6, "pnl_change": 60},
                    {"symbol": "BBB", "shocked_price": 30,
                     "net_position": 5, "pnl_change": -50},
                    {"symbol": "CCC", "shocked_price": 7,
                     "net_position": 0, "pnl_change": 0},
                ],
                "stressed_mark_to_market_pnl": -96,
                "risk_exposure": 600 + 150,
                "total_pnl_change": 10,
            },
            {
                "name": "boom",
                "positions": [
                    {"symbol": "AAA", "shocked_price": 120,
                     "net_position": -6, "pnl_change": -60},
                    {"symbol": "BBB", "shocked_price": 50,
                     "net_position": 5, "pnl_change": 50},
                    {"symbol": "CCC", "shocked_price": 7,
                     "net_position": 0, "pnl_change": 0},
                ],
                "stressed_mark_to_market_pnl": -116,
                "risk_exposure": 720 + 250,
                "total_pnl_change": -10,
            },
            {
                "name": "flat",
                "positions": [
                    {"symbol": "AAA", "shocked_price": 110,
                     "net_position": -6, "pnl_change": 0},
                    {"symbol": "BBB", "shocked_price": 40,
                     "net_position": 5, "pnl_change": 0},
                    {"symbol": "CCC", "shocked_price": 7,
                     "net_position": 0, "pnl_change": 0},
                ],
                "stressed_mark_to_market_pnl": -106,
                "risk_exposure": 860,
                "total_pnl_change": 0,
            },
        ],
        "worst_scenario": "boom",
    }


# ---------------------------------------------------------------------------
# Success path and result shape
# ---------------------------------------------------------------------------


def test_stress_report_success_shape_and_analysis():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())
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
    assert "portfolio_analysis" not in r
    assert "rejection_code" not in r
    assert r["portfolio_stress_analysis"] == _expected_analysis()


def test_result_and_analysis_key_sets_are_exact():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    assert set(analysis) == {"account_id", "baseline", "scenarios", "worst_scenario"}
    assert set(analysis["baseline"]) == {
        "positions", "mark_to_market_pnl", "risk_exposure"
    }
    baseline_keys = {
        "symbol", "mark_price", "net_position", "cash_balance",
        "position_market_value", "mark_to_market_pnl",
    }
    assert all(set(p) == baseline_keys for p in analysis["baseline"]["positions"])
    scenario_keys = {
        "name", "positions", "stressed_mark_to_market_pnl",
        "risk_exposure", "total_pnl_change",
    }
    position_keys = {"symbol", "shocked_price", "net_position", "pnl_change"}
    for scen in analysis["scenarios"]:
        assert set(scen) == scenario_keys
        assert all(set(p) == position_keys for p in scen["positions"])


def test_scenarios_keep_request_order_and_positions_are_sorted():
    requested = [
        scenario("zzz", {"CCC": 7, "AAA": 100, "BBB": 30}),
        scenario("aaa", {"BBB": 50, "CCC": 7, "AAA": 120}),
        scenario("mmm", {"AAA": 105, "BBB": 45, "CCC": 7}),
    ]
    events = _rich_stream() + [
        stress("r1", "BBB", 4, "fund", {"CCC": 7, "AAA": 110, "BBB": 40}, requested)
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    assert [s["name"] for s in analysis["scenarios"]] == ["zzz", "aaa", "mmm"]
    for scen in analysis["scenarios"]:
        assert [p["symbol"] for p in scen["positions"]] == ["AAA", "BBB", "CCC"]
    assert [p["symbol"] for p in analysis["baseline"]["positions"]] == [
        "AAA", "BBB", "CCC"
    ]


def test_worst_scenario_picks_minimum_change():
    # AAA-only long account: lower shocked price means a worse pnl change.
    events = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, [
            scenario("up", {"AAA": 110}),
            scenario("down", {"AAA": 90}),
            scenario("flat", {"AAA": 100}),
        ]),
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    up, down, flat = analysis["scenarios"]
    # fund is long 2 (bought at 100): a price drop loses, a rise gains.
    assert down["total_pnl_change"] == -20
    assert up["total_pnl_change"] == 20
    assert flat["total_pnl_change"] == 0
    assert down["stressed_mark_to_market_pnl"] == (
        analysis["baseline"]["mark_to_market_pnl"] - 20
    )
    assert analysis["baseline"]["mark_to_market_pnl"] == 0
    assert analysis["worst_scenario"] == "down"


def test_worst_scenario_tie_resolves_on_name_order():
    # Both scenarios keep every shocked price at the baseline: total change 0
    # for both, so the lexicographically smaller name wins.
    events = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, [
            scenario("zzz", {"AAA": 100}),
            scenario("aaa", {"AAA": 100}),
        ]),
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_stress_analysis"]
    assert [s["total_pnl_change"] for s in analysis["scenarios"]] == [0, 0]
    assert analysis["worst_scenario"] == "aaa"


def test_stress_report_echoes_envelope_symbol_book_only():
    events = _rich_stream() + [
        add("a5", "AAA", 5, "s9", "SELL", "LIMIT", 3, 123),
        stress("r1", "AAA", 6, "fund", BASE_MARKS, _scenarios()),
    ]
    r = replay_events(events)["results"][-1]
    assert r["asks"] == [{"price": 123, "quantity": 3}]
    assert r["bids"] == []


def test_stress_report_is_read_only():
    events = _rich_stream()
    before = replay_events(events)["snapshot"]
    with_report = replay_events(
        events + [stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())]
    )["snapshot"]

    def engine_states(snapshot):
        return {s["symbol"]: s["state"]["engine"]
                for s in snapshot["content"]["symbols"]}

    before_states = engine_states(before)
    after_states = engine_states(with_report)
    assert set(before_states) == set(after_states)
    for sym in before_states:
        assert canonical_json(before_states[sym]) == canonical_json(after_states[sym])
    assert "r1" not in after_states["AAA"]["event_ids"]
    aaa_state = [s for s in with_report["content"]["symbols"]
                 if s["symbol"] == "AAA"][0]["state"]
    assert aaa_state["last_sequence"] == 5
    follow = replay_events(
        [add("a6", "AAA", 6, "b9", "BUY", "LIMIT", 1, 123)],
        snapshot=with_report,
    )
    assert follow["results"][0]["result"] == "RESTING"
    assert follow["results"][0]["trades"] == []


def test_report_does_not_consume_trade_ids_on_a_trading_symbol():
    snap = replay_events(
        _rich_stream() + [stress("r0", "BBB", 4, "fund", BASE_MARKS, _scenarios())]
    )["snapshot"]
    out = replay_events(
        [add("b4", "BBB", 5, "x2", "SELL", "LIMIT", 1, 9),
         add("b5", "BBB", 6, "b9", "BUY", "LIMIT", 1, 9)],
        snapshot=snap,
    )
    assert [t["trade_id"] for t in out["results"][1]["trades"]] == [3]


# ---------------------------------------------------------------------------
# Account knownness and attribution (mirrors PORTFOLIO_REPORT)
# ---------------------------------------------------------------------------


def test_account_known_through_released_twap_slice():
    out = replay_events([
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "BUY", 3, 3, 70, "tp"),
        twap_slice("d3", "DDD", 3, "p1"),
        stress("r1", "DDD", 4, "tp", {"DDD": 70}, [scenario("s", {"DDD": 80})]),
    ])
    base = out["results"][-1]["portfolio_stress_analysis"]["baseline"]["positions"][0]
    assert base["net_position"] == 1
    assert base["cash_balance"] == -70
    scen = out["results"][-1]["portfolio_stress_analysis"]["scenarios"][0]
    assert scen["positions"][0]["pnl_change"] == 10
    assert scen["total_pnl_change"] == 10
    # Bought at 70, marked at 70: baseline pnl 0; the shocked mark gives +10.
    assert scen["stressed_mark_to_market_pnl"] == 10


def test_account_known_through_plan_start_without_any_slice():
    out = replay_events([
        vwap_start("d1", "DDD", 1, "p1", "BUY", 4, [1, 3], 70, "vp"),
        stress("r1", "DDD", 2, "vp", {"DDD": 70}, [scenario("s", {"DDD": 70})]),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    base = r["portfolio_stress_analysis"]["baseline"]["positions"][0]
    assert base["symbol"] == "DDD"
    assert base["net_position"] == 0
    assert base["mark_to_market_pnl"] == 0


def test_report_can_be_first_event_on_its_envelope_symbol():
    out = replay_events([
        add("d1", "DDD", 1, "o1", "BUY", "LIMIT", 2, 70, account_id="fund"),
        stress("r1", "EEE", 1, "fund", {"DDD": 70}, [scenario("s", {"DDD": 70})]),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["bids"] == [] and r["asks"] == []
    assert [p["symbol"] for p in
            r["portfolio_stress_analysis"]["baseline"]["positions"]] == ["DDD"]
    eee = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "EEE"][0]
    assert eee["state"]["last_sequence"] == 1
    assert eee["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# UNKNOWN_ACCOUNT
# ---------------------------------------------------------------------------


def test_unknown_account_rejects_and_consumes_id_and_sequence():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ACCOUNT)
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert "portfolio_stress_analysis" not in r
    stale = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
        stress("rX", "AAA", 2, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    assert stale["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
        stress("r2", "AAA", 3, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


def test_unknown_account_report_id_is_occupied():
    base = [stress("r1", "AAA", 1, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})])]
    out = replay_events(base + [
        stress("r1", "AAA", 1, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    assert out["results"][1]["status"] == DUPLICATE
    out2 = replay_events(base + [
        stress("r1", "AAA", 2, "gone", {"AAA": 101}, [scenario("s", {"AAA": 101})]),
    ])
    assert out2["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


def test_unknown_account_takes_precedence_over_mark_mismatch():
    out = replay_events([
        stress("r1", "AAA", 1, "gone", {"AAA": 100, "BBB": 50},
               [scenario("s", {"AAA": 100})]),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT


# ---------------------------------------------------------------------------
# MARK_PRICE_MISMATCH
# ---------------------------------------------------------------------------


def test_mark_price_mismatch_cases():
    base = _rich_stream()
    one_good = scenario("good", BASE_MARKS)

    # Baseline map problems.
    baseline_cases = [
        {"AAA": 110, "BBB": 40},                       # missing CCC
        {**BASE_MARKS, "DDD": 9},                      # extra DDD
        {"DDD": 9},                                    # wrong only
        {},                                            # empty
    ]
    for marks in baseline_cases:
        out = replay_events(base + [
            stress("r1", "AAA", 5, "fund", marks, [one_good])
        ])
        r = out["results"][-1]
        assert (r["status"], r["rejection_code"]) == (REJECTED, MARK_PRICE_MISMATCH), marks
        assert "portfolio_stress_analysis" not in r

    # Scenario price map problems.
    scenario_cases = [
        {"AAA": 100, "BBB": 30},
        {**BASE_MARKS, "DDD": 9},
        {"DDD": 9},
        {},
    ]
    for prices in scenario_cases:
        out = replay_events(base + [
            stress("r1", "AAA", 5, "fund", BASE_MARKS,
                   [scenario("good", BASE_MARKS), scenario("bad", prices)])
        ])
        r = out["results"][-1]
        assert (r["status"], r["rejection_code"]) == (REJECTED, MARK_PRICE_MISMATCH), prices
        assert r["trades"] == []


def test_mark_price_mismatch_consumes_id_and_sequence():
    query = stress("r1", "AAA", 2, "fund", {"BBB": 50}, [scenario("s", {"BBB": 50})])
    prefix = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        query,
    ]
    out = replay_events(prefix + [
        stress("r2", "AAA", 2, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})])
    ])
    assert out["results"][1]["rejection_code"] == MARK_PRICE_MISMATCH
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events(prefix + [
        stress("r3", "AAA", 3, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})])
    ])
    assert follow["results"][2]["status"] == ACCEPTED
    assert out["results"][1]["asks"] == [{"price": 100, "quantity": 2}]


# ---------------------------------------------------------------------------
# INVALID_EVENT: structural validation consumes nothing
# ---------------------------------------------------------------------------


def _stress_payload(**overrides):
    payload = {
        "event_id": "r1",
        "type": PORTFOLIO_STRESS_REPORT,
        "account_id": "fund",
        "mark_prices": {"AAA": 100},
        "scenarios": [scenario("s", {"AAA": 100})],
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize("bad", [
    _stress_payload(account_id=""),                                   # empty account
    _stress_payload(account_id=7),                                    # non-string account
    _stress_payload(account_id=None),                                 # null account
    _stress_payload(mark_prices=None),                                # null map
    _stress_payload(mark_prices=[]),                                  # list map
    _stress_payload(mark_prices=100),                                 # integer map
    _stress_payload(mark_prices={"": 100}),                           # empty symbol key
    _stress_payload(mark_prices={1: 100}),                            # non-string key
    _stress_payload(mark_prices={"AAA": True}),                       # bool price
    _stress_payload(mark_prices={"AAA": 0}),                          # zero price
    _stress_payload(mark_prices={"AAA": -3}),                         # negative price
    _stress_payload(mark_prices={"AAA": 1.5}),                        # float price
    _stress_payload(mark_prices={"AAA": "100"}),                      # string price
    _stress_payload(scenarios=None),                                  # null scenarios
    _stress_payload(scenarios=[]),                                    # empty scenarios
    _stress_payload(scenarios={}),                                    # object scenarios
    _stress_payload(scenarios="s"),                                   # string scenarios
    _stress_payload(scenarios=[scenario("s", {"AAA": 100}), "x"]),    # non-object member
    _stress_payload(scenarios=[{"name": "s"}]),                       # missing prices
    _stress_payload(scenarios=[{"prices": {"AAA": 100}}]),            # missing name
    _stress_payload(scenarios=[
        {"name": "s", "prices": {"AAA": 100}, "extra": 1}]),         # extra member field
    _stress_payload(scenarios=[scenario("", {"AAA": 100})]),          # empty name
    _stress_payload(scenarios=[scenario(7, {"AAA": 100})]),           # non-string name
    _stress_payload(scenarios=[scenario(None, {"AAA": 100})]),        # null name
    _stress_payload(scenarios=[
        scenario("s", {"AAA": 100}), scenario("s", {"AAA": 101})]),  # duplicate name
    _stress_payload(scenarios=[scenario("s", None)]),                 # null prices
    _stress_payload(scenarios=[scenario("s", [])]),                   # list prices
    _stress_payload(scenarios=[scenario("s", {"": 100})]),            # empty price key
    _stress_payload(scenarios=[scenario("s", {1: 100})]),             # non-string price key
    _stress_payload(scenarios=[scenario("s", {"AAA": True})]),        # bool shocked price
    _stress_payload(scenarios=[scenario("s", {"AAA": 0})]),           # zero shocked price
    _stress_payload(scenarios=[scenario("s", {"AAA": -1})]),          # negative
    _stress_payload(scenarios=[scenario("s", {"AAA": 1.5})]),        # float
    _stress_payload(scenarios=[scenario("s", {"AAA": "100"})]),      # string
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
     "account_id": "fund", "mark_prices": {"AAA": 100}},             # missing scenarios
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
     "account_id": "fund",
     "scenarios": [scenario("s", {"AAA": 100})]},                    # missing mark_prices
    {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
     "mark_prices": {"AAA": 100},
     "scenarios": [scenario("s", {"AAA": 100})]},                    # missing account_id
    {"event_id": "r1", "account_id": "fund",
     "mark_prices": {"AAA": 100},
     "scenarios": [scenario("s", {"AAA": 100})]},                    # missing type
    {"account_id": "fund", "mark_prices": {"AAA": 100},
     "scenarios": [scenario("s", {"AAA": 100})]},                    # missing event_id/type
    _stress_payload(order_id="o1"),                                  # extra field
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
    out = replay_events(base + [
        bad,
        stress("r2", "AAA", 2, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    # The malformed id was not consumed either.
    retry = replay_events(base + [
        bad,
        stress("r1", "AAA", 2, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    assert retry["results"][1]["rejection_code"] == INVALID_EVENT
    assert retry["results"][2]["status"] == ACCEPTED


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([
        stress("r1", "EEE", 1, "fund", {"EEE": 1}, [scenario("s", {"EEE": 0})])
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "r1", "type": PORTFOLIO_STRESS_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100},
                   "scenarios": [scenario("s", {"AAA": 100})]}},
    ])
    assert out["results"][1]["status"] == ACCEPTED


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "OTHER", "type": PORTFOLIO_STRESS_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100},
                   "scenarios": [scenario("s", {"AAA": 100})]}},
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency and ordering precedence
# ---------------------------------------------------------------------------


def test_identical_report_replay_is_duplicate_and_not_recomputed():
    query = stress("r1", "AAA", 2, "fund", {"AAA": 100},
                   [scenario("s", {"AAA": 100})])
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        query,
        stress("r1", "AAA", 2, "fund", {"AAA": 100},
               [scenario("s", {"AAA": 100})]),
        add("a2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][2]["status"] == DUPLICATE
    assert "portfolio_stress_analysis" not in out["results"][2]
    assert out["results"][2]["trades"] == []
    assert out["results"][2]["book_changes"] == {"bids": [], "asks": []}
    assert out["results"][3]["result"] == "FILLED"


def test_same_report_id_with_different_scenarios_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, [scenario("s", {"AAA": 101})]),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_same_report_id_as_plain_portfolio_report_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "type": PORTFOLIO_REPORT, "account_id": "fund",
         "mark_prices": {"AAA": 100}},
        stress("r1", "AAA", 3, "fund", {"AAA": 100},
               [scenario("s", {"AAA": 100})]),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_business_checks():
    good = [scenario("s", {"AAA": 100})]
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, good),
        stress("r2", "AAA", 1, "gone", {"AAA": 100}, good),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 3, "fund", {"AAA": 100}, good),
        stress("r3", "AAA", 2, "fund", {"AAA": 100}, good),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_stress_report_output_is_byte_for_byte_deterministic():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund",
               {"CCC": 7, "AAA": 110, "BBB": 40},
               [scenario("z", {"BBB": 30, "CCC": 7, "AAA": 100}),
                scenario("a", {"AAA": 120, "CCC": 7, "BBB": 50})]),
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
# Historical reconstruction skips the read-only stress query
# ---------------------------------------------------------------------------


def test_book_reconstruction_skips_stress_report():
    events = _rich_stream() + [
        add("a5", "AAA", 5, "s9", "SELL", "LIMIT", 3, 123),
        stress("r1", "AAA", 6, "fund", BASE_MARKS, _scenarios()),
        {"event_id": "r2", "symbol": "AAA", "sequence": 7,
         "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 6},
    ]
    out = replay_events(events)
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    recon = r["book_reconstruction"]
    # As of sequence 6 the only resting AAA order is the s9 ask added at
    # sequence 5; the stress report occupied sequence 6 without matching.
    assert recon["bid_queues"] == []
    ask_queues = recon["ask_queues"]
    assert len(ask_queues) == 1
    assert ask_queues[0]["price"] == 123
    assert [o["order_id"] for o in ask_queues[0]["orders"]] == ["s9"]


# ---------------------------------------------------------------------------
# Snapshot export / restore
# ---------------------------------------------------------------------------


def test_resumed_replay_with_stress_reports_matches_one_shot():
    stream = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios()),
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "SELL", 2, 2, 70, "fund"),
        twap_slice("d3", "DDD", 3, "p1"),
        stress("r2", "CCC", 3, "fund",
               {"AAA": 105, "BBB": 42, "CCC": 8, "DDD": 66},
               [scenario("down", {"AAA": 100, "BBB": 40, "CCC": 8, "DDD": 60}),
                scenario("up", {"AAA": 110, "BBB": 45, "CCC": 8, "DDD": 70})]),
        stress("r3", "ZZZ", 1, "gone", {"AAA": 1}, [scenario("s", {"AAA": 1})]),
        stress("r4", "AAA", 6, "fund", {"AAA": 1, "BBB": 2},
               [scenario("s", {"AAA": 1, "BBB": 2})]),
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
        [stress("r1", "BBB", 4, "fund", BASE_MARKS, _scenarios())],
        snapshot=snapshot,
    )
    assert out["results"][0]["portfolio_stress_analysis"] == _expected_analysis()


def test_duplicate_stress_report_is_still_idempotent_after_restore():
    snapshot = replay_events(
        _rich_stream() + [stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())]
    )["snapshot"]
    out = replay_events(
        [stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())],
        snapshot=snapshot,
    )
    assert out["results"][0]["status"] == DUPLICATE


def test_snapshot_after_named_stress_report_event():
    events = _rich_stream() + [
        stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())
    ]
    out = replay_events(events, snapshot_after={"symbol": "AAA", "sequence": 5})
    aaa = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 5
    assert "r1" in {e["event_id"] for e in aaa["state"]["event_log"]}
    assert "r1" not in aaa["state"]["engine"]["event_ids"]


def test_business_rejected_stress_report_roundtrips_through_snapshot():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        stress("r1", "AAA", 2, "gone", {"AAA": 100}, [scenario("s", {"AAA": 100})]),
    ])
    snapshot = out["snapshot"]
    from order_book_engine import restore_replayer, export_snapshot
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [stress("r1", "AAA", 2, "gone", {"AAA": 100},
                [scenario("s", {"AAA": 100})])],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


# ---------------------------------------------------------------------------
# Stateful EventReplayer
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
               [scenario("s", {"AAA": 99})])
    ])
    assert r2[0]["status"] == ACCEPTED
    analysis = r2[0]["portfolio_stress_analysis"]
    assert analysis["baseline"]["positions"][0]["net_position"] == 0
    assert analysis["scenarios"][0]["positions"][0]["pnl_change"] == 0


# ---------------------------------------------------------------------------
# Baseline entry point does not accept the event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_stress_report_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1", "type": PORTFOLIO_STRESS_REPORT,
        "account_id": "fund", "mark_prices": {"AAA": 100},
        "scenarios": [scenario("s", {"AAA": 100})],
    }
    result = engine.handle_object_position(query)
    assert result[1] == "REJECTED"
    assert result[2] == INVALID_SCHEMA
    assert engine.handle_object_position(copy.deepcopy(query))[2] == INVALID_SCHEMA


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
        stress("r1", "AAA", 5, "fund", BASE_MARKS, _scenarios())
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["portfolio_stress_analysis"] == _expected_analysis()
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_stress_business_rejections_roundtrip():
    code, out, _ = _run_cli({"events": [
        stress("r0", "AAA", 1, "gone", {"AAA": 100},
               [scenario("s", {"AAA": 100})]),
        add("a1", "BBB", 1, "o1", "BUY", "LIMIT", 1, 10, account_id="fund"),
        stress("r1", "AAA", 2, "fund", {"AAA": 1, "BBB": 2},
               [scenario("s", {"AAA": 1, "BBB": 2})]),
    ]})
    assert code == 0
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT
    assert out["results"][2]["rejection_code"] == MARK_PRICE_MISMATCH
