"""Tests for the multi-security PORTFOLIO_REPORT event."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    INVALID_EVENT,
    MARK_PRICE_MISMATCH,
    OUT_OF_ORDER,
    PORTFOLIO_REPORT,
    REJECTED,
    SEQUENCE_GAP,
    EventReplayer,
    UNKNOWN_ACCOUNT,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine import event_cli


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


def iceberg(event_id, symbol, sequence, order_id, side, quantity, price, display, **extra):
    return add(event_id, symbol, sequence, order_id, side, "ICEBERG", quantity, price,
               display_quantity=display, **extra)


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def cancel(event_id, symbol, sequence, order_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "CANCEL", "order_id": order_id}


def portfolio(event_id, symbol, sequence, account_id, mark_prices, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": PORTFOLIO_REPORT, "account_id": account_id,
             "mark_prices": mark_prices}
    event.update(extra)
    return event


def twap_start(event_id, symbol, sequence, plan_id, side, total, slices,
               order_type, benchmark, price=None, account_id=None):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "TWAP_START", "plan_id": plan_id, "side": side,
             "total_quantity": total, "slice_count": slices,
             "order_type": order_type, "benchmark_price": benchmark}
    if price is not None:
        event["price"] = price
    if account_id is not None:
        event["account_id"] = account_id
    return event


def twap_slice(event_id, symbol, sequence, plan_id):
    return {"event_id": event_id, "symbol": symbol, "sequence": sequence,
            "type": "TWAP_SLICE", "plan_id": plan_id}


# ---------------------------------------------------------------------------
# Successful reports
# ---------------------------------------------------------------------------


def test_portfolio_report_basic_two_symbols():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="acct"),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 4, 50, account_id="acct"),
        add("e3", "AAA", 2, "b2", "BUY", "LIMIT", 2, 100, account_id="other"),
        add("e4", "BBB", 2, "b4", "BUY", "LIMIT", 3, 50, account_id="other"),
        portfolio("q1", "AAA", 3, "acct", {"AAA": 110, "BBB": 40}),
    ])
    r = out["results"][-1]
    assert (r["status"], r["result"]) == (ACCEPTED, "REPORTED")
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    # The envelope symbol's book is echoed unchanged (3 of s1 remain).
    assert r["asks"] == [{"price": 100, "quantity": 3}]
    assert r["bids"] == []

    analysis = r["portfolio_analysis"]
    assert analysis["account_id"] == "acct"
    assert analysis["mark_prices"] == {"AAA": 110, "BBB": 40}
    assert [p["symbol"] for p in analysis["positions"]] == ["AAA", "BBB"]

    aaa, bbb = analysis["positions"]
    assert aaa == {
        "symbol": "AAA",
        "mark_price": 110,
        "buy_quantity": 0,
        "sell_quantity": 2,
        "buy_notional": 0,
        "sell_notional": 200,
        "net_position": -2,
        "cash_balance": 200,
        "buy_vwap": None,
        "sell_vwap": {"numerator": 200, "denominator": 2},
        "turnover_notional": 200,
        "position_value": -220,
        "risk_exposure": 220,
        "mark_to_market_pnl": -20,
    }
    assert bbb["buy_quantity"] == 0
    assert bbb["sell_quantity"] == 3
    assert bbb["sell_notional"] == 150
    assert bbb["net_position"] == -3
    assert bbb["cash_balance"] == 150
    assert bbb["buy_vwap"] is None
    assert bbb["sell_vwap"] == {"numerator": 150, "denominator": 3}
    assert bbb["turnover_notional"] == 150
    assert bbb["position_value"] == -120
    assert bbb["risk_exposure"] == 120
    assert bbb["mark_to_market_pnl"] == 30

    assert analysis["totals"] == {
        "buy_notional": 0,
        "sell_notional": 350,
        "cash_balance": 350,
        "turnover_notional": 350,
        "position_value": -340,
        "risk_exposure": 340,
        "mark_to_market_pnl": 10,
    }


def test_positions_include_maker_iceberg_and_taker_across_symbols():
    # AAA: acct sells from a replenishing iceberg as maker; BBB: acct buys as
    # taker against an anonymous offer. One report then covers both.
    out = replay_events([
        iceberg("e1", "AAA", 1, "i1", "SELL", 10, 100, 3, account_id="acct"),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 5, 100),
        add("e3", "AAA", 3, "b1", "BUY", "LIMIT", 10, 100, account_id="other"),
        add("e4", "BBB", 1, "x1", "SELL", "LIMIT", 4, 50),
        add("e5", "BBB", 2, "b2", "BUY", "LIMIT", 3, 50,
            time_in_force="IOC", account_id="acct"),
        portfolio("q1", "AAA", 4, "acct", {"AAA": 110, "BBB": 40}),
    ])
    aaa, bbb = out["results"][-1]["portfolio_analysis"]["positions"]
    # b1 takes i1's first slice (3), s2 (5) and i1's replenished slice (2):
    # the iceberg maker sells 5 for acct under one order id.
    assert aaa["sell_quantity"] == 5
    assert aaa["sell_notional"] == 500
    assert aaa["sell_vwap"] == {"numerator": 500, "denominator": 5}
    assert aaa["net_position"] == -5
    assert aaa["cash_balance"] == 500
    assert aaa["turnover_notional"] == 500
    assert aaa["position_value"] == -550
    assert aaa["risk_exposure"] == 550
    assert aaa["mark_to_market_pnl"] == -50
    # The IOC child buys 3 as taker for acct.
    assert bbb["buy_quantity"] == 3
    assert bbb["buy_notional"] == 150
    assert bbb["net_position"] == 3
    assert bbb["cash_balance"] == -150
    assert bbb["position_value"] == 120
    assert bbb["risk_exposure"] == 120
    assert bbb["mark_to_market_pnl"] == -30

    totals = out["results"][-1]["portfolio_analysis"]["totals"]
    assert totals == {
        "buy_notional": 150,
        "sell_notional": 500,
        "cash_balance": 350,
        "turnover_notional": 650,
        "position_value": -430,
        "risk_exposure": 670,
        "mark_to_market_pnl": -80,
    }


def test_replacement_taker_fill_is_attributed_to_inherited_account():
    # No self-trade interaction: acct rests a low bid, a market seller arrives
    # after a replace lifting the bid; the replacement trades as acct taker.
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100, account_id="acct"),
        add("e2", "AAA", 2, "s2", "SELL", "LIMIT", 3, 101),
        replace("e3", "AAA", 3, "o1", 3, 101),
        portfolio("q1", "AAA", 4, "acct", {"AAA": 101}),
    ])
    pos = out["results"][-1]["portfolio_analysis"]["positions"][0]
    # Replacement buys 3 @101 as taker against s2.
    assert pos["buy_quantity"] == 3
    assert pos["buy_notional"] == 303
    assert pos["net_position"] == 3
    assert pos["cash_balance"] == -303
    assert pos["buy_vwap"] == {"numerator": 303, "denominator": 3}
    assert pos["sell_vwap"] is None
    assert pos["position_value"] == 303
    assert pos["risk_exposure"] == 303
    assert pos["mark_to_market_pnl"] == 0


def test_released_twap_slice_counts_under_plan_account():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100),
        twap_start("e2", "AAA", 2, "p1", "BUY", 4, 2, "LIMIT", 99,
                   price=100, account_id="acct"),
        twap_slice("e3", "AAA", 3, "p1"),
        portfolio("q1", "BBB", 1, "acct", {"AAA": 110}),
    ])
    r = out["results"][-1]
    # Query rides a different envelope symbol but reads every security.
    assert r["symbol"] == "BBB"
    assert r["bids"] == [] and r["asks"] == []
    pos = r["portfolio_analysis"]["positions"][0]
    assert pos["symbol"] == "AAA"
    assert pos["buy_quantity"] == 2
    assert pos["buy_notional"] == 200
    assert pos["net_position"] == 2
    assert pos["cash_balance"] == -200
    assert pos["position_value"] == 220
    assert pos["risk_exposure"] == 220
    assert pos["mark_to_market_pnl"] == 20


def test_orders_without_account_and_unrelated_accounts_are_excluded():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 3, 100),                # anonymous
        add("e2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100, account_id="x"),
        add("e3", "AAA", 3, "s3", "SELL", "LIMIT", 2, 100, account_id="acct"),
        add("e4", "AAA", 4, "b2", "BUY", "LIMIT", 2, 100, account_id="x"),
        portfolio("q1", "AAA", 5, "acct", {"AAA": 100}),
    ])
    pos = out["results"][-1]["portfolio_analysis"]["positions"][0]
    # Only acct's own sell of 2 to account x is attributed.
    assert pos["sell_quantity"] == 2
    assert pos["net_position"] == -2


def test_account_with_no_trades_reports_zeroed_position():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
    ])
    pos = out["results"][-1]["portfolio_analysis"]["positions"][0]
    assert pos["buy_quantity"] == 0
    assert pos["sell_quantity"] == 0
    assert pos["buy_notional"] == 0
    assert pos["sell_notional"] == 0
    assert pos["net_position"] == 0
    assert pos["cash_balance"] == 0
    assert pos["buy_vwap"] is None
    assert pos["sell_vwap"] is None
    assert pos["turnover_notional"] == 0
    assert pos["position_value"] == 0
    assert pos["risk_exposure"] == 0
    assert pos["mark_to_market_pnl"] == 0


def test_positions_sorted_by_symbol_including_unicode():
    out = replay_events([
        add("e1", "z", 1, "o1", "BUY", "LIMIT", 1, 10, account_id="a"),
        add("e2", "A", 1, "o2", "BUY", "LIMIT", 1, 10, account_id="a"),
        add("e3", "证券", 1, "o3", "BUY", "LIMIT", 1, 10, account_id="a"),
        add("e4", "M", 1, "o4", "BUY", "LIMIT", 1, 10, account_id="a"),
        portfolio("q1", "A", 2, "a", {"A": 1, "M": 2, "z": 3, "证券": 4}),
    ])
    assert [p["symbol"] for p in out["results"][-1]["portfolio_analysis"]["positions"]] == [
        "A", "M", "z", "证券",
    ]


def test_report_is_byte_deterministic():
    events = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="acct"),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 4, 50, account_id="acct"),
        add("e3", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100, account_id="other"),
        portfolio("q1", "AAA", 3, "acct", {"AAA": 110, "BBB": 40}),
    ]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
    assert a == b
    assert "portfolio_analysis" in a.decode("utf-8")


# ---------------------------------------------------------------------------
# UNKNOWN_ACCOUNT / MARK_PRICE_MISMATCH rejections
# ---------------------------------------------------------------------------


def test_unknown_account_rejection_consumes_id_and_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "ghost", {"AAA": 100}),
        # Identical retry of the consumed id is a duplicate; sequence stays at 2.
        portfolio("q1", "AAA", 2, "ghost", {"AAA": 100}),
        add("e2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100, account_id="acct"),
    ])
    rej = out["results"][1]
    assert rej["status"] == REJECTED
    assert rej["rejection_code"] == UNKNOWN_ACCOUNT
    assert rej["trades"] == []
    assert rej["book_changes"] == {"bids": [], "asks": []}
    assert rej["asks"] == [{"price": 100, "quantity": 1}]
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][3]["status"] == ACCEPTED


def test_account_only_on_rejected_add_is_unknown():
    out = replay_events([
        add("e1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("e2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 3, "acct", {"AAA": 100}),
    ])
    # e2 is DUPLICATE_ORDER_ID: the account never appeared on an accepted ADD.
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    assert out["results"][2]["rejection_code"] == UNKNOWN_ACCOUNT


def test_mark_price_mismatch_when_key_missing_or_extra():
    setup = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 1, 50, account_id="acct"),
    ]
    # Missing BBB.
    out = replay_events(setup + [portfolio("q1", "AAA", 2, "acct", {"AAA": 100})])
    assert out["results"][-1]["rejection_code"] == MARK_PRICE_MISMATCH
    # Extra CCC the account never appeared on.
    out = replay_events(setup + [
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100, "BBB": 50, "CCC": 1}),
    ])
    assert out["results"][-1]["rejection_code"] == MARK_PRICE_MISMATCH
    # Empty map.
    out = replay_events(setup + [portfolio("q1", "AAA", 2, "acct", {})])
    assert out["results"][-1]["rejection_code"] == MARK_PRICE_MISMATCH


def test_mark_price_mismatch_consumes_id_and_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100, "BBB": 50}),
        # An identical retry is a duplicate; a new id on the stale slot 2 is
        # out of order and consumes nothing, so sequence 3 still succeeds.
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100, "BBB": 50}),
        portfolio("q2", "AAA", 2, "acct", {"AAA": 100}),
        portfolio("q3", "AAA", 3, "acct", {"AAA": 100}),
    ])
    assert out["results"][1]["rejection_code"] == MARK_PRICE_MISMATCH
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][3]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][4]["status"] == ACCEPTED


def test_unknown_account_takes_precedence_over_mark_mismatch():
    out = replay_events([
        portfolio("q1", "AAA", 1, "ghost", {"AAA": 100}),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT


def test_rejected_report_echoes_envelope_symbol_book_on_any_symbol():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 2, 50),
        # Known only on AAA; query on BBB envelope with mismatched keys.
        portfolio("q1", "BBB", 2, "acct", {"AAA": 100, "BBB": 50}),
    ])
    r = out["results"][-1]
    assert r["rejection_code"] == MARK_PRICE_MISMATCH
    # The envelope symbol BBB's own untouched book is echoed.
    assert r["asks"] == [{"price": 50, "quantity": 2}]


# ---------------------------------------------------------------------------
# INVALID_EVENT structural validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"account_id": "acct"},                                              # missing mark_prices
    {"mark_prices": {"AAA": 100}},                                       # missing account_id
    {"account_id": "acct", "mark_prices": {"AAA": 100}, "extra": 1},     # extra field
    {"account_id": "", "mark_prices": {"AAA": 100}},                     # empty account
    {"account_id": 7, "mark_prices": {"AAA": 100}},                      # non-string account
    {"account_id": None, "mark_prices": {"AAA": 100}},                   # null account
    {"account_id": "acct", "mark_prices": []},                           # map not object
    {"account_id": "acct", "mark_prices": None},                         # null map
    {"account_id": "acct", "mark_prices": {"": 100}},                    # empty symbol key
    {"account_id": "acct", "mark_prices": {7: 100}},                     # non-string key
    {"account_id": "acct", "mark_prices": {"AAA": 0}},                   # zero mark
    {"account_id": "acct", "mark_prices": {"AAA": -1}},                  # negative mark
    {"account_id": "acct", "mark_prices": {"AAA": True}},                # bool mark
    {"account_id": "acct", "mark_prices": {"AAA": 1.5}},                 # float mark
    {"account_id": "acct", "mark_prices": {"AAA": "100"}},               # string mark
])
def test_malformed_portfolio_events_are_invalid(bad):
    event = {"event_id": "q1", "symbol": "AAA", "sequence": 1,
             "type": PORTFOLIO_REPORT}
    event.update(bad)
    out = replay_events([event])
    r = out["results"][0]
    assert r["status"] == REJECTED
    assert r["rejection_code"] == INVALID_EVENT
    assert r["trades"] == []
    assert "portfolio_analysis" not in r


def test_invalid_event_consumes_neither_id_nor_sequence():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 0}),      # invalid mark
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),    # id/seq reusable, succeeds
        add("e2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    assert out["results"][3]["status"] == ACCEPTED


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        {"event_id": "q1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "q1", "type": PORTFOLIO_REPORT,
                   "account_id": "acct", "mark_prices": {"AAA": 100}}},
    ])
    assert out["results"][-1]["status"] == ACCEPTED


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events([
        {"event_id": "q1", "symbol": "AAA", "sequence": 1,
         "event": {"event_id": "OTHER", "type": PORTFOLIO_REPORT,
                   "account_id": "acct", "mark_prices": {"AAA": 100}}},
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Envelope ordering and idempotency rules
# ---------------------------------------------------------------------------


def test_portfolio_sequence_gap_and_out_of_order_consume_nothing():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 3, "acct", {"AAA": 100}),
        portfolio("q2", "AAA", 1, "acct", {"AAA": 100}),
        portfolio("q3", "AAA", 2, "acct", {"AAA": 100}),
    ])
    assert (out["results"][1]["rejection_code"],
            out["results"][1]["expected_sequence"]) == (SEQUENCE_GAP, 2)
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    assert out["results"][3]["status"] == ACCEPTED


def test_exact_duplicate_portfolio_query_is_idempotent():
    q = portfolio("q1", "AAA", 2, "acct", {"AAA": 100})
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        q,
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
        add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100),
    ])
    dup = out["results"][2]
    assert dup["status"] == DUPLICATE
    assert dup["trades"] == []
    assert dup["book_changes"] == {"bids": [], "asks": []}
    # The duplicate reflects the current (post-q1) book, never re-queried.
    assert dup["asks"] == [{"price": 100, "quantity": 1}]
    assert "portfolio_analysis" not in dup
    # Sequence 3 is the next slot, exactly as without the retry.
    assert out["results"][3]["status"] == ACCEPTED


def test_same_portfolio_event_id_with_different_marks_conflicts():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
        portfolio("q1", "AAA", 3, "acct", {"AAA": 101}),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_portfolio_event_id_is_global_across_symbols():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
        portfolio("q1", "BBB", 1, "acct", {"AAA": 100}),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_portfolio_event_does_not_enter_engine_journal_or_trade_ids():
    out = replay_events([
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
        add("e2", "AAA", 3, "b1", "BUY", "LIMIT", 1, 100, account_id="other"),
    ])
    state = out["snapshot"]["content"]["symbols"][0]["state"]
    engine_ids = state["engine"]["event_ids"]
    assert "q1" not in engine_ids
    # q1 lives in the replay event log only, and still counts for sequence.
    assert [e["event_id"] for e in state["event_log"]] == ["e1", "q1", "e2"]
    assert state["last_sequence"] == 3
    assert state["engine"]["next_trade_id"] == 2
    assert [t["trade_id"] for t in out["results"][2]["trades"]] == [1]


def test_portfolio_on_fresh_symbol_commits_like_other_business_rejections():
    out = replay_events([portfolio("q1", "NEW", 1, "ghost", {})])
    r = out["results"][0]
    assert r["rejection_code"] == UNKNOWN_ACCOUNT
    assert r["bids"] == [] and r["asks"] == []
    # A committed rejection occupies the sequence on its envelope symbol; the
    # symbol state exists with an empty engine and the query in the replay log.
    symbols = out["snapshot"]["content"]["symbols"]
    assert [s["symbol"] for s in symbols] == ["NEW"]
    state = symbols[0]["state"]
    assert state["last_sequence"] == 1
    assert state["engine"]["event_ids"] == []
    assert [e["event_id"] for e in state["event_log"]] == ["q1"]


def test_invalid_portfolio_on_fresh_symbol_does_not_create_it():
    out = replay_events([portfolio("q1", "NEW", 1, "ghost", {"x": 0})])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["snapshot"]["content"]["symbols"] == []


# ---------------------------------------------------------------------------
# Stateful EventReplayer
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_portfolio_reports():
    replayer = EventReplayer()
    replayer.submit([add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="acct")])
    r = replayer.submit([portfolio("q1", "AAA", 2, "acct", {"AAA": 100})])
    assert r[0]["result"] == "REPORTED"
    # The book helper and engine stay untouched.
    assert replayer.book("AAA") == ([], [{"price": 100, "quantity": 2}])


# ---------------------------------------------------------------------------
# Snapshot resumption equivalence
# ---------------------------------------------------------------------------


def _portfolio_segment_events():
    part1 = [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 10, 100, account_id="acct"),
        add("e2", "BBB", 1, "x1", "SELL", "LIMIT", 6, 50, account_id="acct"),
        add("e3", "AAA", 2, "b1", "BUY", "LIMIT", 4, 100, account_id="other"),
        portfolio("q0", "AAA", 3, "acct", {"AAA": 100, "BBB": 50}),
    ]
    part2 = [
        add("e4", "BBB", 2, "b2", "BUY", "LIMIT", 6, 50, account_id="other"),
        # Unknown-account rejection committed on BBB.
        portfolio("q1", "BBB", 3, "ghost", {"AAA": 1}),
        # Mismatch rejection committed on AAA.
        portfolio("q2", "AAA", 4, "acct", {"AAA": 100, "BBB": 50, "CCC": 9}),
        portfolio("q3", "AAA", 5, "acct", {"AAA": 110, "BBB": 40}),
    ]
    return part1, part2


def test_resumed_portfolio_replay_matches_uninterrupted():
    part1, part2 = _portfolio_segment_events()
    one_shot = replay_events(part1 + part2)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(part2, snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][len(part1):]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_snapshot_roundtrip_preserves_portfolio_report_log():
    part1, _ = _portfolio_segment_events()
    snapshot = replay_events(part1)["snapshot"]
    replayer = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(replayer)) == canonical_json(snapshot)


def test_resumed_report_reflects_trades_before_and_after_snapshot():
    part1, part2 = _portfolio_segment_events()
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events(part2, snapshot=snapshot)
    final = out["results"][-1]["portfolio_analysis"]
    aaa, bbb = final["positions"]
    # acct sold 4 @100 on AAA (6 remain) and 6 @50 on BBB (all gone).
    assert aaa["sell_quantity"] == 4
    assert aaa["sell_notional"] == 400
    assert aaa["net_position"] == -4
    assert bbb["sell_quantity"] == 6
    assert bbb["sell_notional"] == 300
    assert bbb["net_position"] == -6
    assert final["totals"]["cash_balance"] == 700
    assert final["totals"]["position_value"] == (-4 * 110) + (-6 * 40)
    assert final["totals"]["risk_exposure"] == 4 * 110 + 6 * 40
    assert final["totals"]["mark_to_market_pnl"] == (
        700 + (-4 * 110) + (-6 * 40)
    )
    assert final["totals"]["turnover_notional"] == 700


def test_sequence_continues_after_snapshot_for_every_symbol():
    part1, part2 = _portfolio_segment_events()
    snapshot = replay_events(part1)["snapshot"]
    out = replay_events(part2, snapshot=snapshot)
    # BBB accepted events q1(business reject, seq 3) and e4 was seq 2.
    statuses = [(r["symbol"], r["sequence"], r.get("rejection_code") or r["status"])
                for r in out["results"]]
    assert ("BBB", 3, UNKNOWN_ACCOUNT) in statuses
    assert ("AAA", 4, MARK_PRICE_MISMATCH) in statuses


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
    parsed = json.loads(body) if body else None
    return code, parsed, stderr.getvalue()


def test_cli_events_supports_portfolio_report_end_to_end():
    request = {"events": [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="acct"),
        portfolio("q1", "AAA", 2, "acct", {"AAA": 100}),
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["result"] == "REPORTED"
    assert r["portfolio_analysis"]["positions"][0]["net_position"] == 0
    assert out["snapshot"]["format_version"] == "event-replay/2"


def test_cli_resumes_portfolio_query_from_snapshot():
    first = {"events": [
        add("e1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="acct"),
    ]}
    _, out1, _ = _run_cli(first)
    second = {"events": [portfolio("q1", "AAA", 2, "acct", {"AAA": 100})],
              "snapshot": out1["snapshot"]}
    code, out2, _ = _run_cli(second)
    assert code == 0
    assert out2["results"][0]["result"] == "REPORTED"
    assert out2["results"][0]["asks"] == [{"price": 100, "quantity": 2}]
