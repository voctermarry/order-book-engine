"""Tests for the cross-security PORTFOLIO_REPORT event in the multi-symbol stream."""

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
    PORTFOLIO_REPORT,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ACCOUNT,
    UNKNOWN_EXECUTION_PLAN,
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


def portfolio(event_id, symbol, sequence, account_id, mark_prices, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": PORTFOLIO_REPORT,
        "account_id": account_id,
        "mark_prices": mark_prices,
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
# ---------------------------------------------------------------------------


def _rich_stream():
    """fund's activity, with anonymous counterparties:

    AAA: fund sells 2@100 as maker (b1 anonymous), the remaining 4 are
    REPLACED to 101 and sold to an anonymous market buyer (maker again,
    post-replace): sells 6, notional 2*100 + 4*101 = 604.
    BBB: fund's iceberg buys 4@50 as taker against x1, then makes the last
    1@50 against an anonymous market seller: buys 5, notional 250.
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


def _expected_rich_analysis():
    return {
        "account_id": "fund",
        "positions": [
            {
                "symbol": "AAA",
                "mark_price": 110,
                "buy_quantity": 0,
                "sell_quantity": 6,
                "buy_notional": 0,
                "sell_notional": 604,
                "net_position": -6,
                "cash_balance": 604,
                "buy_vwap": None,
                "sell_vwap": {"numerator": 604, "denominator": 6},
                "turnover_notional": 604,
                "position_market_value": -660,
                "risk_exposure": 660,
                "mark_to_market_pnl": 604 - 660,
            },
            {
                "symbol": "BBB",
                "mark_price": 40,
                "buy_quantity": 5,
                "sell_quantity": 0,
                "buy_notional": 250,
                "sell_notional": 0,
                "net_position": 5,
                "cash_balance": -250,
                "buy_vwap": {"numerator": 250, "denominator": 5},
                "sell_vwap": None,
                "turnover_notional": 250,
                "position_market_value": 200,
                "risk_exposure": 200,
                "mark_to_market_pnl": -250 + 200,
            },
            {
                "symbol": "CCC",
                "mark_price": 7,
                "buy_quantity": 0,
                "sell_quantity": 0,
                "buy_notional": 0,
                "sell_notional": 0,
                "net_position": 0,
                "cash_balance": 0,
                "buy_vwap": None,
                "sell_vwap": None,
                "turnover_notional": 0,
                "position_market_value": 0,
                "risk_exposure": 0,
                "mark_to_market_pnl": 0,
            },
        ],
        "totals": {
            "buy_notional": 250,
            "sell_notional": 604,
            "cash_balance": 604 - 250,
            "turnover_notional": 854,
            "position_market_value": -660 + 200,
            "risk_exposure": 660 + 200,
            "mark_to_market_pnl": (604 - 250) + (-660 + 200),
        },
    }


# ---------------------------------------------------------------------------
# Success path and result shape
# ---------------------------------------------------------------------------


def test_portfolio_report_success_shape_and_analysis():
    events = _rich_stream() + [
        portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})
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
    # The envelope symbol's book is echoed unchanged (AAA is fully flat here).
    assert r["bids"] == []
    assert r["asks"] == []
    assert "execution_plan" not in r
    assert "rejection_code" not in r
    assert r["portfolio_analysis"] == _expected_rich_analysis()


def test_portfolio_report_echoes_envelope_symbol_book_not_others():
    # BBB still shows fund's filled iceberg? No: i1 ended fully FILLED, so BBB
    # is flat. Add a fresh resting bid on the envelope symbol instead.
    events = _rich_stream() + [
        add("a5", "AAA", 5, "s9", "SELL", "LIMIT", 3, 123),
        portfolio("r1", "AAA", 6, "fund", {"AAA": 110, "BBB": 40, "CCC": 7}),
    ]
    out = replay_events(events)
    r = out["results"][-1]
    assert r["asks"] == [{"price": 123, "quantity": 3}]
    assert r["bids"] == []


def test_positions_are_ordered_by_symbol_and_keys_are_exact():
    events = _rich_stream() + [
        portfolio("r1", "BBB", 4, "fund", {"BBB": 40, "CCC": 7, "AAA": 110})
    ]
    analysis = replay_events(events)["results"][-1]["portfolio_analysis"]
    assert [p["symbol"] for p in analysis["positions"]] == ["AAA", "BBB", "CCC"]
    position_keys = {
        "symbol", "mark_price", "buy_quantity", "sell_quantity",
        "buy_notional", "sell_notional", "net_position", "cash_balance",
        "buy_vwap", "sell_vwap", "turnover_notional",
        "position_market_value", "risk_exposure", "mark_to_market_pnl",
    }
    assert all(set(p) == position_keys for p in analysis["positions"])
    assert set(analysis) == {"account_id", "positions", "totals"}
    assert set(analysis["totals"]) == {
        "buy_notional", "sell_notional", "cash_balance",
        "turnover_notional", "position_market_value",
        "risk_exposure", "mark_to_market_pnl",
    }


def test_portfolio_report_is_read_only():
    events = _rich_stream()
    before = replay_events(events)["snapshot"]
    with_report = replay_events(
        events + [portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})]
    )["snapshot"]
    # The only differences the report leaves are the replay-log entries; no
    # engine state (orders, trades, accounts, next trade id) moves.
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
    # The next AAA trade still takes id 1 (AAA never traded in the fixture).
    follow = replay_events(
        [add("a6", "AAA", 6, "b9", "BUY", "LIMIT", 1, 123)],
        snapshot=with_report,
    )
    assert follow["results"][0]["result"] == "RESTING"
    assert follow["results"][0]["trades"] == []


def test_report_does_not_consume_trade_ids_on_a_trading_symbol():
    # BBB spent trade ids 1-2 in the fixture; further trades keep numbering 3.
    snap = replay_events(
        _rich_stream() + [portfolio("r0", "BBB", 4, "fund",
                                    {"AAA": 110, "BBB": 40, "CCC": 7})]
    )["snapshot"]
    out = replay_events(
        [add("b4", "BBB", 5, "x2", "SELL", "LIMIT", 1, 9),
         add("b5", "BBB", 6, "b9", "BUY", "LIMIT", 1, 9)],
        snapshot=snap,
    )
    assert [t["trade_id"] for t in out["results"][1]["trades"]] == [3]


# ---------------------------------------------------------------------------
# Account knownness
# ---------------------------------------------------------------------------


def test_account_known_through_cancelled_order_and_anonymous_counterparty():
    # fund rests on AAA then cancels; the account is still known there.
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 10, account_id="fund"),
        cancel("a2", "AAA", 2, "o1"),
        portfolio("r1", "AAA", 3, "fund", {"AAA": 11}),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert [p["symbol"] for p in r["portfolio_analysis"]["positions"]] == ["AAA"]


def test_account_known_through_released_twap_slice():
    out = replay_events([
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "BUY", 3, 3, 70, "tp"),
        twap_slice("d3", "DDD", 3, "p1"),
        portfolio("r1", "DDD", 4, "tp", {"DDD": 70}),
    ])
    position = out["results"][-1]["portfolio_analysis"]["positions"][0]
    # 3 units over 3 slices: the first slice releases exactly 1 unit.
    assert position["buy_quantity"] == 1
    assert position["buy_notional"] == 70
    assert position["net_position"] == 1


def test_account_known_through_plan_start_without_any_slice():
    out = replay_events([
        vwap_start("d1", "DDD", 1, "p1", "BUY", 4, [1, 3], 70, "vp"),
        portfolio("r1", "DDD", 2, "vp", {"DDD": 70}),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    position = r["portfolio_analysis"]["positions"][0]
    assert position["symbol"] == "DDD"
    assert position["buy_quantity"] == 0
    assert position["buy_vwap"] is None


def test_account_from_rejected_add_is_unknown():
    # The second ADD is rejected (duplicate order id); its account never
    # registers, even though the rejected event id is occupied.
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("a2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100, account_id="ghost"),
        portfolio("r1", "AAA", 3, "ghost", {"AAA": 100}),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ACCOUNT)


def test_report_can_be_first_event_on_its_envelope_symbol():
    # The account is only known on DDD, but the query arrives under a brand
    # new envelope symbol EEE with sequence 1: it succeeds, echoes EEE's empty
    # book and reports DDD.
    out = replay_events([
        add("d1", "DDD", 1, "o1", "BUY", "LIMIT", 2, 70, account_id="fund"),
        portfolio("r1", "EEE", 1, "fund", {"DDD": 70}),
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["bids"] == [] and r["asks"] == []
    assert [p["symbol"] for p in r["portfolio_analysis"]["positions"]] == ["DDD"]
    # EEE now exists with exactly one accepted event and no engine journal.
    eee = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "EEE"][0]
    assert eee["state"]["last_sequence"] == 1
    assert eee["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# UNKNOWN_ACCOUNT
# ---------------------------------------------------------------------------


def test_unknown_account_rejects_and_consumes_id_and_sequence():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "gone", {"AAA": 100}),
    ])
    r = out["results"][-1]
    assert (r["status"], r["rejection_code"]) == (REJECTED, UNKNOWN_ACCOUNT)
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert "portfolio_analysis" not in r
    # Sequence 2 was consumed: another event at sequence 2 is out of order.
    stale = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "gone", {"AAA": 100}),
        portfolio("rX", "AAA", 2, "fund", {"AAA": 100}),
    ])
    assert stale["results"][2]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "gone", {"AAA": 100}),
        portfolio("r2", "AAA", 3, "fund", {"AAA": 100}),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


def test_unknown_account_report_id_is_occupied():
    base = [portfolio("r1", "AAA", 1, "gone", {"AAA": 100})]
    out = replay_events(base + [
        portfolio("r1", "AAA", 1, "gone", {"AAA": 100}),
    ])
    # Identical retry is a DUPLICATE even though the account is still unknown.
    assert out["results"][1]["status"] == DUPLICATE
    out2 = replay_events(base + [
        portfolio("r1", "AAA", 2, "gone", {"AAA": 101}),
    ])
    assert out2["results"][1]["rejection_code"] == EVENT_ID_CONFLICT


def test_unknown_account_on_brand_new_symbol_still_registers_symbol():
    out = replay_events([portfolio("r1", "EEE", 1, "gone", {"EEE": 100})])
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT
    symbols = [s["symbol"] for s in out["snapshot"]["content"]["symbols"]]
    assert symbols == ["EEE"]


# ---------------------------------------------------------------------------
# MARK_PRICE_MISMATCH
# ---------------------------------------------------------------------------


def test_mark_price_mismatch_cases():
    base = _rich_stream()
    full = {"AAA": 110, "BBB": 40, "CCC": 7}

    # Missing CCC, extra DDD, wrong-only, and empty map.
    cases = [
        {"AAA": 110, "BBB": 40},
        {**full, "DDD": 9},
        {"DDD": 9},
        {},
    ]
    for marks in cases:
        out = replay_events(base + [portfolio("r1", "AAA", 5, "fund", marks)])
        r = out["results"][-1]
        assert (r["status"], r["rejection_code"]) == (REJECTED, MARK_PRICE_MISMATCH), marks
        assert r["trades"] == []
        assert "portfolio_analysis" not in r


def test_mark_price_mismatch_consumes_id_and_sequence():
    prefix = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "fund", {"BBB": 50}),
    ]
    out = replay_events(prefix + [portfolio("r2", "AAA", 2, "fund", {"AAA": 100})])
    assert out["results"][1]["rejection_code"] == MARK_PRICE_MISMATCH
    # Sequence 2 was consumed: another well-formed event at sequence 2 is
    # rejected as out of order and never re-evaluated for its mark set.
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    # The next correct sequence is 3 and succeeds.
    follow = replay_events(prefix + [portfolio("r3", "AAA", 3, "fund", {"AAA": 100})])
    assert follow["results"][2]["status"] == ACCEPTED
    # The mismatch id is occupied and echoed with the unchanged book.
    assert out["results"][1]["asks"] == [{"price": 100, "quantity": 2}]


def test_unknown_account_takes_precedence_over_mark_mismatch():
    out = replay_events([
        portfolio("r1", "AAA", 1, "gone", {"AAA": 100, "BBB": 50}),
    ])
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT


# ---------------------------------------------------------------------------
# INVALID_EVENT: structural validation consumes nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"account_id": "fund", "mark_prices": {"AAA": 100}},               # missing event_id/type
    {"event_id": "r1", "account_id": "fund", "mark_prices": {"AAA": 100}},  # missing type
    {"event_id": "r1", "type": PORTFOLIO_REPORT,
     "mark_prices": {"AAA": 100}},                                     # missing account_id
    {"event_id": "r1", "type": PORTFOLIO_REPORT,
     "account_id": "fund"},                                            # missing mark_prices
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 100}, "order_id": "o1"},                   # extra field
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "",
     "mark_prices": {"AAA": 100}},                                     # empty account id
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": 7,
     "mark_prices": {"AAA": 100}},                                     # non-string account
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": None,
     "mark_prices": {"AAA": 100}},                                     # null account
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": None},                                             # null map
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": []},                                               # list map
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": "AAA:100"},                                        # string map
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": 100},                                              # integer map
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"": 100}},                                        # empty symbol key
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {1: 100}},                                         # non-string symbol key
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": True}},                                    # bool price
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 0}},                                       # zero price
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": -3}},                                      # negative price
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": 1.5}},                                     # float price
    {"event_id": "r1", "type": PORTFOLIO_REPORT, "account_id": "fund",
     "mark_prices": {"AAA": "100"}},                                   # string price
])
def test_malformed_portfolio_events_are_invalid(bad):
    event = {"symbol": "AAA", "sequence": 1, **bad}
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        event,
    ]) if "event_id" in bad else replay_events([event])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT, bad
    assert out["results"][-1]["trades"] == []


def test_invalid_event_consumes_neither_id_nor_sequence():
    base = [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund")]
    bad = portfolio("r1", "AAA", 2, "fund", {"AAA": True})
    # The malformed event consumed no sequence slot: another event at
    # sequence 2 is accepted.
    out = replay_events(base + [bad, portfolio("r2", "AAA", 2, "fund", {"AAA": 100})])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    # It also consumed no event id: r1 itself is usable at sequence 2 in a
    # fresh run over the same prefix.
    retry = replay_events(base + [bad, portfolio("r1", "AAA", 2, "fund", {"AAA": 100})])
    assert retry["results"][1]["rejection_code"] == INVALID_EVENT
    assert retry["results"][2]["status"] == ACCEPTED


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([portfolio("r1", "EEE", 1, "fund", {"EEE": 0})])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "r1", "type": PORTFOLIO_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100}}},
    ])
    assert out["results"][1]["status"] == ACCEPTED


def test_nested_payload_event_id_mismatch_is_invalid():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        {"event_id": "r1", "symbol": "AAA", "sequence": 2,
         "event": {"event_id": "OTHER", "type": PORTFOLIO_REPORT,
                   "account_id": "fund", "mark_prices": {"AAA": 100}}},
    ])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Envelope, idempotency and ordering precedence
# ---------------------------------------------------------------------------


def test_identical_report_replay_is_duplicate_and_not_recomputed():
    query = portfolio("r1", "AAA", 2, "fund", {"AAA": 100})
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        query,
        # Stale sequence 2, identical content: idempotent duplicate.
        portfolio("r1", "AAA", 2, "fund", {"AAA": 100}),
        # Sequence 3 then proceeds normally.
        add("a2", "AAA", 3, "s2", "SELL", "LIMIT", 1, 100),
    ])
    assert out["results"][2]["status"] == DUPLICATE
    assert out["results"][2]["trades"] == []
    assert out["results"][2]["book_changes"] == {"bids": [], "asks": []}
    assert out["results"][3]["result"] == "FILLED"


def test_same_report_id_with_different_mark_prices_conflicts():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "fund", {"AAA": 100}),
        portfolio("r1", "AAA", 3, "fund", {"AAA": 101}),
    ])
    assert out["results"][2]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_business_checks():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        portfolio("r1", "AAA", 3, "fund", {"AAA": 100}),
        portfolio("r2", "AAA", 1, "gone", {"AAA": 100}),
    ])
    assert out["results"][1]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][2]["rejection_code"] == OUT_OF_ORDER
    # Neither occupied its slot: sequence 2 still works.
    follow = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        portfolio("r1", "AAA", 3, "fund", {"AAA": 100}),
        portfolio("r3", "AAA", 2, "fund", {"AAA": 100}),
    ])
    assert follow["results"][2]["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_portfolio_report_output_is_byte_for_byte_deterministic():
    events = _rich_stream() + [
        portfolio("r1", "AAA", 5, "fund", {"CCC": 7, "AAA": 110, "BBB": 40}),
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


def test_resumed_replay_with_portfolio_reports_matches_one_shot():
    stream = _rich_stream() + [
        portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7}),
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "SELL", 2, 2, 70, "fund"),
        twap_slice("d3", "DDD", 3, "p1"),
        portfolio("r2", "CCC", 3, "fund",
                  {"AAA": 105, "BBB": 42, "CCC": 8, "DDD": 66}),
        portfolio("r3", "ZZZ", 1, "gone", {"AAA": 1}),       # business reject
        portfolio("r4", "AAA", 6, "fund", {"AAA": 1, "BBB": 2}),  # mismatch
    ]
    # Split inside the AAA activity to exercise mid-stream resume.
    part1 = stream[:4]
    one_shot = replay_events(stream)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(stream[4:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(
        one_shot["results"][4:]
    )
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_portfolio_report_works_after_restore_and_snapshot_format_is_unchanged():
    part1 = _rich_stream()
    snapshot = replay_events(part1)["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    out = replay_events(
        [portfolio("r1", "BBB", 4, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})],
        snapshot=snapshot,
    )
    assert out["results"][0]["portfolio_analysis"] == _expected_rich_analysis()


def test_duplicate_report_is_still_idempotent_after_restore():
    snapshot = replay_events(
        _rich_stream() + [portfolio("r1", "AAA", 5, "fund",
                                    {"AAA": 110, "BBB": 40, "CCC": 7})]
    )["snapshot"]
    out = replay_events(
        [portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})],
        snapshot=snapshot,
    )
    assert out["results"][0]["status"] == DUPLICATE


def test_snapshot_after_named_portfolio_report_event():
    events = _rich_stream() + [
        portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})
    ]
    out = replay_events(
        events, snapshot_after={"symbol": "AAA", "sequence": 5}
    )
    aaa = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 5
    assert "r1" in {e["event_id"] for e in aaa["state"]["event_log"]}
    assert "r1" not in aaa["state"]["engine"]["event_ids"]


def test_business_rejected_report_roundtrips_through_snapshot():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100, account_id="fund"),
        portfolio("r1", "AAA", 2, "gone", {"AAA": 100}),
    ])
    snapshot = out["snapshot"]
    # Export/restore must accept the replay-only id and keep it occupied.
    from order_book_engine import restore_replayer, export_snapshot
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [portfolio("r1", "AAA", 2, "gone", {"AAA": 100})],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


# ---------------------------------------------------------------------------
# EventReplayer stateful session
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_portfolio_reports():
    from order_book_engine import EventReplayer
    replayer = EventReplayer()
    r1 = replayer.submit([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 2, 100, account_id="fund")
    ])
    assert r1[0]["result"] == "RESTING"
    r2 = replayer.submit([portfolio("r1", "AAA", 2, "fund", {"AAA": 100})])
    assert r2[0]["status"] == ACCEPTED
    assert r2[0]["portfolio_analysis"]["positions"][0]["net_position"] == 0


# ---------------------------------------------------------------------------
# Baseline entry points do not accept the event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_portfolio_report_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1", "type": PORTFOLIO_REPORT,
        "account_id": "fund", "mark_prices": {"AAA": 100},
    }
    result = engine.handle_object_position(query)
    assert result[1] == "REJECTED"
    assert result[2] == INVALID_SCHEMA
    # The structural rejection consumes no event id: the same payload is
    # rejected identically on a second submission rather than as a duplicate.
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


def test_cli_events_supports_portfolio_report_end_to_end():
    request = {"events": _rich_stream() + [
        portfolio("r1", "AAA", 5, "fund", {"AAA": 110, "BBB": 40, "CCC": 7})
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "REPORTED"
    assert r["portfolio_analysis"] == _expected_rich_analysis()
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_portfolio_business_rejections_roundtrip():
    code, out, _ = _run_cli({"events": [
        portfolio("r0", "AAA", 1, "gone", {"AAA": 100}),
        add("a1", "BBB", 1, "o1", "BUY", "LIMIT", 1, 10, account_id="fund"),
        portfolio("r1", "AAA", 2, "fund", {"AAA": 1, "BBB": 2}),
    ]})
    assert code == 0
    assert out["results"][0]["rejection_code"] == UNKNOWN_ACCOUNT
    assert out["results"][2]["rejection_code"] == MARK_PRICE_MISMATCH
