"""Tests for the all-security SESSION_RECONCILIATION event in the multi-symbol stream."""

from __future__ import annotations

import copy
import io
import json

import pytest

from order_book_engine import (
    ACCEPTED,
    BREAKS_FOUND,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    FORMAT_VERSION,
    INVALID_EVENT,
    OUT_OF_ORDER,
    RECONCILED,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    TWAP_SLICE,
    TWAP_START,
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


def replace(event_id, symbol, sequence, order_id, quantity, price, **extra):
    event = {"event_id": event_id, "symbol": symbol, "sequence": sequence,
             "type": "REPLACE", "order_id": order_id,
             "quantity": quantity, "price": price}
    event.update(extra)
    return event


def reconcile(event_id, symbol, sequence, expected_trades, expected_accounts, **extra):
    event = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "type": SESSION_RECONCILIATION,
        "expected_trades": expected_trades,
        "expected_accounts": expected_accounts,
    }
    event.update(extra)
    return event


def trade(symbol, trade_id, maker, taker, price, quantity):
    return {
        "symbol": symbol,
        "trade_id": trade_id,
        "maker_order_id": maker,
        "taker_order_id": taker,
        "price": price,
        "quantity": quantity,
    }


def account(symbol, account_id, net_position, cash_balance):
    return {
        "symbol": symbol,
        "account_id": account_id,
        "net_position": net_position,
        "cash_balance": cash_balance,
    }


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


# ---------------------------------------------------------------------------
# Fixture: trades and accounts across three securities
# ---------------------------------------------------------------------------


def _rich_stream():
    """fund's activity, with anonymous counterparties:

    AAA: fund sells 2@100 as maker, then (post-replace) 4@101 as maker.
    BBB: fund's iceberg buys 4@50 as taker, then makes 1@50.
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
        add("b2", "BBB", 2, "i1", "BUY", "ICEBERG", 5, 50,
            display_quantity=2, account_id="fund"),
        add("b3", "BBB", 3, "z1", "SELL", "MARKET", 1),
        # CCC sequence 1..2
        add("c1", "CCC", 1, "o1", "BUY", "LIMIT", 3, 10, account_id="fund"),
        cancel("c2", "CCC", 2, "o1"),
    ]


def _rich_expected_trades():
    return [
        trade("AAA", 1, "s1", "b1", 100, 2),
        trade("AAA", 2, "s1", "b2", 101, 4),
        trade("BBB", 1, "x1", "i1", 50, 4),
        trade("BBB", 2, "i1", "z1", 50, 1),
    ]


def _rich_expected_accounts():
    return [
        account("AAA", "fund", -6, 604),
        account("BBB", "fund", 5, -250),
        account("CCC", "fund", 0, 0),
    ]


# ---------------------------------------------------------------------------
# Success path and result shape
# ---------------------------------------------------------------------------


def test_session_reconciliation_reconciled_shape():
    events = _rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts())
    ]
    r = replay_events(events)["results"][-1]
    assert r["event_id"] == "r1"
    assert r["symbol"] == "AAA"
    assert r["sequence"] == 5
    assert r["status"] == ACCEPTED
    assert r["result"] == RECONCILED
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert r["reconciliation"] == {"trade_breaks": [], "account_breaks": []}
    assert "rejection_code" not in r
    assert "execution_plan" not in r


def test_session_reconciliation_echoes_envelope_symbol_book_only():
    events = _rich_stream() + [
        add("a5", "AAA", 5, "s9", "SELL", "LIMIT", 3, 123),
        reconcile("r1", "AAA", 6, _rich_expected_trades(), _rich_expected_accounts()),
    ]
    r = replay_events(events)["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["asks"] == [{"price": 123, "quantity": 3}]
    assert r["bids"] == []


def test_empty_session_reconciles_against_empty_expectations():
    out = replay_events([reconcile("r1", "AAA", 1, [], [])])
    r = out["results"][0]
    assert r["status"] == ACCEPTED
    assert r["result"] == RECONCILED
    assert r["bids"] == [] and r["asks"] == []


def test_query_can_be_first_event_on_its_envelope_symbol():
    out = replay_events(_rich_stream() + [
        reconcile("r1", "EEE", 1, _rich_expected_trades(), _rich_expected_accounts())
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == RECONCILED
    eee = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "EEE"][0]
    assert eee["state"]["last_sequence"] == 1
    assert eee["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# Breaks: reasons, content and stable ordering
# ---------------------------------------------------------------------------


def test_breaks_found_reports_all_three_reasons():
    expected_trades = _rich_expected_trades()
    # Drop one actual trade, add one unknown, corrupt one field.
    expected_trades = [t for t in expected_trades if not (
        t["symbol"] == "BBB" and t["trade_id"] == 2)]
    expected_trades.append(trade("AAA", 9, "s1", "b9", 100, 1))
    expected_trades[0] = trade("AAA", 1, "s1", "b1", 100, 5)  # quantity differs
    expected_accounts = _rich_expected_accounts()
    expected_accounts = [a for a in expected_accounts if a["symbol"] != "CCC"]
    expected_accounts.append(account("DDD", "fund", 1, -10))
    expected_accounts[1] = account("BBB", "fund", 4, -250)  # net_position differs

    out = replay_events(_rich_stream() + [
        reconcile("r1", "AAA", 5, expected_trades, expected_accounts)
    ])
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == BREAKS_FOUND
    recon = r["reconciliation"]

    trade_breaks = recon["trade_breaks"]
    assert [(b["identifier"]["symbol"], b["identifier"]["trade_id"])
            for b in trade_breaks] == [("AAA", 1), ("AAA", 9), ("BBB", 2)]
    by_id = {(b["identifier"]["symbol"], b["identifier"]["trade_id"]): b
             for b in trade_breaks}
    mismatch = by_id[("AAA", 1)]
    assert mismatch["reason"] == "FIELD_MISMATCH"
    assert mismatch["expected"] == trade("AAA", 1, "s1", "b1", 100, 5)
    assert mismatch["actual"] == trade("AAA", 1, "s1", "b1", 100, 2)
    missing_actual = by_id[("AAA", 9)]
    assert missing_actual["reason"] == "MISSING_ACTUAL"
    assert missing_actual["actual"] is None
    assert missing_actual["expected"] == trade("AAA", 9, "s1", "b9", 100, 1)
    missing_expected = by_id[("BBB", 2)]
    assert missing_expected["reason"] == "MISSING_EXPECTED"
    assert missing_expected["expected"] is None
    assert missing_expected["actual"] == trade("BBB", 2, "i1", "z1", 50, 1)

    account_breaks = recon["account_breaks"]
    assert [(b["identifier"]["symbol"], b["identifier"]["account_id"])
            for b in account_breaks] == [
        ("BBB", "fund"), ("CCC", "fund"), ("DDD", "fund"),
    ]
    by_acc = {(b["identifier"]["symbol"], b["identifier"]["account_id"]): b
              for b in account_breaks}
    assert by_acc[("BBB", "fund")]["reason"] == "FIELD_MISMATCH"
    assert by_acc[("BBB", "fund")]["actual"] == account("BBB", "fund", 5, -250)
    # CCC's account is actual (known via the cancelled order) but not expected.
    assert by_acc[("CCC", "fund")]["reason"] == "MISSING_EXPECTED"
    assert by_acc[("CCC", "fund")]["expected"] is None
    assert by_acc[("CCC", "fund")]["actual"] == account("CCC", "fund", 0, 0)
    # DDD's account is expected but never existed.
    assert by_acc[("DDD", "fund")]["reason"] == "MISSING_ACTUAL"
    assert by_acc[("DDD", "fund")]["actual"] is None
    for b in trade_breaks + account_breaks:
        assert set(b) == {"identifier", "expected", "actual", "reason"}


def test_breaks_order_symbols_before_ids():
    expected = [
        trade("BBB", 1, "m", "t", 10, 1),
        trade("AAA", 2, "m", "t", 10, 1),
        trade("AAA", 1, "m", "t", 10, 1),
    ]
    out = replay_events([reconcile("r1", "ZZZ", 1, expected, [])])
    breaks = out["results"][0]["reconciliation"]["trade_breaks"]
    assert [(b["identifier"]["symbol"], b["identifier"]["trade_id"])
            for b in breaks] == [("AAA", 1), ("AAA", 2), ("BBB", 1)]
    assert all(b["reason"] == "MISSING_ACTUAL" for b in breaks)


def test_trade_ids_are_per_symbol_not_global():
    # AAA and BBB each spend their own trade id 1; both reconcile.
    events = [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 10),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 10),
        add("b1", "BBB", 1, "s2", "SELL", "LIMIT", 2, 20),
        add("b2", "BBB", 2, "b2", "BUY", "LIMIT", 2, 20),
        reconcile("r1", "AAA", 3,
                  [trade("AAA", 1, "s1", "b1", 10, 1),
                   trade("BBB", 1, "s2", "b2", 20, 2)],
                  []),
    ]
    assert replay_events(events)["results"][-1]["result"] == RECONCILED


# ---------------------------------------------------------------------------
# Actual accounts: zero-trade and plan-established accounts
# ---------------------------------------------------------------------------


def test_zero_trade_account_from_cancelled_order_is_actual():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 3, 10, account_id="fund"),
        cancel("a2", "AAA", 2, "o1"),
        reconcile("r1", "AAA", 3, [], [account("AAA", "fund", 0, 0)]),
    ])
    assert out["results"][-1]["result"] == RECONCILED


def test_account_from_plan_start_without_any_slice_is_actual():
    out = replay_events([
        vwap_start("d1", "DDD", 1, "p1", "BUY", 4, [1, 3], 70, "vp"),
        reconcile("r1", "DDD", 2, [], [account("DDD", "vp", 0, 0)]),
    ])
    assert out["results"][-1]["result"] == RECONCILED


def test_released_twap_slice_trades_and_account_are_actual():
    out = replay_events([
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 2, 70),
        twap_start("d2", "DDD", 2, "p1", "BUY", 3, 3, 70, "tp"),
        twap_slice("d3", "DDD", 3, "p1"),
        reconcile("r1", "DDD", 4,
                  [trade("DDD", 1, "w1", "p1#1", 70, 1)],
                  [account("DDD", "tp", 1, -70)]),
    ])
    assert out["results"][-1]["result"] == RECONCILED


def test_orders_without_account_never_create_actual_accounts():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 10),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 10),
        reconcile("r1", "AAA", 3,
                  [trade("AAA", 1, "s1", "b1", 10, 1)], []),
    ])
    assert out["results"][-1]["result"] == RECONCILED


def test_account_from_rejected_add_is_not_actual():
    out = replay_events([
        add("a1", "AAA", 1, "o1", "BUY", "LIMIT", 1, 100),
        add("a2", "AAA", 2, "o1", "SELL", "LIMIT", 1, 100, account_id="ghost"),
        reconcile("r1", "AAA", 3, [], []),
    ])
    assert out["results"][1]["rejection_code"] == "DUPLICATE_ORDER_ID"
    assert out["results"][-1]["result"] == RECONCILED


# ---------------------------------------------------------------------------
# Read-only guarantees
# ---------------------------------------------------------------------------


def test_session_reconciliation_is_read_only():
    events = _rich_stream()
    before = replay_events(events)["snapshot"]
    with_query = replay_events(events + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts())
    ])["snapshot"]

    def engine_states(snapshot):
        return {s["symbol"]: s["state"]["engine"]
                for s in snapshot["content"]["symbols"]}

    before_states = engine_states(before)
    after_states = engine_states(with_query)
    assert set(before_states) == set(after_states)
    for sym in before_states:
        assert canonical_json(before_states[sym]) == canonical_json(after_states[sym])
    # The query id lives solely in the replay log, not in any engine journal.
    for sym, engine_state in after_states.items():
        assert "r1" not in engine_state["event_ids"]
    aaa = [s for s in with_query["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 5
    # The next AAA trade still takes the next trade id of the fixture (3).
    follow = replay_events(
        [add("a6", "AAA", 6, "s8", "SELL", "LIMIT", 1, 101),
         add("a7", "AAA", 7, "b8", "BUY", "LIMIT", 1, 101)],
        snapshot=with_query,
    )
    assert [t["trade_id"] for t in follow["results"][1]["trades"]] == [3]


def test_query_does_not_move_plans_or_price_limits():
    stream = [
        add("d1", "DDD", 1, "w1", "SELL", "LIMIT", 9, 70),
        twap_start("d2", "DDD", 2, "p1", "BUY", 3, 3, 70, "tp"),
        reconcile("r1", "DDD", 3, [], [account("DDD", "tp", 0, 0)]),
        twap_slice("d4", "DDD", 4, "p1"),
    ]
    out = replay_events(stream)
    assert out["results"][2]["result"] == RECONCILED
    # The slice after the query is still slice number 1 of the plan.
    assert out["results"][3]["execution_plan"]["slice_number"] == 1


# ---------------------------------------------------------------------------
# INVALID_EVENT: structural validation consumes nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"expected_trades": [], "expected_accounts": []},                 # missing event_id/type
    {"event_id": "r1", "expected_trades": [], "expected_accounts": []},  # missing type
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_accounts": []},                                        # missing expected_trades
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": []},                                          # missing expected_accounts
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": [], "expected_accounts": [], "order_id": "o1"},  # extra field
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": None, "expected_accounts": []},               # null trades array
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": [], "expected_accounts": {}},                 # object accounts array
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": ["x"], "expected_accounts": []},              # non-object member
    {"event_id": "r1", "type": SESSION_RECONCILIATION,
     "expected_trades": [None], "expected_accounts": []},             # null member
])
def test_malformed_session_reconciliation_envelopes_are_invalid(bad):
    event = {"symbol": "AAA", "sequence": 1, **bad}
    out = replay_events([event])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["trades"] == []


def _bad_trade(**overrides):
    item = trade("AAA", 1, "s1", "b1", 100, 2)
    item.update(overrides)
    return item


@pytest.mark.parametrize("item", [
    _bad_trade(symbol=""),                       # empty symbol
    _bad_trade(symbol=7),                        # non-string symbol
    _bad_trade(symbol=None),                     # null symbol
    _bad_trade(trade_id=0),                      # zero trade id
    _bad_trade(trade_id=-1),                     # negative trade id
    _bad_trade(trade_id=True),                   # bool trade id
    _bad_trade(trade_id=1.5),                    # float trade id
    _bad_trade(trade_id="1"),                    # string trade id
    _bad_trade(maker_order_id=""),               # empty maker
    _bad_trade(maker_order_id=None),             # null maker
    _bad_trade(taker_order_id=""),               # empty taker
    _bad_trade(price=0),                         # zero price
    _bad_trade(price=-5),                        # negative price
    _bad_trade(price=False),                     # bool price
    _bad_trade(quantity=0),                      # zero quantity
    _bad_trade(quantity=-2),                     # negative quantity
    _bad_trade(quantity=True),                   # bool quantity
])
def test_malformed_expected_trade_members_are_invalid(item):
    out = replay_events([reconcile("r1", "AAA", 1, [item], [])])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT, item


def test_expected_trade_missing_and_extra_fields_are_invalid():
    missing = {"symbol": "AAA", "trade_id": 1, "maker_order_id": "s1",
               "taker_order_id": "b1", "price": 100}  # no quantity
    extra = dict(trade("AAA", 1, "s1", "b1", 100, 2), event_id="x")
    for item in (missing, extra):
        out = replay_events([reconcile("r1", "AAA", 1, [item], [])])
        assert out["results"][0]["rejection_code"] == INVALID_EVENT, item


def test_duplicate_expected_trade_identifier_is_invalid():
    pair = [trade("AAA", 1, "s1", "b1", 100, 2),
            trade("AAA", 1, "s2", "b2", 101, 3)]
    out = replay_events([reconcile("r1", "AAA", 1, pair, [])])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    # The same trade id on another symbol is a different identifier.
    ok = [trade("AAA", 1, "s1", "b1", 100, 2),
          trade("BBB", 1, "s2", "b2", 101, 3)]
    out2 = replay_events([reconcile("r1", "AAA", 1, ok, [])])
    assert "rejection_code" not in out2["results"][0]


def _bad_account(**overrides):
    item = account("AAA", "fund", 0, 0)
    item.update(overrides)
    return item


@pytest.mark.parametrize("item", [
    _bad_account(symbol=""),                       # empty symbol
    _bad_account(symbol=None),                     # null symbol
    _bad_account(account_id=""),                   # empty account id
    _bad_account(account_id=7),                    # non-string account id
    _bad_account(account_id=None),                 # null account id
    _bad_account(net_position=True),               # bool net position
    _bad_account(net_position=1.5),                # float net position
    _bad_account(net_position="0"),                # string net position
    _bad_account(cash_balance=False),              # bool cash balance
    _bad_account(cash_balance=2.5),                # float cash balance
])
def test_malformed_expected_account_members_are_invalid(item):
    out = replay_events([reconcile("r1", "AAA", 1, [], [item])])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT, item


def test_expected_account_missing_and_extra_fields_are_invalid():
    missing = {"symbol": "AAA", "account_id": "fund", "net_position": 0}
    extra = dict(account("AAA", "fund", 0, 0), mark_price=3)
    for item in (missing, extra):
        out = replay_events([reconcile("r1", "AAA", 1, [], [item])])
        assert out["results"][0]["rejection_code"] == INVALID_EVENT, item


def test_duplicate_expected_account_identifier_is_invalid():
    pair = [account("AAA", "fund", 0, 0), account("AAA", "fund", 1, 1)]
    out = replay_events([reconcile("r1", "AAA", 1, [], pair)])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    ok = [account("AAA", "fund", 0, 0), account("BBB", "fund", 0, 0)]
    out2 = replay_events([reconcile("r1", "AAA", 1, [], ok)])
    assert "rejection_code" not in out2["results"][0]


def test_negative_account_numerics_are_legal():
    out = replay_events(_rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(),
                  _rich_expected_accounts())
    ])
    assert out["results"][-1]["result"] == RECONCILED


def test_invalid_event_consumes_neither_id_nor_sequence():
    base = [add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund")]
    bad = reconcile("r1", "AAA", 2, [_bad_trade(price=0)], [])
    good = reconcile("r2", "AAA", 2, [],
                     [account("AAA", "fund", 0, 0)])
    out = replay_events(base + [bad, good])
    assert out["results"][1]["rejection_code"] == INVALID_EVENT
    assert out["results"][2]["status"] == ACCEPTED
    # The invalid event also consumed no event id: r1 itself is reusable.
    retry = replay_events(base + [bad, reconcile("r1", "AAA", 2, [],
                                                 [account("AAA", "fund", 0, 0)])])
    assert retry["results"][1]["rejection_code"] == INVALID_EVENT
    assert retry["results"][2]["status"] == ACCEPTED


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([reconcile("r1", "EEE", 1, [_bad_trade(quantity=-1)], [])])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    out = replay_events([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 1, 10, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 10),
        {"event_id": "r1", "symbol": "AAA", "sequence": 3,
         "event": {"event_id": "r1", "type": SESSION_RECONCILIATION,
                   "expected_trades": [trade("AAA", 1, "s1", "b1", 10, 1)],
                   "expected_accounts": [account("AAA", "fund", -1, 10)]}},
    ])
    assert out["results"][2]["result"] == RECONCILED


# ---------------------------------------------------------------------------
# Envelope, idempotency and ordering precedence
# ---------------------------------------------------------------------------


def test_identical_query_replay_is_duplicate():
    events = _rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts()),
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts()),
        add("a5", "AAA", 6, "s9", "SELL", "LIMIT", 1, 200),
    ]
    out = replay_events(events)
    assert out["results"][-2]["status"] == DUPLICATE
    assert out["results"][-2]["trades"] == []
    assert out["results"][-1]["result"] == "RESTING"


def test_same_query_id_with_different_content_conflicts():
    events = _rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts()),
        reconcile("r1", "AAA", 6, [], []),
    ]
    assert replay_events(events)["results"][-1]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_evaluation():
    events = _rich_stream() + [
        reconcile("r1", "AAA", 7, _rich_expected_trades(), _rich_expected_accounts()),
        reconcile("r2", "AAA", 4, [], []),
    ]
    out = replay_events(events)
    assert out["results"][-2]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][-1]["rejection_code"] == OUT_OF_ORDER
    # Neither occupied its slot: sequence 5 still works afterwards.
    follow = _rich_stream() + [
        reconcile("r1", "AAA", 7, [], []),
        reconcile("r2", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts()),
    ]
    assert replay_events(follow)["results"][-1]["result"] == RECONCILED


def test_breaks_found_still_consumes_id_and_sequence():
    prefix = _rich_stream() + [reconcile("r1", "AAA", 5, [], [])]
    out = replay_events(prefix + [reconcile("r2", "AAA", 5, [], [])])
    assert out["results"][-2]["result"] == BREAKS_FOUND
    assert out["results"][-1]["rejection_code"] == OUT_OF_ORDER
    retry = replay_events(prefix + [reconcile("r1", "AAA", 5, [], [])])
    assert retry["results"][-1]["status"] == DUPLICATE


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_session_reconciliation_output_is_byte_for_byte_deterministic():
    events = _rich_stream() + [
        reconcile("r1", "AAA", 5,
                  list(reversed(_rich_expected_trades())) + [trade("ZZZ", 3, "m", "t", 1, 1)],
                  list(reversed(_rich_expected_accounts()))),
    ]
    a = canonical_json(replay_events(events))
    b = canonical_json(replay_events(copy.deepcopy(events)))
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


# ---------------------------------------------------------------------------
# Snapshot export / restore
# ---------------------------------------------------------------------------


def test_resumed_replay_with_session_reconciliation_matches_one_shot():
    stream = _rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts()),
        reconcile("r2", "BBB", 4, [], []),  # BREAKS_FOUND
        reconcile("r3", "ZZZ", 1, [_bad_trade(symbol="")], []),  # INVALID_EVENT
        reconcile("r4", "CCC", 3, _rich_expected_trades(), _rich_expected_accounts()),
    ]
    part1 = stream[:4]
    one_shot = replay_events(stream)
    snapshot = replay_events(part1)["snapshot"]
    segmented = replay_events(stream[4:], snapshot=snapshot)
    assert canonical_json(segmented["results"]) == canonical_json(one_shot["results"][4:])
    assert canonical_json(segmented["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_session_reconciliation_works_after_restore():
    snapshot = replay_events(_rich_stream())["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    out = replay_events(
        [reconcile("r1", "BBB", 4, _rich_expected_trades(), _rich_expected_accounts())],
        snapshot=snapshot,
    )
    assert out["results"][0]["result"] == RECONCILED


def test_duplicate_query_is_still_idempotent_after_restore():
    snapshot = replay_events(_rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts())
    ])["snapshot"]
    out = replay_events(
        [reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts())],
        snapshot=snapshot,
    )
    assert out["results"][0]["status"] == DUPLICATE


def test_breaks_found_result_roundtrips_through_snapshot_byte_identically():
    from order_book_engine import export_snapshot, restore_replayer

    events = _rich_stream() + [reconcile("r1", "AAA", 5, [], [])]
    out = replay_events(events)
    assert out["results"][-1]["result"] == BREAKS_FOUND
    snapshot = out["snapshot"]
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    # Re-deriving the same query from the restored state is byte-identical.
    prefix_snapshot = replay_events(events[:-1])["snapshot"]
    replayed = replay_events([events[-1]], snapshot=prefix_snapshot)
    assert canonical_json(replayed["results"]) == canonical_json(out["results"][-1:])


# ---------------------------------------------------------------------------
# EventReplayer stateful session
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_session_reconciliation():
    from order_book_engine import EventReplayer

    replayer = EventReplayer()
    replayer.submit([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 2, 100),
    ])
    out = replayer.submit([
        reconcile("r1", "AAA", 3,
                  [trade("AAA", 1, "s1", "b1", 100, 2)],
                  [account("AAA", "fund", -2, 200)]),
    ])
    assert out[0]["status"] == ACCEPTED
    assert out[0]["result"] == RECONCILED


# ---------------------------------------------------------------------------
# Baseline entry points do not accept the event
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_session_reconciliation_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1", "type": SESSION_RECONCILIATION,
        "expected_trades": [], "expected_accounts": [],
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


def test_cli_events_supports_session_reconciliation_end_to_end():
    request = {"events": _rich_stream() + [
        reconcile("r1", "AAA", 5, _rich_expected_trades(), _rich_expected_accounts())
    ]}
    code, out, err = _run_cli(request)
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == RECONCILED
    assert r["reconciliation"] == {"trade_breaks": [], "account_breaks": []}
    assert out["snapshot"]["format_version"] == FORMAT_VERSION


def test_cli_events_session_reconciliation_breaks_roundtrip():
    code, out, _ = _run_cli({"events": _rich_stream() + [
        reconcile("r1", "AAA", 5, [], [])
    ]})
    assert code == 0
    r = out["results"][-1]
    assert r["result"] == BREAKS_FOUND
    assert len(r["reconciliation"]["trade_breaks"]) == 4
    assert len(r["reconciliation"]["account_breaks"]) == 3
