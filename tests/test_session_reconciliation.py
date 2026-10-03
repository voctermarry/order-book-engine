"""Tests for the read-only whole-session SESSION_RECONCILIATION event."""

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
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    TWAP_SLICE,
    TWAP_START,
    EventReplayer,
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


def etrade(symbol, trade_id, maker, taker, price, quantity, **extra):
    item = {
        "symbol": symbol,
        "trade_id": trade_id,
        "maker_order_id": maker,
        "taker_order_id": taker,
        "price": price,
        "quantity": quantity,
    }
    item.update(extra)
    return item


def eaccount(symbol, account_id, net_position, cash_balance, **extra):
    item = {
        "symbol": symbol,
        "account_id": account_id,
        "net_position": net_position,
        "cash_balance": cash_balance,
    }
    item.update(extra)
    return item


def reconciliation(event_id, symbol, sequence, expected_trades, expected_accounts, **extra):
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


# ---------------------------------------------------------------------------
# Fixture: trades and accounts across three securities
# ---------------------------------------------------------------------------


def _rich_stream():
    """AAA: fund sells 3@100 as maker to anonymous b1 (one trade).

    BBB: w1 sells 2@70; fund's TWAP plan buys 1 (one slice, one trade),
    so fund holds +1 @70.
    CCC: quiet rests a bid and cancels it: known account, zero trades.
    DDD: a started TWAP plan (account planner) that never releases a slice.
    """
    return [
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 5, 100, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 3, 100),
        add("b1e", "BBB", 1, "w1", "SELL", "LIMIT", 2, 70, account_id="w1acct"),
        twap_start("b2e", "BBB", 2, "p1", "BUY", 2, 2, 70, "fund"),
        twap_slice("b3e", "BBB", 3, "p1"),
        add("c1e", "CCC", 1, "o1", "BUY", "LIMIT", 2, 10, account_id="quiet"),
        cancel("c2e", "CCC", 2, "o1"),
        twap_start("d1e", "DDD", 1, "p2", "SELL", 4, 4, 90, "planner"),
    ]


def _expected_records():
    trades = [
        etrade("AAA", 1, "s1", "b1", 100, 3),
        etrade("BBB", 1, "w1", "p1#1", 70, 1),
    ]
    accounts = [
        eaccount("AAA", "fund", -3, 300),
        eaccount("BBB", "fund", 1, -70),
        eaccount("BBB", "w1acct", -1, 70),
        eaccount("CCC", "quiet", 0, 0),
        eaccount("DDD", "planner", 0, 0),
    ]
    return trades, accounts


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


def test_clean_session_reconciles():
    trades, accounts = _expected_records()
    out = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ])
    r = out["results"][-1]
    assert (r["event_id"], r["symbol"], r["sequence"]) == ("r1", "AAA", 3)
    assert r["status"] == ACCEPTED
    assert r["result"] == "RECONCILED"
    assert r["reconciliation"] == {"trade_breaks": [], "account_breaks": []}
    assert r["trades"] == []
    assert r["book_changes"] == {"bids": [], "asks": []}
    assert "rejection_code" not in r
    # Envelope symbol's book is echoed (AAA still shows s1's remainder).
    assert r["asks"] == [{"price": 100, "quantity": 2}]
    assert r["bids"] == []
    assert set(r) == {
        "event_id", "symbol", "sequence", "status", "result",
        "trades", "book_changes", "bids", "asks", "reconciliation",
    }


def test_per_symbol_trade_ids_are_independent():
    # Trade 1 exists on both AAA and BBB: the composite key distinguishes
    # them, so neither is a schema duplicate nor a break.
    trades, accounts = _expected_records()
    assert [t["trade_id"] for t in trades] == [1, 1]
    r = replay_events(_rich_stream() + [
        reconciliation("r1", "BBB", 4, trades, accounts)
    ])["results"][-1]
    assert r["result"] == "RECONCILED"


def test_plan_started_but_never_released_account_is_actual_zero():
    # Drop the DDD planner expectation: it must surface as MISSING_EXPECTED,
    # proving the unreleased plan account is part of the actual set.
    trades, accounts = _expected_records()
    accounts = [a for a in accounts if not (a["symbol"] == "DDD")]
    r = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ])["results"][-1]
    assert r["result"] == "BREAKS_FOUND"
    assert r["reconciliation"]["account_breaks"] == [
        {
            "identifier": ["DDD", "planner"],
            "expected": None,
            "actual": eaccount("DDD", "planner", 0, 0),
            "reason": "MISSING_EXPECTED",
        }
    ]


def test_breaks_of_each_kind_sorted_by_composite_key():
    # Expected:
    #   AAA trade 1 field mismatch (wrong quantity); BBB trade 1 omitted
    #   entirely -> MISSING_EXPECTED; CCC trade 7 extra -> MISSING_ACTUAL.
    trades = [
        etrade("AAA", 1, "s1", "b1", 100, 2),
        etrade("CCC", 7, "x", "y", 1, 1),
    ]
    accounts = [
        eaccount("AAA", "fund", -3, 300),       # matches
        eaccount("BBB", "fund", 2, -70),        # field mismatch
        eaccount("AAA", "ghost", 0, 0),         # missing actual
        # BBB w1 omitted -> missing expected; CCC quiet omitted too.
        eaccount("DDD", "planner", 0, 0),
    ]
    r = replay_events(_rich_stream() + [
        reconciliation("r1", "CCC", 3, trades, accounts)
    ])["results"][-1]
    assert r["result"] == "BREAKS_FOUND"
    tb = r["reconciliation"]["trade_breaks"]
    assert [b["identifier"] for b in tb] == [
        ["AAA", 1], ["BBB", 1], ["CCC", 7],
    ]
    assert [b["reason"] for b in tb] == [
        "FIELD_MISMATCH", "MISSING_EXPECTED", "MISSING_ACTUAL",
    ]
    assert tb[0]["actual"] == etrade("AAA", 1, "s1", "b1", 100, 3)
    assert tb[1]["actual"] == etrade("BBB", 1, "w1", "p1#1", 70, 1)
    assert tb[1]["expected"] is None
    assert tb[2]["actual"] is None
    ab = r["reconciliation"]["account_breaks"]
    assert [b["identifier"] for b in ab] == [
        ["AAA", "ghost"],
        ["BBB", "fund"],
        ["BBB", "w1acct"],
        ["CCC", "quiet"],
    ]
    assert [b["reason"] for b in ab] == [
        "MISSING_ACTUAL", "FIELD_MISMATCH",
        "MISSING_EXPECTED", "MISSING_EXPECTED",
    ]
    assert ab[0]["actual"] is None
    assert ab[1]["expected"] == eaccount("BBB", "fund", 2, -70)
    assert ab[1]["actual"] == eaccount("BBB", "fund", 1, -70)


def test_empty_expectations_on_fresh_session_reconciles():
    r = replay_events([reconciliation("r1", "AAA", 1, [], [])])["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "RECONCILED"
    assert r["bids"] == [] and r["asks"] == []


def test_query_on_brand_new_envelope_symbol_echoes_empty_book():
    out = replay_events(_rich_stream() + [
        reconciliation("r1", "ZZZ", 1, *_expected_records())
    ])
    r = out["results"][-1]
    assert r["result"] == "RECONCILED"
    assert r["bids"] == [] and r["asks"] == []
    zzz = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "ZZZ"][0]
    assert zzz["state"]["last_sequence"] == 1
    assert zzz["state"]["engine"]["event_ids"] == []


# ---------------------------------------------------------------------------
# Read-only behaviour
# ---------------------------------------------------------------------------


def test_query_is_read_only_across_the_whole_snapshot():
    stream = _rich_stream()
    before = replay_events(stream)["snapshot"]
    after = replay_events(stream + [
        reconciliation("rX", "AAA", 3, [], [])            # breaks, but read-only
    ])["snapshot"]
    # Symbols are identical (the query registers only its envelope symbol,
    # AAA, which already exists).
    assert [s["symbol"] for s in before["content"]["symbols"]] == [
        s["symbol"] for s in after["content"]["symbols"]
    ]
    for sb, sa in zip(before["content"]["symbols"], after["content"]["symbols"]):
        assert sb["symbol"] == sa["symbol"]
        assert canonical_json(sb["state"]["engine"]) == canonical_json(sa["state"]["engine"])
        assert sb["state"]["price_limits"] == sa["state"]["price_limits"]
        # Plans and their progress are untouched too.
        assert canonical_json(sb["state"]["plans"]) == canonical_json(sa["state"]["plans"])
    # Only AAA's replay event log gains the query id.
    aaa_after = [s for s in after["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert "rX" in {e["event_id"] for e in aaa_after["state"]["event_log"]}
    assert "rX" not in aaa_after["state"]["engine"]["event_ids"]
    assert aaa_after["state"]["last_sequence"] == 3


def test_query_does_not_spend_trade_ids_or_move_price_limits():
    config = {"price_limits": {"AAA": {"lower": 90, "upper": 110}}}
    stream = _rich_stream()
    snap = replay_events(
        stream + [reconciliation("r0", "AAA", 3, [], [])], config=config
    )["snapshot"]
    out = replay_events(
        [add("a4", "AAA", 4, "b9", "BUY", "LIMIT", 2, 100)],
        config=config, snapshot=snap,
    )
    # The read-only query spent no trade id: the next AAA trade keeps the
    # number it would have had anyway (AAA already recorded trade 1).
    assert [t["trade_id"] for t in out["results"][0]["trades"]] == [2]


def test_repeated_queries_change_nothing_but_their_log_slots():
    stream = _rich_stream()
    trades, accounts = _expected_records()
    out1 = replay_events(stream + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ])["snapshot"]
    out2 = replay_events(stream + [
        reconciliation("r1", "AAA", 3, trades, accounts),
        reconciliation("r2", "AAA", 4, trades, accounts),
    ])["snapshot"]
    first = {s["symbol"]: s for s in out1["content"]["symbols"]}
    second = {s["symbol"]: s for s in out2["content"]["symbols"]}
    for symbol in first:
        assert canonical_json(first[symbol]["state"]["engine"]) == canonical_json(
            second[symbol]["state"]["engine"]
        )


# ---------------------------------------------------------------------------
# INVALID_EVENT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [
    {"expected_accounts": []},                                          # missing trades
    {"expected_trades": []},                                            # missing accounts
    {"expected_trades": [], "expected_accounts": [], "order_id": "o"},  # extra field
    {"expected_trades": None, "expected_accounts": []},                 # null trades
    {"expected_trades": {}, "expected_accounts": []},                   # non-list trades
    {"expected_trades": [], "expected_accounts": None},                 # null accounts
    {"expected_trades": ["x"], "expected_accounts": []},                # non-object member
    {"expected_trades": [[]], "expected_accounts": []},                 # non-object member
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",         # missing taker
         "taker_order_id": "x", "price": 1, "quantity": 1, "q": 1}],    # extra field
     "expected_accounts": []},
    {"expected_trades": [
        {"symbol": "", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # empty symbol
    {"expected_trades": [
        {"symbol": 7, "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # non-string symbol
    {"expected_trades": [
        {"symbol": None, "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # null symbol
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 0, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # non-positive id
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": True, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # bool id
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1.5, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # float id
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": "1", "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # string id
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "",
         "taker_order_id": "t", "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # empty maker
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": 7, "price": 1, "quantity": 1}],
     "expected_accounts": []},                                           # non-string taker
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 0, "quantity": 1}],
     "expected_accounts": []},                                           # zero price
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": -2, "quantity": 1}],
     "expected_accounts": []},                                           # negative price
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": True, "quantity": 1}],
     "expected_accounts": []},                                           # bool price
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": False}],
     "expected_accounts": []},                                           # bool quantity
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": 1, "quantity": 1.0}],
     "expected_accounts": []},                                           # float quantity
    {"expected_trades": [
        {"symbol": "AAA", "trade_id": 1, "maker_order_id": "m",
         "taker_order_id": "t", "price": "1", "quantity": 1}],
     "expected_accounts": []},                                           # string price
    {"expected_trades": [
        etrade("AAA", 1, "m", "t", 1, 1), etrade("AAA", 1, "m", "t", 1, 1)],
     "expected_accounts": []},                                           # duplicate key
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": 1}]},       # missing cash
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": 1,
         "cash_balance": 2, "mark_price": 3}]},                          # extra field
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "", "net_position": 0,
         "cash_balance": 0}]},                                           # empty account
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "", "account_id": "a", "net_position": 0,
         "cash_balance": 0}]},                                           # empty symbol
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": 9, "account_id": "a", "net_position": 0,
         "cash_balance": 0}]},                                           # non-string symbol
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": 3, "net_position": 0,
         "cash_balance": 0}]},                                           # non-string account
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": True,
         "cash_balance": 0}]},                                           # bool position
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": 0,
         "cash_balance": False}]},                                       # bool cash
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": 0.5,
         "cash_balance": 0}]},                                           # float position
    {"expected_trades": [],
     "expected_accounts": [
        {"symbol": "AAA", "account_id": "a", "net_position": 0,
         "cash_balance": "0"}]},                                         # string cash
    {"expected_trades": [],
     "expected_accounts": [
        eaccount("AAA", "a", 0, 0), eaccount("AAA", "a", 1, 1)]},       # duplicate key
])
def test_malformed_session_events_are_invalid(bad):
    # The "distinct symbols trade id" probe is valid; check it separately.
    event = {"event_id": "r1", "symbol": "AAA", "sequence": 1,
             "type": SESSION_RECONCILIATION, **bad}
    result = replay_events([event])["results"][0]
    assert result["rejection_code"] == INVALID_EVENT, bad
    assert result["trades"] == []
    assert result["book_changes"] == {"bids": [], "asks": []}


def test_same_trade_id_on_different_symbols_is_valid():
    bad = {
        "expected_trades": [
            etrade("AAA", 1, "m", "t", 1, 1), etrade("BBB", 1, "m", "t", 1, 1)
        ],
        "expected_accounts": [],
    }
    event = {"event_id": "r1", "symbol": "AAA", "sequence": 1,
             "type": SESSION_RECONCILIATION, **bad}
    result = replay_events([event])["results"][-1]
    assert result["status"] == ACCEPTED
    assert result["result"] == "BREAKS_FOUND"


def test_malformed_event_consumes_neither_id_nor_sequence():
    stream = _rich_stream()
    bad = reconciliation("r1", "AAA", 3,
                         [etrade("AAA", 1, "s1", "b1", 100, True)], [])
    # Sequence 3 stays free and event id r1 stays free.
    out = replay_events(stream + [
        bad,
        reconciliation("r2", "AAA", 3, [], []),
    ])
    assert out["results"][-2]["rejection_code"] == INVALID_EVENT
    assert out["results"][-1]["status"] == ACCEPTED
    retry = replay_events(stream + [
        bad,
        reconciliation("r1", "AAA", 3, [], []),
    ])
    assert retry["results"][-2]["rejection_code"] == INVALID_EVENT
    assert retry["results"][-1]["status"] == ACCEPTED
    assert retry["results"][-1]["event_id"] == "r1"


def test_invalid_event_on_unknown_symbol_does_not_create_it():
    out = replay_events([
        reconciliation("r1", "ZZZ", 1, None, [])
    ])
    assert out["results"][0]["rejection_code"] == INVALID_EVENT
    assert out["results"][0]["bids"] == []
    assert out["snapshot"]["content"]["symbols"] == []


def test_nested_payload_form_is_supported():
    trades, accounts = _expected_records()
    out = replay_events(_rich_stream() + [
        {"event_id": "r1", "symbol": "AAA", "sequence": 3,
         "event": {"event_id": "r1", "type": SESSION_RECONCILIATION,
                   "expected_trades": trades, "expected_accounts": accounts}}
    ])
    assert out["results"][-1]["status"] == ACCEPTED
    assert out["results"][-1]["result"] == "RECONCILED"


def test_nested_payload_event_id_mismatch_is_invalid():
    trades, accounts = _expected_records()
    out = replay_events(_rich_stream() + [
        {"event_id": "r1", "symbol": "AAA", "sequence": 3,
         "event": {"event_id": "OTHER", "type": SESSION_RECONCILIATION,
                   "expected_trades": trades, "expected_accounts": accounts}}
    ])
    assert out["results"][-1]["rejection_code"] == INVALID_EVENT


# ---------------------------------------------------------------------------
# Idempotency and sequencing
# ---------------------------------------------------------------------------


def test_identical_query_replay_is_duplicate():
    trades, accounts = _expected_records()
    query = reconciliation("r1", "AAA", 3, trades, accounts)
    out = replay_events(_rich_stream() + [
        query,
        reconciliation("r1", "AAA", 3, trades, accounts),   # stale duplicate
        add("a4", "AAA", 4, "b9", "BUY", "LIMIT", 1, 99),
    ])
    assert out["results"][-2]["status"] == DUPLICATE
    assert out["results"][-1]["result"] == "RESTING"


def test_same_query_id_with_different_records_conflicts():
    out = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 3, [], []),
        reconciliation("r1", "AAA", 4, [], [eaccount("AAA", "x", 0, 0)]),
    ])
    assert out["results"][-1]["rejection_code"] == EVENT_ID_CONFLICT


def test_sequence_gap_and_out_of_order_precede_the_comparison():
    trades, accounts = _expected_records()
    out = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 5, trades, accounts),   # gap
        reconciliation("r2", "AAA", 1, trades, accounts),   # backwards
    ])
    assert out["results"][-2]["rejection_code"] == SEQUENCE_GAP
    assert out["results"][-1]["rejection_code"] == OUT_OF_ORDER
    follow = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 5, trades, accounts),
        reconciliation("r3", "AAA", 3, trades, accounts),
    ])
    assert follow["results"][-1]["status"] == ACCEPTED
    assert follow["results"][-1]["result"] == "RECONCILED"


def test_valid_query_occupies_event_id_in_replay_log_only():
    trades, accounts = _expected_records()
    out = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ])
    aaa = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 3
    assert "r1" in {e["event_id"] for e in aaa["state"]["event_log"]}
    assert "r1" not in aaa["state"]["engine"]["event_ids"]


# ---------------------------------------------------------------------------
# Determinism and snapshot restore
# ---------------------------------------------------------------------------


def test_output_is_byte_for_byte_deterministic():
    trades, accounts = _expected_records()
    # Unordered input arrays must not perturb the output: breaks are sorted.
    scrambled_trades = list(reversed(trades))
    scrambled_accounts = list(reversed(accounts))
    events_a = _rich_stream() + [
        reconciliation("r1", "AAA", 3, scrambled_trades, scrambled_accounts)
    ]
    events_b = copy.deepcopy(events_a)
    assert canonical_json(replay_events(events_a)) == canonical_json(replay_events(events_b))


def test_restored_results_and_breaks_and_rejections_are_byte_identical():
    trades, accounts = _expected_records()
    tail = [
        # Breaks found.
        reconciliation("r1", "AAA", 3,
                       [etrade("AAA", 1, "s1", "b1", 100, 2)], []),
        # A structurally invalid event after the query.
        reconciliation("bad", "AAA", 4, [etrade("AAA", 1, "s1", "b1", 100, True)], []),
        # The corrected sequence-4 clean query.
        reconciliation("r2", "AAA", 4, trades, accounts),
    ]
    one_shot = replay_events(_rich_stream() + tail)
    snapshot = replay_events(_rich_stream())["snapshot"]
    resumed = replay_events(copy.deepcopy(tail), snapshot=snapshot)
    assert canonical_json(resumed["results"]) == canonical_json(
        one_shot["results"][len(_rich_stream()):]
    )
    assert canonical_json(resumed["snapshot"]) == canonical_json(one_shot["snapshot"])


def test_query_roundtrips_through_snapshot_and_stays_idempotent():
    trades, accounts = _expected_records()
    snapshot = replay_events(_rich_stream() + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ])["snapshot"]
    assert snapshot["format_version"] == FORMAT_VERSION
    from order_book_engine import restore_replayer, export_snapshot
    restored = restore_replayer(copy.deepcopy(snapshot))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    follow = replay_events(
        [reconciliation("r1", "AAA", 3, trades, accounts)],
        snapshot=export_snapshot(restored),
    )
    assert follow["results"][0]["status"] == DUPLICATE


def test_snapshot_after_named_reconciliation_event():
    trades, accounts = _expected_records()
    out = replay_events(
        _rich_stream() + [reconciliation("r1", "AAA", 3, trades, accounts)],
        snapshot_after={"symbol": "AAA", "sequence": 3},
    )
    aaa = [s for s in out["snapshot"]["content"]["symbols"] if s["symbol"] == "AAA"][0]
    assert aaa["state"]["last_sequence"] == 3
    assert "r1" in {e["event_id"] for e in aaa["state"]["event_log"]}


# ---------------------------------------------------------------------------
# Stateful EventReplayer
# ---------------------------------------------------------------------------


def test_stateful_replayer_supports_session_reconciliation():
    replayer = EventReplayer()
    replayer.submit([
        add("a1", "AAA", 1, "s1", "SELL", "LIMIT", 2, 100, account_id="fund"),
        add("a2", "AAA", 2, "b1", "BUY", "LIMIT", 1, 100),
    ])
    r = replayer.submit([
        reconciliation("r1", "AAA", 3,
                       [etrade("AAA", 1, "s1", "b1", 100, 1)],
                       [eaccount("AAA", "fund", -1, 100)])
    ])
    assert r[0]["status"] == ACCEPTED
    assert r[0]["result"] == "RECONCILED"
    assert replayer.book("AAA") == ([], [{"price": 100, "quantity": 1}])


# ---------------------------------------------------------------------------
# Baseline single-security entry point stays unchanged
# ---------------------------------------------------------------------------


def test_baseline_engine_rejects_session_reconciliation_as_invalid_schema():
    engine = Engine()
    query = {
        "event_id": "e1",
        "type": SESSION_RECONCILIATION,
        "expected_trades": [],
        "expected_accounts": [],
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


def test_cli_events_supports_session_reconciliation_end_to_end():
    trades, accounts = _expected_records()
    code, out, err = _run_cli({"events": _rich_stream() + [
        reconciliation("r1", "AAA", 3, trades, accounts)
    ]})
    assert code == 0
    assert err == ""
    r = out["results"][-1]
    assert r["status"] == ACCEPTED
    assert r["result"] == "RECONCILED"
    assert r["reconciliation"] == {"trade_breaks": [], "account_breaks": []}
    assert out["snapshot"]["format_version"] == FORMAT_VERSION
