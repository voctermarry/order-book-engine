"""Seed-driven property tests for the complete multi-symbol replay state machine.

Where ``test_segmented_replay_determinism.py`` pins one hand-written stream,
this module generates *many* fixed-seed multi-symbol streams and asserts the
long-term conservation, idempotency and recovery properties of the public
replay semantics as a state machine. Nothing here adds a product interface,
an event type or an output field; the tests drive only the documented public
surface (:func:`replay_events`, :class:`EventReplayer`,
:func:`export_snapshot` / :func:`restore_replayer`) and read state back
through the same public results, snapshots and read-only reports.

Every stream is produced from one integer seed by a tiny deterministic LCG;
rebuilding from the same seed yields the byte-identical event list, so any
failure reproduces verbatim from the seed and the offending event prefix.

A generated stream mixes, over three securities (two with a static price-limit
band, one without that gets a band intraday):

* plain GTC limit orders, market orders, IOC and FOK orders and iceberg
  orders, plus cancels and replaces (including a replace that carries a
  display quantity onto a plain resting target);
* a completing TWAP, a TWAP breached by an intraday PRICE_LIMIT_UPDATE and
  then cancelled, a completing market VWAP and a market POV with a positive
  release, a zero release and a cancel;
* the whole read-only report family (execution, impact, portfolio,
  portfolio-stress, session reconciliation, plan TCA, historical book
  reconstruction and current-book liquidity depth);
* every documented committed and non-committed failure: structurally
  invalid envelopes, a sequence gap, a stale sequence, a verbatim duplicate
  delivery, an event-id conflict, unknown order / plan targets, a duplicate
  plan, a clash on a reserved derived child id, price-limit breaches and a
  self-trade prevention block.

For every stream the same observations are required along three execution
paths of the identical public semantics:

1. one :func:`replay_events` call over the whole list;
2. one :class:`EventReplayer` driven in several :meth:`submit` segments;
3. a session exported at a prefix boundary and continued through
   :func:`restore_replayer`.

The three paths agree byte for byte (via :func:`canonical_json`) on every
shared per-event result, each security's final book, the trade details, the
last per-plan summaries, the active price-limit interval (read back through
a historical reconstruction probe) and the normalized final snapshot
document. A separate instrumented run inserts every read-only report kind
into the stream and proves that later trade ids, queue priority and plan
releases do not move. A final group checks snapshot tampering: a digest or
structure violation is ``SNAPSHOT_CORRUPT``, a changed format version is
``SNAPSHOT_VERSION_UNSUPPORTED`` and a changed configuration is
``CONFIG_MISMATCH``.
"""

from __future__ import annotations

import copy
import os
import subprocess
import sys
from pathlib import Path

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_LIQUIDITY_REPORT,
    BOOK_RECONSTRUCTION_REPORT,
    CONFIG_MISMATCH,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EventReplayer,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PLAN_TCA_REPORT,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    SnapshotError,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine.engine import (
    DUPLICATE_EVENT_ID,
    DUPLICATE_ORDER_ID,
    EXECUTION_REPORT,
    IMPACT_REPORT,
)
from order_book_engine.event_replay.handlers import (
    _account_symbols,
    _actual_session_accounts,
    _actual_session_trades,
)
from order_book_engine.event_replay.serialization import _digest

BUY, SELL = "BUY", "SELL"
LIMIT, MARKET, ICEBERG = "LIMIT", "MARKET", "ICEBERG"
IOC, FOK = "IOC", "FOK"

REPO_ROOT = Path(__file__).resolve().parents[1]

# Two securities carry a static band; the third starts unlimited and receives
# an intraday band through a PRICE_LIMIT_UPDATE, so both seed configurations
# exercise every code path.
CONFIG = {"price_limits": {
    "AAA": {"lower": 90, "upper": 120},
    "BBB": {"lower": 60, "upper": 90},
}}
SYMBOLS = ["AAA", "BBB", "CCC"]
CENTRAL = {"AAA": 100, "BBB": 75, "CCC": 50}
STATIC_BAND = {"AAA": (90, 120), "BBB": (60, 90), "CCC": None}
JOINT_ACCOUNT = "joint.fund"

# A fixed, small seed corpus (seeds are never drawn at collection time).
SEEDS = (0x5EED, 1, 2, 7, 42, 31337, 999983)


# ---------------------------------------------------------------------------
# Deterministic pseudo-random source
# ---------------------------------------------------------------------------


class LcgRng:
    """A fixed, tiny 31-bit LCG; the only source of variation in a stream."""

    __slots__ = ("state",)

    def __init__(self, seed: int) -> None:
        self.state = seed & 0xFFFFFFFF

    def u32(self) -> int:
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def rint(self, low: int, high: int) -> int:
        return low + self.u32() % (high - low + 1)

    def pick(self, values):
        return values[self.u32() % len(values)]


# ---------------------------------------------------------------------------
# Stream generator
# ---------------------------------------------------------------------------
#
# The generator drives one real EventReplayer live: every candidate decision
# (available liquidity, a resting replace target, the current band, the
# accounts known to the session) is read from actual state, while the RNG
# chooses only quantities and ids. Events are first built per security (each
# security's script is independent of the other securities), the per-symbol
# lists are merged round-robin, and the cross-security read-only reports are
# appended as one global tail once every security has traded.


class _SymbolBuilder:
    def __init__(self, generator: "StreamGenerator", symbol: str) -> None:
        self.g = generator
        self.symbol = symbol
        self.seq = 0
        # oid -> {"side", "price", "ice", "account"} for external ADD orders.
        self.orders: dict[str, dict[str, object]] = {}
        self.resting: set[str] = set()
        # pid -> {"algo", "otype", "price", "account", "status"}
        self.plans: dict[str, dict[str, object]] = {}
        self.items: list[dict[str, object]] = []
        self.no = 0
        self.np = 0
        band = STATIC_BAND[symbol]
        self.band: tuple[int, int] | None = band
        self.central = CENTRAL[symbol]

    def cur(self) -> int:
        return self.seq + 1

    def envelope(self, payload: dict[str, object], sequence,
                 event_id) -> dict[str, object]:
        event = {
            "event_id": event_id,
            "symbol": self.symbol,
            "sequence": self.cur() if sequence is None else sequence,
        }
        event.update(payload)
        return event

    def emit(self, payload: dict[str, object], *, advance: bool = True,
             sequence=None, event_id=None, label=None) -> dict[str, object]:
        event = self.envelope(payload, sequence, event_id or self.g.event_id())
        result = self.g.session.submit([copy.deepcopy(event)])[0]
        # The label records the event's per-symbol occurrence index, not just
        # its id: a later duplicate redelivery or id conflict reuses an id,
        # so an id-only lookup could resolve to the wrong result.
        local_index = len(self.items)
        self.items.append(event)
        if label is not None:
            self.g.labels[label] = (
                self.symbol, event["event_id"], local_index)
        if advance:
            self.seq += 1
        self._absorb(event, result)
        return result

    def _absorb(self, event: dict[str, object], result: dict[str, object]) -> None:
        """Own the generator's lightweight total-remainder ledger plus the
        metadata future target selection needs."""
        etype = event.get("type")
        accepted = result.get("status") == ACCEPTED
        if accepted and etype == "ADD" and event["order_id"] not in self.orders:
            self.orders[event["order_id"]] = {
                "side": event["side"],
                "price": event.get("price"),
                "ice": event["order_type"] == ICEBERG,
                "account": event.get("account_id"),
            }
            self.g._remaining[(self.symbol, event["order_id"])] = event["quantity"]
        if accepted:
            if etype in ("ADD", "REPLACE") and result.get("result") in (
                "RESTING", "PARTIALLY_FILLED_RESTING", "REPLACED"
            ):
                self.resting.add(event["order_id"])
            elif etype == "CANCEL" and result.get("result") == "CANCELLED":
                self.resting.discard(event["order_id"])
            elif etype == PRICE_LIMIT_UPDATE:
                self.band = (event["lower_price"], event["upper_price"])
            elif etype in (TWAP_START, VWAP_START, POV_START):
                self.plans[event["plan_id"]] = {
                    "algo": etype.split("_")[0],
                    "otype": event["order_type"],
                    "price": event.get("price"),
                    "account": event.get("account_id"),
                    "status": "ACTIVE",
                }
        # Every trade (including plan-child trades against external makers)
        # consumes total remainder on both known sides.
        consumed: dict[str, int] = {}
        for trade in result.get("trades", []):
            for role in ("maker_order_id", "taker_order_id"):
                consumed[trade[role]] = consumed.get(trade[role], 0) + (
                    trade["quantity"])
        for oid, qty in consumed.items():
            key = (self.symbol, oid)
            if key in self.g._remaining:
                self.g._remaining[key] -= qty
                if self.g._remaining[key] <= 0:
                    self.resting.discard(oid)
        if accepted and etype == "REPLACE":
            meta = self.orders.get(event["order_id"])
            if meta is not None:
                meta["price"] = event["price"]
                if "display_quantity" in event:
                    meta["ice"] = True
                taker_qty = sum(
                    trade["quantity"] for trade in result.get("trades", [])
                    if trade["taker_order_id"] == event["order_id"])
                self.g._remaining[(self.symbol, event["order_id"])] = (
                    event["quantity"] - taker_qty)
        elif accepted and etype == "CANCEL" and result.get("result") == "CANCELLED":
            self.g._remaining[(self.symbol, event["order_id"])] = 0
        elif accepted and etype == "ADD" and result.get("result") not in (
            "RESTING", "PARTIALLY_FILLED_RESTING"
        ):
            # Immediate leftovers (market/IOC/FOK/STP takers) never rest.
            self.g._remaining[(self.symbol, event["order_id"])] = 0
        plan = result.get("execution_plan")
        if plan is not None and plan.get("status") in ("COMPLETED", "CANCELLED"):
            pid = event.get("plan_id")
            if pid is not None and pid in self.plans:
                self.plans[pid]["status"] = plan["status"]

    # -- pricing and selection ---------------------------------------------

    def in_band(self, price: int) -> bool:
        return self.band is None or self.band[0] <= price <= self.band[1]

    def band_price(self, around: int, spread_low: int = 0,
                   spread_high: int = 0) -> int:
        for _ in range(64):
            price = around + self.g.rng.rint(spread_low, spread_high)
            if price > 0 and self.in_band(price):
                return price
        return self.band[0] if self.band is not None else around

    def ask_liquidity(self) -> int:
        _bids, asks = self.g.session.book(self.symbol)
        return sum(level["quantity"] for level in asks)

    def new_order_id(self) -> str:
        self.no += 1
        return f"{self.symbol}.o{self.no}"

    def new_plan_id(self) -> str:
        self.np += 1
        return f"{self.symbol}.P{self.np}"

    def resting_plain(self) -> list[str]:
        return [
            oid for oid in sorted(self.resting)
            if oid in self.orders and not self.orders[oid]["ice"]
        ]

    # -- typed event builders ----------------------------------------------

    def add(self, side, order_type, quantity, price=None, tif=None,
            account=None, display=None, order_id=None, *, sequence=None,
            advance=True, label=None):
        payload = {
            "type": "ADD",
            "order_id": order_id or self.new_order_id(),
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
        }
        if price is not None:
            payload["price"] = price
        if tif is not None:
            payload["time_in_force"] = tif
        if account is not None:
            payload["account_id"] = account
        if display is not None:
            payload["display_quantity"] = display
        return self.emit(payload, sequence=sequence, advance=advance,
                         label=label)

    def cancel(self, order_id, *, sequence=None, advance=True, label=None):
        return self.emit({"type": "CANCEL", "order_id": order_id},
                         sequence=sequence, advance=advance, label=label)

    def replace(self, order_id, quantity, price, display=None, *,
                sequence=None, advance=True, label=None):
        payload = {"type": "REPLACE", "order_id": order_id,
                   "quantity": quantity, "price": price}
        if display is not None:
            payload["display_quantity"] = display
        return self.emit(payload, sequence=sequence, advance=advance,
                         label=label)

    def limit_update(self, lower, upper, *, label=None):
        return self.emit({"type": "PRICE_LIMIT_UPDATE",
                          "lower_price": lower, "upper_price": upper},
                         label=label)


class StreamGenerator:
    """Builds one reproducible multi-symbol stream for one seed."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = LcgRng(seed)
        self.session = EventReplayer(copy.deepcopy(CONFIG))
        self.counter = 0
        self.symbols = {name: _SymbolBuilder(self, name) for name in SYMBOLS}
        self.labels: dict[str, tuple[str, str]] = {}
        # Results for labelled envelopes whose event id cannot be echoed
        # (e.g. a structurally invalid numeric event id).
        self.anchor_results: dict[str, dict[str, object]] = {}
        # Running total remainder per (symbol, order id) for resting selection.
        self._remaining: dict[tuple[str, str], int] = {}
        self.phase1_events: list[dict[str, object]] = []
        self.events: list[dict[str, object]] = []

    def event_id(self) -> str:
        self.counter += 1
        return f"ev{self.counter:04d}"

    def remaining(self, symbol, order_id):
        return self._remaining.get((symbol, order_id))

    # -- per-symbol trading script ------------------------------------------

    def _trading_script(self, sg: _SymbolBuilder) -> None:
        rng, central, name = self.rng, sg.central, sg.symbol
        acct = lambda suffix: f"{name}.{suffix}"  # noqa: E731

        def add_sells(count: int, account: str) -> None:
            for _ in range(count):
                oid = sg.new_order_id()
                price = sg.band_price(central, 0, 1)
                sg.add(SELL, LIMIT, rng.rint(2, 4), price, account=account,
                       order_id=oid)

        def external_add(side, order_type, quantity, **kwargs):
            oid = kwargs.pop("order_id", None) or sg.new_order_id()
            result = sg.add(side, order_type, quantity, order_id=oid, **kwargs)
            return result, oid

        # 1) Seed the book --------------------------------------------------
        external_add(SELL, LIMIT, rng.rint(4, 6),
                     price=sg.band_price(central, 0, 1),
                     account=acct("alpha"), label="first")
        external_add(BUY, LIMIT, rng.rint(2, 4),
                     price=sg.band_price(central, -3, -2),
                     account=acct("beta"))
        ice_total = rng.rint(10, 14)
        external_add(SELL, ICEBERG, ice_total,
                     price=sg.band_price(central, 0, 0),
                     account=acct("ice"), display=rng.rint(2, 3),
                     label="iceberg")
        external_add(SELL, LIMIT, rng.rint(1, 2),
                     price=sg.band_price(central, 0, 1))
        # A shared-account maker on every security makes the cross-security
        # portfolio reports genuinely multi-symbol.
        external_add(SELL, LIMIT, 2,
                     price=sg.band_price(central, 0, 1),
                     account=JOINT_ACCOUNT)

        # The unlimited security receives an intraday band before any breach
        # or LIMIT plan; every price used so far sits inside the new band.
        if sg.band is None:
            sg.limit_update(central - 8, central + 8, label="band_introduced")
            external_add(SELL, LIMIT, 2,
                         price=sg.band_price(central, 0, 1),
                         account=acct("inband"))

        # 2) Takers: partial market, IOC, a fillable FOK, a short FOK, STP. --
        external_add(BUY, MARKET, 1, account=acct("taker"),
                     label="market_partial")
        external_add(BUY, LIMIT, rng.rint(1, 2),
                     price=sg.band_price(central, 2, 3), tif=IOC)
        if sg.ask_liquidity() < 1:
            add_sells(1, acct("liq"))
        liquidity = sg.ask_liquidity()
        external_add(BUY, MARKET, rng.rint(1, max(1, liquidity)), tif=FOK,
                     label="fok_fill")
        external_add(BUY, MARKET, 999, tif=FOK, label="fok_fail")

        # Self-trade prevention: first sweep every resting ask with anonymous
        # market buys, then put one same-account sell at best bid + 1. It is
        # guaranteed to be the only (and therefore first) ask the incoming
        # same-account market buy meets, so STP blocks before any trade.
        bids_before, asks_before = self.session.book(name)
        assert bids_before, "the seeded bid must still rest at this point"
        while True:
            _bids, asks = self.session.book(name)
            if not asks:
                break
            external_add(BUY, MARKET, asks[0]["quantity"])
        best_bid = self.session.book(name)[0][0]["price"]
        stp_price = best_bid + 1
        assert sg.in_band(stp_price)
        _stp_result, stp_oid = external_add(
            SELL, LIMIT, 1, price=stp_price, account=acct("stp"))
        stp_cross, _oid = external_add(
            BUY, MARKET, 1, account=acct("stp"), label="self_trade")
        assert stp_cross["result"] == "SELF_TRADE_PREVENTED"
        assert stp_cross["trades"] == []
        assert _stp_result["trades"] == []

        # 3) Cancels: one resting target and one unknown order. -------------
        targets = sg.resting_plain()
        if targets:
            target = rng.pick(targets)
            sg.cancel(target, label="cancel_ok")
        sg.cancel(f"{name}.ghost", label="cancel_unknown")

        # 4) Replaces: accepted, unknown target, display-quantity onto a
        #    plain resting target (the engine's pre-commit rule), then a
        #    duplicate baseline order id.
        targets = sg.resting_plain()
        if targets:
            target = rng.pick(targets)
            sg.replace(target, rng.rint(1, 5),
                       sg.orders[target]["price"], label="replace_ok")
        sg.replace(f"{name}.ghost", 1, central, label="replace_unknown")
        targets = sg.resting_plain()
        if targets:
            target = rng.pick(targets)
            sg.replace(target, rng.rint(1, 5),
                       sg.orders[target]["price"], display=1,
                       label="replace_plain_display")
        known_id = next(iter(sg.orders))
        meta = sg.orders[known_id]
        sg.add(meta["side"], LIMIT, 1,
               meta["price"] or sg.band_price(central, 0, 0),
               order_id=known_id, label="duplicate_order")

        # 5) Price-limit breach against the active band. -------------------
        upper = sg.band[1]
        sg.add(SELL, LIMIT, 1, upper + rng.rint(1, 3),
               label="price_breach")

        # 6) Parent orders. --------------------------------------------------
        # 6a) Completing LIMIT TWAP; the limit is the current upper band.
        add_sells(3, acct("liq"))
        t1 = sg.new_plan_id()
        twap_price = sg.band[1]
        sg.emit({"type": TWAP_START, "plan_id": t1, "side": BUY,
                 "total_quantity": 6, "slice_count": 3, "order_type": LIMIT,
                 "benchmark_price": central, "price": twap_price,
                 "account_id": acct("tw")}, label="twap_start")
        first_slice = sg.emit({"type": TWAP_SLICE, "plan_id": t1},
                              label="twap_slice_1")
        sg.emit({"type": TWAP_REPORT, "plan_id": t1})
        sg.emit({"type": TWAP_SLICE, "plan_id": t1})
        completing = sg.emit({"type": TWAP_SLICE, "plan_id": t1},
                             label="twap_completes")
        assert completing["execution_plan"]["status"] == "COMPLETED"

        # 6b) LIMIT TWAP breached after an intraday narrowing, then cancelled;
        #     a second cancel names the now-closed plan.
        add_sells(2, acct("liq2"))
        t2 = sg.new_plan_id()
        plan_price = sg.band[1]
        sg.emit({"type": TWAP_START, "plan_id": t2, "side": BUY,
                 "total_quantity": 6, "slice_count": 3, "order_type": LIMIT,
                 "benchmark_price": central, "price": plan_price,
                 "account_id": acct("tc")}, label="twap2_start")
        sg.emit({"type": TWAP_SLICE, "plan_id": t2})
        lower = sg.band[0]
        narrowed_upper = max(lower, plan_price - 1)
        sg.limit_update(lower, narrowed_upper, label="band_narrowed")
        sg.emit({"type": TWAP_SLICE, "plan_id": t2},
                label="twap_slice_breach")
        sg.limit_update(lower, plan_price, label="band_restored")
        sg.emit({"type": TWAP_CANCEL, "plan_id": t2}, label="twap_cancel")
        sg.emit({"type": TWAP_CANCEL, "plan_id": t2},
                label="twap_cancel_closed")

        # 6c) Completing market VWAP.
        add_sells(2, acct("liq3"))
        v1 = sg.new_plan_id()
        sg.emit({"type": VWAP_START, "plan_id": v1, "side": BUY,
                 "total_quantity": 5, "volume_weights": [3, 2],
                 "order_type": MARKET, "benchmark_price": central,
                 "account_id": acct("vw")}, label="vwap_start")
        sg.emit({"type": VWAP_SLICE, "plan_id": v1})
        vw_done = sg.emit({"type": VWAP_SLICE, "plan_id": v1},
                          label="vwap_completes")
        assert vw_done["execution_plan"]["status"] == "COMPLETED"
        sg.emit({"type": VWAP_REPORT, "plan_id": v1})

        # 6d) Market POV: positive release, zero release, report, cancel.
        add_sells(4, acct("liq4"))
        p1 = sg.new_plan_id()
        sg.emit({"type": POV_START, "plan_id": p1, "side": BUY,
                 "total_quantity": 10, "participation_bps": 5000,
                 "order_type": MARKET, "benchmark_price": central,
                 "account_id": acct("pov")}, label="pov_start")
        positive = sg.emit({"type": POV_VOLUME, "plan_id": p1,
                            "market_volume_increment": 6},
                           label="pov_positive")
        assert positive["trades"], "fresh makers must give the POV child a fill"
        assert positive["execution_plan"]["release_number"] == 1
        zero = sg.emit({"type": POV_VOLUME, "plan_id": p1,
                        "market_volume_increment": 1}, label="pov_zero")
        assert zero["trades"] == []
        assert zero["execution_plan"]["child_order_id"] is None
        sg.emit({"type": POV_REPORT, "plan_id": p1})
        sg.emit({"type": POV_CANCEL, "plan_id": p1}, label="pov_cancel")

        # 6e) Plan lifecycle business errors and derived-id protection.
        sg.emit({"type": TWAP_SLICE, "plan_id": f"{name}.ghostP"},
                label="plan_unknown")
        sg.emit({"type": VWAP_SLICE, "plan_id": p1}, label="slice_on_pov")
        sg.emit({"type": POV_VOLUME, "plan_id": v1,
                 "market_volume_increment": 4}, label="volume_on_vwap")
        sg.emit({"type": TWAP_START, "plan_id": t1, "side": BUY,
                 "total_quantity": 3, "slice_count": 1, "order_type": MARKET,
                 "benchmark_price": central}, label="duplicate_plan")
        # T2#3 stayed reserved when T2 was cancelled. An external event
        # spending that derived id as its *event id* rejects with the
        # replay-layer DUPLICATE_EVENT_ID, before matching or the price-limit
        # check; a different order id proves the id clash is what rejects it.
        reserved_child = f"{t2}#3"
        sg.emit({"event_id": reserved_child, "type": "ADD",
                 "order_id": f"{name}.clash", "side": BUY,
                 "order_type": LIMIT, "quantity": 1,
                 "price": sg.band_price(central, -2, -1)},
                label="reserved_clash")

        # 7) A final resting buyer keeps every closing book non-empty. -------
        final_qty = rng.rint(1, 3)
        result, _oid = external_add(
            BUY, LIMIT, final_qty,
            price=sg.band_price(central, -3, -2),
            account=acct("end"), label="final_buy")
        assert result["result"] == "RESTING"

        # 8) Non-committed / ordering failure block.
        self._failure_block(sg)

    def _failure_block(self, sg: _SymbolBuilder) -> None:
        """One of every non-committed failure, closing on a legal event.

        None of the failures advances the symbol sequence or occupies an id;
        the legal event that follows proves it.
        """
        name, central = sg.symbol, sg.central
        reused_id = self.event_id()
        reused_seq = sg.cur()
        invalid = sg.emit({"type": "ADD", "order_id": f"{name}.badq",
                           "side": BUY, "order_type": LIMIT,
                           "quantity": "x", "price": central},
                          event_id=reused_id, sequence=reused_seq,
                          advance=False, label="invalid_event")
        assert invalid["rejection_code"] == INVALID_EVENT
        sg.emit({"type": "ADD", "order_id": f"{name}.reused", "side": BUY,
                 "order_type": LIMIT, "quantity": 1,
                 "price": sg.band_price(central, -2, -1)},
                event_id=reused_id, sequence=reused_seq,
                label="invalid_recovered")

        # A numeric event id is rejected before any id can even be echoed;
        # remember its result object directly.
        numeric = sg.emit({"type": "ADD", "order_id": f"{name}.badeid",
                           "side": BUY, "order_type": LIMIT,
                           "quantity": 1, "price": central},
                          event_id=12345, sequence=sg.cur(), advance=False)
        self.anchor_results["invalid_envelope"] = numeric
        assert numeric["event_id"] is None
        assert numeric["rejection_code"] == INVALID_EVENT

        expected = sg.cur()
        gap = sg.emit({"type": IMPACT_REPORT, "side": BUY, "quantity": 1,
                       "benchmark_price": central},
                      event_id=self.event_id(), sequence=expected + 1,
                      advance=False, label="sequence_gap")
        assert gap["rejection_code"] == SEQUENCE_GAP
        stale = sg.emit({"type": IMPACT_REPORT, "side": BUY, "quantity": 1,
                         "benchmark_price": central},
                        event_id=self.event_id(), sequence=1, advance=False,
                        label="out_of_order")
        assert stale["rejection_code"] == OUT_OF_ORDER

        # Verbatim redelivery of this symbol's first event (stale sequence and
        # all): DUPLICATE and never traded again.
        first_event = sg.items[0]
        duplicate = self.session.submit([copy.deepcopy(first_event)])[0]
        sg.items.append(copy.deepcopy(first_event))
        self.anchor_results["first_event_duplicate"] = duplicate
        assert duplicate["status"] == DUPLICATE
        assert duplicate["trades"] == []

        # Same identifier, different normalized content: a conflict.
        conflict_event = {"event_id": first_event["event_id"], "symbol": name,
                          "sequence": sg.cur(), "type": "CANCEL",
                          "order_id": f"{name}.zz"}
        conflict = self.session.submit([copy.deepcopy(conflict_event)])[0]
        sg.items.append(conflict_event)
        self.anchor_results["first_event_conflict"] = conflict
        assert conflict["rejection_code"] == EVENT_ID_CONFLICT

        # The legal expected sequence commits last.
        sg.add(BUY, LIMIT, 1, sg.band_price(central, -3, -2),
               account=f"{name}.aftererr", label="progress_after_failures")

    # -- global read-only report tail ---------------------------------------

    def _report_tail(self) -> None:
        for name in SYMBOLS:
            sg = self.symbols[name]
            central = sg.central
            known_order = next(iter(sg.orders))
            twap_pid = next(
                pid for pid, meta in sg.plans.items()
                if meta.get("algo") == "TWAP" and meta.get("status") != "CANCELLED"
            )
            phase1_last = sg.seq

            def tail_emit(payload):
                return sg.emit(payload)

            tail_emit({"type": EXECUTION_REPORT, "order_id": known_order,
                       "benchmark_price": central})
            tail_emit({"type": EXECUTION_REPORT,
                       "order_id": f"{name}.ghost",
                       "benchmark_price": central})
            tail_emit({"type": IMPACT_REPORT, "side": BUY, "quantity": 2,
                       "benchmark_price": central})
            tail_emit({"type": BOOK_RECONSTRUCTION_REPORT,
                       "target_sequence": 0})
            tail_emit({"type": BOOK_RECONSTRUCTION_REPORT,
                       "target_sequence": 1})
            tail_emit({"type": BOOK_RECONSTRUCTION_REPORT,
                       "target_sequence": phase1_last})
            # One beyond the sequence currently committed: rejected.
            tail_emit({"type": BOOK_RECONSTRUCTION_REPORT,
                       "target_sequence": sg.seq + 1})
            # Current-book depth summaries: a single touch, the whole visible
            # depth and a depth beyond the number of resting levels.
            liquidity_one = tail_emit({"type": BOOK_LIQUIDITY_REPORT,
                                       "depth": 1})
            assert liquidity_one["result"] == "REPORTED"
            assert liquidity_one["liquidity_analysis"]["depth"] == 1
            liquidity_all = tail_emit({"type": BOOK_LIQUIDITY_REPORT,
                                       "depth": 20})
            assert liquidity_all["result"] == "REPORTED"
            assert len(liquidity_all["liquidity_analysis"]["bid_levels"]) <= 20
            assert len(liquidity_all["liquidity_analysis"]["ask_levels"]) <= 20
            tail_emit({"type": PLAN_TCA_REPORT, "plan_id": twap_pid,
                       "mark_price": central})
            tail_emit({"type": PLAN_TCA_REPORT,
                       "plan_id": f"{name}.ghostP", "mark_price": central})

        sg0 = self.symbols[SYMBOLS[0]]

        def global_emit(payload):
            return sg0.emit(payload)

        joint_symbols = _account_symbols(self.session, JOINT_ACCOUNT)
        assert joint_symbols == set(SYMBOLS)
        marks = {sym: CENTRAL[sym] for sym in sorted(joint_symbols)}
        reconciled = global_emit({
            "type": PORTFOLIO_REPORT, "account_id": JOINT_ACCOUNT,
            "mark_prices": dict(marks),
        })
        assert reconciled["result"] == "REPORTED"
        missing = dict(marks)
        del missing[SYMBOLS[-1]]
        mismatch = global_emit({
            "type": PORTFOLIO_REPORT, "account_id": JOINT_ACCOUNT,
            "mark_prices": missing,
        })
        assert mismatch["rejection_code"] == "MARK_PRICE_MISMATCH"
        unknown_account = global_emit({
            "type": PORTFOLIO_REPORT, "account_id": "nobody.who",
            "mark_prices": {},
        })
        assert unknown_account["rejection_code"] == "UNKNOWN_ACCOUNT"

        scenarios = [
            {"name": "down",
             "prices": {sym: CENTRAL[sym] - 1 for sym in sorted(joint_symbols)}},
            {"name": "up",
             "prices": {sym: CENTRAL[sym] + 1 for sym in sorted(joint_symbols)}},
        ]
        global_emit({"type": PORTFOLIO_STRESS_REPORT,
                     "account_id": JOINT_ACCOUNT, "mark_prices": dict(marks),
                     "scenarios": scenarios})
        global_emit({"type": PORTFOLIO_STRESS_REPORT,
                     "account_id": "nobody.who", "mark_prices": {},
                     "scenarios": [{"name": "down", "prices": {}}]})

        expected_trades = list(_actual_session_trades(self.session).values())
        expected_accounts = list(_actual_session_accounts(self.session).values())
        perfect = global_emit({
            "type": SESSION_RECONCILIATION,
            "expected_trades": copy.deepcopy(expected_trades),
            "expected_accounts": copy.deepcopy(expected_accounts),
        })
        assert perfect["result"] == "RECONCILED"
        broken_trades = copy.deepcopy(expected_trades)
        broken_trades[0]["quantity"] += 1
        breaks = global_emit({
            "type": SESSION_RECONCILIATION,
            "expected_trades": broken_trades,
            "expected_accounts": copy.deepcopy(expected_accounts),
        })
        assert breaks["result"] == "BREAKS_FOUND"

    # -- assembly ------------------------------------------------------------

    def build(self) -> "BuiltStream":
        for name in SYMBOLS:
            self._trading_script(self.symbols[name])
        phase1: list[dict[str, object]] = []
        positions = {name: 0 for name in SYMBOLS}
        lengths = {name: len(self.symbols[name].items) for name in SYMBOLS}
        step = 0
        while any(positions[name] < lengths[name] for name in SYMBOLS):
            name = SYMBOLS[step % len(SYMBOLS)]
            if positions[name] < lengths[name]:
                phase1.append(self.symbols[name].items[positions[name]])
                positions[name] += 1
            step += 1
        self.phase1_events = phase1
        self._report_tail()
        # The tail builders appended to each symbol's items after the
        # phase-1 merge snapshot; split them off per symbol.
        phase1_counts = {name: 0 for name in SYMBOLS}
        for item in phase1:
            phase1_counts[item["symbol"]] += 1
        tail_events: list[dict[str, object]] = []
        for name in SYMBOLS:
            tail_events.extend(
                self.symbols[name].items[phase1_counts[name]:])
        self.events = phase1 + tail_events
        return BuiltStream(self, tail_events)


class BuiltStream:
    """The generated stream plus anchor metadata for the assertions."""

    def __init__(self, generator: StreamGenerator,
                 tail_events: list[dict[str, object]]) -> None:
        self.seed = generator.seed
        self.events = copy.deepcopy(generator.events)
        self.labels = dict(generator.labels)
        self.anchor_results = copy.deepcopy(generator.anchor_results)
        self.phase1_length = len(generator.phase1_events)
        self.tail_events = copy.deepcopy(tail_events)


_STREAM_CACHE: dict[int, BuiltStream] = {}


def generated_stream(seed: int) -> BuiltStream:
    if seed not in _STREAM_CACHE:
        _STREAM_CACHE[seed] = StreamGenerator(seed).build()
    return _STREAM_CACHE[seed]


# ---------------------------------------------------------------------------
# Observation helpers
# ---------------------------------------------------------------------------


def run_oneshot(stream: BuiltStream):
    out = replay_events(copy.deepcopy(stream.events), config=CONFIG)
    return out["results"], out["snapshot"]


def run_segmented(stream: BuiltStream, cuts):
    replayer = EventReplayer(copy.deepcopy(CONFIG))
    results = []
    start = 0
    for cut in list(cuts) + [len(stream.events)]:
        results.extend(replayer.submit(copy.deepcopy(stream.events[start:cut])))
        start = cut
    return results, replayer


def run_restored(stream: BuiltStream, boundary: int):
    prefix = replay_events(copy.deepcopy(stream.events[:boundary]),
                           config=CONFIG)
    replayer = restore_replayer(copy.deepcopy(prefix["snapshot"]),
                                copy.deepcopy(CONFIG))
    suffix_results = replayer.submit(copy.deepcopy(stream.events[boundary:]))
    return suffix_results, replayer


def labeled_event(stream, label):
    """The exact event occurrence a label names (ids can repeat later)."""
    symbol, _event_id, local_index = stream.labels[label]
    seen = -1
    for global_index, event in enumerate(stream.events):
        if event["symbol"] == symbol:
            seen += 1
            if seen == local_index:
                return global_index, event
    raise KeyError(label)


def labeled_result(stream, results, label):
    global_index, _event = labeled_event(stream, label)
    return results[global_index]


def trades_by_symbol(results):
    grouped = {name: [] for name in SYMBOLS}
    for result in results:
        for trade in result["trades"]:
            grouped[result["symbol"]].append((trade, result["event_id"]))
    return grouped


def attach_plan_summaries(events, results):
    """symbol -> plan_id -> last execution_plan summary (event order).

    Nested plain-string keys keep the structure canonical-JSON serializable.
    """
    summaries = {name: {} for name in SYMBOLS}
    for event, result in zip(events, results):
        plan = result.get("execution_plan")
        pid = event.get("plan_id")
        if plan is not None and pid is not None:
            summaries[event["symbol"]][pid] = plan
    return summaries


def reconstruction_probes(replayer, tag="probe"):
    """Identical final-state historical probes, one per security."""
    probes = []
    for index, name in enumerate(SYMBOLS):
        sequence = replayer._symbols[name].last_sequence + 1
        probes.append({
            "event_id": f"{tag}.{name}", "symbol": name,
            "sequence": sequence, "type": BOOK_RECONSTRUCTION_REPORT,
            "target_sequence": sequence - 1,
        })
    return replayer.submit(probes)


def boundaries_for(seed, total):
    if seed in (SEEDS[0], SEEDS[1]):
        return list(range(total + 1))
    chosen = {0, total}
    rng = LcgRng((seed ^ 0x9E3779B1) & 0xFFFFFFFF)
    while len(chosen) < 14:
        chosen.add(rng.rint(0, total))
    return sorted(chosen)


# ---------------------------------------------------------------------------
# Generator sanity and reproducibility
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_generator_is_purely_seed_driven(seed):
    first = generated_stream(seed).events
    second = StreamGenerator(seed).build().events
    assert canonical_json(second) == canonical_json(first)


@pytest.mark.parametrize("seed", SEEDS)
def test_generated_stream_hits_every_documented_outcome(seed):
    stream = generated_stream(seed)
    results, _snapshot = run_oneshot(stream)

    def labeled(label):
        return labeled_result(stream, results, label)

    for label in ("first", "iceberg", "market_partial", "fok_fill",
                  "cancel_ok", "replace_ok", "twap_start", "twap_slice_1",
                  "twap_completes", "vwap_start", "vwap_completes",
                  "pov_start", "pov_positive", "final_buy",
                  "invalid_recovered", "progress_after_failures"):
        result = labeled(label)
        assert result["status"] == ACCEPTED, (seed, label,
                                              result.get("rejection_code"))

    fok_fail = labeled("fok_fail")
    assert fok_fail["result"] == "UNFILLED_CANCELLED"
    assert fok_fail["trades"] == []
    assert labeled("self_trade")["result"] in (
        "SELF_TRADE_PREVENTED", "PARTIALLY_FILLED_SELF_TRADE_PREVENTED")

    assert labeled("cancel_unknown")["rejection_code"] == "UNKNOWN_ORDER"
    assert labeled("replace_unknown")["rejection_code"] == "UNKNOWN_ORDER"
    assert (labeled("replace_plain_display")["rejection_code"]
            == "INVALID_SCHEMA")
    assert labeled("duplicate_order")["rejection_code"] == DUPLICATE_ORDER_ID
    assert labeled("price_breach")["rejection_code"] == PRICE_LIMIT_EXCEEDED
    assert labeled("plan_unknown")["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"
    assert labeled("slice_on_pov")["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"
    assert labeled("volume_on_vwap")["rejection_code"] == "UNKNOWN_EXECUTION_PLAN"
    assert (labeled("duplicate_plan")["rejection_code"]
            == "DUPLICATE_EXECUTION_PLAN")
    assert (labeled("twap_cancel_closed")["rejection_code"]
            == "EXECUTION_PLAN_CLOSED")
    assert labeled("reserved_clash")["rejection_code"] == DUPLICATE_EVENT_ID
    assert (labeled("twap_slice_breach")["rejection_code"]
            == PRICE_LIMIT_EXCEEDED)
    assert labeled("band_narrowed")["result"] == "PRICE_LIMIT_UPDATED"
    assert labeled("band_restored")["result"] == "PRICE_LIMIT_UPDATED"

    assert labeled("invalid_event")["rejection_code"] == INVALID_EVENT
    assert labeled("sequence_gap")["rejection_code"] == SEQUENCE_GAP
    assert labeled("out_of_order")["rejection_code"] == OUT_OF_ORDER

    assert stream.anchor_results["invalid_envelope"]["rejection_code"] == INVALID_EVENT
    assert (stream.anchor_results["first_event_duplicate"]["status"]
            == DUPLICATE)
    assert (stream.anchor_results["first_event_conflict"]["rejection_code"]
            == EVENT_ID_CONFLICT)

    codes = [result.get("rejection_code") for result in results]
    outcomes = [result.get("result") for result in results]
    assert "TARGET_SEQUENCE_NOT_FOUND" in codes
    assert "UNKNOWN_ACCOUNT" in codes
    assert "MARK_PRICE_MISMATCH" in codes
    assert "RECONCILED" in outcomes
    assert "BREAKS_FOUND" in outcomes

    # The current-book liquidity queries in the tail were all accepted and
    # echoed their requested depth.
    liquidity_results = [
        result for event, result in zip(stream.events, results)
        if event.get("type") == BOOK_LIQUIDITY_REPORT
    ]
    assert liquidity_results
    for result in liquidity_results:
        assert result["status"] == ACCEPTED
        assert result["result"] == "REPORTED"
        assert result["trades"] == []
        assert result["book_changes"] == {"bids": [], "asks": []}
        assert result["liquidity_analysis"]["depth"] in (1, 20)

    # Every security genuinely trades.
    grouped = trades_by_symbol(results)
    for name in SYMBOLS:
        assert grouped[name], (seed, name)


# ---------------------------------------------------------------------------
# Three execution paths agree
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_oneshot_and_segmented_submit_agree_byte_for_byte(seed):
    stream = generated_stream(seed)
    one_results, one_snapshot = run_oneshot(stream)
    rng = LcgRng((seed ^ 0x9E3779B1) & 0xFFFFFFFF)
    cuts = sorted({rng.rint(1, len(stream.events) - 1) for _ in range(4)})
    segmented, replayer = run_segmented(stream, cuts)
    assert canonical_json(segmented) == canonical_json(one_results)
    one_session = EventReplayer(copy.deepcopy(CONFIG))
    one_session.submit(copy.deepcopy(stream.events))
    for name in SYMBOLS:
        assert canonical_json(replayer.book(name)) == canonical_json(
            one_session.book(name))
    # The segmented session's normalized final snapshot matches the one-shot
    # session's bytes exactly.
    assert canonical_json(export_snapshot(replayer)) == canonical_json(
        one_snapshot)


@pytest.mark.parametrize("seed", SEEDS)
def test_three_paths_agree_on_results_trades_plans_limits_and_snapshot(seed):
    stream = generated_stream(seed)
    one_results, one_snapshot = run_oneshot(stream)
    one_replayer = restore_replayer(copy.deepcopy(one_snapshot),
                                    copy.deepcopy(CONFIG))
    one_probes = reconstruction_probes(one_replayer)
    one_plans = attach_plan_summaries(stream.events, one_results)

    for boundary in boundaries_for(seed, len(stream.events)):
        suffix, restored = run_restored(stream, boundary)

        # Per-event suffix: status, rejection precedence, trades and ids,
        # queue diffs, bids/asks, plan summaries and report analyses.
        assert canonical_json(suffix) == canonical_json(
            one_results[boundary:]), (seed, boundary)

        # Normalized final snapshot and final books.
        assert canonical_json(export_snapshot(restored)) == canonical_json(
            one_snapshot), (seed, boundary)
        for name in SYMBOLS:
            assert canonical_json(restored.book(name)) == canonical_json(
                one_replayer.book(name)), (seed, boundary, name)

        # Active price-limit intervals read back identically through the
        # historical reconstruction query.
        assert canonical_json(reconstruction_probes(restored)) == canonical_json(
            one_probes), (seed, boundary)

    # The zero boundary covers the whole stream: full results and the last
    # summary of every plan on every path.
    whole_suffix, restored = run_restored(stream, 0)
    assert canonical_json(whole_suffix) == canonical_json(one_results)
    assert (canonical_json(attach_plan_summaries(stream.events, whole_suffix))
            == canonical_json(one_plans))
    assert canonical_json(export_snapshot(restored)) == canonical_json(
        one_snapshot)


@pytest.mark.parametrize("seed", SEEDS)
def test_trade_journal_matches_observed_trade_details(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    grouped = trades_by_symbol(results)
    for name in SYMBOLS:
        journal = restored._symbols[name].engine.trade_history()
        # Public result trades omit the internal engine event-id tag; order,
        # ids, prices, quantities and trade ids match occurrence by occurrence
        # (arrays are never sorted).
        assert [{k: v for k, v in row.items() if k != "event_id"}
                for row in journal] == [
            trade for trade, _event_id in grouped[name]
        ]
        # The journal tag names the accepting engine event: the external
        # ADD envelope id, or the synthesized child id for a released plan
        # slice (a child taker always trades under its derived id).
        external_add_ids = {
            event["event_id"]
            for event in stream.events
            if event.get("symbol") == name and event.get("type") == "ADD"
        }
        plan_ids = set(restored._symbols[name].plans)
        for row in journal:
            tag = row["event_id"]
            taker = row["taker_order_id"]
            if any(taker.startswith(f"{pid}#") for pid in plan_ids):
                assert tag == taker
            else:
                assert tag in external_add_ids
        assert [trade["trade_id"] for trade in journal] == list(
            range(1, len(journal) + 1))


# ---------------------------------------------------------------------------
# State-machine properties
# ---------------------------------------------------------------------------


NON_COMMITTED_CODES = frozenset({
    INVALID_EVENT, EVENT_ID_CONFLICT, SEQUENCE_GAP, OUT_OF_ORDER,
})


@pytest.mark.parametrize("seed", SEEDS)
def test_committed_sequences_are_1_to_n_per_symbol(seed):
    stream = generated_stream(seed)
    results, _ = run_oneshot(stream)
    for name in SYMBOLS:
        committed = [
            result for result in results
            if result["symbol"] == name
            and result["status"] != DUPLICATE
            and result.get("rejection_code") not in NON_COMMITTED_CODES
        ]
        assert [result["sequence"] for result in committed] == list(
            range(1, len(committed) + 1)), (seed, name)


@pytest.mark.parametrize("seed", SEEDS)
def test_trade_ids_are_per_symbol_consecutive_and_unique(seed):
    stream = generated_stream(seed)
    results, _ = run_oneshot(stream)
    for name in SYMBOLS:
        ids = [trade["trade_id"]
               for result in results if result["symbol"] == name
               for trade in result["trades"]]
        assert ids == list(range(1, len(ids) + 1)), (seed, name)
        assert len(ids) == len(set(ids)), seed


@pytest.mark.parametrize("seed", SEEDS)
def test_quantity_conservation_for_orders_and_plans(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))

    # External order ledger built purely from the public event/result stream:
    #   supplied  = every ADD quantity + every REPLACE quantity;
    #   removed   = the pre-replace remainder + IOC/FOK/STP/cancel leftovers;
    #   filled    = cumulative trade journal quantity (both roles);
    #   resting   = the live remainder in the final engine state.
    supplied: dict[tuple[str, str], int] = {}
    removed: dict[tuple[str, str], int] = {}
    remaining: dict[tuple[str, str], int] = {}

    for event, result in zip(stream.events, results):
        if result.get("status") != ACCEPTED:
            continue
        name = event["symbol"]
        etype = event.get("type")
        if etype == "ADD":
            key = (name, event["order_id"])
            supplied[key] = supplied.get(key, 0) + event["quantity"]
            remaining[key] = event["quantity"]
        elif etype == "REPLACE":
            key = (name, event["order_id"])
            removed[key] = removed.get(key, 0) + remaining.get(key, 0)
            supplied[key] = supplied.get(key, 0) + event["quantity"]
            remaining[key] = event["quantity"]
        elif etype == "CANCEL" and result.get("result") == "CANCELLED":
            key = (name, event["order_id"])
            removed[key] = removed.get(key, 0) + remaining.get(key, 0)
            remaining[key] = 0
        for trade in result.get("trades", []):
            for role in ("maker_order_id", "taker_order_id"):
                key = (name, trade[role])
                if key in remaining:
                    remaining[key] -= trade["quantity"]
        if etype == "ADD" and result.get("result") not in (
            "RESTING", "PARTIALLY_FILLED_RESTING", "FILLED",
        ):
            key = (name, event["order_id"])
            taker_qty = sum(
                trade["quantity"] for trade in result.get("trades", [])
                if trade["taker_order_id"] == event["order_id"])
            removed[key] = removed.get(key, 0) + event["quantity"] - taker_qty
            remaining[key] = 0

    journal_filled: dict[tuple[str, str], int] = {}
    final_remaining: dict[tuple[str, str], int] = {}
    for name in SYMBOLS:
        state = restored._symbols[name]
        for oid, record in state.engine.dump_state()["orders"].items():
            final_remaining[(name, oid)] = (
                record["remaining"] if record["status"] == "RESTING" else 0
            )
        for trade in state.engine.trade_history():
            for role in ("maker_order_id", "taker_order_id"):
                key = (name, trade[role])
                journal_filled[key] = (
                    journal_filled.get(key, 0) + trade["quantity"])

    # Released plan child orders join the same ledger; their unfilled
    # remainder is an immediate cancel (children are IOC and never rest).
    for name in SYMBOLS:
        for pid, plan in restored._symbols[name].plans.items():
            for index in range(plan.released):
                slice_qty = plan.slice_quantities[index]
                child = f"{pid}#{index + 1}"
                key = (name, child)
                supplied[key] = supplied.get(key, 0) + slice_qty
                fills = journal_filled.get(key, 0)
                assert fills <= slice_qty, (seed, child)
                removed[key] = removed.get(key, 0) + slice_qty - fills

    for key, total in supplied.items():
        fills = journal_filled.get(key, 0)
        resting = final_remaining.get(key, 0)
        gone = removed.get(key, 0)
        assert fills + resting + gone == total, (
            seed, key, {"supplied": total, "filled": fills,
                        "resting": resting, "removed": gone})

    # Per-plan decomposition: total = filled + released-slice leftovers +
    # cancelled unreleased quantity; nothing rests and no fill is hidden.
    for name in SYMBOLS:
        for pid, plan in restored._symbols[name].plans.items():
            leftovers = 0
            child_fills = 0
            for index in range(plan.released):
                child = f"{pid}#{index + 1}"
                fills = journal_filled.get((name, child), 0)
                leftovers += plan.slice_quantities[index] - fills
                child_fills += fills
            assert child_fills == plan.filled_quantity, (seed, pid)
            assert plan.filled_quantity <= plan.released_quantity
            assert (plan.filled_quantity + leftovers
                    + plan.cancelled_quantity == plan.total_quantity), (seed, pid)
            if plan.status == "CANCELLED":
                assert plan.cancelled_quantity == (
                    plan.total_quantity - plan.released_quantity)
            else:
                assert plan.cancelled_quantity == 0


@pytest.mark.parametrize("seed", SEEDS)
def test_iceberg_visible_slice_bounded_and_reserve_hidden(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    # The current peak of every iceberg order, read from the public event
    # stream: an accepted REPLACE carrying display_quantity supersedes the
    # original peak; this stream never creates one, but the map stays general.
    peaks: dict[tuple[str, str], int] = {}
    for event in stream.events:
        if event.get("type") == "ADD" and event.get("order_type") == ICEBERG:
            peaks[(event["symbol"], event["order_id"])] = event["display_quantity"]
        elif event.get("type") == "REPLACE" and "display_quantity" in event:
            peaks[(event["symbol"], event["order_id"])] = event["display_quantity"]

    for name in SYMBOLS:
        last_sequence = restored._symbols[name].last_sequence
        probe = restored.submit([{
            "event_id": f"iceprobe.{name}", "symbol": name,
            "sequence": last_sequence + 1,
            "type": BOOK_RECONSTRUCTION_REPORT,
            "target_sequence": last_sequence,
        }])[0]
        assert probe["status"] == ACCEPTED
        queues = (probe["book_reconstruction"]["bid_queues"]
                  + probe["book_reconstruction"]["ask_queues"])
        reconstructed_totals: dict[int, int] = {}
        for queue in queues:
            reconstructed_totals[queue["price"]] = (
                reconstructed_totals.get(queue["price"], 0)
                + queue["visible_quantity"])
            for order in queue["orders"]:
                if order["order_type"] == "ICEBERG":
                    # The public slice is at least one unit, never exceeds the
                    # peak or the total remainder, and the hidden reserve never
                    # enters the level aggregate.
                    peak = peaks[(name, order["order_id"])]
                    assert 1 <= order["visible_quantity"] <= min(
                        peak, order["remaining_quantity"]), (seed, name, order)
        bids, asks = restored.book(name)
        public_totals = {level["price"]: level["quantity"]
                         for level in bids + asks}
        assert public_totals == reconstructed_totals, (seed, name)


@pytest.mark.parametrize("seed", SEEDS)
def test_fok_is_atomic(seed):
    stream = generated_stream(seed)
    results, _ = run_oneshot(stream)
    for event, result in zip(stream.events, results):
        if event.get("type") != "ADD" or event.get("time_in_force") != FOK:
            continue
        if result["status"] != ACCEPTED:
            continue
        traded = sum(trade["quantity"] for trade in result["trades"])
        if traded:
            assert traded == event["quantity"]
            assert result["result"] == "FILLED"
        else:
            assert result["result"] in (
                "UNFILLED_CANCELLED", "SELF_TRADE_PREVENTED")
        assert result["result"] not in ("RESTING",
                                        "PARTIALLY_FILLED_RESTING")


@pytest.mark.parametrize("seed", SEEDS)
def test_duplicate_redelivery_returns_duplicate_and_never_trades(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    replayer = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    for label in ("first", "fok_fill", "price_breach", "twap_slice_1",
                  "cancel_unknown"):
        _index, original = labeled_event(stream, label)
        name = original["symbol"]
        before = canonical_json(replayer.book(name))
        repeated = replayer.submit([copy.deepcopy(original)])[0]
        assert repeated["status"] == DUPLICATE, (seed, label)
        assert repeated["trades"] == [], (seed, label)
        assert canonical_json(replayer.book(name)) == before


@pytest.mark.parametrize("seed", SEEDS)
def test_conflicts_and_ordering_errors_do_not_advance_state(seed):
    stream = generated_stream(seed)
    _results, snapshot = run_oneshot(stream)
    replayer = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    candidates = [
        labeled_event(stream, label)[1]
        for label in ("sequence_gap", "out_of_order", "invalid_event")
    ]
    for original in candidates:
        books_before = {name: canonical_json(replayer.book(name))
                        for name in SYMBOLS}
        result = replayer.submit([copy.deepcopy(original)])[0]
        assert result["status"] == REJECTED
        assert result["rejection_code"] in NON_COMMITTED_CODES
        assert result["trades"] == []
        for name in SYMBOLS:
            assert canonical_json(replayer.book(name)) == books_before[name]


# ---------------------------------------------------------------------------
# Read-only reports never perturb later trade ids, queues or plan releases
# ---------------------------------------------------------------------------


READ_ONLY_PROBES = (
    EXECUTION_REPORT,
    IMPACT_REPORT,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    SESSION_RECONCILIATION,
    PLAN_TCA_REPORT,
    BOOK_RECONSTRUCTION_REPORT,
    BOOK_LIQUIDITY_REPORT,
)


def probe_payload(kind, symbol, sequence, counter):
    central = CENTRAL[symbol]
    payload = {"event_id": f"inject{counter:05d}", "symbol": symbol,
               "sequence": sequence, "type": kind}
    if kind == EXECUTION_REPORT:
        payload.update(order_id=f"{symbol}.injected.ghost",
                       benchmark_price=central)
    elif kind == IMPACT_REPORT:
        payload.update(side=BUY, quantity=1, benchmark_price=central)
    elif kind == PORTFOLIO_REPORT:
        payload.update(account_id=f"{symbol}.injected.nobody",
                       mark_prices={})
    elif kind == PORTFOLIO_STRESS_REPORT:
        payload.update(account_id=f"{symbol}.injected.nobody",
                       mark_prices={},
                       scenarios=[{"name": "down", "prices": {}}])
    elif kind == SESSION_RECONCILIATION:
        payload.update(expected_trades=[], expected_accounts=[])
    elif kind == PLAN_TCA_REPORT:
        payload.update(plan_id=f"{symbol}.injected.ghost",
                       mark_price=central)
    elif kind == BOOK_LIQUIDITY_REPORT:
        payload.update(depth=3)
    else:
        payload.update(target_sequence=0)
    return payload


@pytest.mark.parametrize("seed", SEEDS)
def test_read_only_reports_do_not_perturb_later_outcomes(seed):
    stream = generated_stream(seed)
    baseline_results, baseline_snapshot = run_oneshot(stream)

    # Probes may only be inserted strictly before every symbol's failure
    # block (where gap/stale sequences live). All failure blocks sit in the
    # merged phase-1 tail, so restrict inserts to a region safely ahead of
    # the first one.
    first_failure, _failure_event = labeled_event(stream, "invalid_event")
    safe_limit = max(8, (stream.phase1_length - 1) // 2)
    assert safe_limit < first_failure

    rng = LcgRng((seed ^ 0xABCDEF) & 0xFFFFFFFF)
    positions = sorted({rng.rint(1, safe_limit) for _ in range(7)})
    first_event_id = {
        name: next(event["event_id"] for event in stream.events
                   if event.get("symbol") == name)
        for name in SYMBOLS
    }
    # Global index of each symbol's committed events in the uninterrupted
    # stream, so an absolute reconstruction target can be mapped across the
    # inserted query slots.
    committed_global_index = {name: [] for name in SYMBOLS}
    for index, result in enumerate(baseline_results):
        if (result["status"] != DUPLICATE
                and result.get("rejection_code") not in NON_COMMITTED_CODES):
            committed_global_index[result["symbol"]].append(index)

    def target_delta(symbol, original_index, target):
        """Inserted read-only slots strictly before reconstruction target.

        Target 0 names the empty pre-session book and never moves. A positive
        target maps to the committed business event at that slot; a future
        (rejected) target maps to every probe inserted before the query.
        """
        if target == 0:
            return 0
        indices = committed_global_index[symbol]
        if target <= len(indices):
            limit_index = indices[target - 1]
        else:
            limit_index = original_index
        return sum(
            1 for pos in positions
            if pos <= limit_index and stream.events[pos]["symbol"] == symbol
        )

    first_event_kind = {
        name: next(event["type"] for event in stream.events
                   if event.get("symbol") == name)
        for name in SYMBOLS
    }
    occurrences: dict[tuple[str, object], int] = {}
    instrumented: list[dict[str, object]] = []
    # Parallel metadata: ("probe", seq_shift, target_delta) or
    # ("real", seq_shift, target_delta). The stale-sequence probe and the
    # verbatim first-event redelivery keep their original envelopes, so their
    # shift is zero by construction.
    metadata: list[tuple[str, int, int]] = []
    shifts = {name: 0 for name in SYMBOLS}
    counter = 0
    for index, event in enumerate(stream.events):
        if index in positions:
            symbol = event["symbol"]
            counter += 1
            kind = READ_ONLY_PROBES[(counter - 1) % len(READ_ONLY_PROBES)]
            committed_here = sum(
                1 for previous in stream.events[:index]
                if previous["symbol"] == symbol
            )
            probe = probe_payload(kind, symbol,
                                  committed_here + shifts[symbol] + 1, counter)
            instrumented.append(probe)
            metadata.append(("probe", 0, 0))
            shifts[symbol] += 1

        symbol = event["symbol"]
        seen_key = (symbol, event.get("event_id"))
        occurrences[seen_key] = occurrences.get(seen_key, 0) + 1
        is_stale_envelope = (
            event.get("type") == IMPACT_REPORT and event.get("sequence") == 1
        )
        is_verbatim_redelivery = (
            event.get("type") == first_event_kind[symbol]
            and event.get("event_id") == first_event_id[symbol]
            and occurrences[seen_key] > 1
        )
        applied = 0 if (is_stale_envelope or is_verbatim_redelivery) else shifts[symbol]
        delta = 0
        shifted = copy.deepcopy(event)
        shifted["sequence"] = event["sequence"] + applied
        # A reconstruction target names an absolute sequence slot; the
        # inserted read-only queries occupy slots themselves. Translate the
        # target to the slot naming the same business point (target 0 is the
        # slot-independent empty book).
        if event.get("type") == BOOK_RECONSTRUCTION_REPORT:
            delta = target_delta(symbol, index, event.get("target_sequence", 0))
            shifted["target_sequence"] = event["target_sequence"] + delta
        instrumented.append(shifted)
        metadata.append(("real", applied, delta))

    session = EventReplayer(copy.deepcopy(CONFIG))
    all_results = session.submit(copy.deepcopy(instrumented))

    def is_injected(value):
        event_id = value.get("event_id")
        return isinstance(event_id, str) and event_id.startswith("inject")

    probe_results = [result for result, (kind, _shift, _delta)
                     in zip(all_results, metadata) if kind == "probe"]
    real_pairs = [
        (result, shift, delta)
        for result, (kind, shift, delta) in zip(all_results, metadata)
        if kind == "real"
    ]
    assert len(probe_results) == len(positions)
    for result in probe_results:
        assert result["trades"] == []
        assert result["book_changes"] == {"bids": [], "asks": []}

    # Normalize shifted per-symbol sequences, expected_sequence hints and the
    # absolute reconstruction target slots back to uninterrupted numbering;
    # stale and redelivered envelopes keep their original sequences (zero
    # shift). The analyses themselves must then match byte for byte.
    shifts_seen = {name: 0 for name in SYMBOLS}
    normalized = []
    for item, (kind, shift, delta), result in zip(
            instrumented, metadata, all_results):
        if kind == "probe":
            shifts_seen[item["symbol"]] += 1
            continue
        fixed = dict(result)
        fixed["sequence"] = result["sequence"] - shift
        if "expected_sequence" in fixed:
            fixed["expected_sequence"] -= shifts_seen[result["symbol"]]
        if (item.get("type") == BOOK_RECONSTRUCTION_REPORT and delta
                and "book_reconstruction" in fixed):
            reconstruction = dict(fixed["book_reconstruction"])
            reconstruction["target_sequence"] -= delta
            fixed["book_reconstruction"] = reconstruction
        normalized.append(fixed)
    assert canonical_json(normalized) == canonical_json(baseline_results), seed
    real_results = [result for result, _shift, _delta in real_pairs]

    # Trade ids / order untouched; queue priority and books unchanged.
    assert canonical_json(trades_by_symbol(real_results)) == canonical_json(
        trades_by_symbol(baseline_results))
    baseline_replayer = restore_replayer(copy.deepcopy(baseline_snapshot),
                                         copy.deepcopy(CONFIG))
    for name in SYMBOLS:
        assert canonical_json(session.book(name)) == canonical_json(
            baseline_replayer.book(name))
    assert canonical_json(attach_plan_summaries(stream.events, real_results)) == (
        canonical_json(attach_plan_summaries(stream.events, baseline_results)))

    # The probes themselves occupied ids/sequences: continuing the
    # instrumented session with a per-symbol next-sequence legal event works,
    # while a stale sequence is rejected — proving occupancy, not mutation.
    name = SYMBOLS[0]
    last = session._symbols[name].last_sequence
    continued = session.submit([{
        "event_id": "after.probes", "symbol": name, "sequence": last + 1,
        "type": IMPACT_REPORT, "side": BUY, "quantity": 1,
        "benchmark_price": CENTRAL[name],
    }])[0]
    assert continued["status"] == ACCEPTED


# ---------------------------------------------------------------------------
# Snapshot export / restoration and tampering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS[:3])
def test_prefix_snapshot_restore_is_byte_identical_at_interior_markers(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    for marker_index in (1, stream.phase1_length // 2,
                         stream.phase1_length - 1):
        marker_event = stream.events[marker_index]
        marker = {"symbol": marker_event["symbol"],
                  "sequence": marker_event["sequence"]}
        marked = replay_events(copy.deepcopy(stream.events[:marker_index + 1]),
                               config=CONFIG)
        continued = replay_events(
            copy.deepcopy(stream.events[marker_index + 1:]), config=CONFIG,
            snapshot=marked["snapshot"])
        assert canonical_json(continued["results"]) == canonical_json(
            results[marker_index + 1:])
        assert canonical_json(continued["snapshot"]) == canonical_json(snapshot)


@pytest.mark.parametrize("seed", SEEDS)
def test_restored_suffix_is_byte_identical_to_uninterrupted(seed):
    stream = generated_stream(seed)
    results, snapshot = run_oneshot(stream)
    # The property at a handful of boundaries per stream, including a boundary
    # inside the read-only report tail.
    total = len(stream.events)
    tail = stream.phase1_length
    for boundary in {0, 2, tail, tail + 3, total - 1, total}:
        suffix, _restored = run_restored(stream, boundary)
        assert canonical_json(suffix) == canonical_json(results[boundary:]), (
            seed, boundary)


def test_snapshot_digest_tampering_is_snapshot_corrupt():
    _results, snapshot = run_oneshot(generated_stream(SEEDS[0]))
    tampered = copy.deepcopy(snapshot)
    tampered["content_digest"] = "0" * 64
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert exc.value.code == SNAPSHOT_CORRUPT

    tampered = copy.deepcopy(snapshot)
    tampered["content"]["events"][0]["content"] = "{}"
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_snapshot_structural_tampering_re_signed_is_corrupt():
    _results, snapshot = run_oneshot(generated_stream(SEEDS[0]))

    def re_sign(document):
        document["content_digest"] = _digest(
            {key: value for key, value in document.items()
             if key != "content_digest"})
        return document

    tampered = re_sign(copy.deepcopy(snapshot))
    tampered["content"]["symbols"].append(
        copy.deepcopy(tampered["content"]["symbols"][0]))
    re_sign(tampered)
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert exc.value.code == SNAPSHOT_CORRUPT

    tampered = copy.deepcopy(snapshot)
    engine = tampered["content"]["symbols"][0]["state"]["engine"]
    engine["next_trade_id"] += 100
    re_sign(tampered)
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert exc.value.code == SNAPSHOT_CORRUPT


def test_snapshot_version_tampering_is_unsupported():
    _results, snapshot = run_oneshot(generated_stream(SEEDS[0]))
    tampered = copy.deepcopy(snapshot)
    tampered["format_version"] = "event-replay/9999-never"
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert exc.value.code == SNAPSHOT_VERSION_UNSUPPORTED


def test_snapshot_config_mismatch_is_config_mismatch():
    _results, snapshot = run_oneshot(generated_stream(SEEDS[0]))
    mismatched = copy.deepcopy(CONFIG)
    mismatched["price_limits"] = {"AAA": {"lower": 1, "upper": 1000000}}
    with pytest.raises(SnapshotError) as exc:
        restore_replayer(copy.deepcopy(snapshot), mismatched)
    assert exc.value.code == CONFIG_MISMATCH


def test_replace_display_quantity_onto_plain_target_keeps_snapshot_consistent():
    """Regression: the engine's pre-commit INVALID_SCHEMA rejection still
    occupies the replay event id/sequence, and its snapshot must restore.

    The baseline engine rejects a REPLACE carrying display_quantity onto a
    plain resting target *before* spending its own journal event id; the
    replay layer occupies that journal id on the shared commit path so the
    per-symbol engine journal and the replay event log describe exactly the
    same events. Without that occupancy every later snapshot export fails
    verification as SNAPSHOT_CORRUPT.
    """
    events = [
        {"event_id": "e1", "symbol": "S", "sequence": 1, "type": "ADD",
         "order_id": "o1", "side": "SELL", "order_type": "LIMIT",
         "quantity": 5, "price": 100},
        {"event_id": "e2", "symbol": "S", "sequence": 2, "type": "REPLACE",
         "order_id": "o1", "quantity": 5, "price": 100,
         "display_quantity": 2},
        {"event_id": "e3", "symbol": "S", "sequence": 3, "type": "ADD",
         "order_id": "o2", "side": "BUY", "order_type": "LIMIT",
         "quantity": 2, "price": 100},
    ]
    for boundary in range(4):
        prefix = replay_events(copy.deepcopy(events[:boundary]), config=None)
        continued = replay_events(copy.deepcopy(events[boundary:]),
                                  config=None, snapshot=prefix["snapshot"])
        one_shot = replay_events(copy.deepcopy(events), config=None)
        assert canonical_json(continued["results"]) == canonical_json(
            one_shot["results"][boundary:]), boundary
        assert canonical_json(continued["snapshot"]) == canonical_json(
            one_shot["snapshot"]), boundary
    # Explicit anchors on the rejection itself.
    out = replay_events(copy.deepcopy(events), config=None)
    assert out["results"][1]["rejection_code"] == "INVALID_SCHEMA"
    restore_replayer(copy.deepcopy(out["snapshot"]), None)


# ---------------------------------------------------------------------------
# Prefix reproducibility and hash-seed independence of the wire output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS[:4])
def test_every_prefix_reproduces_identically(seed):
    stream = generated_stream(seed)
    total = len(stream.events)
    for boundary in {0, 1, 5, total // 3, 2 * total // 3, total - 1, total}:
        first = replay_events(copy.deepcopy(stream.events[:boundary]),
                              config=CONFIG)
        second = replay_events(copy.deepcopy(stream.events[:boundary]),
                               config=CONFIG)
        assert canonical_json(first["results"]) == canonical_json(
            second["results"])
        assert canonical_json(first["snapshot"]) == canonical_json(
            second["snapshot"])


def test_generated_stream_cli_output_is_hash_seed_independent():
    stream = generated_stream(SEEDS[0])
    document = canonical_json({"events": stream.events, "config": CONFIG})
    outputs = set()
    for hash_seed in (0, 1, 7, 12345):
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = str(hash_seed)
        code = ("from order_book_engine.cli import main; "
                "import sys; sys.exit(main(['events']))")
        completed = subprocess.run(
            [sys.executable, "-c", code],
            input=document + b"\n",
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr.decode("utf-8")
        assert completed.stderr == b""
        outputs.add(completed.stdout)
    assert len(outputs) == 1
