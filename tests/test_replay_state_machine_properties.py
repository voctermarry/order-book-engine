"""Deterministic property tests for the full multi-security replay state machine.

This suite pins the *composed* conservation, idempotency and recovery
semantics of the public replay surface — it adds no event type, field or
product behaviour; it only drives the documented public interface
(:func:`replay_events`, :class:`EventReplayer`, :func:`export_snapshot`,
:func:`restore_replayer`, :func:`canonical_json`) against reproducible
generated event streams.

Stream generation
-----------------
Every stream is a pure function of its pinned seed: a ``random.Random(seed)``
drives a generator that interleaves two configured securities (``AAA`` with a
static price-limit band, ``BBB`` without), a dedicated self-trade security
(``CCC``) and a late-registered security (``ZZZ``). The stream mixes

* plain LIMIT (GTC/IOC/FOK), MARKET (default/IOC/FOK) and ICEBERG adds,
  cancels and replaces, some carrying ``account_id``;
* TWAP/VWAP/POV parent-order commands across their whole lifecycle
  (start, slice/volume release — including a deterministic zero release —
  cancel, report, closed-plan and duplicate-plan rejections);
* intraday ``PRICE_LIMIT_UPDATE`` adjustments and out-of-band limit
  submissions rejected with ``PRICE_LIMIT_EXCEEDED``;
* every read-only report (``EXECUTION_REPORT``, ``IMPACT_REPORT``,
  ``PORTFOLIO_REPORT``, ``PORTFOLIO_STRESS_REPORT``,
  ``SESSION_RECONCILIATION``, ``PLAN_TCA_REPORT``,
  ``BOOK_RECONSTRUCTION_REPORT`` and the plan reports);
* injected faults with their published outcomes: structurally invalid events
  (``INVALID_EVENT``), sequence holes (``SEQUENCE_GAP``), stale sequences
  (``OUT_OF_ORDER``), verbatim redeliveries (``DUPLICATE``), event-id
  conflicts (``EVENT_ID_CONFLICT``), unknown orders/plans and guaranteed
  self-trade prevention pairs.

The generator tracks the exact per-symbol sequence, the active price-limit
band, plan progress and account registrations, so a stream always continues
to commit after every injected fault. Each event carries a parallel
annotation recording the published outcome the harness must observe.

Reproduction: any failure names its seed and event prefix; the identical
stream is rebuilt with ``_Generator(seed).generate()``.

Properties checked
------------------
* Three execution paths — one-shot :func:`replay_events`, segmented
  :class:`EventReplayer` submits and snapshot export + ``restore_replayer``
  continuation — agree byte for byte at *every* committable boundary:
  per-event results, per-security final books, trade detail, plan summaries,
  active price limits and the final canonical snapshot.
* Inserting read-only queries into the stream changes nothing but the
  occupied ids/sequences: stripped of the envelope sequence fields, every
  kept event's result (trade ids, queue priority, plan releases) is
  identical and the final books match.
* Per-security ``trade_id`` runs 1, 2, 3, ... with no gaps; per-order and
  per-plan traded quantities stay within their budgets; plan summaries
  conserve released/filled/notional and agree with their own vwap/slippage
  identities; iceberg visible slices never exceed the peak and hidden
  reserves never enter the book aggregates (checked through
  ``BOOK_RECONSTRUCTION_REPORT`` at the current sequence); failed FOK orders
  are atomic; structural errors consume neither id nor sequence while
  committed business rejections consume exactly their own; duplicates never
  re-trade and conflicts/ordering errors never advance state.
* A snapshot exported at any prefix restores a session whose suffix output
  is byte-identical to the uninterrupted run; tampering with the digest,
  structure or version yields ``SNAPSHOT_CORRUPT`` /
  ``SNAPSHOT_VERSION_UNSUPPORTED`` and a mismatched caller configuration
  yields ``CONFIG_MISMATCH``.
"""

from __future__ import annotations

import copy
import random

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_RECONSTRUCTION_REPORT,
    CONFIG_MISMATCH,
    DUPLICATE,
    DUPLICATE_EXECUTION_PLAN,
    EVENT_ID_CONFLICT,
    EXECUTION_PLAN_CLOSED,
    EXECUTION_REPORT,
    EventReplayer,
    IMPACT_REPORT,
    INVALID_EVENT,
    MARK_PRICE_MISMATCH,
    OUT_OF_ORDER,
    PLAN_TCA_REPORT,
    PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT,
    POV_CANCEL,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    SNAPSHOT_CORRUPT,
    SNAPSHOT_VERSION_UNSUPPORTED,
    SnapshotError,
    TARGET_SEQUENCE_NOT_FOUND,
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ACCOUNT,
    UNKNOWN_EXECUTION_PLAN,
    UNKNOWN_ORDER,
    VWAP_CANCEL,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine.event_replay.validation import _allocate_slices


# ---------------------------------------------------------------------------
# Configuration, seeds and shared vocabulary
# ---------------------------------------------------------------------------

CONFIG = {"price_limits": {"AAA": {"lower": 90, "upper": 120}}}

#: Pinned seeds; a failure is reproduced by regenerating the stream from its
#: seed (``_Generator(seed).generate()``) and replaying the named prefix.
SEEDS = (20241011, 20241012, 20241013)

STEPS = 70

ENVELOPE_CODES = frozenset(
    {INVALID_EVENT, SEQUENCE_GAP, OUT_OF_ORDER, EVENT_ID_CONFLICT}
)

FAULT_KINDS = frozenset({"invalid", "gap", "out_of_order", "duplicate", "conflict"})

PLAN_COMMAND_TYPES = frozenset(
    {TWAP_START, TWAP_SLICE, TWAP_CANCEL, TWAP_REPORT,
     VWAP_START, VWAP_SLICE, VWAP_CANCEL, VWAP_REPORT,
     POV_START, POV_VOLUME, POV_CANCEL, POV_REPORT}
)

#: Read-only event types: they occupy an event id and a sequence but never
#: move any trading state, so the insertion test drops and renumbers them.
READ_ONLY_TYPES = frozenset(
    {EXECUTION_REPORT, IMPACT_REPORT, PORTFOLIO_REPORT, PORTFOLIO_STRESS_REPORT,
     SESSION_RECONCILIATION, PLAN_TCA_REPORT, BOOK_RECONSTRUCTION_REPORT,
     TWAP_REPORT, VWAP_REPORT, POV_REPORT}
)

ALL_EVENT_TYPES = PLAN_COMMAND_TYPES | frozenset(
    {"ADD", "CANCEL", "REPLACE", EXECUTION_REPORT, IMPACT_REPORT,
     PORTFOLIO_REPORT, PORTFOLIO_STRESS_REPORT, SESSION_RECONCILIATION,
     PLAN_TCA_REPORT, BOOK_RECONSTRUCTION_REPORT, PRICE_LIMIT_UPDATE}
)

ANALYSIS_KEY_BY_TYPE = {
    EXECUTION_REPORT: "execution_analysis",
    IMPACT_REPORT: "impact_analysis",
    PORTFOLIO_REPORT: "portfolio_analysis",
    PORTFOLIO_STRESS_REPORT: "portfolio_stress_analysis",
    SESSION_RECONCILIATION: "reconciliation",
    PLAN_TCA_REPORT: "plan_tca_analysis",
    BOOK_RECONSTRUCTION_REPORT: "book_reconstruction",
}


# ---------------------------------------------------------------------------
# The seeded stream generator
# ---------------------------------------------------------------------------


class _Generator:
    """Builds one deterministic annotated multi-security event stream.

    ``events`` is the replay input; ``notes`` runs parallel to it and records
    the published outcome each event must produce. The generator models the
    per-symbol sequence, the active price-limit band, plan progress and
    account registrations exactly, so every non-fault event commits and every
    injected fault is followed by a stream that keeps committing.
    """

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.events: list[object] = []
        self.notes: list[dict] = []
        self.next_seq: dict[str, int] = {}
        self.bands: dict[str, tuple[int, int]] = {"AAA": (90, 120)}
        self.orders: dict[tuple[str, str], dict] = {}
        self.order_ids: dict[str, list[str]] = {}
        self.plans: dict[tuple[str, str], dict] = {}
        self.plan_ids: dict[str, list[str]] = {}
        self.accounts: dict[str, set[str]] = {}
        self.budget: dict[tuple[str, str], int] = {}
        self.committed: list[int] = []
        self._eid = 0
        self._oid = 0
        self._pid = 0
        self._self_pairs = 0
        self._prev_self_maker: str | None = None
        self._prev_self_price: int | None = None

    # -- emission primitives ------------------------------------------------

    def _new_eid(self) -> str:
        self._eid += 1
        return f"e{self._eid:05d}"

    def _seq(self, symbol: str) -> int:
        return self.next_seq.get(symbol, 1)

    def _envelope(self, symbol: str, **payload) -> dict:
        event = {
            "event_id": self._new_eid(),
            "symbol": symbol,
            "sequence": self._seq(symbol),
        }
        event.update(payload)
        return event

    def _emit(self, event, note, commit_symbol: str | None = None) -> None:
        self.events.append(event)
        self.notes.append(note)
        if commit_symbol is not None:
            self.next_seq[commit_symbol] = self._seq(commit_symbol) + 1
            self.committed.append(len(self.events) - 1)

    def symbols_seen(self) -> set[str]:
        return {
            event["symbol"]
            for event in self.events
            if isinstance(event, dict)
        }

    # -- baseline order events ----------------------------------------------

    def add_order(self, symbol, *, otype=None, side=None, price=None, qty=None,
                  display=None, tif=None, account="random"):
        rng = self.rng
        self._oid += 1
        order_id = f"o{self._oid}"
        otype = otype or rng.choices(
            ["LIMIT", "MARKET", "ICEBERG"], weights=[6, 2, 2])[0]
        side = side or rng.choice(["BUY", "SELL"])
        qty = qty if qty is not None else rng.randint(1, 10)
        if account == "random":
            account = rng.choice([None, None, "acct-a", "acct-b", "acct-c"])
        event = self._envelope(
            symbol, type="ADD", order_id=order_id, side=side,
            order_type=otype, quantity=qty)
        note = {"kind": "normal", "type": "ADD"}
        band = self.bands.get(symbol)
        in_band = True
        if otype == "LIMIT":
            price = price if price is not None else rng.randint(80, 130)
            event["price"] = price
            tif = tif or rng.choices(["GTC", "IOC", "FOK"], weights=[5, 2, 2])[0]
            if tif != "GTC":
                event["time_in_force"] = tif
                note["no_rest"] = True
            if tif == "FOK":
                note["fok"] = True
            in_band = band is None or band[0] <= price <= band[1]
        elif otype == "MARKET":
            tif = tif or rng.choices([None, "IOC", "FOK"], weights=[3, 2, 2])[0]
            if tif is not None:
                event["time_in_force"] = tif
            note["no_rest"] = True
            if tif == "FOK":
                note["fok"] = True
        else:  # ICEBERG
            price = price if price is not None else rng.randint(80, 130)
            display = display if display is not None else rng.randint(1, qty)
            event["price"] = price
            event["display_quantity"] = display
            in_band = band is None or band[0] <= price <= band[1]
        if account is not None:
            event["account_id"] = account
        self.orders[(symbol, order_id)] = {
            "iceberg": otype == "ICEBERG",
            "display": display if otype == "ICEBERG" else None,
        }
        self.budget[(symbol, order_id)] = qty
        self.order_ids.setdefault(symbol, []).append(order_id)
        if not in_band:
            # A fresh id with a valid schema can only fail on the active band.
            note["expect_code"] = PRICE_LIMIT_EXCEEDED
        elif account is not None:
            self.accounts.setdefault(account, set()).add(symbol)
        self._emit(event, note, commit_symbol=symbol)
        return order_id

    def cancel_order(self, symbol, *, order_id=None):
        rng = self.rng
        note = {"kind": "normal", "type": "CANCEL"}
        if order_id is None:
            known = self.order_ids.get(symbol, [])
            if known and rng.random() < 0.85:
                order_id = rng.choice(known)
            else:
                order_id = "no-such-order"
        if order_id == "no-such-order":
            note["expect_code"] = UNKNOWN_ORDER
        event = self._envelope(symbol, type="CANCEL", order_id=order_id)
        self._emit(event, note, commit_symbol=symbol)

    def replace_order(self, symbol, *, order_id=None):
        rng = self.rng
        if order_id is None:
            known = self.order_ids.get(symbol, [])
            if known and rng.random() < 0.85:
                order_id = rng.choice(known)
            else:
                order_id = "no-such-order"
        qty = rng.randint(1, 10)
        price = rng.randint(80, 130)
        event = self._envelope(
            symbol, type="REPLACE", order_id=order_id, quantity=qty, price=price)
        note = {"kind": "normal", "type": "REPLACE"}
        if order_id == "no-such-order":
            note["expect_code"] = UNKNOWN_ORDER
        else:
            # An upper bound is enough for the conservation check: a failed
            # replace only inflates the budget, a successful one really adds
            # the new remaining quantity on top of what already filled.
            key = (symbol, order_id)
            self.budget[key] = self.budget.get(key, 0) + qty
        self._emit(event, note, commit_symbol=symbol)

    # -- parent-order plan commands ------------------------------------------

    def start_plan(self, symbol, algo, **over):
        rng = self.rng
        self._pid += 1
        plan_id = f"p{self._pid}"
        side = over.get("side", rng.choice(["BUY", "SELL"]))
        order_type = over.get("order_type", rng.choice(["LIMIT", "MARKET"]))
        benchmark = over.get("benchmark", rng.randint(80, 120))
        account = over.get("account", rng.choice([None, "acct-a", "acct-b"]))
        weights = None
        bps = None
        if algo == "TWAP":
            total = over.get("total", rng.randint(2, 12))
            slices = over.get("slices", rng.randint(1, total))
            payload = {
                "type": TWAP_START, "plan_id": plan_id, "side": side,
                "total_quantity": total, "slice_count": slices,
                "order_type": order_type, "benchmark_price": benchmark,
            }
            slice_total = slices
        elif algo == "VWAP":
            total = over.get("total", rng.randint(3, 12))
            weights = over.get(
                "weights",
                [rng.randint(1, 5) for _ in range(rng.randint(1, min(4, total)))])
            payload = {
                "type": VWAP_START, "plan_id": plan_id, "side": side,
                "total_quantity": total, "volume_weights": weights,
                "order_type": order_type, "benchmark_price": benchmark,
            }
            slice_total = len(weights)
        else:
            total = over.get("total", rng.randint(5, 30))
            bps = over.get("bps", rng.randint(500, 5000))
            payload = {
                "type": POV_START, "plan_id": plan_id, "side": side,
                "total_quantity": total, "participation_bps": bps,
                "order_type": order_type, "benchmark_price": benchmark,
            }
            slice_total = None
        price = None
        if order_type == "LIMIT":
            price = over.get("price", rng.randint(80, 130))
            payload["price"] = price
        if account is not None:
            payload["account_id"] = account
        event = self._envelope(symbol, **payload)
        band = self.bands.get(symbol)
        in_band = order_type == "MARKET" or band is None or band[0] <= price <= band[1]
        if not in_band:
            # A fresh plan id can only fail on the active band; no plan is
            # created and no derived id is reserved.
            note = {"kind": "normal", "type": payload["type"],
                    "expect_code": PRICE_LIMIT_EXCEEDED}
            self._emit(event, note, commit_symbol=symbol)
            return None
        note = {"kind": "normal", "type": payload["type"],
                "book_static": True, "plan_id": plan_id}
        self.plans[(symbol, plan_id)] = {
            "algo": algo, "status": "ACTIVE", "total": total,
            "slices": slice_total, "released_count": 0, "released_qty": 0,
            "market_volume": 0, "bps": bps, "price": price,
            "order_type": order_type, "benchmark": benchmark, "side": side,
            "releases": [],
        }
        self.plan_ids.setdefault(symbol, []).append(plan_id)
        if algo == "TWAP":
            base, extra = divmod(total, slices)
            for k in range(1, slices + 1):
                self.budget[(symbol, f"{plan_id}#{k}")] = base + (1 if k <= extra else 0)
        elif algo == "VWAP":
            for k, slice_qty in enumerate(_allocate_slices(total, weights), start=1):
                self.budget[(symbol, f"{plan_id}#{k}")] = slice_qty
        if account is not None:
            self.accounts.setdefault(account, set()).add(symbol)
        self._emit(event, note, commit_symbol=symbol)
        return plan_id

    def _plan_band_breach(self, symbol, plan) -> bool:
        if plan["order_type"] != "LIMIT":
            return False
        band = self.bands.get(symbol)
        return band is not None and not (band[0] <= plan["price"] <= band[1])

    def slice_plan(self, symbol, *, plan_id=None, type=None):
        rng = self.rng
        if plan_id is None:
            candidates = [
                pid for pid in self.plan_ids.get(symbol, [])
                if self.plans[(symbol, pid)]["algo"] in ("TWAP", "VWAP")
            ]
            if candidates and rng.random() < 0.9:
                plan_id = rng.choice(candidates)
            else:
                plan_id = "no-such-plan"
        type = type or rng.choice([TWAP_SLICE, VWAP_SLICE])
        event = self._envelope(symbol, type=type, plan_id=plan_id)
        plan = self.plans.get((symbol, plan_id))
        if plan is None:
            note = {"kind": "normal", "type": type,
                    "expect_code": UNKNOWN_EXECUTION_PLAN}
        else:
            note = {"kind": "normal", "type": type, "plan_id": plan_id}
            if plan["status"] != "ACTIVE":
                note["expect_code"] = EXECUTION_PLAN_CLOSED
            elif self._plan_band_breach(symbol, plan):
                note["expect_code"] = PRICE_LIMIT_EXCEEDED
            else:
                plan["released_count"] += 1
                if plan["released_count"] == plan["slices"]:
                    plan["status"] = "COMPLETED"
        self._emit(event, note, commit_symbol=symbol)

    def pov_volume(self, symbol, *, plan_id=None, increment=None):
        rng = self.rng
        if plan_id is None:
            candidates = [
                pid for pid in self.plan_ids.get(symbol, [])
                if self.plans[(symbol, pid)]["algo"] == "POV"
            ]
            if candidates and rng.random() < 0.9:
                plan_id = rng.choice(candidates)
            else:
                plan_id = "no-such-plan"
        increment = increment if increment is not None else rng.randint(1, 20)
        event = self._envelope(
            symbol, type=POV_VOLUME, plan_id=plan_id,
            market_volume_increment=increment)
        plan = self.plans.get((symbol, plan_id))
        if plan is None:
            note = {"kind": "normal", "type": POV_VOLUME,
                    "expect_code": UNKNOWN_EXECUTION_PLAN}
        else:
            note = {"kind": "normal", "type": POV_VOLUME, "plan_id": plan_id}
            if plan["status"] != "ACTIVE":
                note["expect_code"] = EXECUTION_PLAN_CLOSED
            elif self._plan_band_breach(symbol, plan):
                # The pre-release band check fires before any accumulation.
                note["expect_code"] = PRICE_LIMIT_EXCEEDED
            else:
                plan["market_volume"] += increment
                target = min(
                    plan["total"],
                    plan["market_volume"] * plan["bps"] // 10000)
                release = target - plan["released_qty"]
                if release > 0:
                    plan["released_count"] += 1
                    plan["released_qty"] = target
                    plan["releases"].append(release)
                    child = f"{plan_id}#{plan['released_count']}"
                    self.budget[(symbol, child)] = release
                    if plan["released_qty"] >= plan["total"]:
                        plan["status"] = "COMPLETED"
        self._emit(event, note, commit_symbol=symbol)

    def cancel_plan(self, symbol, *, plan_id=None, type=None):
        rng = self.rng
        if plan_id is None:
            candidates = list(self.plan_ids.get(symbol, []))
            if candidates and rng.random() < 0.9:
                plan_id = rng.choice(candidates)
            else:
                plan_id = "no-such-plan"
        type = type or rng.choice([TWAP_CANCEL, VWAP_CANCEL, POV_CANCEL])
        event = self._envelope(symbol, type=type, plan_id=plan_id)
        plan = self.plans.get((symbol, plan_id))
        if plan is None:
            note = {"kind": "normal", "type": type,
                    "expect_code": UNKNOWN_EXECUTION_PLAN}
        else:
            note = {"kind": "normal", "type": type,
                    "book_static": True, "plan_id": plan_id}
            if plan["status"] != "ACTIVE":
                note["expect_code"] = EXECUTION_PLAN_CLOSED
            else:
                plan["status"] = "CANCELLED"
        self._emit(event, note, commit_symbol=symbol)

    def report_plan(self, symbol, *, plan_id=None, type=None):
        rng = self.rng
        if plan_id is None:
            candidates = list(self.plan_ids.get(symbol, []))
            if candidates and rng.random() < 0.9:
                plan_id = rng.choice(candidates)
            else:
                plan_id = "no-such-plan"
        type = type or rng.choice([TWAP_REPORT, VWAP_REPORT, POV_REPORT])
        event = self._envelope(symbol, type=type, plan_id=plan_id)
        note = {"kind": "report", "type": type, "book_static": True}
        if (symbol, plan_id) in self.plans:
            note["plan_id"] = plan_id
        else:
            note["expect_code"] = UNKNOWN_EXECUTION_PLAN
        self._emit(event, note, commit_symbol=symbol)

    # -- price-limit adjustment ----------------------------------------------

    def price_limit_update(self, symbol, *, lower=None, upper=None):
        rng = self.rng
        lower = lower if lower is not None else rng.randint(60, 100)
        upper = upper if upper is not None else lower + rng.randint(0, 40)
        self.bands[symbol] = (lower, upper)
        event = self._envelope(
            symbol, type=PRICE_LIMIT_UPDATE,
            lower_price=lower, upper_price=upper)
        note = {"kind": "normal", "type": PRICE_LIMIT_UPDATE,
                "book_static": True, "band": (lower, upper)}
        self._emit(event, note, commit_symbol=symbol)

    # -- read-only reports ----------------------------------------------------

    def execution_report(self, symbol):
        rng = self.rng
        pool = list(self.order_ids.get(symbol, []))
        for plan_id in self.plan_ids.get(symbol, []):
            plan = self.plans[(symbol, plan_id)]
            for k in range(1, plan["released_count"] + 1):
                pool.append(f"{plan_id}#{k}")
        pool.append("no-such-order")
        event = self._envelope(
            symbol, type=EXECUTION_REPORT, order_id=rng.choice(pool),
            benchmark_price=rng.randint(80, 120))
        self._emit(event, {"kind": "report", "type": EXECUTION_REPORT,
                           "book_static": True}, commit_symbol=symbol)

    def impact_report(self, symbol):
        rng = self.rng
        event = self._envelope(
            symbol, type=IMPACT_REPORT, side=rng.choice(["BUY", "SELL"]),
            quantity=rng.randint(1, 15),
            benchmark_price=rng.randint(80, 120))
        self._emit(event, {"kind": "report", "type": IMPACT_REPORT,
                           "book_static": True}, commit_symbol=symbol)

    def portfolio_report(self, *, variant=None):
        rng = self.rng
        symbol = rng.choice(["AAA", "BBB"])
        if variant is None:
            variant = rng.choices(
                ["exact", "mismatch", "unknown"], weights=[5, 2, 1])[0]
        note = {"kind": "report", "type": PORTFOLIO_REPORT, "book_static": True}
        if variant == "unknown" or not self.accounts:
            account = "acct-never-seen"
            marks = {"AAA": 100}
            note["expect_code"] = UNKNOWN_ACCOUNT
        else:
            account = rng.choice(sorted(self.accounts))
            symbols = sorted(self.accounts[account])
            marks = {s: rng.randint(80, 120) for s in symbols}
            if variant == "mismatch":
                if len(symbols) > 1 and rng.random() < 0.5:
                    del marks[rng.choice(symbols)]
                else:
                    marks["ZZZ9"] = 100
                note["expect_code"] = MARK_PRICE_MISMATCH
        event = self._envelope(
            symbol, type=PORTFOLIO_REPORT, account_id=account, mark_prices=marks)
        self._emit(event, note, commit_symbol=symbol)

    def portfolio_stress(self, *, variant=None):
        rng = self.rng
        symbol = rng.choice(["AAA", "BBB"])
        if variant is None:
            variant = rng.choices(
                ["exact", "mismatch", "unknown"], weights=[5, 2, 1])[0]
        note = {"kind": "report", "type": PORTFOLIO_STRESS_REPORT,
                "book_static": True}
        if variant == "unknown" or not self.accounts:
            account = "acct-never-seen"
            symbols = ["AAA"]
            note["expect_code"] = UNKNOWN_ACCOUNT
        else:
            account = rng.choice(sorted(self.accounts))
            symbols = sorted(self.accounts[account])
        marks = {s: rng.randint(80, 120) for s in symbols}
        if variant == "mismatch":
            marks["ZZZ9"] = 100
            note["expect_code"] = MARK_PRICE_MISMATCH
        scenarios = [{
            "name": name,
            "prices": {s: rng.randint(60, 140) for s in symbols},
        } for name in ("up", "down")[: rng.randint(1, 2)]]
        event = self._envelope(
            symbol, type=PORTFOLIO_STRESS_REPORT, account_id=account,
            mark_prices=marks, scenarios=scenarios)
        self._emit(event, note, commit_symbol=symbol)

    def session_reconciliation(self):
        rng = self.rng
        symbol = rng.choice(["AAA", "BBB"])
        if rng.random() < 0.5:
            expected_trades, expected_accounts = [], []
        else:
            # Fabricated ids never match real orders, so this is BREAKS_FOUND;
            # either accepted outcome is a valid read-only probe.
            expected_trades = [{
                "symbol": "AAA", "trade_id": 1, "maker_order_id": "xm",
                "taker_order_id": "xt", "price": 1, "quantity": 1,
            }]
            expected_accounts = [{
                "symbol": "AAA", "account_id": "xa",
                "net_position": 1, "cash_balance": -1,
            }]
        event = self._envelope(
            symbol, type=SESSION_RECONCILIATION,
            expected_trades=expected_trades, expected_accounts=expected_accounts)
        self._emit(event, {"kind": "report", "type": SESSION_RECONCILIATION,
                           "book_static": True}, commit_symbol=symbol)

    def plan_tca(self, symbol, *, plan_id=None):
        rng = self.rng
        if plan_id is None:
            candidates = list(self.plan_ids.get(symbol, []))
            plan_id = rng.choice(candidates) if candidates else "no-such-plan"
        event = self._envelope(
            symbol, type=PLAN_TCA_REPORT, plan_id=plan_id,
            mark_price=rng.randint(80, 120))
        note = {"kind": "report", "type": PLAN_TCA_REPORT, "book_static": True}
        if plan_id == "no-such-plan":
            note["expect_code"] = UNKNOWN_EXECUTION_PLAN
        self._emit(event, note, commit_symbol=symbol)

    def book_reconstruction(self, symbol, *, target=None):
        rng = self.rng
        last = self._seq(symbol) - 1
        if target is None:
            roll = rng.random()
            if roll < 0.4 or last == 0:
                target = last
            elif roll < 0.8:
                target = rng.randint(0, last)
            else:
                target = last + rng.randint(1, 3)
        elif target == "current":
            target = last
        elif target == "future":
            target = last + rng.randint(1, 3)
        event = self._envelope(
            symbol, type=BOOK_RECONSTRUCTION_REPORT, target_sequence=target)
        note = {"kind": "report", "type": BOOK_RECONSTRUCTION_REPORT,
                "book_static": True,
                "recon": {"target": target, "at_current": target == last},
                "band": self.bands.get(symbol)}
        if target > last:
            note["expect_code"] = TARGET_SEQUENCE_NOT_FOUND
        self._emit(event, note, commit_symbol=symbol)

    # -- self-trade prevention pairs ------------------------------------------

    def self_pair(self):
        """A resting sell immediately met by a huge same-account market buy.

        ``CCC`` only ever carries these pairs: the sell cannot cross anything
        (no bids ever rest there) and the buy's quantity dwarfs the whole
        book, so prevention is guaranteed to trigger at the pair's own maker.
        Each pair's buy first sweeps the previous pair's still-resting sell
        (a different account), so the first pair prevents purely and every
        later pair prevents after exactly one external trade.
        """
        symbol = "CCC"
        self._self_pairs += 1
        account = f"self-{self._self_pairs}"
        price = 900 + 10 * self._self_pairs
        maker = self.add_order(
            symbol, otype="LIMIT", side="SELL", price=price, qty=5,
            tif="GTC", account=account)
        self._oid += 1
        taker = f"o{self._oid}"
        event = self._envelope(
            symbol, type="ADD", order_id=taker, side="BUY",
            order_type="MARKET", quantity=100000, account_id=account)
        self.orders[(symbol, taker)] = {"iceberg": False, "display": None}
        self.budget[(symbol, taker)] = 100000
        self.order_ids.setdefault(symbol, []).append(taker)
        note = {"kind": "self_trade", "type": "ADD",
                "maker": maker, "taker": taker,
                "prev_maker": self._prev_self_maker,
                "prev_price": self._prev_self_price}
        self._prev_self_maker = maker
        self._prev_self_price = price
        self._emit(event, note, commit_symbol=symbol)

    # -- fault injection -------------------------------------------------------

    def inject_duplicate(self):
        if not self.committed:
            return
        target = self.rng.choice(self.committed)
        event = copy.deepcopy(self.events[target])
        self._emit(event, {"kind": "duplicate", "target": target})

    def inject_conflict(self):
        if not self.committed:
            return
        target = self.rng.choice(self.committed)
        original = self.events[target]
        symbol = original["symbol"]
        # Same id, same symbol, different normalized content.
        event = {
            "event_id": original["event_id"],
            "symbol": symbol,
            "sequence": self._seq(symbol),
            "type": "CANCEL",
            "order_id": f"conflict-{self._new_eid()}",
        }
        self._emit(event, {"kind": "conflict", "target": target})

    def inject_gap(self, symbol=None):
        symbol = symbol or self.rng.choice(["AAA", "BBB"])
        expected = self._seq(symbol)
        offset = self.rng.choice([1, 2, 5])
        event = {
            "event_id": self._new_eid(), "symbol": symbol,
            "sequence": expected + offset,
            "type": "CANCEL", "order_id": "no-such-order",
        }
        self._emit(event, {"kind": "gap", "expected": expected,
                           "offset": offset})

    def inject_out_of_order(self, symbol=None):
        eligible = [s for s in ("AAA", "BBB", "CCC", "ZZZ") if self._seq(s) >= 2]
        if not eligible:
            return
        symbol = symbol or self.rng.choice(eligible)
        expected = self._seq(symbol)
        stale = self.rng.randint(1, expected - 1)
        event = {
            "event_id": self._new_eid(), "symbol": symbol,
            "sequence": stale,
            "type": "CANCEL", "order_id": "no-such-order",
        }
        self._emit(event, {"kind": "out_of_order", "expected": expected,
                           "orig_seq": stale})

    def inject_invalid(self, symbol, *, variant=None):
        rng = self.rng
        if variant is None:
            variant = rng.randrange(9)
        note = {"kind": "invalid"}
        if variant == 8:
            self._emit("junk-not-an-event", note)
            return
        self._oid += 1
        order_id = f"ox{self._oid}"
        base = self._envelope(
            symbol, type="ADD", order_id=order_id, side="BUY",
            order_type="LIMIT", quantity=3, price=100)
        if variant == 0:
            del base["price"]                      # LIMIT without a price
        elif variant == 1:
            base["quantity"] = "3"                 # wrong field type
        elif variant == 2:
            base["order_type"] = "MARKET"
            base["time_in_force"] = "GTC"          # MARKET may not be GTC
            del base["price"]
        elif variant == 3:
            base["order_type"] = "ICEBERG"
            base["display_quantity"] = 2
            base["time_in_force"] = "IOC"          # ICEBERG is GTC-only
        elif variant == 4:
            base["bogus_field"] = 1                # unknown field
        elif variant == 5:
            base["type"] = "BOGUS"                 # unknown event type
        elif variant == 6:
            base["sequence"] = "1"                 # envelope type error
        else:
            base = {
                "event_id": self._new_eid(), "symbol": symbol,
                "sequence": self._seq(symbol), "type": "TWAP_START",
                "plan_id": f"pb{self._pid}", "side": "BUY",
                "total_quantity": 2, "slice_count": 5,  # slices > total
                "order_type": "MARKET", "benchmark_price": 100,
            }
        self._emit(base, note)

    def inject_dup_plan(self):
        if not self.plans:
            return
        symbol, plan_id = self.rng.choice(sorted(self.plans))
        event = self._envelope(
            symbol, type=TWAP_START, plan_id=plan_id, side="BUY",
            total_quantity=4, slice_count=2, order_type="MARKET",
            benchmark_price=100)
        note = {"kind": "normal", "type": TWAP_START, "book_static": True,
                "expect_code": DUPLICATE_EXECUTION_PLAN}
        self._emit(event, note, commit_symbol=symbol)

    # -- the full stream -------------------------------------------------------

    def generate(self, steps: int = STEPS):
        # Anchors: both configured symbols known, one account each.
        self.add_order("AAA", otype="LIMIT", side="SELL", price=100, qty=5,
                       tif="GTC", account="acct-a")
        self.add_order("BBB", otype="LIMIT", side="SELL", price=70, qty=4,
                       tif="GTC", account="acct-b")
        table = [
            (lambda: self.add_order("AAA"), 16),
            (lambda: self.add_order("BBB"), 14),
            (lambda: self.cancel_order("AAA"), 5),
            (lambda: self.cancel_order("BBB"), 4),
            (lambda: self.replace_order("AAA"), 4),
            (lambda: self.replace_order("BBB"), 3),
            (lambda: self.start_plan("AAA", "TWAP"), 3),
            (lambda: self.start_plan("BBB", "TWAP"), 3),
            (lambda: self.start_plan("BBB", "VWAP"), 3),
            (lambda: self.start_plan("BBB", "POV"), 3),
            (lambda: self.slice_plan("AAA"), 4),
            (lambda: self.slice_plan("BBB"), 5),
            (lambda: self.pov_volume("BBB"), 4),
            (lambda: self.cancel_plan("BBB"), 2),
            (lambda: self.report_plan("AAA"), 2),
            (lambda: self.report_plan("BBB"), 2),
            (lambda: self.price_limit_update("AAA"), 2),
            (lambda: self.execution_report("AAA"), 2),
            (lambda: self.execution_report("BBB"), 2),
            (lambda: self.impact_report("AAA"), 2),
            (lambda: self.portfolio_report(), 2),
            (lambda: self.portfolio_stress(), 1),
            (lambda: self.session_reconciliation(), 1),
            (lambda: self.plan_tca("AAA"), 1),
            (lambda: self.plan_tca("BBB"), 1),
            (lambda: self.book_reconstruction("AAA"), 2),
            (lambda: self.book_reconstruction("BBB"), 1),
            (lambda: self.inject_invalid("AAA"), 2),
            (lambda: self.inject_invalid("BBB"), 1),
            (lambda: self.inject_gap(), 2),
            (lambda: self.inject_out_of_order(), 2),
            (lambda: self.inject_duplicate(), 2),
            (lambda: self.inject_conflict(), 2),
            (lambda: self.self_pair(), 2),
        ]
        actions = [action for action, _ in table]
        weights = [weight for _, weight in table]
        for _ in range(steps):
            self.rng.choices(actions, weights=weights)[0]()
        self._tail()
        return self

    def _tail(self):
        """Deterministic tail: every event type and published outcome, once."""
        # The whole plan lifecycle, including closed-plan rejections.
        twap = self.start_plan("BBB", "TWAP", total=4, slices=2,
                               order_type="LIMIT", price=75, account="acct-tail")
        self.slice_plan("BBB", plan_id=twap, type=TWAP_SLICE)
        self.report_plan("BBB", plan_id=twap, type=TWAP_REPORT)
        self.slice_plan("BBB", plan_id=twap, type=TWAP_SLICE)   # COMPLETED
        self.slice_plan("BBB", plan_id=twap, type=TWAP_SLICE)   # CLOSED
        self.cancel_plan("BBB", plan_id=twap, type=TWAP_CANCEL)  # CLOSED
        vwap = self.start_plan("AAA", "VWAP", total=6, weights=[2, 1, 1],
                               order_type="MARKET", account="acct-tail")
        self.slice_plan("AAA", plan_id=vwap, type=VWAP_SLICE)
        self.report_plan("AAA", plan_id=vwap, type=VWAP_REPORT)
        self.cancel_plan("AAA", plan_id=vwap, type=VWAP_CANCEL)
        self.slice_plan("AAA", plan_id=vwap, type=VWAP_SLICE)   # CLOSED
        pov = self.start_plan("BBB", "POV", total=10, bps=5000,
                              order_type="LIMIT", price=75)
        self.pov_volume("BBB", plan_id=pov, increment=12)       # release 6
        self.pov_volume("BBB", plan_id=pov, increment=1)        # zero release
        self.pov_volume("BBB", plan_id=pov, increment=7)        # release 4, COMPLETED
        self.pov_volume("BBB", plan_id=pov, increment=5)        # CLOSED
        self.report_plan("BBB", plan_id=pov, type=POV_REPORT)
        pov2 = self.start_plan("BBB", "POV", total=8, bps=2500,
                               order_type="MARKET")
        self.cancel_plan("BBB", plan_id=pov2, type=POV_CANCEL)
        self.pov_volume("BBB", plan_id=pov2, increment=3)       # CLOSED
        self.report_plan("BBB", plan_id=pov2, type=POV_REPORT)
        self.inject_dup_plan()
        self.slice_plan("BBB", plan_id="no-such-plan")
        self.cancel_order("AAA", order_id="no-such-order")
        self.replace_order("AAA")
        # Active-band replacement and an out-of-band limit submission.
        self.price_limit_update("AAA", lower=85, upper=115)
        self.add_order("AAA", otype="LIMIT", side="SELL", price=200, qty=1)
        self.add_order("AAA", otype="LIMIT", side="SELL", price=100, qty=2)
        # A guaranteed-unfilled FOK: the visible plus hidden liquidity at
        # 75 or better is nowhere near this size.
        self.add_order("BBB", otype="LIMIT", side="BUY", price=75,
                       qty=100000, tif="FOK")
        # Every read-only report kind.
        self.execution_report("AAA")
        self.impact_report("AAA")
        self.portfolio_report(variant="exact")
        self.portfolio_report(variant="mismatch")
        self.portfolio_report(variant="unknown")
        self.portfolio_stress(variant="exact")
        self.session_reconciliation()
        self.plan_tca("AAA", plan_id=vwap)
        self.plan_tca("AAA", plan_id="no-such-plan")
        self.book_reconstruction("AAA", target="current")
        self.book_reconstruction("BBB", target="current")
        self.book_reconstruction("AAA", target="future")
        # Guaranteed self-trade prevention, twice (pure then partial).
        self.self_pair()
        self.self_pair()
        # Faults, including an invalid event on a brand-new symbol that must
        # not register it (the next ZZZ event commits at sequence 1).
        self.inject_invalid("AAA", variant=1)
        self.inject_invalid("ZZZ", variant=0)
        self.add_order("ZZZ", otype="LIMIT", side="SELL", price=50, qty=3)
        self.inject_invalid("AAA", variant=8)
        self.inject_gap("AAA")
        self.inject_out_of_order("AAA")
        self.inject_duplicate()
        self.inject_conflict()
        # Resting liquidity (one iceberg with a hidden reserve) at the end.
        self.add_order("BBB", otype="ICEBERG", side="SELL", price=80, qty=9,
                       display=3)
        self.add_order("AAA", otype="LIMIT", side="BUY", price=95, qty=4)


# ---------------------------------------------------------------------------
# The invariant harness
# ---------------------------------------------------------------------------


def _levels(book):
    return {level["price"]: level["quantity"] for level in book}


def _assert_book_shape(bids, asks, ctx):
    for book, descending in ((bids, True), (asks, False)):
        prices = [level["price"] for level in book]
        assert prices == sorted(prices, reverse=descending), ctx
        assert len(set(prices)) == len(prices), ctx
        for level in book:
            assert set(level) == {"price", "quantity"}, ctx
            assert isinstance(level["price"], int) and level["price"] > 0, ctx
            assert isinstance(level["quantity"], int) and level["quantity"] > 0, ctx


def _apply_changes(previous, changes, ctx):
    levels = dict(previous)
    for change in changes:
        assert set(change) == {"price", "quantity"}, ctx
        price, quantity = change["price"], change["quantity"]
        assert isinstance(quantity, int) and quantity >= 0, ctx
        # A change entry always differs from the previous level; a zero
        # quantity only ever drains a level that was live.
        assert previous.get(price, 0) != quantity, ctx
        if quantity == 0:
            assert price in previous, ctx
            del levels[price]
        else:
            levels[price] = quantity
    return levels


def assert_stream_invariants(gen: _Generator, results: list[dict]) -> None:
    """Check every published conservation and occupancy law on one result list."""
    events, notes = gen.events, gen.notes
    assert len(results) == len(events) == len(notes)
    prev_book: dict[str, tuple] = {}
    known_symbols: set[str] = set()
    next_trade_id: dict[str, int] = {}
    plan_fills: dict[tuple[str, str], dict] = {}
    order_fills: dict[tuple[str, str], int] = {}

    for index, (event, note, result) in enumerate(zip(events, notes, results)):
        ctx = f"event {index} note={note!r} result={result!r}"
        kind = note["kind"]
        symbol = event.get("symbol") if isinstance(event, dict) else None

        assert result["status"] in (ACCEPTED, REJECTED, DUPLICATE), ctx
        for field in ("event_id", "symbol", "sequence", "status",
                      "trades", "book_changes", "bids", "asks"):
            assert field in result, ctx
        if kind != "invalid" and isinstance(event, dict):
            assert result["event_id"] == event["event_id"], ctx
            assert result["symbol"] == event["symbol"], ctx
            assert result["sequence"] == event["sequence"], ctx
        _assert_book_shape(result["bids"], result["asks"], ctx)
        previous = prev_book.get(symbol, ([], []))
        previous_bids = _levels(previous[0])
        previous_asks = _levels(previous[1])

        # -- the published outcome for this annotation ----------------------
        if kind == "invalid":
            assert result["status"] == REJECTED, ctx
            assert result["rejection_code"] == INVALID_EVENT, ctx
        elif kind == "gap":
            assert result["status"] == REJECTED, ctx
            assert result["rejection_code"] == SEQUENCE_GAP, ctx
            assert result["expected_sequence"] == note["expected"], ctx
        elif kind == "out_of_order":
            assert result["status"] == REJECTED, ctx
            assert result["rejection_code"] == OUT_OF_ORDER, ctx
            assert result["expected_sequence"] == note["expected"], ctx
        elif kind == "duplicate":
            assert result["status"] == DUPLICATE, ctx
            assert "result" not in result and "rejection_code" not in result, ctx
        elif kind == "conflict":
            assert result["status"] == REJECTED, ctx
            assert result["rejection_code"] == EVENT_ID_CONFLICT, ctx
        elif kind == "self_trade":
            assert result["status"] == ACCEPTED, ctx
            if note["prev_maker"] is None:
                # The book is empty ahead of the pair's own maker: pure
                # prevention with no prior trade.
                assert result["result"] == "SELF_TRADE_PREVENTED", ctx
                assert result["trades"] == [], ctx
            else:
                # The buy first sweeps the previous pair's resting sell (a
                # different account), then prevention cancels the remainder
                # at its own maker.
                assert result["result"] == (
                    "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"), ctx
                assert result["trades"] == [{
                    "trade_id": result["trades"][0]["trade_id"],
                    "maker_order_id": note["prev_maker"],
                    "taker_order_id": note["taker"],
                    "price": note["prev_price"],
                    "quantity": 5,
                }], ctx
            # The prevented maker never trades in this event.
            assert all(
                trade["maker_order_id"] != note["maker"]
                for trade in result["trades"]
            ), ctx
        else:
            # A committing event: never an envelope fault, never a duplicate.
            assert result["status"] in (ACCEPTED, REJECTED), ctx
            if result["status"] == REJECTED:
                assert result["rejection_code"] not in ENVELOPE_CODES, ctx
            if "expect_code" in note:
                assert result["status"] == REJECTED, ctx
                assert result["rejection_code"] == note["expect_code"], ctx

        # -- occupancy: faults move nothing ---------------------------------
        if kind in FAULT_KINDS:
            assert result["trades"] == [], ctx
            assert result["book_changes"] == {"bids": [], "asks": []}, ctx
            if symbol in known_symbols:
                assert (result["bids"], result["asks"]) == previous, ctx
            else:
                # A pre-dispatch failure against an unknown security echoes
                # the empty book and must not register it.
                assert result["bids"] == [] and result["asks"] == [], ctx
        elif result["status"] == REJECTED:
            # A committed business rejection: no trades, no book movement.
            assert result["trades"] == [], ctx
            assert result["book_changes"] == {"bids": [], "asks": []}, ctx
            assert (result["bids"], result["asks"]) == previous, ctx
        elif result["status"] == ACCEPTED:
            # The book diff applied to the previous book yields the new book.
            assert _apply_changes(
                previous_bids, result["book_changes"]["bids"], ctx
            ) == _levels(result["bids"]), ctx
            assert _apply_changes(
                previous_asks, result["book_changes"]["asks"], ctx
            ) == _levels(result["asks"]), ctx
            if note.get("book_static"):
                assert result["trades"] == [], ctx
                assert (result["bids"], result["asks"]) == previous, ctx
            if note.get("no_rest") and not result["trades"]:
                # IOC/FOK/MARKET orders never rest: no trades means no change.
                assert (result["bids"], result["asks"]) == previous, ctx
            if note.get("fok") and result.get("result") == "UNFILLED_CANCELLED":
                # FOK atomicity: no trades, no book change, no trade id spent.
                assert (result["bids"], result["asks"]) == previous, ctx

        # -- per-security trade ids run 1, 2, 3, ... --------------------------
        for trade in result["trades"]:
            expected = next_trade_id.get(symbol, 1)
            assert trade["trade_id"] == expected, ctx
            next_trade_id[symbol] = expected + 1
            assert trade["quantity"] > 0 and trade["price"] > 0, ctx
            for role in ("maker_order_id", "taker_order_id"):
                key = (symbol, trade[role])
                order_fills[key] = order_fills.get(key, 0) + trade["quantity"]

        # -- field exclusivity of the optional analysis objects --------------
        for event_type, key in ANALYSIS_KEY_BY_TYPE.items():
            if key in result:
                assert note.get("type") == event_type, ctx
                assert result["status"] == ACCEPTED, ctx
        if "execution_plan" in result:
            assert note.get("type") in PLAN_COMMAND_TYPES, ctx
        if "active_price_limits" in result:
            assert note.get("type") == PRICE_LIMIT_UPDATE, ctx
            assert result["status"] == ACCEPTED, ctx
        if "self_trade_prevention" in result:
            assert result.get("result") in (
                "SELF_TRADE_PREVENTED", "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"), ctx

        # -- plan summary conservation ----------------------------------------
        plan_summary = result.get("execution_plan")
        if plan_summary is not None:
            plan_id = note.get("plan_id")
            assert plan_id is not None, ctx
            key = (symbol, plan_id)
            acc = plan_fills.setdefault(key, {"filled": 0, "notional": 0})
            for trade in result["trades"]:
                acc["filled"] += trade["quantity"]
                acc["notional"] += trade["price"] * trade["quantity"]
            assert plan_summary["filled_quantity"] == acc["filled"], ctx
            assert plan_summary["executed_notional"] == acc["notional"], ctx
            released = plan_summary["released_quantity"]
            filled = plan_summary["filled_quantity"]
            assert released >= filled >= 0, ctx
            assert plan_summary["cancelled_quantity"] >= 0, ctx
            if filled > 0:
                assert plan_summary["vwap"] == {
                    "numerator": acc["notional"], "denominator": filled}, ctx
            else:
                assert plan_summary["vwap"] is None, ctx
            assert plan_summary["status"] in ("ACTIVE", "COMPLETED", "CANCELLED"), ctx
            plan = gen.plans.get(key)
            if plan is not None:
                assert released <= plan["total"], ctx
                slippage = acc["notional"] - plan["benchmark"] * filled
                if plan["side"] == "SELL":
                    slippage = -slippage
                assert plan_summary["slippage_notional"] == slippage, ctx
                if plan_summary["status"] == "COMPLETED":
                    assert released == plan["total"], ctx
                if plan_summary["status"] == "CANCELLED":
                    assert released + plan_summary["cancelled_quantity"] == plan["total"], ctx
                if plan["algo"] == "VWAP":
                    assert plan_summary["algorithm"] == "VWAP", ctx
                elif plan["algo"] == "POV":
                    assert plan_summary["algorithm"] == "POV", ctx
                else:
                    assert "algorithm" not in plan_summary, ctx
            if plan_summary["status"] != "ACTIVE":
                assert plan_summary.get("remaining_slices", 0) == 0, ctx
                assert plan_summary.get("unreleased_quantity", 0) == 0, ctx

        # -- price-limit update echo -------------------------------------------
        if note.get("type") == PRICE_LIMIT_UPDATE and result["status"] == ACCEPTED:
            lower, upper = note["band"]
            assert result["result"] == PRICE_LIMIT_UPDATED, ctx
            assert result["active_price_limits"] == {
                "lower_price": lower, "upper_price": upper}, ctx

        # -- historical reconstruction at the current sequence -----------------
        recon = note.get("recon")
        if recon and result["status"] == ACCEPTED:
            view = result["book_reconstruction"]
            assert view["symbol"] == symbol, ctx
            assert view["target_sequence"] == recon["target"], ctx
            if recon["at_current"]:
                band = note["band"]
                expected_limits = (
                    None if band is None
                    else {"lower_price": band[0], "upper_price": band[1]}
                )
                assert view["active_price_limits"] == expected_limits, ctx
                for queues, book, descending in (
                    (view["bid_queues"], result["bids"], True),
                    (view["ask_queues"], result["asks"], False),
                ):
                    prices = [queue["price"] for queue in queues]
                    assert prices == sorted(prices, reverse=descending), ctx
                    aggregate = {}
                    for queue in queues:
                        visible_total = 0
                        for order in queue["orders"]:
                            info = gen.orders[(symbol, order["order_id"])]
                            if order["order_type"] == "ICEBERG":
                                assert info["iceberg"], ctx
                                # The public slice never exceeds the peak and
                                # is always fully replenished while resting.
                                assert order["visible_quantity"] == min(
                                    info["display"], order["remaining_quantity"]), ctx
                            else:
                                assert order["order_type"] == "LIMIT", ctx
                                assert order["visible_quantity"] == (
                                    order["remaining_quantity"]), ctx
                            visible_total += order["visible_quantity"]
                        assert queue["visible_quantity"] == visible_total, ctx
                        aggregate[queue["price"]] = visible_total
                    # Hidden iceberg reserves never enter the book aggregates.
                    assert aggregate == _levels(book), ctx

        # -- commit bookkeeping --------------------------------------------------
        committed = (
            result["status"] == ACCEPTED
            or (result["status"] == REJECTED
                and result["rejection_code"] not in ENVELOPE_CODES)
        )
        if committed:
            known_symbols.add(symbol)
            prev_book[symbol] = (result["bids"], result["asks"])

    # Quantity conservation per order and per plan child: nothing ever fills
    # beyond the quantity its events made available.
    for key, filled in order_fills.items():
        assert filled <= gen.budget.get(key, 0), (
            f"{key} filled {filled} beyond its submitted quantity"
        )

    # The dedicated self-trade security ends with exactly the last pair's
    # prevented maker resting: every earlier pair's sell was swept by the
    # next pair's taker before prevention fired.
    if gen._self_pairs:
        ccc_bids, ccc_asks = prev_book.get("CCC", ([], []))
        assert ccc_bids == []
        assert ccc_asks == [
            {"price": 900 + 10 * gen._self_pairs, "quantity": 5}
        ]


# ---------------------------------------------------------------------------
# Stream generation and the one-shot reference path
# ---------------------------------------------------------------------------


def _generate(seed: int) -> _Generator:
    return _Generator(seed).generate()


@pytest.mark.parametrize("seed", SEEDS)
def test_seed_reproduces_identical_stream_and_results(seed):
    first = _generate(seed)
    second = _generate(seed)
    assert canonical_json(first.events) == canonical_json(second.events)
    assert canonical_json(first.notes) == canonical_json(second.notes)
    replay_first = replay_events(copy.deepcopy(first.events), config=CONFIG)
    replay_second = replay_events(copy.deepcopy(second.events), config=CONFIG)
    assert canonical_json(replay_first) == canonical_json(replay_second)


# ---------------------------------------------------------------------------
# Three execution paths agree at every committable boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_three_execution_paths_agree_at_every_boundary(seed):
    gen = _generate(seed)
    events = gen.events

    # Path A: the one-shot replay is the reference execution.
    baseline = replay_events(copy.deepcopy(events), config=CONFIG)
    results, snapshot = baseline["results"], baseline["snapshot"]
    assert_stream_invariants(gen, results)

    reference = EventReplayer(copy.deepcopy(CONFIG))
    reference.submit(copy.deepcopy(events))
    books = {symbol: reference.book(symbol) for symbol in gen.symbols_seen()}

    for boundary in range(len(events) + 1):
        prefix = copy.deepcopy(events[:boundary])
        suffix = copy.deepcopy(events[boundary:])

        # Path B: one EventReplayer, segmented submits at the boundary.
        segmented = EventReplayer(copy.deepcopy(CONFIG))
        head = segmented.submit(prefix)
        tail = segmented.submit(suffix)
        assert canonical_json(head + tail) == canonical_json(results), (
            f"seed {seed}: EventReplayer segments diverge at boundary {boundary}"
        )
        assert canonical_json(export_snapshot(segmented)) == canonical_json(snapshot), (
            f"seed {seed}: segmented final snapshot diverges at boundary {boundary}"
        )

        # Path C: export at the boundary, restore, continue the suffix.
        exported = replay_events(prefix, config=CONFIG)["snapshot"]
        restored = restore_replayer(exported, copy.deepcopy(CONFIG))
        continued = restored.submit(suffix)
        assert canonical_json(continued) == canonical_json(results[boundary:]), (
            f"seed {seed}: restored suffix diverges at boundary {boundary}"
        )
        assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot), (
            f"seed {seed}: restored final snapshot diverges at boundary {boundary}"
        )
        for symbol, book in books.items():
            assert restored.book(symbol) == book, (
                f"seed {seed}: restored book for {symbol} diverges at {boundary}"
            )


# ---------------------------------------------------------------------------
# Read-only queries occupy ids/sequences but change no trading outcome
# ---------------------------------------------------------------------------


def _strip_sequences(value):
    if isinstance(value, dict):
        return {
            key: _strip_sequences(item)
            for key, item in value.items()
            if key not in ("sequence", "expected_sequence")
        }
    if isinstance(value, list):
        return [_strip_sequences(item) for item in value]
    return value


def _filtered_stream(events, notes):
    """Drop the read-only queries and renumber the remaining stream compactly.

    Fault injections are rebased onto the renumbered sequences; an injection
    whose anchor was itself a dropped query is dropped too (it exercises
    nothing the kept stream could observe).
    """
    out_events: list[object] = []
    kept_indices: list[int] = []
    counters: dict[str, int] = {}
    sequence_map: dict[str, dict[int, int]] = {}
    old_to_new: dict[int, int] = {}

    for index, (event, note) in enumerate(zip(events, notes)):
        kind = note["kind"]
        if kind == "report":
            continue
        if not isinstance(event, dict):
            out_events.append(copy.deepcopy(event))
            kept_indices.append(index)
            continue
        symbol = event["symbol"]
        counter = counters.get(symbol, 1)
        new_event = copy.deepcopy(event)
        if kind in ("duplicate", "conflict"):
            target = note["target"]
            if notes[target]["kind"] == "report":
                continue
            if kind == "duplicate":
                new_event = copy.deepcopy(out_events[old_to_new[target]])
            else:
                new_event["sequence"] = counter
        elif kind == "gap":
            new_event["sequence"] = counter + note["offset"]
        elif kind == "out_of_order":
            mapped = sequence_map.get(symbol, {}).get(note["orig_seq"])
            if mapped is None:
                continue
            new_event["sequence"] = mapped
        elif kind == "invalid":
            # Kept byte-identical: the sequence value is irrelevant to the
            # INVALID_EVENT outcome (structural validation runs first), and
            # some variants are invalid *because* of the envelope itself.
            pass
        else:
            new_event["sequence"] = counter
            counters[symbol] = counter + 1
            sequence_map.setdefault(symbol, {})[event["sequence"]] = counter
        old_to_new[index] = len(out_events)
        out_events.append(new_event)
        kept_indices.append(index)
    return out_events, kept_indices


@pytest.mark.parametrize("seed", SEEDS)
def test_read_only_queries_do_not_change_trading_outcomes(seed):
    gen = _generate(seed)
    probed_results = replay_events(copy.deepcopy(gen.events), config=CONFIG)["results"]

    filtered_events, kept_indices = _filtered_stream(gen.events, gen.notes)
    filtered_results = replay_events(filtered_events, config=CONFIG)["results"]

    # Every kept event — trades with their ids, queue-priority-sensitive book
    # diffs, plan releases and rejections — is identical once the envelope
    # sequence fields (which the dropped queries legitimately shifted) are
    # stripped.
    assert len(filtered_results) == len(kept_indices)
    assert canonical_json([_strip_sequences(r) for r in filtered_results]) == (
        canonical_json([_strip_sequences(probed_results[i]) for i in kept_indices])
    )

    # The final per-security books are identical too.
    probed_session = EventReplayer(copy.deepcopy(CONFIG))
    probed_session.submit(copy.deepcopy(gen.events))
    filtered_session = EventReplayer(copy.deepcopy(CONFIG))
    filtered_session.submit(copy.deepcopy(filtered_events))
    for symbol in gen.symbols_seen():
        assert probed_session.book(symbol) == filtered_session.book(symbol), symbol


# ---------------------------------------------------------------------------
# Snapshot restoration and tamper handling
# ---------------------------------------------------------------------------


def _mid_stream_snapshot(seed):
    gen = _generate(seed)
    boundary = len(gen.events) // 2
    snapshot = replay_events(
        copy.deepcopy(gen.events[:boundary]), config=CONFIG)["snapshot"]
    return gen, boundary, snapshot


def test_restored_snapshot_reexports_identical_bytes():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    restored = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    assert canonical_json(export_snapshot(restored)) == canonical_json(snapshot)
    # Restoring the re-export changes nothing either.
    restored_again = restore_replayer(export_snapshot(restored), copy.deepcopy(CONFIG))
    assert canonical_json(export_snapshot(restored_again)) == canonical_json(snapshot)


def test_tampered_snapshot_content_is_snapshot_corrupt():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    tampered = copy.deepcopy(snapshot)
    tampered["content"]["events"][0]["content"] = '"tampered"'
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert excinfo.value.code == SNAPSHOT_CORRUPT


def test_tampered_content_digest_is_snapshot_corrupt():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    tampered = copy.deepcopy(snapshot)
    tampered["content_digest"] = "0" * 64
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert excinfo.value.code == SNAPSHOT_CORRUPT


def test_unknown_snapshot_envelope_field_is_snapshot_corrupt():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    tampered = copy.deepcopy(snapshot)
    tampered["unexpected"] = 1
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert excinfo.value.code == SNAPSHOT_CORRUPT


def test_unsupported_snapshot_format_version():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    tampered = copy.deepcopy(snapshot)
    tampered["format_version"] = "event-replay/1"
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(tampered, copy.deepcopy(CONFIG))
    assert excinfo.value.code == SNAPSHOT_VERSION_UNSUPPORTED


def test_restore_with_mismatched_config_is_config_mismatch():
    _, _, snapshot = _mid_stream_snapshot(SEEDS[0])
    other_config = {"price_limits": {"AAA": {"lower": 1, "upper": 2}}}
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(copy.deepcopy(snapshot), other_config)
    assert excinfo.value.code == CONFIG_MISMATCH
    # The snapshot carries a price-limits block; the default config lacks it.
    with pytest.raises(SnapshotError) as excinfo:
        restore_replayer(copy.deepcopy(snapshot), None)
    assert excinfo.value.code == CONFIG_MISMATCH


# ---------------------------------------------------------------------------
# Coverage anchors: the generated streams really visit the whole surface
# ---------------------------------------------------------------------------


def test_generated_streams_cover_the_public_surface():
    kinds: set[str] = set()
    committed_types: set[str] = set()
    codes: set[str] = set()
    statuses: set[str] = set()
    outcomes: set[str] = set()
    fok_unfilled = False
    for seed in SEEDS:
        gen = _generate(seed)
        results = replay_events(copy.deepcopy(gen.events), config=CONFIG)["results"]
        for note, result in zip(gen.notes, results):
            kinds.add(note["kind"])
            statuses.add(result["status"])
            if note["kind"] not in FAULT_KINDS and note.get("type"):
                committed_types.add(note["type"])
            if result["status"] == REJECTED:
                codes.add(result["rejection_code"])
            if result["status"] == ACCEPTED:
                outcomes.add(result.get("result"))
            if note.get("fok") and result.get("result") == "UNFILLED_CANCELLED":
                fok_unfilled = True

    assert kinds >= {
        "normal", "invalid", "gap", "out_of_order", "duplicate",
        "conflict", "self_trade", "report",
    }
    assert committed_types >= ALL_EVENT_TYPES
    assert codes >= {
        INVALID_EVENT, SEQUENCE_GAP, OUT_OF_ORDER, EVENT_ID_CONFLICT,
        PRICE_LIMIT_EXCEEDED, UNKNOWN_ORDER, UNKNOWN_EXECUTION_PLAN,
        EXECUTION_PLAN_CLOSED, DUPLICATE_EXECUTION_PLAN,
        MARK_PRICE_MISMATCH, UNKNOWN_ACCOUNT, TARGET_SEQUENCE_NOT_FOUND,
    }
    assert statuses == {ACCEPTED, REJECTED, DUPLICATE}
    assert outcomes >= {
        "RESTING", "FILLED", "CANCELLED", "REPLACED", "REPORTED",
        "SELF_TRADE_PREVENTED", "PARTIALLY_FILLED_SELF_TRADE_PREVENTED",
        "UNFILLED_CANCELLED", "PRICE_LIMIT_UPDATED",
    }
    assert fok_unfilled
    assert "RECONCILED" in outcomes or "BREAKS_FOUND" in outcomes
