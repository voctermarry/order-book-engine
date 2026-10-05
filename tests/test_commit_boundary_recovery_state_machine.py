"""Deterministic state-machine tests for recovery from *any* commit boundary.

This module pins one cross-cutting invariant of the documented public replay
surface (:func:`replay_events`, :class:`EventReplayer`,
:func:`export_snapshot` / :func:`restore_replayer`, :func:`canonical_json`
and the read-only reports): an ordered, multi-security event stream may be
cut after *any* committed event, exported to a snapshot, restored and
continued, and the continuation must be indistinguishable — object for
object on every remaining response and byte for byte on the final snapshot
— from the same stream run in one uninterrupted submission.

It deliberately does **not** add an event type, a product interface or an
output field, and it never touches production code. Every expected value is
taken from the public event list, the public per-event result objects, the
public ``book`` projection or a public read-only report. The snapshot
document is only exported, restored and serialized — its internal layout is
never read to construct an expectation. No runtime dependency is added;
variation comes exclusively from a fixed-seed corpus feeding a tiny
deterministic LCG, so a failing case reproduces verbatim from the seed and
the generated event list alone.

Every generated track interleaves two securities and visits:

* plain GTC limit orders (including two resting orders at one price that
  pin price-time priority), IOC and FOK orders, an iceberg whose current
  visible slice is partly consumed and whose hidden reserve survives across
  a recovery boundary, cancels and replaces;
* a LIMIT TWAP of which two slices are released before a boundary (the plan
  stays ACTIVE: released quantity and reserved derived child ids must both
  survive) and a completing MARKET TWAP;
* an accepted intraday ``PRICE_LIMIT_UPDATE`` (narrow then restore) and the
  sequence-occupying ``PRICE_LIMIT_EXCEEDED`` business rejection it causes;
  further sequence-occupying committed rejections come from an unknown
  cancel/replace target, a duplicate order id, an unknown plan command and a
  closed-plan cancel;
* the read-only report family, including two
  ``BOOK_RECONSTRUCTION_REPORT`` queries and two
  ``BOOK_LIQUIDITY_REPORT`` queries per symbol whose answers must be
  identical on the uninterrupted run and every recovery path, and which
  must never move a later trade, book or trade id;
* the non-committed faults — a structurally invalid event, a sequence gap, a
  stale sequence, a verbatim duplicate delivery and an event-id conflict —
  all of which consume neither an id nor a sequence.

A parallel *clean* track drops only the five non-committed faults per symbol
and renumbers every surviving envelope sequence (translating reconstruction
target slots); it asserts that recovering from the structural/ordering
faults leaves the matching results identical to a stream that never saw
them.
"""

from __future__ import annotations

import copy

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_LIQUIDITY_REPORT,
    BOOK_RECONSTRUCTION_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EventReplayer,
    IMPACT_REPORT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    PLAN_TCA_REPORT,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    REJECTED,
    SEQUENCE_GAP,
    SESSION_RECONCILIATION,
    TWAP_CANCEL,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine.engine import (
    DUPLICATE_EVENT_ID,
    DUPLICATE_ORDER_ID,
    EXECUTION_REPORT,
    ICEBERG,
    IOC,
    FOK,
    LIMIT,
    MARKET,
    BUY,
    SELL,
    UNKNOWN_ORDER,
)

# ---------------------------------------------------------------------------
# Fixed configuration: both securities start inside a static band; the
# stream narrows and restores the band intraday on each.
# ---------------------------------------------------------------------------

SYMBOLS = ("AAA", "BBB")
CENTRAL = {"AAA": 100, "BBB": 50}
_STATIC_BAND = {"AAA": (90, 110), "BBB": (40, 60)}
CONFIG = {"price_limits": {
    "AAA": {"lower": 90, "upper": 110},
    "BBB": {"lower": 40, "upper": 60},
}}

# A small fixed corpus; every stream is a pure function of its seed.
SEEDS = (0, 1, 2, 7, 42)

# Rejection codes that commit nothing: the event id stays free and the
# per-symbol sequence does not advance.
NON_COMMITTED_CODES = frozenset({
    INVALID_EVENT, EVENT_ID_CONFLICT, SEQUENCE_GAP, OUT_OF_ORDER,
})


# ---------------------------------------------------------------------------
# Deterministic pseudo-random source
# ---------------------------------------------------------------------------


class _Lcg:
    """A fixed 31-bit LCG — the only source of variation in a track."""

    __slots__ = ("state",)

    def __init__(self, seed: int) -> None:
        self.state = seed & 0xFFFFFFFF

    def u32(self) -> int:
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def rint(self, low: int, high: int) -> int:
        return low + self.u32() % (high - low + 1)


# ---------------------------------------------------------------------------
# Bounded single-security script builder
# ---------------------------------------------------------------------------
#
# Sequences are assigned at finalize time, so the structural faults (which
# deliberately carry a hole or a stale sequence) can be dropped for the clean
# track and the surviving envelopes simply renumber 1..N. Event ids are
# unique across the whole stream except the explicit duplicate/conflict
# faults, which reuse the first event id of their security.
#
# Deterministic liquidity layout (c = central price):
#   ask 1 @ c+1 (later cancelled)
#   plain maker 3 @ c            (fully taken by the IOC)
#   iceberg 10 @ c, peak 2       (partly consumed; reserve survives)
#   ask 2 @ c+1 = replace target (replaced to rest at c)
#   bid 3 @ c-2
#   IOC buy 3 @ c+1, FOK buy 5, FOK buy 999 (atomic failure)
#   two priority sells 2 @ c+2 (never touched: no later buy crosses c+1)


class _Script:
    """One security's ordered, not-yet-sequenced script."""

    def __init__(self, symbol: str, rng: _Lcg) -> None:
        self.symbol = symbol
        self.rng = rng
        self.c = CENTRAL[symbol]
        self.items: list[tuple[dict[str, object], str]] = []
        self.no = 0
        self.np = 0
        # Public generator-side identities the tests anchor on.
        self.iceberg_id: str | None = None
        self.prio_first: str | None = None
        self.prio_second: str | None = None
        self.limit_twap: str | None = None

    # -- id / item helpers --------------------------------------------------

    def oid(self) -> str:
        self.no += 1
        return f"{self.symbol}.o{self.no}"

    def pid(self) -> str:
        self.np += 1
        return f"{self.symbol}.P{self.np}"

    def put(self, payload: dict[str, object], kind: str = "event") -> None:
        self.items.append((payload, kind))

    # -- payload builders ---------------------------------------------------

    def add(self, side, order_type, quantity, price=None, tif=None,
            account=None, display=None, order_id=None):
        order_id = order_id or self.oid()
        payload = {
            "type": "ADD",
            "order_id": order_id,
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
        self.put(payload)
        return order_id

    def cancel(self, order_id):
        self.put({"type": "CANCEL", "order_id": order_id})

    def replace(self, order_id, quantity, price, display=None):
        payload = {"type": "REPLACE", "order_id": order_id,
                   "quantity": quantity, "price": price}
        if display is not None:
            payload["display_quantity"] = display
        self.put(payload)

    def limit_update(self, lower, upper):
        self.put({"type": PRICE_LIMIT_UPDATE,
                  "lower_price": lower, "upper_price": upper})

    def plan(self, ptype, plan_id, **extra):
        payload = {"type": ptype, "plan_id": plan_id}
        payload.update(extra)
        self.put(payload)

    # -- the script ----------------------------------------------------------

    def build(self) -> None:
        rng, c = self.rng, self.c
        lower, upper = _STATIC_BAND[self.symbol]

        def ask(qty, price, account=None):
            return self.add(SELL, LIMIT, qty, price, account=account)

        def bid(qty, account=None):
            return self.add(BUY, LIMIT, qty, c - rng.rint(1, 2),
                            account=account)

        # 1) Seed the book.
        cancel_target = ask(1, c + 1, account=f"{self.symbol}.a")
        plain_maker = ask(3, c, account=f"{self.symbol}.m")
        self.iceberg_id = self.add(SELL, ICEBERG, 10, price=c,
                                  account=f"{self.symbol}.ice", display=2)
        replace_target = ask(2, c + 1, account=f"{self.symbol}.r")
        bid(3, account=f"{self.symbol}.b")

        # 2) IOC takes the plain maker at c; FOK walks the iceberg (two full
        #    peaks plus one unit), leaving visible 1 / reserve 6; the
        #    oversized FOK fails atomically with no trade.
        self.add(BUY, LIMIT, 3, price=c + 1, tif=IOC)
        self.add(BUY, MARKET, 5, tif=FOK)
        self.add(BUY, MARKET, 999, tif=FOK)

        # 3) Two same-price resting sells pin price-time priority. They are
        #    never matched again (every later buy prices at or below c+1).
        self.prio_first = ask(2, c + 2, account=f"{self.symbol}.p1")
        self.prio_second = ask(2, c + 2, account=f"{self.symbol}.p2")

        # 4) Accepted cancel, then a committed unknown-target rejection.
        self.cancel(cancel_target)
        self.cancel(f"{self.symbol}.ghost")

        # 5) The replacement moves the c+1 maker to c (it rests behind the
        #    iceberg); an unknown target is a committed UNKNOWN_ORDER.
        self.replace(replace_target, 2, c)
        self.replace(f"{self.symbol}.ghost", 1, c)
        assert plain_maker  # tracked for the duplicate-order rejection below

        # 6) Narrow the band, breach it with a fresh order id (so identifier
        #    precedence cannot mask the price-limit code), then restore.
        self.limit_update(lower, c + 1)
        self.add(SELL, LIMIT, 1, c + 3, order_id=self.oid())
        self.limit_update(lower, upper)

        # 7) LIMIT TWAP: two of three slices released (plan stays ACTIVE
        #    across the boundary), with a read-only report between them.
        ask(2, c, account=f"{self.symbol}.twliq1")
        twap = self.pid()
        self.limit_twap = twap
        self.plan(TWAP_START, twap, side=BUY, total_quantity=6,
                  slice_count=3, order_type=LIMIT, benchmark_price=c,
                  price=c, account_id=f"{self.symbol}.tw")
        self.plan(TWAP_SLICE, twap)
        ask(2, c, account=f"{self.symbol}.twliq2")
        self.plan(TWAP_SLICE, twap)
        self.plan(TWAP_REPORT, twap)

        # 8) Read-only reports: execution query (known + unknown order),
        #    impact what-if, two historical reconstructions (the empty
        #    pre-session book and the immediately preceding committed slot;
        #    the positive target is patched after sequencing) and two
        #    current-book liquidity summaries (the touch only, and deeper).
        self.put({"type": EXECUTION_REPORT, "order_id": replace_target,
                  "benchmark_price": c})
        self.put({"type": EXECUTION_REPORT,
                  "order_id": f"{self.symbol}.ghost", "benchmark_price": c})
        self.put({"type": IMPACT_REPORT, "side": BUY, "quantity": 1,
                  "benchmark_price": c})
        self.put({"type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0})
        self.put({"type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0})
        self.put({"type": BOOK_LIQUIDITY_REPORT, "depth": 1})
        self.put({"type": BOOK_LIQUIDITY_REPORT, "depth": 5})

        # 9) Further committed business rejections: duplicate baseline order
        #    id and an unknown-plan slice command.
        self.add(SELL, LIMIT, 1, c, order_id=plain_maker)
        self.plan(TWAP_SLICE, f"{self.symbol}.ghostP")

        # 10) Final TWAP slice (COMPLETED) and a cancel of the closed plan.
        ask(2, c, account=f"{self.symbol}.twliq3")
        self.plan(TWAP_SLICE, twap)
        self.plan(TWAP_CANCEL, twap)

        # 11) A completing MARKET TWAP: IOC children never rest.
        ask(2, c, account=f"{self.symbol}.mt1")
        ask(2, c, account=f"{self.symbol}.mt2")
        mtwap = self.pid()
        self.plan(TWAP_START, mtwap, side=BUY, total_quantity=4,
                  slice_count=2, order_type=MARKET, benchmark_price=c,
                  account_id=f"{self.symbol}.mtw")
        self.plan(TWAP_SLICE, mtwap)
        self.plan(TWAP_SLICE, mtwap)

        # 12) Plan TCA and a whole-session query (read-only).
        self.put({"type": PLAN_TCA_REPORT, "plan_id": twap,
                  "mark_price": c})
        self.put({"type": SESSION_RECONCILIATION,
                  "expected_trades": [], "expected_accounts": []})

        # 13) A closing resting bid keeps the closing book non-empty.
        bid(1, account=f"{self.symbol}.end")

    # -- faults --------------------------------------------------------------

    def insert_faults(self) -> None:
        """Splice the five non-committed faults among real events.

        They never sit first or last; the clean track later drops exactly
        these items. Gap/stale use structurally valid read-only payloads;
        the invalid item corrupts an ADD quantity; duplicate and conflict
        reuse this symbol's first event id (resolved at finalize).
        """
        total = len(self.items)

        def position(fraction):
            return max(2, min(total - 2, int(total * fraction)))

        invalid = {
            "type": "ADD", "order_id": self.oid(), "side": SELL,
            "order_type": LIMIT, "quantity": 1, "price": self.c + 1,
        }
        self.items.insert(position(0.30), (invalid, "invalid"))
        impact = {"type": IMPACT_REPORT, "side": BUY, "quantity": 1,
                  "benchmark_price": self.c}
        self.items.insert(position(0.45), (dict(impact), "gap"))
        self.items.insert(position(0.60), (dict(impact), "stale"))
        self.items.insert(position(0.75), ({}, "duplicate"))
        self.items.insert(position(0.88),
                          ({"type": "CANCEL",
                            "order_id": f"{self.symbol}.zz"}, "conflict"))

    # -- sequencing ----------------------------------------------------------

    def finalize(self, event_id_prefix: str):
        """Assign global event ids and per-symbol sequences.

        Returns ``(event, committed)`` rows in script order; ``committed``
        is False only for the five non-committed faults. The exact rejected
        sequence carried by a gap/stale/redelivered envelope is preserved.
        """
        first_payload = {
            **self.items[0][0], "event_id": f"{event_id_prefix}.e001",
        }
        self.items[0] = (first_payload, "event")

        rows = []
        next_sequence = 1
        counter = 1
        for index, (payload, kind) in enumerate(self.items):
            if index == 0:
                event_id = first_payload["event_id"]
                payload = first_payload
            else:
                counter += 1
                event_id = f"{event_id_prefix}.e{counter:03d}"
                payload = {**payload, "event_id": event_id}

            if kind == "event":
                sequence = next_sequence
                next_sequence += 1
                rows.append((self._wrap(payload, sequence), True))
            elif kind == "invalid":
                bad = {**payload, "quantity": "x"}
                rows.append((self._wrap(bad, next_sequence), False))
            elif kind == "gap":
                rows.append((self._wrap(payload, next_sequence + 1), False))
            elif kind == "stale":
                rows.append((self._wrap(payload, 1), False))
            elif kind == "duplicate":
                rows.append((self._wrap(dict(first_payload), 1), False))
            else:  # conflict
                clash = {"event_id": first_payload["event_id"],
                         "type": "CANCEL",
                         "order_id": f"{self.symbol}.zz"}
                rows.append((self._wrap(clash, next_sequence), False))
        return rows

    def _wrap(self, payload, sequence):
        return {"event_id": payload["event_id"], "symbol": self.symbol,
                "sequence": sequence,
                **{k: v for k, v in payload.items() if k != "event_id"}}


# ---------------------------------------------------------------------------
# Track assembly
# ---------------------------------------------------------------------------


class Track:
    """A merged two-symbol stream and its fault metadata for one seed."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        rng = _Lcg(seed)
        scripts = {symbol: _Script(symbol, rng) for symbol in SYMBOLS}
        for script in scripts.values():
            script.build()
            script.insert_faults()

        rows = {symbol: scripts[symbol].finalize(f"s{seed}.{symbol}")
                for symbol in SYMBOLS}

        self.iceberg_ids = {s: scripts[s].iceberg_id for s in SYMBOLS}
        self.prio_ids = {
            s: (scripts[s].prio_first, scripts[s].prio_second)
            for s in SYMBOLS
        }
        self.limit_twap_ids = {s: scripts[s].limit_twap for s in SYMBOLS}

        # Patch the second reconstruction query of each symbol to target the
        # committed slot immediately before that query.
        for symbol in SYMBOLS:
            recon_indexes = [
                i for i, (event, _committed) in enumerate(rows[symbol])
                if event.get("type") == BOOK_RECONSTRUCTION_REPORT
            ]
            assert len(recon_indexes) == 2, (seed, symbol)
            zero_at, tip_at = recon_indexes
            assert rows[symbol][zero_at][0]["target_sequence"] == 0
            tip_sequence = rows[symbol][tip_at][0]["sequence"]
            rows[symbol][tip_at][0]["target_sequence"] = tip_sequence - 1
            assert tip_sequence - 1 >= 1

        # Round-robin merge.
        events, committed = [], []
        positions = {s: 0 for s in SYMBOLS}
        lengths = {s: len(rows[s]) for s in SYMBOLS}
        step = 0
        while any(positions[s] < lengths[s] for s in SYMBOLS):
            symbol = SYMBOLS[step % len(SYMBOLS)]
            if positions[symbol] < lengths[symbol]:
                event, is_committed = rows[symbol][positions[symbol]]
                events.append(event)
                committed.append(is_committed)
                positions[symbol] += 1
            step += 1
        self.events = events
        self.committed = committed

    # -- clean (fault-free) projection --------------------------------------

    def clean_events(self):
        """Drop the non-committed faults and renumber surviving envelopes.

        Every committed business rejection stays; reconstruction targets are
        translated to the equivalent committed slots.
        """
        old_to_new = {}
        new_seq = {s: 1 for s in SYMBOLS}
        for event, is_committed in zip(self.events, self.committed):
            if not is_committed:
                continue
            symbol = event["symbol"]
            old_to_new[(symbol, event["sequence"])] = new_seq[symbol]
            new_seq[symbol] += 1

        cleaned = []
        next_sequence = {s: 1 for s in SYMBOLS}
        for event, is_committed in zip(self.events, self.committed):
            if not is_committed:
                continue
            symbol = event["symbol"]
            fixed = dict(event)
            fixed["sequence"] = next_sequence[symbol]
            next_sequence[symbol] += 1
            if fixed.get("type") == BOOK_RECONSTRUCTION_REPORT:
                target = fixed["target_sequence"]
                if target > 0:
                    fixed["target_sequence"] = old_to_new[(symbol, target)]
            cleaned.append(fixed)
        return cleaned


_TRACK_CACHE: dict[int, Track] = {}


def track(seed: int) -> Track:
    if seed not in _TRACK_CACHE:
        _TRACK_CACHE[seed] = Track(seed)
    return _TRACK_CACHE[seed]


# ---------------------------------------------------------------------------
# Observation helpers (public surface only)
# ---------------------------------------------------------------------------


def run_baseline(events, config=CONFIG):
    out = replay_events(copy.deepcopy(events), config=config)
    return out["results"], out["snapshot"]


def restore_after_prefix(events, length, config=CONFIG):
    prefix = replay_events(copy.deepcopy(events[:length]), config=config)
    replayer = restore_replayer(copy.deepcopy(prefix["snapshot"]),
                                copy.deepcopy(config))
    suffix = replayer.submit(copy.deepcopy(events[length:]))
    return suffix, replayer


def restore_at_prefix(events, length, config=CONFIG):
    """A restored session positioned exactly after ``events[:length]``.

    Unlike :func:`restore_after_prefix` no suffix is submitted, so callers
    can issue read-only probes naming the prefix's own next sequence.
    """
    prefix = replay_events(copy.deepcopy(events[:length]), config=config)
    return restore_replayer(copy.deepcopy(prefix["snapshot"]),
                            copy.deepcopy(config))


def trade_rows(results):
    """Public per-symbol trade observations in occurrence order."""
    grouped = {symbol: [] for symbol in SYMBOLS}
    for result in results:
        for trade in result.get("trades", []):
            grouped[result["symbol"]].append(trade)
    return grouped


def recon_answers(results):
    return [r for r in results
            if r.get("status") == ACCEPTED and "book_reconstruction" in r]


def recon_probe(symbol, sequence, target, tag):
    return {
        "event_id": f"probe.{tag}.{symbol}",
        "symbol": symbol,
        "sequence": sequence,
        "type": BOOK_RECONSTRUCTION_REPORT,
        "target_sequence": target,
    }


def find_order(answer, order_id):
    """The queue-order record for ``order_id`` in a reconstruction answer."""
    for side in ("bid_queues", "ask_queues"):
        for level in answer["book_reconstruction"][side]:
            for order in level["orders"]:
                if order["order_id"] == order_id:
                    return level, order
    return None, None


def final_sequence(events, symbol):
    return max(ev["sequence"] for ev in events if ev["symbol"] == symbol)


# ---------------------------------------------------------------------------
# Generator sanity and coverage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_track_is_purely_seed_driven(seed):
    assert canonical_json(Track(seed).events) == canonical_json(
        track(seed).events)


@pytest.mark.parametrize("seed", SEEDS)
def test_track_covers_every_required_outcome(seed):
    tr = track(seed)
    results, _ = run_baseline(tr.events)
    assert len(results) == len(tr.events)

    # Both securities interleave and genuinely trade.
    assert {r["symbol"] for r in results} == set(SYMBOLS)
    trades = trade_rows(results)
    for symbol in SYMBOLS:
        assert trades[symbol], (seed, symbol)

    codes = [r.get("rejection_code") for r in results]
    statuses = [r.get("status") for r in results]

    # Non-committed faults, each at least once.
    assert codes.count(INVALID_EVENT) == len(SYMBOLS)
    assert codes.count(SEQUENCE_GAP) == len(SYMBOLS)
    assert codes.count(OUT_OF_ORDER) == len(SYMBOLS)
    assert codes.count(EVENT_ID_CONFLICT) == len(SYMBOLS)
    assert statuses.count(DUPLICATE) == len(SYMBOLS)

    # Committed business rejections that must occupy a sequence.
    assert PRICE_LIMIT_EXCEEDED in codes
    assert codes.count(UNKNOWN_ORDER) >= 2 * len(SYMBOLS)
    assert DUPLICATE_ORDER_ID in codes

    # Accepted feature outcomes.
    assert any(r.get("result") == "PRICE_LIMIT_UPDATED" for r in results)
    assert any(r.get("result") == "UNFILLED_CANCELLED" for r in results)
    plans = [r["execution_plan"] for r in results if "execution_plan" in r]
    assert any(p["status"] == "ACTIVE" for p in plans)
    assert any(p["status"] == "COMPLETED" for p in plans)

    # Event-kind coverage.
    types = {ev.get("type") for ev in tr.events}
    assert {"ADD", "CANCEL", "REPLACE", TWAP_START, TWAP_SLICE, TWAP_CANCEL,
            TWAP_REPORT, PRICE_LIMIT_UPDATE, EXECUTION_REPORT, IMPACT_REPORT,
            PLAN_TCA_REPORT, SESSION_RECONCILIATION,
            BOOK_RECONSTRUCTION_REPORT, BOOK_LIQUIDITY_REPORT} <= types
    assert any(ev.get("order_type") == ICEBERG for ev in tr.events)
    assert any(ev.get("time_in_force") == IOC for ev in tr.events)
    assert any(ev.get("time_in_force") == FOK for ev in tr.events)
    assert len(recon_answers(results)) == 2 * len(SYMBOLS)
    liquidity_answers = [
        r for r in results
        if r.get("status") == ACCEPTED and "liquidity_analysis" in r
    ]
    assert len(liquidity_answers) == 2 * len(SYMBOLS)
    assert {r["liquidity_analysis"]["depth"] for r in liquidity_answers} == {1, 5}


# ---------------------------------------------------------------------------
# The core invariant: any commit boundary restores object/byte-identically
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_every_committed_boundary_restores_object_and_byte_identical(seed):
    tr = track(seed)
    results, final_snapshot = run_baseline(tr.events)
    final_session = restore_replayer(copy.deepcopy(final_snapshot),
                                     copy.deepcopy(CONFIG))
    final_books = {s: canonical_json(final_session.book(s)) for s in SYMBOLS}

    boundaries = [i for i, is_committed in enumerate(tr.committed)
                  if is_committed]
    for boundary in boundaries:
        prefix = replay_events(copy.deepcopy(tr.events[:boundary + 1]),
                               config=CONFIG)
        replayer = restore_replayer(copy.deepcopy(prefix["snapshot"]),
                                    copy.deepcopy(CONFIG))
        suffix = replayer.submit(copy.deepcopy(tr.events[boundary + 1:]))

        # Object-for-object equality of every remaining response: status,
        # rejection precedence, trades and their ids/order, book diffs,
        # bids/asks, plan summaries and every read-only analysis.
        expected_suffix = results[boundary + 1:]
        assert len(suffix) == len(expected_suffix), (seed, boundary)
        for got, want in zip(suffix, expected_suffix):
            assert got == want, (
                seed, boundary, want.get("event_id"),
                canonical_json([got]), canonical_json([want]))

        # Byte-identical normalized final snapshot and final books.
        assert canonical_json(export_snapshot(replayer)) == canonical_json(
            final_snapshot), (seed, boundary)
        for symbol in SYMBOLS:
            assert canonical_json(replayer.book(symbol)) == final_books[
                symbol], (seed, boundary, symbol)


@pytest.mark.parametrize("seed", SEEDS)
def test_replay_events_snapshot_form_matches_at_all_boundaries(seed):
    # The same property through the replay_events(..., snapshot=...) form,
    # including the empty prefix and a boundary after the final event.
    tr = track(seed)
    results, final_snapshot = run_baseline(tr.events)
    boundaries = {0, len(tr.events)}
    boundaries.update(i for i, c in enumerate(tr.committed) if c)
    for boundary in sorted(boundaries):
        prefix = replay_events(copy.deepcopy(tr.events[:boundary]),
                               config=CONFIG)
        continued = replay_events(copy.deepcopy(tr.events[boundary:]),
                                  config=CONFIG, snapshot=prefix["snapshot"])
        assert canonical_json(continued["results"]) == canonical_json(
            results[boundary:]), (seed, boundary)
        assert canonical_json(continued["snapshot"]) == canonical_json(
            final_snapshot), (seed, boundary)


# ---------------------------------------------------------------------------
# Trade numbering: consecutive 1..N and continuous across boundaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_trade_numbering_is_consecutive_and_continuous_across_recovery(seed):
    tr = track(seed)
    results, _ = run_baseline(tr.events)
    want = trade_rows(results)
    for symbol in SYMBOLS:
        ids = [t["trade_id"] for t in want[symbol]]
        assert ids == list(range(1, len(ids) + 1)), (seed, symbol)

    # At the first-trade, a middle and the last committed boundary, prefix
    # trades plus restored-suffix trades must reproduce the exact journal.
    committed_indexes = [i for i, c in enumerate(tr.committed) if c]
    sample = sorted({
        committed_indexes[0],
        committed_indexes[len(committed_indexes) // 3],
        committed_indexes[2 * len(committed_indexes) // 3],
        committed_indexes[-1],
    })
    for boundary in sample:
        suffix, _replayer = restore_after_prefix(tr.events, boundary + 1)
        merged = list(results[:boundary + 1]) + suffix
        assert canonical_json(trade_rows(merged)) == canonical_json(want), (
            seed, boundary)


# ---------------------------------------------------------------------------
# Same-price priority and the iceberg current visible slice do not drift
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_same_price_priority_survives_every_recovery(seed):
    """The two same-priced resting makers keep their time order on the
    uninterrupted run and on a mid-stream restored-and-continued run."""
    tr = track(seed)
    results, _ = run_baseline(tr.events)
    committed_indexes = [i for i, c in enumerate(tr.committed) if c]
    mid = committed_indexes[len(committed_indexes) // 2]
    suffix, restored = restore_after_prefix(tr.events, mid + 1)
    # The suffix itself must already be object-identical (core invariant).
    assert canonical_json(suffix) == canonical_json(results[mid + 1:])

    # One uninterrupted session and one mid-stream restored session, both at
    # the final tip; probe queues read-only at the tip for each symbol.
    full = EventReplayer(copy.deepcopy(CONFIG))
    full.submit(copy.deepcopy(tr.events))

    for symbol in SYMBOLS:
        tip = final_sequence(tr.events, symbol)
        probe = recon_probe(symbol, tip + 1, tip, f"prio{seed}")
        full_probe = full.submit([copy.deepcopy(probe)])[0]
        restored_answer = restored.submit([copy.deepcopy(probe)])[0]
        assert restored_answer == full_probe, (seed, symbol)

        first, second = tr.prio_ids[symbol]
        level, first_order = find_order(full_probe, first)
        assert level is not None and first_order is not None
        ids_at_level = [o["order_id"] for o in level["orders"]]
        assert ids_at_level.index(first) < ids_at_level.index(second), (
            seed, symbol, ids_at_level)


@pytest.mark.parametrize("seed", SEEDS)
def test_iceberg_current_slice_and_reserve_survive_recovery(seed):
    """At the ACTIVE-plan boundary the iceberg's partly consumed slice and
    hidden reserve reconstruct identically on the continuous run and on the
    restored prefix; the visible slice never exceeds its peak or remainder
    and the hidden reserve never enters the level aggregate."""
    tr = track(seed)
    results, _ = run_baseline(tr.events)
    full = EventReplayer(copy.deepcopy(CONFIG))
    full.submit(copy.deepcopy(tr.events))

    # The iceberg peak is fixed by the generator at two units.
    iceberg_peak = 2
    iceberg_total = {
        symbol: next(ev["quantity"] for ev in tr.events
                     if ev.get("order_id") == tr.iceberg_ids[symbol])
        for symbol in SYMBOLS
    }

    def iceberg_fills(symbol, upto):
        """Public fills attributed to the iceberg within results[:upto]."""
        total = 0
        for result in results[:upto]:
            if result["symbol"] != symbol:
                continue
            for trade in result.get("trades", []):
                if trade["maker_order_id"] == tr.iceberg_ids[symbol]:
                    total += trade["quantity"]
        return total

    for symbol in SYMBOLS:
        # Second accepted TWAP slice: the plan is ACTIVE and the iceberg
        # still carries a hidden reserve.
        slice_indexes = [
            i for i, (ev, r) in enumerate(zip(tr.events, results))
            if ev["symbol"] == symbol and ev.get("type") == TWAP_SLICE
            and r.get("status") == ACCEPTED
        ]
        active_cut = slice_indexes[1]
        target = tr.events[active_cut]["sequence"]
        assert results[active_cut]["execution_plan"]["status"] == "ACTIVE"

        # Historical answer on the fully run session.
        tip = final_sequence(tr.events, symbol)
        historical = full.submit([
            recon_probe(symbol, tip + 1, target, f"iceh{seed}")])[0]
        # Answer on the prefix restored exactly to the target.
        at_target = restore_at_prefix(tr.events, active_cut + 1)
        direct = at_target.submit([
            recon_probe(symbol, target + 1, target, f"iced{seed}")])[0]
        # The response also echoes the *current* live book, which legitimately
        # differs between the target point and the final tip; the historical
        # reconstruction payload itself must be identical.
        assert direct["book_reconstruction"] == historical["book_reconstruction"], (
            seed, symbol)

        _level, order = find_order(direct, tr.iceberg_ids[symbol])
        assert order is not None, (seed, symbol)
        # Expected remainder derived solely from the public trade stream.
        expected_remaining = (
            iceberg_total[symbol] - iceberg_fills(symbol, active_cut + 1))
        assert order["remaining_quantity"] == expected_remaining, (
            seed, symbol, order)
        # The reserve genuinely survives the recovery boundary.
        assert order["remaining_quantity"] > iceberg_peak, (seed, symbol, order)
        assert order["visible_quantity"] in (1, iceberg_peak)
        assert order["visible_quantity"] <= order["remaining_quantity"]

        # Every visible slice is bounded and aggregates count visible only.
        for side in ("bid_queues", "ask_queues"):
            for queued_level in direct["book_reconstruction"][side]:
                assert queued_level["visible_quantity"] == sum(
                    o["visible_quantity"] for o in queued_level["orders"])
                for queued_order in queued_level["orders"]:
                    peak = (iceberg_peak
                            if queued_order["order_type"] == ICEBERG
                            else queued_order["remaining_quantity"])
                    assert 1 <= queued_order["visible_quantity"] <= min(
                        peak, queued_order["remaining_quantity"])


# ---------------------------------------------------------------------------
# Plans: released quantity and reserved derived ids continue across recovery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_plan_progress_and_reserved_derived_ids_survive_recovery(seed):
    tr = track(seed)
    results, _ = run_baseline(tr.events)

    for symbol in SYMBOLS:
        slice_indexes = [
            i for i, (ev, r) in enumerate(zip(tr.events, results))
            if ev["symbol"] == symbol and ev.get("type") == TWAP_SLICE
            and r.get("status") == ACCEPTED
        ]
        active_cut = slice_indexes[1]
        plan_at_cut = results[active_cut]["execution_plan"]
        assert plan_at_cut["status"] == "ACTIVE"
        assert plan_at_cut["released_quantity"] == 4
        assert plan_at_cut["filled_quantity"] >= 1

        plan_id = tr.limit_twap_ids[symbol]
        reserved_child = f"{plan_id}#3"
        last_seq = tr.events[active_cut]["sequence"]

        # On the restored prefix the reserved child id blocks reuse both as
        # an engine order id and as an envelope event id.
        restored = restore_at_prefix(tr.events, active_cut + 1)
        clash_order = {
            "event_id": f"clash.order.{symbol}", "symbol": symbol,
            "sequence": last_seq + 1, "type": "ADD",
            "order_id": reserved_child, "side": BUY, "order_type": LIMIT,
            "quantity": 1, "price": CENTRAL[symbol],
        }
        clash_order_result = restored.submit([clash_order])[0]
        assert clash_order_result["status"] == REJECTED
        assert clash_order_result["rejection_code"] == DUPLICATE_ORDER_ID
        assert clash_order_result["trades"] == []

        clash_event = {
            "event_id": reserved_child, "symbol": symbol,
            "sequence": last_seq + 2, "type": "ADD",
            "order_id": f"{symbol}.other", "side": BUY,
            "order_type": LIMIT, "quantity": 1, "price": CENTRAL[symbol],
        }
        clash_event_result = restored.submit([clash_event])[0]
        assert clash_event_result["rejection_code"] == DUPLICATE_EVENT_ID
        assert clash_event_result["trades"] == []

        # A fresh restore (the clashes occupied sequences) completes the
        # stream with byte-identical plan summaries.
        suffix, _restored2 = restore_after_prefix(tr.events, active_cut + 1)
        for got, want in zip(suffix, results[active_cut + 1:]):
            assert got == want, (seed, symbol, want.get("event_id"))


# ---------------------------------------------------------------------------
# Active price-limit interval continuity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_active_price_limit_interval_is_continuous_across_recovery(seed):
    """Restoring before the narrowing update and replaying the suffix still
    classifies the out-of-band limit PRICE_LIMIT_EXCEEDED and accepts the
    later restore update — the active interval continues, it is not reset
    to the static band on recovery."""
    tr = track(seed)
    results, _ = run_baseline(tr.events)

    for symbol in SYMBOLS:
        first_index = next(
            i for i, ev in enumerate(tr.events) if ev["symbol"] == symbol)
        breach_index = next(
            i for i, (ev, r) in enumerate(zip(tr.events, results))
            if ev["symbol"] == symbol
            and r.get("rejection_code") == PRICE_LIMIT_EXCEEDED)

        suffix, _restored = restore_after_prefix(tr.events, first_index + 1)
        by_id = {r["event_id"]: r for r in suffix}
        assert (by_id[tr.events[breach_index]["event_id"]]["rejection_code"]
                == PRICE_LIMIT_EXCEEDED), (seed, symbol)

        tail_updates = [
            r for ev, r in zip(tr.events[first_index + 1:], suffix)
            if ev["symbol"] == symbol
            and ev.get("type") == PRICE_LIMIT_UPDATE
        ]
        assert [r["result"] for r in tail_updates] == [
            "PRICE_LIMIT_UPDATED", "PRICE_LIMIT_UPDATED"]


# ---------------------------------------------------------------------------
# Non-committed faults leave no snapshot trace; committed rejections do
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_non_committed_rejections_do_not_change_snapshot(seed):
    tr = track(seed)
    _results, snapshot = run_baseline(tr.events)
    replayer = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    original_bytes = canonical_json(snapshot)

    # Issue every non-committed fault on both symbols first; none may move a
    # book or the snapshot document.
    for symbol in SYMBOLS:
        last_seq = final_sequence(tr.events, symbol)
        before = canonical_json(replayer.book(symbol))
        faults = [
            {"event_id": f"x.inv.{symbol}", "symbol": symbol,
             "sequence": last_seq + 1, "type": "ADD",
             "order_id": f"{symbol}.x", "side": BUY, "order_type": LIMIT,
             "quantity": "n", "price": CENTRAL[symbol]},
            {"event_id": f"x.gap.{symbol}", "symbol": symbol,
             "sequence": last_seq + 2, "type": IMPACT_REPORT, "side": BUY,
             "quantity": 1, "benchmark_price": CENTRAL[symbol]},
            {"event_id": f"x.stale.{symbol}", "symbol": symbol,
             "sequence": 1, "type": IMPACT_REPORT, "side": BUY,
             "quantity": 1, "benchmark_price": CENTRAL[symbol]},
        ]
        for fault in faults:
            result = replayer.submit([copy.deepcopy(fault)])[0]
            assert result["status"] == REJECTED, (seed, fault["event_id"])
            assert result["rejection_code"] in NON_COMMITTED_CODES
            assert result["trades"] == []
            assert canonical_json(replayer.book(symbol)) == before

    # Snapshot bytes unchanged by any of the rejected faults.
    assert canonical_json(export_snapshot(replayer)) == original_bytes

    # The legal next-sequence event still commits exactly once per symbol,
    # proving the faults occupied neither id nor sequence.
    for symbol in SYMBOLS:
        last_seq = final_sequence(tr.events, symbol)
        good = {"event_id": f"x.good.{symbol}", "symbol": symbol,
                "sequence": last_seq + 1, "type": IMPACT_REPORT, "side": BUY,
                "quantity": 1, "benchmark_price": CENTRAL[symbol]}
        assert replayer.submit([good])[0]["status"] == ACCEPTED


@pytest.mark.parametrize("seed", SEEDS)
def test_faults_recovered_match_a_fault_free_trajectory(seed):
    """Dropping the non-committed faults changes no matching outcome.

    The dirty stream's committed responses, with the renumbered envelope
    sequence and the reconstruction target echo normalized away, equal the
    clean stream's responses object for object; trades are identical in
    detail and in per-symbol order.
    """
    tr = track(seed)
    dirty_results, _ = run_baseline(tr.events)
    clean_results, _ = run_baseline(tr.clean_events())

    dirty_committed = [
        r for r, is_committed in zip(dirty_results, tr.committed)
        if is_committed
    ]
    assert len(dirty_committed) == len(clean_results)

    def view(result_list):
        normalized = []
        for r in result_list:
            item = {k: v for k, v in r.items()
                    if k not in ("sequence", "expected_sequence")}
            recon = item.get("book_reconstruction")
            if isinstance(recon, dict):
                item["book_reconstruction"] = {
                    k: v for k, v in recon.items()
                    if k != "target_sequence"
                }
            normalized.append(item)
        return normalized

    assert canonical_json(view(dirty_committed)) == canonical_json(
        view(clean_results)), seed
    assert canonical_json(trade_rows(dirty_committed)) == canonical_json(
        trade_rows(clean_results)), seed


@pytest.mark.parametrize("seed", SEEDS)
def test_committed_business_rejections_retain_id_and_sequence(seed):
    tr = track(seed)
    results, snapshot = run_baseline(tr.events)

    # Per symbol, committed sequences are a contiguous 1..N: no committed
    # business rejection left a hole or spent an id twice.
    for symbol in SYMBOLS:
        seqs = [
            r["sequence"] for r, is_committed in zip(results, tr.committed)
            if is_committed and r["symbol"] == symbol
        ]
        assert seqs == list(range(1, len(seqs) + 1)), (seed, symbol)

    # Verbatim redelivery of every committed rejection (business code and
    # rejection code preserved) is a DUPLICATE that trades nothing.
    replayer = restore_replayer(copy.deepcopy(snapshot), copy.deepcopy(CONFIG))
    committed_rejections = [
        ev for ev, r, is_committed in zip(tr.events, results, tr.committed)
        if is_committed and r.get("status") == REJECTED
    ]
    assert committed_rejections
    for event in committed_rejections:
        redelivery = replayer.submit([copy.deepcopy(event)])[0]
        assert redelivery["status"] == DUPLICATE, (seed, event["event_id"])
        assert redelivery["trades"] == []


# ---------------------------------------------------------------------------
# BOOK_RECONSTRUCTION_REPORT: identical on every path and never perturbing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_embedded_reconstruction_queries_match_on_every_recovery_path(seed):
    tr = track(seed)
    results, _ = run_baseline(tr.events)
    baseline_answers = recon_answers(results)
    assert len(baseline_answers) == 2 * len(SYMBOLS)

    # At sampled committed boundaries every reconstruction query answered in
    # the suffix returns the exact object the uninterrupted run returned.
    committed_indexes = [i for i, c in enumerate(tr.committed) if c]
    sample = {0, committed_indexes[len(committed_indexes) // 4],
              committed_indexes[len(committed_indexes) // 2],
              committed_indexes[-1]}
    for boundary in sorted(sample):
        suffix, _ = restore_after_prefix(tr.events, boundary + 1)
        assert canonical_json(recon_answers(suffix)) == canonical_json(
            recon_answers(results[boundary + 1:])), (seed, boundary)

    # Whole stream replayed from an empty prefix gives the same answers.
    whole, _ = restore_after_prefix(tr.events, 0)
    assert canonical_json(recon_answers(whole)) == canonical_json(
        baseline_answers)


def _insert_probe(tr, seed):
    """Build a copy of the stream with one extra reconstruction query for
    the first symbol at a midpoint. Returns ``(instrumented, insert_at)``
    with later per-symbol sequences and positive reconstruction targets
    shifted by the one occupied query slot; stale/redelivered fault
    envelopes keep their deliberately-old sequence.
    """
    symbol = SYMBOLS[0]
    insert_at = next(
        i for i, ev in enumerate(tr.events)
        if ev["symbol"] == symbol and i > len(tr.events) // 4
        and tr.committed[i])
    committed_before = sum(
        1 for j in range(insert_at)
        if tr.events[j]["symbol"] == symbol and tr.committed[j])
    probe = {
        "event_id": f"extra.probe.{seed}", "symbol": symbol,
        "sequence": committed_before + 1,
        "type": BOOK_RECONSTRUCTION_REPORT, "target_sequence": 0,
    }

    instrumented = []
    shifts = {s: 0 for s in SYMBOLS}
    for i, event in enumerate(tr.events):
        if i == insert_at:
            instrumented.append(copy.deepcopy(probe))
            shifts[symbol] = 1
        fixed = dict(event)
        if event["symbol"] == symbol:
            keep_stale = (
                not tr.committed[i] and event["sequence"] == 1)
            if not keep_stale:
                fixed["sequence"] = event["sequence"] + shifts[symbol]
            if (fixed.get("type") == BOOK_RECONSTRUCTION_REPORT
                    and fixed["target_sequence"] > 0):
                fixed["target_sequence"] = (
                    event["target_sequence"] + shifts[symbol])
        instrumented.append(fixed)
    return instrumented, insert_at, probe


@pytest.mark.parametrize("seed", SEEDS)
def test_extra_reconstruction_query_never_perturbs_later_state(seed):
    tr = track(seed)
    instrumented, insert_at, probe = _insert_probe(tr, seed)

    base = EventReplayer(copy.deepcopy(CONFIG))
    base_results = base.submit(copy.deepcopy(tr.events))
    inst = EventReplayer(copy.deepcopy(CONFIG))
    inst_results = inst.submit(copy.deepcopy(instrumented))

    # The injected query is accepted and changes no trade or book.
    probe_results = [r for ev, r in zip(instrumented, inst_results)
                     if ev["event_id"] == probe["event_id"]]
    assert len(probe_results) == 1 and probe_results[0]["status"] == ACCEPTED
    assert canonical_json(trade_rows(inst_results)) == canonical_json(
        trade_rows(base_results)), seed
    for symbol in SYMBOLS:
        assert canonical_json(inst.book(symbol)) == canonical_json(
            base.book(symbol)), (seed, symbol)

    # Same non-perturbation across a recovery: restore at the prefix, issue
    # the query, continue the shifted remainder.
    prefix = replay_events(copy.deepcopy(instrumented[:insert_at]),
                           config=CONFIG)
    restored = restore_replayer(copy.deepcopy(prefix["snapshot"]),
                                copy.deepcopy(CONFIG))
    probe_after_restore = restored.submit([copy.deepcopy(probe)])[0]
    assert probe_after_restore == probe_results[0]
    restored.submit(copy.deepcopy(instrumented[insert_at + 1:]))
    for symbol in SYMBOLS:
        assert canonical_json(restored.book(symbol)) == canonical_json(
            inst.book(symbol)), (seed, symbol)
