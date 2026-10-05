"""Seed-driven commit-boundary recovery tests for the public replay surface.

This module pins one cross-functional invariant on the documented public
entry points — :func:`replay_events`, :class:`EventReplayer`,
:func:`export_snapshot`, :func:`restore_replayer` and :func:`canonical_json` —
without adding an event type, an output field or any production change:

    *recovery at any committed-event boundary is indistinguishable from an
    uninterrupted run.*

Every trajectory is produced by the self-contained bounded generator below
from one integer seed (a tiny fixed LCG is the only source of variation), so
any failure reproduces verbatim from the seed and the generated event list;
no runtime dependency is added. Each trajectory interleaves two securities
and visits, in one stream:

* plain resting limit orders, a market sweep, IOC and FOK takers, an iceberg
  order whose visible slice is consumed and tail-replenished, a cancel and a
  replace;
* one execution-plan lifecycle slice (TWAP, VWAP or POV, chosen by the seed)
  left ACTIVE with reserved derived child ids;
* an intraday PRICE_LIMIT_UPDATE per security (one security starts with a
  static configured band, the other receives its band mid-stream);
* read-only EXECUTION_REPORT / IMPACT_REPORT / BOOK_RECONSTRUCTION_REPORT
  queries plus the plan's own report;
* the non-committed failures — a structurally invalid event, a sequence gap,
  a stale sequence, a verbatim duplicate delivery and an event-id conflict —
  each followed by the corrected event at the expected sequence;
* committed business rejections (an unknown-order cancel and a price-limit
  breach) that must keep occupying their event id and their sequence.

The tests then assert, purely through public responses, snapshots and
read-only queries (never through private state):

1. one continuous commit gives the baseline responses and final snapshot;
   exporting a snapshot after *every* committed event, restoring through
   :func:`restore_replayer` and submitting the remaining suffix reproduces
   the baseline suffix responses object for object and the final snapshot
   byte for byte, with per-security trade ids continuing without a gap;
2. a non-committed structural or ordering rejection leaves the exported
   snapshot byte-identical, and the corrected stream (faults removed) yields
   the identical matching results on every shared event;
3. committed business rejections keep their id/sequence occupancy across
   recovery: verbatim redelivery is DUPLICATE, altered content is
   EVENT_ID_CONFLICT, the occupied sequence stays OUT_OF_ORDER;
4. across every probed boundary the recovered session shows the same
   same-price queue priority, iceberg visible slice, plan released quantity,
   reserved derived ids and active price-limit interval as the continuous
   session — all read back through read-only public queries;
5. BOOK_RECONSTRUCTION_REPORT probes return byte-identical results on the
   continuous and on every recovered path, and issuing the queries moves
   neither later trades, books nor trade ids.
"""

from __future__ import annotations

import copy

import pytest

from order_book_engine import (
    ACCEPTED,
    BOOK_RECONSTRUCTION_REPORT,
    DUPLICATE,
    EVENT_ID_CONFLICT,
    EXECUTION_REPORT,
    EventReplayer,
    IMPACT_REPORT,
    INVALID_EVENT,
    OUT_OF_ORDER,
    POV_REPORT,
    POV_START,
    POV_VOLUME,
    PRICE_LIMIT_EXCEEDED,
    PRICE_LIMIT_UPDATE,
    PRICE_LIMIT_UPDATED,
    REJECTED,
    SEQUENCE_GAP,
    TWAP_REPORT,
    TWAP_SLICE,
    TWAP_START,
    UNKNOWN_ORDER,
    VWAP_REPORT,
    VWAP_SLICE,
    VWAP_START,
    canonical_json,
    export_snapshot,
    replay_events,
    restore_replayer,
)
from order_book_engine.engine import DUPLICATE_EVENT_ID

# ---------------------------------------------------------------------------
# Fixed universe: two securities, one statically banded, one banded intraday
# ---------------------------------------------------------------------------

AAA = "AAA"
BBB = "BBB"
SYMBOLS = (AAA, BBB)
CENTRAL = {AAA: 100, BBB: 75}
CONFIG = {"price_limits": {AAA: {"lower": 90, "upper": 120}}}
BAND_UPDATE = {AAA: (95, 110), BBB: (67, 83)}
ALGORITHMS = ("TWAP", "VWAP", "POV")
PLAN_REPORT_TYPES = {"TWAP": TWAP_REPORT, "VWAP": VWAP_REPORT, "POV": POV_REPORT}

# A fixed, small seed corpus; generation is deterministic per seed.
SEEDS = (0, 1, 2, 7, 42, 2026)

#: Rejection codes that consume neither the event id nor the sequence.
NON_COMMITTED_CODES = frozenset(
    {INVALID_EVENT, EVENT_ID_CONFLICT, SEQUENCE_GAP, OUT_OF_ORDER}
)


def _is_committed(result: dict[str, object]) -> bool:
    """Whether a public response consumed its event id and sequence."""
    if result["status"] == ACCEPTED:
        return True
    return (
        result["status"] == REJECTED
        and result.get("rejection_code") not in NON_COMMITTED_CODES
    )


def _last_committed_sequences(results: list[dict[str, object]]) -> dict[str, int]:
    """Per-security last committed sequence, from public responses only."""
    last = {symbol: 0 for symbol in SYMBOLS}
    for result in results:
        if _is_committed(result):
            last[result["symbol"]] = result["sequence"]
    return last


# ---------------------------------------------------------------------------
# Deterministic pseudo-random source (the only variation in a trajectory)
# ---------------------------------------------------------------------------


class _Lcg:
    """A fixed, tiny 31-bit LCG; rebuilding from one seed is byte-exact."""

    __slots__ = ("state",)

    def __init__(self, seed: int) -> None:
        self.state = seed & 0x7FFFFFFF

    def u32(self) -> int:
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def rint(self, low: int, high: int) -> int:
        return low + self.u32() % (high - low + 1)


# ---------------------------------------------------------------------------
# Bounded trajectory generator
# ---------------------------------------------------------------------------
#
# The generator drives one real EventReplayer through the public submit call:
# every anchor (the sweep's trades, the iceberg replenishment echo, the plan
# child's fill, each rejection code) is asserted while the stream is built,
# so a generated trajectory is valid by construction. Events are first built
# per security, then merged round-robin in seed-chosen chunks, preserving
# per-security order.


class _Script:
    """Builds the per-security leg of one trajectory against a live session."""

    def __init__(self, generator: "_TrajectoryGenerator", symbol: str) -> None:
        self.gen = generator
        self.symbol = symbol
        self.central = CENTRAL[symbol]
        self.seq = 0
        self.items: list[tuple[dict[str, object], str | None, bool]] = []

    def cur(self) -> int:
        return self.seq + 1

    def _record(self, event, label, fault, advance):
        result = self.gen.session.submit([copy.deepcopy(event)])[0]
        self.items.append((event, label, fault))
        if advance:
            self.seq += 1
        return result

    def emit(self, payload, *, label=None, fault=False, sequence=None,
             event_id=None, advance=True):
        event = {
            "event_id": event_id or self.gen.next_id(),
            "symbol": self.symbol,
            "sequence": self.cur() if sequence is None else sequence,
        }
        event.update(payload)
        return self._record(event, label, fault, advance)

    def emit_raw(self, event, *, label=None, fault=False):
        return self._record(event, label, fault, False)


class _Trajectory:
    """One generated stream plus the public metadata the tests need."""

    __slots__ = ("seed", "events", "clean_events", "fault_indices",
                 "labels", "plans", "icebergs")

    def __init__(self, seed, events, clean_events, fault_indices, labels,
                 plans, icebergs):
        self.seed = seed
        self.events = events
        self.clean_events = clean_events
        self.fault_indices = fault_indices
        self.labels = labels
        self.plans = plans
        self.icebergs = icebergs


class _TrajectoryGenerator:
    """Builds one reproducible two-security trajectory for one seed."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = _Lcg((seed ^ 0x5BD1E995) & 0x7FFFFFFF)
        self.session = EventReplayer(copy.deepcopy(CONFIG))
        self.counter = 0

    def next_id(self) -> str:
        self.counter += 1
        return f"ev{self.counter:04d}"

    # -- per-security trading script ----------------------------------------

    def _trading_script(self, symbol: str, algorithm: str) -> _Script:
        s = _Script(self, symbol)
        c = s.central
        rng = self.rng

        def acct(tag: str) -> str:
            return f"{symbol}.{tag}"

        # 1) Seed the book: a plain ask, a bid, an iceberg and a second plain
        #    ask queued behind the iceberg at the same price.
        q1 = rng.rint(4, 6)
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.s1",
                         "side": "SELL", "order_type": "LIMIT", "quantity": q1,
                         "price": c, "account_id": acct("alpha")},
                        label="seed_ask")
        assert result["result"] == "RESTING"
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.b1",
                         "side": "BUY", "order_type": "LIMIT", "quantity": 2,
                         "price": c - 3, "account_id": acct("beta")},
                        label="seed_bid")
        assert result["result"] == "RESTING"
        display = rng.rint(2, 3)
        iceberg_total = rng.rint(9, 12)
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.ice",
                         "side": "SELL", "order_type": "ICEBERG",
                         "quantity": iceberg_total, "price": c,
                         "display_quantity": display,
                         "account_id": acct("ice")}, label="iceberg")
        assert result["result"] == "RESTING"
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.s4",
                         "side": "SELL", "order_type": "LIMIT", "quantity": 3,
                         "price": c, "account_id": acct("gamma")},
                        label="same_price_ask")
        assert result["result"] == "RESTING"

        # 2) Takers: a market sweep that consumes the first ask and the
        #    iceberg's whole visible slice (which then tail-replenishes), an
        #    IOC, a fillable FOK and an unfillable FOK.
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.m1",
                         "side": "BUY", "order_type": "MARKET",
                         "quantity": q1 + display,
                         "account_id": acct("taker")}, label="sweep")
        assert [t["maker_order_id"] for t in result["trades"]] == [
            f"{symbol}.s1", f"{symbol}.ice"]
        assert [t["trade_id"] for t in result["trades"]] == [1, 2]
        # The exhausted slice replenished to the full display size at the
        # level tail, behind the same-price plain ask.
        level = next(lev for lev in result["asks"] if lev["price"] == c)
        assert level["quantity"] == 3 + display
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.ioc1",
                         "side": "BUY", "order_type": "LIMIT", "quantity": 1,
                         "price": c, "time_in_force": "IOC",
                         "account_id": acct("taker")}, label="ioc")
        assert result["result"] == "FILLED"
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.fok1",
                         "side": "BUY", "order_type": "MARKET", "quantity": 1,
                         "time_in_force": "FOK",
                         "account_id": acct("taker")}, label="fok_fill")
        assert result["result"] == "FILLED"
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.fok2",
                         "side": "BUY", "order_type": "MARKET",
                         "quantity": 999, "time_in_force": "FOK",
                         "account_id": acct("taker")}, label="fok_fail")
        assert result["result"] == "UNFILLED_CANCELLED"
        assert result["trades"] == []

        # 3) Cancel and replace, plus the first committed business rejection.
        result = s.emit({"type": "CANCEL", "order_id": f"{symbol}.b1"},
                        label="cancel_ok")
        assert result["result"] == "CANCELLED"
        result = s.emit({"type": "CANCEL", "order_id": f"{symbol}.ghost"},
                        label="cancel_unknown")
        assert result["status"] == REJECTED
        assert result["rejection_code"] == UNKNOWN_ORDER
        result = s.emit({"type": "REPLACE", "order_id": f"{symbol}.s4",
                         "quantity": rng.rint(4, 6), "price": c},
                        label="replace_ok")
        assert result["status"] == ACCEPTED

        # 4) The intraday band adjustment, then a breach of the new band: a
        #    committed business rejection occupying id and sequence.
        lower, upper = BAND_UPDATE[symbol]
        result = s.emit({"type": PRICE_LIMIT_UPDATE, "lower_price": lower,
                         "upper_price": upper}, label="band_update")
        assert result["result"] == PRICE_LIMIT_UPDATED
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.far",
                         "side": "SELL", "order_type": "LIMIT", "quantity": 1,
                         "price": upper + 2}, label="breach")
        assert result["status"] == REJECTED
        assert result["rejection_code"] == PRICE_LIMIT_EXCEEDED

        # 5) Parent order: fresh liquidity, one released slice, a read-only
        #    plan report; the plan stays ACTIVE with derived ids reserved.
        for n in (1, 2):
            result = s.emit({"type": "ADD", "order_id": f"{symbol}.liq{n}",
                             "side": "SELL", "order_type": "LIMIT",
                             "quantity": 2, "price": c + 1,
                             "account_id": acct(f"liq{n}")})
            assert result["result"] == "RESTING"
        plan_id = f"{symbol}.P1"
        if algorithm == "TWAP":
            start = {"type": TWAP_START, "plan_id": plan_id, "side": "BUY",
                     "total_quantity": 4, "slice_count": 2,
                     "order_type": "LIMIT", "benchmark_price": c,
                     "price": c + 1, "account_id": acct("plan")}
            release = {"type": TWAP_SLICE, "plan_id": plan_id}
        elif algorithm == "VWAP":
            start = {"type": VWAP_START, "plan_id": plan_id, "side": "BUY",
                     "total_quantity": 4, "volume_weights": [1, 1],
                     "order_type": "LIMIT", "benchmark_price": c,
                     "price": c + 1, "account_id": acct("plan")}
            release = {"type": VWAP_SLICE, "plan_id": plan_id}
        else:
            start = {"type": POV_START, "plan_id": plan_id, "side": "BUY",
                     "total_quantity": 6, "participation_bps": 5000,
                     "order_type": "MARKET", "benchmark_price": c,
                     "account_id": acct("plan")}
            release = {"type": POV_VOLUME, "plan_id": plan_id,
                       "market_volume_increment": 4}
        result = s.emit(start, label="plan_start")
        assert result["status"] == ACCEPTED
        result = s.emit(release, label="plan_slice")
        assert result["status"] == ACCEPTED
        assert result["execution_plan"]["child_order_id"] == f"{plan_id}#1"
        assert result["trades"], "the fresh liquidity must fill the child"
        result = s.emit({"type": PLAN_REPORT_TYPES[algorithm],
                         "plan_id": plan_id}, label="plan_report")
        assert result["status"] == ACCEPTED
        assert result["execution_plan"]["released_quantity"] == 2
        assert result["execution_plan"]["status"] == "ACTIVE"

        # 6) Read-only reports, including two historical reconstructions.
        result = s.emit({"type": EXECUTION_REPORT,
                         "order_id": f"{symbol}.s4", "benchmark_price": c},
                        label="exec_report")
        assert result["status"] == ACCEPTED
        result = s.emit({"type": IMPACT_REPORT, "side": "BUY", "quantity": 1,
                         "benchmark_price": c}, label="impact")
        assert result["status"] == ACCEPTED
        result = s.emit({"type": BOOK_RECONSTRUCTION_REPORT,
                         "target_sequence": 1}, label="recon_early")
        assert result["status"] == ACCEPTED
        result = s.emit({"type": BOOK_RECONSTRUCTION_REPORT,
                         "target_sequence": 3}, label="recon_ice")
        assert result["status"] == ACCEPTED

        # 7) The non-committed failure block. None of these consumes an id
        #    or a sequence; the corrected event at the expected sequence
        #    commits immediately after the structural rejection.
        fixed_id = self.next_id()
        fixed_seq = s.cur()
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.badq",
                         "side": "BUY", "order_type": "LIMIT",
                         "quantity": "not-an-int", "price": c},
                        event_id=fixed_id, sequence=fixed_seq, advance=False,
                        fault=True, label="invalid")
        assert result["rejection_code"] == INVALID_EVENT
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.fixed",
                         "side": "BUY", "order_type": "LIMIT", "quantity": 1,
                         "price": c - 2, "account_id": acct("fix")},
                        event_id=fixed_id, sequence=fixed_seq,
                        label="invalid_fixed")
        assert result["status"] == ACCEPTED
        result = s.emit({"type": IMPACT_REPORT, "side": "BUY", "quantity": 1,
                         "benchmark_price": c},
                        sequence=s.cur() + 1, advance=False,
                        fault=True, label="gap")
        assert result["rejection_code"] == SEQUENCE_GAP
        result = s.emit({"type": IMPACT_REPORT, "side": "BUY", "quantity": 1,
                         "benchmark_price": c},
                        sequence=1, advance=False, fault=True, label="stale")
        assert result["rejection_code"] == OUT_OF_ORDER
        # Verbatim redelivery of this security's first event (stale sequence
        # and all): a duplicate, never re-traded.
        first_event = s.items[0][0]
        result = s.emit_raw(copy.deepcopy(first_event), fault=True,
                            label="dupe")
        assert result["status"] == DUPLICATE
        assert result["trades"] == []
        # Same identifier, different normalized content: a conflict.
        result = s.emit_raw(
            {"event_id": first_event["event_id"], "symbol": symbol,
             "sequence": s.cur(), "type": "CANCEL",
             "order_id": f"{symbol}.zz"},
            fault=True, label="conflict")
        assert result["rejection_code"] == EVENT_ID_CONFLICT
        # The legal expected sequence commits right after the failures.
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.after",
                         "side": "BUY", "order_type": "LIMIT", "quantity": 1,
                         "price": c - 2, "account_id": acct("after")},
                        label="after_faults")
        assert result["status"] == ACCEPTED

        # 8) A final resting seller keeps the closing book non-empty.
        result = s.emit({"type": "ADD", "order_id": f"{symbol}.end",
                         "side": "SELL", "order_type": "LIMIT", "quantity": 2,
                         "price": c + 2}, label="final_sell")
        assert result["result"] == "RESTING"

        s.iceberg_info = {  # type: ignore[attr-defined]
            "order_id": f"{symbol}.ice",
            "display": display,
            "remaining": iceberg_total - display,
        }
        return s

    # -- deterministic interleaving ------------------------------------------

    def _merge(self, scripts):
        queues = {symbol: list(script.items)
                  for symbol, script in scripts.items()}
        merged: list[dict[str, object]] = []
        fault_indices: set[int] = set()
        labels: dict[tuple[str, str], int] = {}
        turn = 0
        while any(queues.values()):
            symbol = SYMBOLS[turn % len(SYMBOLS)]
            turn += 1
            if not queues[symbol]:
                continue
            for _ in range(self.rng.rint(1, 3)):
                if not queues[symbol]:
                    break
                event, label, fault = queues[symbol].pop(0)
                if fault:
                    fault_indices.add(len(merged))
                if label is not None:
                    labels[(symbol, label)] = len(merged)
                merged.append(event)
        return merged, frozenset(fault_indices), labels

    # -- assembly -------------------------------------------------------------

    def build(self) -> _Trajectory:
        scripts = {}
        algorithms = {}
        for offset, symbol in enumerate(SYMBOLS):
            algorithm = ALGORITHMS[
                (self.seed + offset) % len(ALGORITHMS)]
            algorithms[symbol] = algorithm
            scripts[symbol] = self._trading_script(symbol, algorithm)
        merged, fault_indices, labels = self._merge(scripts)
        plans = {
            symbol: {
                "algorithm": algorithms[symbol],
                "plan_id": f"{symbol}.P1",
                "report_type": PLAN_REPORT_TYPES[algorithms[symbol]],
                # One slice released of a fixed schedule (or the first POV
                # release of six reserved ids): child #2 stays reserved.
                "reserved_child": f"{symbol}.P1#2",
                "start_index": labels[(symbol, "plan_start")],
            }
            for symbol in SYMBOLS
        }
        icebergs = {
            symbol: scripts[symbol].iceberg_info  # type: ignore[attr-defined]
            for symbol in SYMBOLS
        }
        return _Trajectory(
            seed=self.seed,
            events=copy.deepcopy(merged),
            clean_events=copy.deepcopy([
                event for index, event in enumerate(merged)
                if index not in fault_indices
            ]),
            fault_indices=fault_indices,
            labels=labels,
            plans=plans,
            icebergs=icebergs,
        )


_TRAJECTORY_CACHE: dict[int, _Trajectory] = {}


def trajectory(seed: int) -> _Trajectory:
    if seed not in _TRAJECTORY_CACHE:
        _TRAJECTORY_CACHE[seed] = _TrajectoryGenerator(seed).build()
    return _TRAJECTORY_CACHE[seed]


def _baseline(traj: _Trajectory):
    out = replay_events(copy.deepcopy(traj.events),
                        config=copy.deepcopy(CONFIG))
    return out["results"], out["snapshot"]


# ---------------------------------------------------------------------------
# Generator sanity: seed reproducibility, boundedness and coverage anchors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_trajectory_rebuilds_identically_from_seed(seed):
    first = trajectory(seed)
    second = _TrajectoryGenerator(seed).build()
    assert canonical_json(second.events) == canonical_json(first.events)
    assert second.fault_indices == first.fault_indices
    assert second.labels == first.labels
    # The stream is bounded: a failure reproduces from the seed and a short
    # event list.
    assert len(first.events) <= 100


@pytest.mark.parametrize("seed", SEEDS)
def test_trajectory_visits_every_required_state_kind(seed):
    traj = trajectory(seed)
    results, _snapshot = _baseline(traj)

    # A genuinely interleaved two-security stream.
    symbols = [event["symbol"] for event in traj.events]
    assert set(symbols) == set(SYMBOLS)
    assert any(a != b for a, b in zip(symbols, symbols[1:]))

    # The required event surface is present.
    types = [event.get("type") for event in traj.events]
    for required in ("ADD", "CANCEL", "REPLACE", PRICE_LIMIT_UPDATE,
                     EXECUTION_REPORT, IMPACT_REPORT,
                     BOOK_RECONSTRUCTION_REPORT):
        assert required in types
    assert any(kind in types for kind in (TWAP_START, VWAP_START, POV_START))
    adds = [event for event in traj.events if event.get("type") == "ADD"]
    assert any(event.get("time_in_force") == "IOC" for event in adds)
    assert any(event.get("time_in_force") == "FOK" for event in adds)
    assert any(event.get("order_type") == "ICEBERG" for event in adds)
    assert any(event.get("order_type") == "MARKET" for event in adds)

    # The generator's fault metadata matches the publicly observed commit
    # behaviour exactly: faults are precisely the non-committed events.
    for index, result in enumerate(results):
        assert (index in traj.fault_indices) != _is_committed(result)

    for symbol in SYMBOLS:
        def labeled(label):
            return results[traj.labels[(symbol, label)]]

        assert labeled("sweep")["status"] == ACCEPTED
        assert labeled("ioc")["result"] == "FILLED"
        assert labeled("fok_fill")["result"] == "FILLED"
        assert labeled("fok_fail")["result"] == "UNFILLED_CANCELLED"
        assert labeled("cancel_ok")["result"] == "CANCELLED"
        assert labeled("cancel_unknown")["rejection_code"] == UNKNOWN_ORDER
        assert labeled("replace_ok")["status"] == ACCEPTED
        assert labeled("band_update")["result"] == PRICE_LIMIT_UPDATED
        assert labeled("breach")["rejection_code"] == PRICE_LIMIT_EXCEEDED
        assert labeled("plan_slice")["status"] == ACCEPTED
        assert labeled("plan_report")["status"] == ACCEPTED
        assert labeled("exec_report")["status"] == ACCEPTED
        assert labeled("impact")["status"] == ACCEPTED
        assert labeled("recon_early")["status"] == ACCEPTED
        assert (labeled("recon_early")["book_reconstruction"]
                ["target_sequence"]) == 1
        assert labeled("invalid")["rejection_code"] == INVALID_EVENT
        assert labeled("invalid_fixed")["status"] == ACCEPTED
        assert labeled("gap")["rejection_code"] == SEQUENCE_GAP
        assert labeled("stale")["rejection_code"] == OUT_OF_ORDER
        assert labeled("dupe")["status"] == DUPLICATE
        assert labeled("conflict")["rejection_code"] == EVENT_ID_CONFLICT
        assert labeled("after_faults")["status"] == ACCEPTED
        assert labeled("final_sell")["result"] == "RESTING"

        # Committed events carry contiguous per-security sequences 1..N.
        committed = [
            result["sequence"] for result in results
            if result["symbol"] == symbol and _is_committed(result)
        ]
        assert committed == list(range(1, len(committed) + 1))


# ---------------------------------------------------------------------------
# The central invariant: recovery after every committed event
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_every_committed_boundary_recovers_byte_for_byte(seed):
    traj = trajectory(seed)
    base_results, base_snapshot = _baseline(traj)

    incremental = EventReplayer(copy.deepcopy(CONFIG))
    prefix_results: list[dict[str, object]] = []
    committed_boundaries = 0
    for index, event in enumerate(traj.events):
        result = incremental.submit([copy.deepcopy(event)])[0]
        prefix_results.append(result)
        # The incremental session agrees with the one-shot baseline.
        assert canonical_json(result) == canonical_json(base_results[index])
        if not _is_committed(result):
            continue
        committed_boundaries += 1

        # Export at this committed boundary, restore, submit the suffix.
        snapshot = export_snapshot(incremental)
        restored = restore_replayer(copy.deepcopy(snapshot),
                                    copy.deepcopy(CONFIG))
        # Restoration alone reproduces the exported snapshot byte for byte.
        assert canonical_json(export_snapshot(restored)) == canonical_json(
            snapshot)

        suffix_results = restored.submit(
            copy.deepcopy(traj.events[index + 1:]))
        # Every recovered response equals the baseline suffix object for
        # object (statuses, rejection codes, trades and ids, book diffs,
        # bids/asks, plan summaries, report analyses).
        assert canonical_json(suffix_results) == canonical_json(
            base_results[index + 1:]), (seed, index)
        # The final snapshot is byte-identical after canonicalization.
        assert canonical_json(export_snapshot(restored)) == canonical_json(
            base_snapshot), (seed, index)

        # Trade numbering continues across the boundary with no gap or
        # reuse, per security.
        for symbol in SYMBOLS:
            prefix_ids = [
                trade["trade_id"] for res in prefix_results
                if res["symbol"] == symbol for trade in res["trades"]
            ]
            suffix_ids = [
                trade["trade_id"] for res in suffix_results
                if res["symbol"] == symbol for trade in res["trades"]
            ]
            observed = prefix_ids + suffix_ids
            assert observed == list(range(1, len(observed) + 1)), (
                seed, index, symbol)

    assert committed_boundaries == len(traj.events) - len(traj.fault_indices)


# ---------------------------------------------------------------------------
# Non-committed rejections: no snapshot drift, identical corrected matching
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_uncommitted_faults_leave_snapshot_and_matching_untouched(seed):
    traj = trajectory(seed)

    # Around every structural/ordering rejection, duplicate and conflict the
    # exported snapshot is byte-identical and the response moves nothing.
    replayer = EventReplayer(copy.deepcopy(CONFIG))
    for index, event in enumerate(traj.events):
        if index in traj.fault_indices:
            before = canonical_json(export_snapshot(replayer))
            result = replayer.submit([copy.deepcopy(event)])[0]
            assert canonical_json(export_snapshot(replayer)) == before, (
                seed, index)
            assert result["trades"] == []
            assert result["book_changes"] == {"bids": [], "asks": []}
        else:
            replayer.submit([copy.deepcopy(event)])

    # The corrected stream (faults removed) is a legal trajectory of its own:
    # per-security sequences stay contiguous because no fault consumed one,
    # and every shared event — including the corrected resubmission of the
    # structurally invalid event — produces the identical matching result.
    faulted, _snapshot = _baseline(traj)
    clean = replay_events(copy.deepcopy(traj.clean_events),
                          config=copy.deepcopy(CONFIG))["results"]
    shared = [result for index, result in enumerate(faulted)
              if index not in traj.fault_indices]
    assert canonical_json(shared) == canonical_json(clean)


# ---------------------------------------------------------------------------
# Committed business rejections keep their id and sequence occupancy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_committed_business_rejections_keep_id_and_sequence_occupancy(seed):
    traj = trajectory(seed)
    base_results, base_snapshot = _baseline(traj)
    last = _last_committed_sequences(base_results)

    continuous = EventReplayer(copy.deepcopy(CONFIG))
    continuous.submit(copy.deepcopy(traj.events))
    restored = restore_replayer(copy.deepcopy(base_snapshot),
                                copy.deepcopy(CONFIG))

    for symbol in SYMBOLS:
        for label, code in (("cancel_unknown", UNKNOWN_ORDER),
                            ("breach", PRICE_LIMIT_EXCEEDED)):
            index = traj.labels[(symbol, label)]
            result = base_results[index]
            assert result["status"] == REJECTED
            assert result["rejection_code"] == code

            # Sequence occupancy: the security's next committed event
            # continues exactly after the rejected one's sequence.
            follower = next(
                res for res in base_results[index + 1:]
                if res["symbol"] == symbol and _is_committed(res))
            assert follower["sequence"] == result["sequence"] + 1

            event = traj.events[index]
            altered = copy.deepcopy(event)
            if altered["type"] == "ADD":
                altered["quantity"] += 1
            else:
                altered["order_id"] = f"{symbol}.other"
            probes = [
                copy.deepcopy(event),   # verbatim redelivery -> DUPLICATE
                altered,                # same id, new content -> CONFLICT
                {"event_id": f"occ{seed}.{symbol}.{label}", "symbol": symbol,
                 "sequence": result["sequence"], "type": IMPACT_REPORT,
                 "side": "BUY", "quantity": 1,
                 "benchmark_price": CENTRAL[symbol]},  # occupied -> stale
            ]
            reference = continuous.submit(copy.deepcopy(probes))
            actual = restored.submit(copy.deepcopy(probes))
            assert [res["status"] for res in actual] == [
                DUPLICATE, REJECTED, REJECTED]
            assert [res.get("rejection_code") for res in actual] == [
                None, EVENT_ID_CONFLICT, OUT_OF_ORDER]
            assert actual[2]["expected_sequence"] == last[symbol] + 1
            # The recovered session enforces occupancy byte-identically.
            assert canonical_json(actual) == canonical_json(reference)


# ---------------------------------------------------------------------------
# Boundary probes: queue priority, iceberg slice, plan and band continuity
# ---------------------------------------------------------------------------

#: Committed events after which the probe batch runs, covering every state
#: kind a boundary must preserve (plus the stream end).
_PROBE_LABELS = (
    "sweep", "replace_ok", "band_update", "breach", "plan_start",
    "plan_slice", "plan_report", "exec_report", "recon_ice",
    "invalid_fixed", "after_faults", "final_sell",
)


@pytest.mark.parametrize("seed", SEEDS)
def test_boundary_probes_show_no_drift_after_restore(seed):
    traj = trajectory(seed)
    boundaries = sorted(
        {traj.labels[(symbol, label)]
         for symbol in SYMBOLS for label in _PROBE_LABELS}
        | {len(traj.events) - 1}
    )
    for boundary in boundaries:
        prefix = copy.deepcopy(traj.events[:boundary + 1])
        continuous = EventReplayer(copy.deepcopy(CONFIG))
        prefix_results = continuous.submit(prefix)
        snapshot = export_snapshot(continuous)
        restored = restore_replayer(copy.deepcopy(snapshot),
                                    copy.deepcopy(CONFIG))
        last = _last_committed_sequences(prefix_results)

        # One identical read-only/boundary probe batch for both sessions:
        # a historical reconstruction of the just-committed state (queue
        # priority, iceberg visible slice, active band), the plan's own
        # report (released quantity) and a reserved derived-id clash.
        probes = []
        sequences = dict(last)
        for symbol in SYMBOLS:
            sequences[symbol] += 1
            probes.append({
                "event_id": f"bp{boundary}.{symbol}.recon",
                "symbol": symbol, "sequence": sequences[symbol],
                "type": BOOK_RECONSTRUCTION_REPORT,
                "target_sequence": last[symbol],
            })
            plan = traj.plans[symbol]
            if plan["start_index"] <= boundary:
                sequences[symbol] += 1
                probes.append({
                    "event_id": f"bp{boundary}.{symbol}.plan",
                    "symbol": symbol, "sequence": sequences[symbol],
                    "type": plan["report_type"], "plan_id": plan["plan_id"],
                })
                sequences[symbol] += 1
                probes.append({
                    "event_id": plan["reserved_child"],
                    "symbol": symbol, "sequence": sequences[symbol],
                    "type": "ADD",
                    "order_id": f"bp{boundary}.{symbol}.clash",
                    "side": "BUY", "order_type": "LIMIT", "quantity": 1,
                    "price": CENTRAL[symbol] - 1,
                })

        reference = continuous.submit(copy.deepcopy(probes))
        actual = restored.submit(copy.deepcopy(probes))
        # Queue order, iceberg visible quantities, the active price-limit
        # interval, the plan summary and the derived-id protection are all
        # byte-identical between the continuous and the recovered session.
        assert canonical_json(actual) == canonical_json(reference), (
            seed, boundary)
        for result in actual:
            if result["event_id"].endswith(".recon"):
                assert result["status"] == ACCEPTED
            elif result["event_id"].endswith(".plan"):
                assert result["status"] == ACCEPTED
                # The released quantity continues exactly: two once the
                # plan's slice committed, zero before it.
                symbol = result["symbol"]
                expected = 2 if (
                    traj.labels[(symbol, "plan_slice")] <= boundary) else 0
                assert result["execution_plan"]["released_quantity"] == expected
            else:
                # The reserved derived child id stays protected: a committed
                # business rejection, never a release or a match.
                assert result["status"] == REJECTED
                assert result["rejection_code"] == DUPLICATE_EVENT_ID
                assert result["trades"] == []


# ---------------------------------------------------------------------------
# BOOK_RECONSTRUCTION_REPORT: identical across recovery, and truly read-only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", SEEDS)
def test_book_reconstruction_identical_across_recovery_and_anchored(seed):
    traj = trajectory(seed)
    base_results, base_snapshot = _baseline(traj)
    last = _last_committed_sequences(base_results)

    continuous = EventReplayer(copy.deepcopy(CONFIG))
    continuous.submit(copy.deepcopy(traj.events))
    restored = restore_replayer(copy.deepcopy(base_snapshot),
                                copy.deepcopy(CONFIG))

    # Sweep every committed target (plus the empty pre-session book) on both
    # the continuous and the recovered final state.
    probes = []
    sequences = dict(last)
    for symbol in SYMBOLS:
        for target in range(0, last[symbol] + 1):
            sequences[symbol] += 1
            probes.append({
                "event_id": f"qr{seed}.{symbol}.{target}",
                "symbol": symbol, "sequence": sequences[symbol],
                "type": BOOK_RECONSTRUCTION_REPORT,
                "target_sequence": target,
            })
    reference = continuous.submit(copy.deepcopy(probes))
    actual = restored.submit(copy.deepcopy(probes))
    assert canonical_json(actual) == canonical_json(reference)
    assert all(result["status"] == ACCEPTED for result in actual)
    assert all(result["trades"] == [] for result in actual)

    # Explicit content anchor on the recovered final view: the iceberg's
    # current visible slice and total remainder did not drift, and the
    # intraday band replaced the static one. The expected slice state is
    # derived purely from the public trade stream and the documented
    # tail-replenishment rule.
    for symbol in SYMBOLS:
        view = next(
            result["book_reconstruction"] for result in actual
            if result["symbol"] == symbol
            and result["book_reconstruction"]["target_sequence"]
            == last[symbol])
        info = traj.icebergs[symbol]
        visible, remaining = info["display"], info["display"] + info["remaining"]
        for result in base_results:
            if result["symbol"] != symbol:
                continue
            for trade in result["trades"]:
                if trade["maker_order_id"] != info["order_id"]:
                    continue
                visible -= trade["quantity"]
                remaining -= trade["quantity"]
                if visible == 0 and remaining > 0:
                    visible = min(info["display"], remaining)
        level = next(queue for queue in view["ask_queues"]
                     if queue["price"] == CENTRAL[symbol])
        iceberg = next(order for order in level["orders"]
                       if order["order_id"] == info["order_id"])
        assert (iceberg["visible_quantity"],
                iceberg["remaining_quantity"]) == (visible, remaining)
        # A hidden reserve survives to the end of every trajectory.
        assert remaining > visible
        lower, upper = BAND_UPDATE[symbol]
        assert view["active_price_limits"] == {
            "lower_price": lower, "upper_price": upper}


@pytest.mark.parametrize("seed", SEEDS)
def test_reconstruction_queries_do_not_move_trades_books_or_trade_ids(seed):
    traj = trajectory(seed)
    base_results, _snapshot = _baseline(traj)
    last = _last_committed_sequences(base_results)

    # One session answers three reconstruction queries before the trading
    # suffix; the other goes straight to the suffix.
    queried = EventReplayer(copy.deepcopy(CONFIG))
    queried.submit(copy.deepcopy(traj.events))
    probes = [
        {"event_id": f"ro{seed}.{k}", "symbol": AAA,
         "sequence": last[AAA] + k + 1, "type": BOOK_RECONSTRUCTION_REPORT,
         "target_sequence": k}
        for k in range(3)
    ]
    probe_results = queried.submit(copy.deepcopy(probes))
    assert all(result["status"] == ACCEPTED for result in probe_results)
    assert all(result["trades"] == [] for result in probe_results)
    assert all(result["book_changes"] == {"bids": [], "asks": []}
               for result in probe_results)

    plain = EventReplayer(copy.deepcopy(CONFIG))
    plain.submit(copy.deepcopy(traj.events))

    def trading_suffix(aaa_offset: int):
        return [
            {"event_id": "tail.q1", "symbol": AAA,
             "sequence": last[AAA] + aaa_offset + 1, "type": "ADD",
             "order_id": "tail.q1", "side": "BUY", "order_type": "MARKET",
             "quantity": 2},
            {"event_id": "tail.q2", "symbol": AAA,
             "sequence": last[AAA] + aaa_offset + 2, "type": "ADD",
             "order_id": "tail.q2", "side": "SELL", "order_type": "LIMIT",
             "quantity": 1, "price": CENTRAL[AAA] + 2},
            {"event_id": "tail.q3", "symbol": BBB,
             "sequence": last[BBB] + 1, "type": "ADD",
             "order_id": "tail.q3", "side": "BUY", "order_type": "MARKET",
             "quantity": 1},
        ]

    queried_suffix = queried.submit(copy.deepcopy(trading_suffix(3)))
    plain_suffix = plain.submit(copy.deepcopy(trading_suffix(0)))

    # The queries spent no trade id and moved no queue: the suffix trades
    # (ids included) and the final books are identical.
    queried_trades = [trade for res in queried_suffix
                      for trade in res["trades"]]
    plain_trades = [trade for res in plain_suffix for trade in res["trades"]]
    assert queried_trades == plain_trades
    assert queried_trades, "the suffix must genuinely trade"
    for symbol in SYMBOLS:
        assert canonical_json(queried.book(symbol)) == canonical_json(
            plain.book(symbol))
