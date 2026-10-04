"""Per-symbol session state and the shared commit/response machinery.

This module owns the state containers every other replay component works
against — one security's book, sequence counter, idempotency log, plans and
active price-limit interval — plus the single deterministic commit path. It
deliberately knows nothing about the :class:`EventReplayer` orchestrator: the
commit context borrows the session's symbol table and global event log, so
the live session and the throwaway historical-rebuild session share exactly
the same commit primitive without either importing the other.
"""

from __future__ import annotations

from ..engine import Engine
from .constants import ACCEPTED, INVALID_EVENT, REJECTED
from .plan import ExecutionPlan  # noqa: F401  (re-exported for typing users)


class _SymbolState:
    __slots__ = ("engine", "last_sequence", "seen", "plans", "plan_index",
                 "price_limits")

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine or Engine()
        self.last_sequence = 0
        # eventId -> canonical payload content, in first-seen input order.
        self.seen: dict[str, str] = {}
        # plan_id -> ExecutionPlan, in start order. Closed plans stay queryable.
        self.plans: dict[str, ExecutionPlan] = {}
        # child order id -> plan_id, for every derived id of every accepted
        # plan, including slices not yet released.
        self.plan_index: dict[str, str] = {}
        # The security's active price-limit interval (closed), or None when
        # unlimited. Seeded from the static config block and replaced
        # wholesale by every accepted PRICE_LIMIT_UPDATE.
        self.price_limits: tuple[int, int] | None = None


class _Applied:
    """The business outcome of one well-formed, in-sequence event.

    Business handlers describe *only* the business result; they never touch
    the symbol sequence, the idempotency logs or the response envelope. The
    replay layer performs the single deterministic commit and assembles the
    response from one place, so every event kind shares identical id
    occupancy, sequence advancement, book echo and change expression.

    ``analysis`` carries the kind-specific read-only object (or objects);
    when it is ``None`` the handler names the response key through
    ``analysis_key``.
    """

    __slots__ = ("status", "code", "result", "trades",
                 "analysis_key", "analysis", "result_after_book",
                 "occupy_engine_id")

    def __init__(
        self,
        status: str,
        *,
        code: str | None = None,
        result: str | None = None,
        trades: list[dict[str, object]] | None = None,
        analysis_key: str | None = None,
        analysis: object = None,
        result_after_book: bool = False,
        occupy_engine_id: bool = False,
    ) -> None:
        self.status = status
        self.code = code
        self.result = result
        self.trades = trades
        self.analysis_key = analysis_key
        self.analysis = analysis
        # A plan slice's "result" is the child engine result and, for
        # historical key-order reasons, is inserted after the book fields.
        self.result_after_book = result_after_book
        # A pre-matching baseline business rejection (a price-limit breach)
        # occupies the per-symbol engine journal as well as the replay log.
        self.occupy_engine_id = occupy_engine_id


def book_changes(
    engine: Engine,
    before_bids: dict[int, int],
    before_asks: dict[int, int],
) -> dict[str, list[dict[str, int]]]:
    """Diff the visible level totals against a pre-dispatch capture."""
    bid_totals, ask_totals = engine.level_totals()
    bid_changes: list[dict[str, int]] = []
    for price in sorted(set(before_bids) | set(bid_totals), reverse=True):
        new_qty = bid_totals.get(price, 0)
        if before_bids.get(price, 0) != new_qty:
            bid_changes.append({"price": price, "quantity": new_qty})
    ask_changes: list[dict[str, int]] = []
    for price in sorted(set(before_asks) | set(ask_totals)):
        new_qty = ask_totals.get(price, 0)
        if before_asks.get(price, 0) != new_qty:
            ask_changes.append({"price": price, "quantity": new_qty})
    return {"bids": bid_changes, "asks": ask_changes}


def invalid_result(
    event: object,
    code: str = INVALID_EVENT,
    *,
    expected_sequence: int | None = None,
    state: _SymbolState | None = None,
) -> dict[str, object]:
    """The response for an event rejected before any commit happened."""
    event_id = symbol = None
    sequence = None
    if isinstance(event, dict):
        eid = event.get("event_id")
        if isinstance(eid, str) and eid != "":
            event_id = eid
        sym = event.get("symbol")
        if isinstance(sym, str) and sym != "":
            symbol = sym
        seq = event.get("sequence")
        if isinstance(seq, int) and not isinstance(seq, bool):
            sequence = seq
    result: dict[str, object] = {
        "event_id": event_id,
        "symbol": symbol,
        "sequence": sequence,
        "status": REJECTED,
        "rejection_code": code,
    }
    if expected_sequence is not None:
        result["expected_sequence"] = expected_sequence
    result["trades"] = []
    result["book_changes"] = {"bids": [], "asks": []}
    if state is None:
        # The event references an unknown symbol, so there is no book to
        # echo; it also must not create the symbol on its own.
        result["bids"] = []
        result["asks"] = []
    else:
        # A pre-dispatch failure against a known symbol echoes the
        # untouched book, exactly like a baseline rejection does.
        result["bids"], result["asks"] = state.engine.snapshot()
    return result


def price_limits_view(
    limits: tuple[int, int] | None,
) -> dict[str, object] | None:
    """The public ``active_price_limits`` shape (``None`` when unlimited)."""
    if limits is None:
        return None
    return {"lower_price": limits[0], "upper_price": limits[1]}


class _CommitContext:
    """Everything the shared commit/response path needs for one event.

    Created once validation, global idempotency and per-symbol sequencing
    have all passed: from that point the event is a committed application.
    The context owns symbol registration (a brand new security's book is
    installed exactly once), the deterministic id/sequence commit, the
    post-event book echo and the final response skeleton. It borrows the
    session's symbol table and global event log rather than the session
    itself, so a scratch historical-rebuild session commits through the very
    same code.
    """

    __slots__ = ("symbols", "events", "event_id", "event_type", "symbol",
                 "sequence", "content", "state", "known_symbol",
                 "before_bids", "before_asks", "_book_captured")

    def __init__(
        self,
        symbols: dict[str, _SymbolState],
        events: dict[str, tuple[str, str]],
        event_id: str,
        event_type: str,
        symbol: str,
        sequence: int,
        content: str,
        state: _SymbolState,
        known_symbol: bool,
    ) -> None:
        self.symbols = symbols
        self.events = events
        self.event_id = event_id
        self.event_type = event_type
        self.symbol = symbol
        self.sequence = sequence
        self.content = content
        self.state = state
        self.known_symbol = known_symbol
        self.before_bids: dict[int, int] | None = None
        self.before_asks: dict[int, int] | None = None
        self._book_captured = False

    def register_symbol(self) -> None:
        """Install a brand new symbol book; a no-op for a known symbol."""
        if not self.known_symbol:
            self.symbols[self.symbol] = self.state

    def capture_book(self) -> None:
        """Remember the pre-dispatch visible levels for the change diff.

        Baseline events and plan commands may move the book; read-only
        queries never do and therefore skip the capture.
        """
        bids, asks = self.state.engine.level_totals()
        self.before_bids = dict(bids)
        self.before_asks = dict(asks)
        self._book_captured = True

    def commit(self) -> None:
        """Deterministically occupy the event id and advance the sequence.

        The same three writes — symbol sequence, per-symbol content log,
        global idempotency index — happen exactly once for every
        structurally valid committed event, accepted or business-rejected.
        Replay-only ids never enter the per-symbol engine journal.
        """
        self.state.last_sequence = self.sequence
        self.state.seen[self.event_id] = self.content
        self.events[self.event_id] = (self.symbol, self.content)

    def finish(self, applied: _Applied) -> dict[str, object]:
        """Commit, echo the book and assemble the response in one place."""
        self.commit()
        if applied.occupy_engine_id:
            self.state.engine.occupy_event_id(self.event_id)
        bids, asks = self.state.engine.snapshot()
        if self._book_captured:
            changes = book_changes(
                self.state.engine, self.before_bids, self.before_asks
            )
        else:
            changes = {"bids": [], "asks": []}
        return self._build(applied, bids, asks, changes)

    def _build(
        self,
        applied: _Applied,
        bids: list[dict[str, int]],
        asks: list[dict[str, int]],
        changes: dict[str, list[dict[str, int]]],
    ) -> dict[str, object]:
        out: dict[str, object] = {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "sequence": self.sequence,
        }
        trades = applied.trades if applied.trades is not None else []
        if applied.status == ACCEPTED:
            out["status"] = ACCEPTED
            if applied.result is not None and not applied.result_after_book:
                out["result"] = applied.result
            out["trades"] = trades
            out["book_changes"] = changes
            out["bids"] = bids
            out["asks"] = asks
            if applied.result is not None and applied.result_after_book:
                out["result"] = applied.result
            if applied.analysis is not None:
                out[applied.analysis_key] = applied.analysis
            return out
        out["status"] = REJECTED
        out["rejection_code"] = applied.code
        out["trades"] = trades
        out["book_changes"] = changes
        out["bids"] = bids
        out["asks"] = asks
        return out
