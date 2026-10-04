"""Historical book reconstruction on a throwaway session.

The rebuild observes only the target security's committed event log: it
never re-runs a read-only report (so a reconstruction query can never
recurse) and it never touches the live session's trading state. The scratch
session is created through ``type(session)`` so this module stays
independent of the orchestrator that defines the session class.
"""

from __future__ import annotations

import json

from ..engine import REPORTED
from .constants import (
    ACCEPTED,
    REJECTED,
    TARGET_SEQUENCE_NOT_FOUND,
    _REPLAY_NOOP_TYPES,
)
from .state import (
    _Applied,
    _CommitContext,
    _SymbolState,
    price_limits_view,
)


def _reconstruct_book(
    session: object,
    symbol: str,
    target_sequence: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], tuple[int, int] | None]:
    """Rebuild one security's book as of a previously committed sequence.

    A fresh throwaway session with the same matching configuration is
    advanced by replaying this security's committed event log in
    sequence order — every entry of ``state.seen`` is one committed
    sequence, in first-seen order. The accepted read-only reports
    (EXECUTION_REPORT, IMPACT_REPORT, PORTFOLIO_REPORT,
    PORTFOLIO_STRESS_REPORT, SESSION_RECONCILIATION, PLAN_TCA_REPORT and
    earlier
    BOOK_RECONSTRUCTION_REPORT queries) occupied a sequence without
    moving anything, so they are committed to the scratch session as
    pure id/sequence markers and never re-dispatched (which also keeps a
    historical reconstruction query from recursing); every baseline
    event, every parent-order command and every PRICE_LIMIT_UPDATE is
    re-dispatched through the ordinary code path, so matching, iceberg
    replenishment, replacement queue loss, plan child orders, price
    rejections and the active interval all reproduce exactly. The
    scratch session is discarded afterwards, so the live session never
    matches, releases a plan slice or moves any trading state.
    """
    scratch = type(session)(session.config)
    for index, (logged_id, content_str) in enumerate(
        session._symbols[symbol].seen.items(), start=1
    ):
        stored_payload = json.loads(content_str)
        stored_type = stored_payload.get("type")
        if stored_type in _REPLAY_NOOP_TYPES:
            # A read-only report occupied the sequence but changed
            # nothing: mirror only its id/sequence commit through the
            # same commit primitive the live path uses, without ever
            # re-running the query.
            scratch_state = scratch._symbols.get(symbol)
            if scratch_state is None:
                scratch_state = _SymbolState()
                scratch_state.price_limits = scratch.price_limits.get(symbol)
                scratch._symbols[symbol] = scratch_state
            _CommitContext(
                scratch._symbols, scratch._events,
                logged_id, stored_type, symbol, index, content_str,
                scratch_state, known_symbol=True,
            ).commit()
        else:
            # The inline envelope form: payload fields plus the envelope
            # symbol and the sequence the log position implies.
            synthetic_event: dict[str, object] = {
                **stored_payload, "symbol": symbol, "sequence": index
            }
            scratch._submit_one(synthetic_event)
        if index == target_sequence:
            break

    scratch_state = scratch._symbols[symbol]
    bid_queues, ask_queues = scratch_state.engine.book_queue_view()
    return bid_queues, ask_queues, scratch_state.price_limits


def apply_book_reconstruction_report(
    session: object, ctx: _CommitContext, payload: dict[str, object]
) -> _Applied:
    """Answer a read-only historical book-queue query for one security.

    The query rebuilds the envelope security's resting queues as of
    ``target_sequence``: target ``0`` denotes the empty book and the
    session-initial price-limit interval before the first event; a
    positive target must be a sequence this security has already
    committed before the query. A target later than the security's last
    committed sequence is a ``TARGET_SEQUENCE_NOT_FOUND`` business
    rejection — unknown securities answer target ``0`` against the
    empty book and reject every positive target. The rebuild runs on a
    throwaway session and never matches, never releases a plan slice and
    never moves an order, a queue, the trade log, an account set, a
    plan, the active interval or the next trade id of the live session.
    The shared commit path occupies the query id and advances the symbol
    sequence, including the business rejection; the id lives solely in
    the replay log, exactly like a PLAN_TCA_REPORT id.
    """
    state = ctx.state
    symbol = ctx.symbol
    target_sequence: int = payload["target_sequence"]
    if target_sequence > state.last_sequence:
        # A committed business rejection: id and envelope sequence move,
        # the book and every other trading state do not.
        return _Applied(REJECTED, code=TARGET_SEQUENCE_NOT_FOUND)

    if target_sequence == 0:
        # The book before the first event is always empty; the active
        # interval is the session-initial one seeded from the static
        # configuration, whether or not the security has traded yet.
        bid_queues: list[dict[str, object]] = []
        ask_queues: list[dict[str, object]] = []
        target_limits = session.price_limits.get(symbol)
    else:
        bid_queues, ask_queues, target_limits = _reconstruct_book(
            session, symbol, target_sequence
        )

    return _Applied(
        ACCEPTED, result=REPORTED,
        analysis_key="book_reconstruction",
        analysis={
            "symbol": symbol,
            "target_sequence": target_sequence,
            "active_price_limits": price_limits_view(target_limits),
            "bid_queues": bid_queues,
            "ask_queues": ask_queues,
        },
    )
