"""Single-instrument order book with price-time priority matching.

The engine is deterministic: given the same sequence of event objects it
produces identical results, trade identifiers and book snapshots.
Rejected events never mutate engine state.
"""

from __future__ import annotations

import copy
import json
from collections import deque

ADD = "ADD"
CANCEL = "CANCEL"
REPLACE = "REPLACE"
EXECUTION_REPORT = "EXECUTION_REPORT"
ACCOUNT_REPORT = "ACCOUNT_REPORT"
DAY_END_RECONCILIATION = "DAY_END_RECONCILIATION"
BUY = "BUY"
SELL = "SELL"
LIMIT = "LIMIT"
MARKET = "MARKET"
ICEBERG = "ICEBERG"

GTC = "GTC"
IOC = "IOC"
FOK = "FOK"

FILLED = "FILLED"
RESTING = "RESTING"
REPLACED = "REPLACED"
PARTIALLY_FILLED_RESTING = "PARTIALLY_FILLED_RESTING"
PARTIALLY_FILLED_CANCELLED = "PARTIALLY_FILLED_CANCELLED"
PARTIALLY_FILLED_SELF_TRADE_PREVENTED = "PARTIALLY_FILLED_SELF_TRADE_PREVENTED"
UNFILLED_CANCELLED = "UNFILLED_CANCELLED"
SELF_TRADE_PREVENTED = "SELF_TRADE_PREVENTED"
CANCELLED = "CANCELLED"
REJECTED = "REJECTED"
REPORTED = "REPORTED"
RECONCILED = "RECONCILED"
BREAKS_FOUND = "BREAKS_FOUND"

MISSING_ACTUAL = "MISSING_ACTUAL"
MISSING_EXPECTED = "MISSING_EXPECTED"
FIELD_MISMATCH = "FIELD_MISMATCH"

MAKER = "MAKER"
TAKER = "TAKER"

INVALID_JSON = "INVALID_JSON"
INVALID_SCHEMA = "INVALID_SCHEMA"
DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
UNKNOWN_ORDER = "UNKNOWN_ORDER"
UNKNOWN_ACCOUNT = "UNKNOWN_ACCOUNT"

_ALL_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity", "account_id"}
)
_CANCEL_KEYS = frozenset({"event_id", "type", "order_id"})
_REPORT_KEYS = frozenset({"event_id", "type", "order_id", "benchmark_price"})
_ACCOUNT_REPORT_KEYS = frozenset({"event_id", "type", "account_id", "mark_price"})
_RECONCILIATION_KEYS = frozenset(
    {"event_id", "type", "expected_trades", "expected_accounts"}
)
_RECONCILED_TRADE_KEYS = frozenset(
    {"trade_id", "maker_order_id", "taker_order_id", "price", "quantity"}
)
_RECONCILED_ACCOUNT_KEYS = frozenset(
    {"account_id", "net_position", "cash_balance"}
)
_REQUIRED_ADD_KEYS = frozenset({"event_id", "type", "order_id", "side", "order_type", "quantity"})
_REQUIRED_REPLACE_KEYS = frozenset({"event_id", "type", "order_id", "quantity", "price"})
_ALLOWED_REPLACE_KEYS = _REQUIRED_REPLACE_KEYS | {"display_quantity"}


def _is_positive_int(value: object) -> bool:
    # ``bool`` is a subclass of ``int`` and must not be accepted.
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_non_empty_str(value: object) -> bool:
    return isinstance(value, str) and value != ""


def _is_int(value: object) -> bool:
    # Accounts may carry negative balances; ``bool`` is an ``int`` subclass and
    # must never be accepted as one.
    return isinstance(value, int) and not isinstance(value, bool)


class Engine:
    """Stateful replay engine processing events in arrival order."""

    def __init__(self, _state: dict[str, object] | None = None) -> None:
        if _state is not None:
            # Internal fast path used when restoring an authoritative snapshot:
            # the state dict is owned by the caller's snapshot machinery, not by
            # user input, so it can be adopted verbatim without copying.
            self._event_ids: set[str] = _state["event_ids"]
            self._order_ids: set[str] = _state["order_ids"]
            self._orders: dict[str, dict[str, object]] = _state["orders"]
            self._bids: dict[int, deque[str]] = _state["bids"]
            self._asks: dict[int, deque[str]] = _state["asks"]
            self._bid_totals: dict[int, int] = _state["bid_totals"]
            self._ask_totals: dict[int, int] = _state["ask_totals"]
            self._next_trade_id: int = _state["next_trade_id"]
            self._accounts: set[str] = _state["accounts"]
            self._trade_log: list[dict[str, object]] = _state["trade_log"]
            return
        self._event_ids = set()
        self._order_ids = set()
        self._orders = {}
        self._bids = {}
        self._asks = {}
        self._bid_totals = {}
        self._ask_totals = {}
        self._next_trade_id = 1
        # Accounts seen on any accepted ADD, however the orders ended up.
        self._accounts = set()
        # Every trade ever executed, in trade_id order, tagged with the id of
        # the event that produced it. Execution reports are built from this
        # journal; matching output keeps its historical shape.
        self._trade_log = []

    def handle_line(self, line: str) -> tuple[str | None, str, str | None, list[dict[str, object]]]:
        """Process one input line (without line terminator).

        Returns ``(event_id, result, reason, trades)`` where ``event_id`` is
        the event id when it can be obtained as a string, otherwise ``None``,
        and ``reason`` is set only for rejected events.
        """
        event_id, result, reason, trades, _stp, _analysis, _position, _recon = (
            self.handle_line_reconciliation(line)
        )
        return event_id, result, reason, trades

    def handle_line_full(
        self, line: str
    ) -> tuple[str | None, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        """Like :meth:`handle_line`, additionally returning the self-trade
        prevention descriptor (``None`` for every other result)."""
        event_id, result, reason, trades, stp, _analysis, _position, _recon = (
            self.handle_line_reconciliation(line)
        )
        return event_id, result, reason, trades, stp

    def handle_line_extended(
        self, line: str
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None, dict[str, object] | None,
    ]:
        """Like :meth:`handle_line_full`, additionally returning the execution
        analysis of an ``EXECUTION_REPORT`` query (``None`` otherwise)."""
        event_id, result, reason, trades, stp, analysis, _position, _recon = (
            self.handle_line_reconciliation(line)
        )
        return event_id, result, reason, trades, stp, analysis

    def handle_line_position(
        self, line: str
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None,
    ]:
        """Like :meth:`handle_line_extended`, additionally returning the
        position analysis of an ``ACCOUNT_REPORT`` query (``None``
        otherwise)."""
        event_id, result, reason, trades, stp, analysis, position, _recon = (
            self.handle_line_reconciliation(line)
        )
        return event_id, result, reason, trades, stp, analysis, position

    def handle_line_reconciliation(
        self, line: str
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None, dict[str, object] | None,
    ]:
        """Like :meth:`handle_line_position`, additionally returning the
        reconciliation report of a ``DAY_END_RECONCILIATION`` query (``None``
        otherwise)."""
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None, REJECTED, INVALID_JSON, [], None, None, None, None
        return self.handle_object_reconciliation(obj)

    def handle_object(
        self, obj: object
    ) -> tuple[str | None, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        event_id, result, reason, trades, stp, _analysis, _position, _recon = (
            self.handle_object_reconciliation(obj)
        )
        return event_id, result, reason, trades, stp

    def handle_object_extended(
        self, obj: object
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None, dict[str, object] | None,
    ]:
        event_id, result, reason, trades, stp, analysis, _position, _recon = (
            self.handle_object_reconciliation(obj)
        )
        return event_id, result, reason, trades, stp, analysis

    def handle_object_position(
        self, obj: object
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None,
    ]:
        event_id, result, reason, trades, stp, analysis, position, _recon = (
            self.handle_object_reconciliation(obj)
        )
        return event_id, result, reason, trades, stp, analysis, position

    def handle_object_reconciliation(
        self, obj: object
    ) -> tuple[
        str | None, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None, dict[str, object] | None,
    ]:
        if not isinstance(obj, dict):
            return None, REJECTED, INVALID_SCHEMA, [], None, None, None, None

        event_id = obj.get("event_id")
        event_id_out = event_id if isinstance(event_id, str) else None

        schema_error = self._schema_error(obj)
        if schema_error is not None:
            return event_id_out, REJECTED, schema_error, [], None, None, None, None

        if event_id in self._event_ids:
            return event_id, REJECTED, DUPLICATE_EVENT_ID, [], None, None, None, None
        if obj["type"] == REPLACE and "display_quantity" in obj:
            target = self._orders.get(obj["order_id"])
            if (
                target is not None
                and target["status"] == RESTING
                and "display_quantity" not in target
            ):
                # Only an iceberg target may be replaced with a display slice;
                # like every schema error this consumes no event id.
                return event_id, REJECTED, INVALID_SCHEMA, [], None, None, None, None
        # The event is well formed, so its id occupies the stream from here,
        # even if a later business rule rejects it.
        self._event_ids.add(event_id)

        if obj["type"] == CANCEL:
            event_id, result, reason, trades = self._cancel(event_id, obj["order_id"])
            return event_id, result, reason, trades, None, None, None, None
        if obj["type"] == REPLACE:
            event_id, result, reason, trades, stp = self._replace(event_id, obj)
            return event_id, result, reason, trades, stp, None, None, None
        if obj["type"] == EXECUTION_REPORT:
            event_id, result, reason, trades, stp, analysis = self._execution_report(
                event_id, obj
            )
            return event_id, result, reason, trades, stp, analysis, None, None
        if obj["type"] == ACCOUNT_REPORT:
            event_id, result, reason, trades, stp, analysis, position = (
                self._account_report(event_id, obj)
            )
            return event_id, result, reason, trades, stp, analysis, position, None
        if obj["type"] == DAY_END_RECONCILIATION:
            return self._day_end_reconciliation(event_id, obj)
        event_id, result, reason, trades, stp = self._add(event_id, obj)
        return event_id, result, reason, trades, stp, None, None, None

    @staticmethod
    def _schema_error(obj: dict[str, object]) -> str | None:
        keys = set(obj)
        event_type = obj.get("type")
        if event_type == EXECUTION_REPORT:
            # A pure query: exactly the four fields, string identifiers and a
            # positive integer benchmark (booleans are not integers).
            if keys != _REPORT_KEYS:
                return INVALID_SCHEMA
            if not isinstance(obj.get("event_id"), str):
                return INVALID_SCHEMA
            if not isinstance(obj.get("order_id"), str):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("benchmark_price")):
                return INVALID_SCHEMA
            return None
        if event_type == ACCOUNT_REPORT:
            # A pure query: exactly the four fields, non-empty string
            # identifiers and a positive integer mark (booleans are not
            # integers).
            if keys != _ACCOUNT_REPORT_KEYS:
                return INVALID_SCHEMA
            if not _is_non_empty_str(obj.get("event_id")):
                return INVALID_SCHEMA
            if not _is_non_empty_str(obj.get("account_id")):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("mark_price")):
                return INVALID_SCHEMA
            return None
        if event_type == DAY_END_RECONCILIATION:
            # A pure query: exactly the four fields, non-empty event id and two
            # arrays. Every member must carry exactly its documented fields
            # with the right scalar types, and duplicated identifiers inside
            # either array are a structural error. Booleans never count as
            # integers and empty strings never as identifiers.
            if keys != _RECONCILIATION_KEYS:
                return INVALID_SCHEMA
            if not _is_non_empty_str(obj.get("event_id")):
                return INVALID_SCHEMA
            expected_trades = obj.get("expected_trades")
            expected_accounts = obj.get("expected_accounts")
            if not isinstance(expected_trades, list) or not isinstance(expected_accounts, list):
                return INVALID_SCHEMA
            seen_trade_ids: set[int] = set()
            for trade in expected_trades:
                if not isinstance(trade, dict) or set(trade) != _RECONCILED_TRADE_KEYS:
                    return INVALID_SCHEMA
                if not _is_positive_int(trade.get("trade_id")):
                    return INVALID_SCHEMA
                if not _is_non_empty_str(trade.get("maker_order_id")):
                    return INVALID_SCHEMA
                if not _is_non_empty_str(trade.get("taker_order_id")):
                    return INVALID_SCHEMA
                if not _is_positive_int(trade.get("price")):
                    return INVALID_SCHEMA
                if not _is_positive_int(trade.get("quantity")):
                    return INVALID_SCHEMA
                if trade["trade_id"] in seen_trade_ids:
                    return INVALID_SCHEMA
                seen_trade_ids.add(trade["trade_id"])
            seen_account_ids: set[str] = set()
            for account in expected_accounts:
                if not isinstance(account, dict) or set(account) != _RECONCILED_ACCOUNT_KEYS:
                    return INVALID_SCHEMA
                if not _is_non_empty_str(account.get("account_id")):
                    return INVALID_SCHEMA
                if not _is_int(account.get("net_position")):
                    return INVALID_SCHEMA
                if not _is_int(account.get("cash_balance")):
                    return INVALID_SCHEMA
                if account["account_id"] in seen_account_ids:
                    return INVALID_SCHEMA
                seen_account_ids.add(account["account_id"])
            return None
        if not keys <= _ALL_KEYS:
            return INVALID_SCHEMA
        if not isinstance(obj.get("event_id"), str):
            return INVALID_SCHEMA

        if event_type == CANCEL:
            if keys != _CANCEL_KEYS:
                return INVALID_SCHEMA
            if not isinstance(obj.get("order_id"), str):
                return INVALID_SCHEMA
            return None

        if event_type == REPLACE:
            # Side and order type are inherited from the target, so the event
            # must not restate them; time-in-force is always GTC.
            if not keys >= _REQUIRED_REPLACE_KEYS:
                return INVALID_SCHEMA
            if not keys <= _ALLOWED_REPLACE_KEYS:
                return INVALID_SCHEMA
            if not isinstance(obj.get("order_id"), str):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("quantity")):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("price")):
                return INVALID_SCHEMA
            if "display_quantity" in obj:
                display_quantity = obj["display_quantity"]
                if (
                    not _is_positive_int(display_quantity)
                    or display_quantity > obj["quantity"]
                ):
                    return INVALID_SCHEMA
            return None

        if event_type != ADD:
            return INVALID_SCHEMA
        if not keys >= _REQUIRED_ADD_KEYS:
            return INVALID_SCHEMA

        if not isinstance(obj.get("order_id"), str):
            return INVALID_SCHEMA
        if obj.get("side") not in (BUY, SELL):
            return INVALID_SCHEMA
        order_type = obj.get("order_type")
        if order_type not in (LIMIT, MARKET, ICEBERG):
            return INVALID_SCHEMA
        if not _is_positive_int(obj.get("quantity")):
            return INVALID_SCHEMA
        if "account_id" in obj and not (
            isinstance(obj.get("account_id"), str) and obj["account_id"] != ""
        ):
            # The account tag is optional but may only be a non-empty string.
            return INVALID_SCHEMA
        if order_type != ICEBERG and "display_quantity" in obj:
            # Only iceberg orders carry a display slice size.
            return INVALID_SCHEMA

        if order_type == ICEBERG:
            # Iceberg orders are always-priced GTC limit orders whose single
            # maximum visible slice must not exceed the total quantity.
            if not _is_positive_int(obj.get("price")):
                return INVALID_SCHEMA
            display_quantity = obj.get("display_quantity")
            if not _is_positive_int(display_quantity) or display_quantity > obj["quantity"]:
                return INVALID_SCHEMA
            tif = obj.get("time_in_force")
            if "time_in_force" in obj and tif != GTC:
                # Only an omitted field or explicit GTC are accepted.
                return INVALID_SCHEMA
            return None

        if "price" in obj:
            price = obj["price"]
            if order_type == LIMIT:
                if not _is_positive_int(price):
                    return INVALID_SCHEMA
            elif price is not None:
                # MARKET orders must not carry a non-null price.
                return INVALID_SCHEMA
        elif order_type == LIMIT:
            return INVALID_SCHEMA

        if "time_in_force" in obj:
            tif = obj["time_in_force"]
            if tif not in (GTC, IOC, FOK):
                # Non-string values and unknown time-in-force values.
                return INVALID_SCHEMA
            if order_type == MARKET and tif == GTC:
                return INVALID_SCHEMA

        return None

    def _books(self, side: str) -> tuple[dict[int, deque[str]], dict[int, int]]:
        """The book and visible totals on which ``side`` rests."""
        if side == BUY:
            return self._bids, self._bid_totals
        return self._asks, self._ask_totals

    def _opposite(self, side: str) -> tuple[dict[int, deque[str]], dict[int, int]]:
        """The book and visible totals against which ``side`` matches."""
        if side == BUY:
            return self._asks, self._ask_totals
        return self._bids, self._bid_totals

    @staticmethod
    def _tradable_fn(side: str, limit: int | None):
        if side == BUY:
            def tradable(price: int) -> bool:
                return limit is None or price <= limit
        else:
            def tradable(price: int) -> bool:
                return limit is None or price >= limit
        return tradable

    def _add(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[str, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        order_id: str = obj["order_id"]
        if order_id in self._order_ids:
            return event_id, REJECTED, DUPLICATE_ORDER_ID, [], None
        self._order_ids.add(order_id)

        side: str = obj["side"]
        order_type: str = obj["order_type"]
        quantity: int = obj["quantity"]
        remaining: int = quantity
        is_iceberg = order_type == ICEBERG
        limit: int | None = obj["price"] if order_type in (LIMIT, ICEBERG) else None
        # LIMIT/ICEBERG default to GTC; MARKET keeps its historical
        # immediate-or-cancel semantics. MARKET with explicit GTC and ICEBERG
        # with any other time-in-force are rejected during schema checks.
        tif: str = obj.get("time_in_force") or (
            GTC if order_type in (LIMIT, ICEBERG) else IOC
        )
        display_quantity: int | None = (
            obj["display_quantity"] if is_iceberg else None
        )
        account_id: str | None = obj.get("account_id")
        if account_id is not None:
            # Any accepted ADD makes its account known, regardless of how the
            # order itself ends up.
            self._accounts.add(account_id)

        # FOK must either match the whole quantity against the pre-event book
        # or do nothing at all: no trades, no book change, no trade ids spent.
        # The precheck walks the book in the same price-time order a real fill
        # would; a resting iceberg offers its full remaining quantity here,
        # including reserve that is not part of the visible book totals. A
        # same-account resting order reached before the quantity is met aborts
        # the event atomically, before any trade could occur.
        if tif == FOK:
            fok = self._fok_probe(side, limit, quantity, account_id)
            if fok is not None:
                record: dict[str, object] = {
                    "side": side,
                    "price": limit,
                    "remaining": quantity,
                    "status": CANCELLED,
                }
                if account_id is not None:
                    record["account_id"] = account_id
                self._orders[order_id] = record
                if fok is False:
                    return event_id, UNFILLED_CANCELLED, None, [], None
                stp = {
                    "maker_order_id": fok,
                    "taker_order_id": order_id,
                    "cancelled_quantity": quantity,
                }
                return event_id, SELF_TRADE_PREVENTED, None, [], stp

        remaining, trades, stp_maker = self._match(
            event_id, order_id, side, limit, remaining, account_id
        )

        record = {"side": side, "price": limit}
        if account_id is not None:
            record["account_id"] = account_id
        stp: dict[str, object] | None = None
        if stp_maker is not None:
            # The whole leftover is cancelled; the blocking passive order is
            # neither traded nor displaced, and no later liquidity is sought.
            result = (
                PARTIALLY_FILLED_SELF_TRADE_PREVENTED if trades else SELF_TRADE_PREVENTED
            )
            record["remaining"] = remaining
            record["status"] = CANCELLED
            stp = {
                "maker_order_id": stp_maker,
                "taker_order_id": order_id,
                "cancelled_quantity": remaining,
            }
        elif remaining == 0:
            result = FILLED
            record["remaining"] = 0
            record["status"] = FILLED
        elif order_type in (LIMIT, ICEBERG) and tif == GTC:
            result = PARTIALLY_FILLED_RESTING if trades else RESTING
            record["remaining"] = remaining
            record["status"] = RESTING
            self._rest(
                order_id, side, limit, remaining,
                display_quantity if is_iceberg else None, record,
            )
        else:
            # IOC leftovers never enter the book; a successful FOK is always
            # fully filled by construction of the pre-event availability check.
            result = PARTIALLY_FILLED_CANCELLED if trades else UNFILLED_CANCELLED
            record["remaining"] = remaining
            record["status"] = CANCELLED

        self._orders[order_id] = record
        return event_id, result, None, trades, stp

    def _fok_probe(
        self, side: str, limit: int | None, quantity: int, account_id: str | None
    ) -> str | bool | None:
        """Probe a FOK request against the pre-event book without mutating it.

        Returns ``None`` when the whole quantity can be filled, ``False`` when
        liquidity runs short, or the id of the first same-account resting order
        reached before the quantity could be met. The walk mirrors a real fill:
        price-time priority across and within levels, and an exhausted iceberg
        slice replenishes at the tail of its level (including the hidden
        reserve) before the level is drained. Queues and slice sizes are copied
        for the simulation so the live book stays untouched.
        """
        opposite, _totals = self._opposite(side)
        tradable = self._tradable_fn(side, limit)
        need = quantity
        prices = sorted(opposite, reverse=side == SELL)
        for price in prices:
            if not tradable(price):
                continue
            sim_remaining: dict[str, int] = {}
            sim_visible: dict[str, int] = {}
            queue: deque[str] = deque()
            for maker_id in opposite[price]:
                maker = self._orders[maker_id]
                sim_remaining[maker_id] = maker["remaining"]
                if maker.get("visible") is not None:
                    sim_visible[maker_id] = maker["visible"]
                queue.append(maker_id)
            while queue and need > 0:
                maker_id = queue.popleft()
                maker = self._orders[maker_id]
                if account_id is not None and maker.get("account_id") == account_id:
                    return maker_id
                if maker_id in sim_visible:
                    available = sim_visible[maker_id]
                else:
                    available = sim_remaining[maker_id]
                need -= available
                sim_remaining[maker_id] -= available
                if maker_id in sim_visible:
                    sim_visible[maker_id] -= available
                    if sim_visible[maker_id] == 0 and sim_remaining[maker_id] > 0:
                        # The replenished slice waits behind every order already
                        # visible at this level, exactly as in a real fill.
                        sim_visible[maker_id] = min(
                            maker["display_quantity"], sim_remaining[maker_id]
                        )
                        queue.append(maker_id)
            if need <= 0:
                return None
        return False

    def _match(
        self,
        event_id: str,
        order_id: str,
        side: str,
        limit: int | None,
        remaining: int,
        account_id: str | None,
    ) -> tuple[int, list[dict[str, object]], str | None]:
        """Match ``order_id`` as taker.

        Returns its leftover, the trades already produced, and the id of the
        first same-account passive order reached (``None`` when matching ended
        normally). On a self-trade block no further liquidity is consulted and
        the blocking order is left exactly as it was.
        """
        opposite, totals = self._opposite(side)
        tradable = self._tradable_fn(side, limit)

        trades: list[dict[str, object]] = []
        blocked_by: str | None = None
        while remaining > 0:
            candidates = [price for price in totals if tradable(price)]
            if not candidates:
                break
            price = min(candidates) if side == BUY else max(candidates)
            queue = opposite[price]
            while remaining > 0 and queue:
                maker_id = queue[0]
                maker = self._orders[maker_id]
                if account_id is not None and maker.get("account_id") == account_id:
                    # No trade, no queue/remaining/slice change; the taker
                    # leftover is cancelled by the caller without skipping on.
                    blocked_by = maker_id
                    return remaining, trades, blocked_by
                if maker.get("visible") is not None:
                    # A resting iceberg can only trade its current slice; the
                    # reserve stays hidden until the slice is exhausted.
                    maker_available = maker["visible"]
                else:
                    maker_available = maker["remaining"]
                matched = min(remaining, maker_available)
                maker["remaining"] -= matched
                remaining -= matched
                totals[price] -= matched
                trade: dict[str, object] = {
                    "trade_id": self._next_trade_id,
                    "maker_order_id": maker_id,
                    "taker_order_id": order_id,
                    "price": price,
                    "quantity": matched,
                }
                trades.append(trade)
                self._trade_log.append({**trade, "event_id": event_id})
                self._next_trade_id += 1
                if maker.get("visible") is not None:
                    maker["visible"] -= matched
                    if maker["visible"] == 0:
                        # The slice leaves the front immediately. With reserve
                        # left, the next slice joins behind every order already
                        # visible at this price, so this same taker may meet the
                        # order again after those orders.
                        queue.popleft()
                        if maker["remaining"] > 0:
                            new_slice = min(
                                maker["display_quantity"], maker["remaining"]
                            )
                            maker["visible"] = new_slice
                            totals[price] += new_slice
                            queue.append(maker_id)
                        else:
                            maker["status"] = FILLED
                elif maker["remaining"] == 0:
                    queue.popleft()
                    maker["status"] = FILLED
            if not queue:
                del opposite[price]
                del totals[price]
        return remaining, trades, blocked_by

    def _rest(
        self,
        order_id: str,
        side: str,
        limit: int,
        remaining: int,
        display_quantity: int | None,
        record: dict[str, object],
    ) -> None:
        """Enter the leftover at the back of its price level's queue."""
        own_book, own_totals = self._books(side)
        own_book.setdefault(limit, deque()).append(order_id)
        if display_quantity is not None:
            # Only the first slice enters the book; the rest is reserve.
            visible = min(display_quantity, remaining)
            record["display_quantity"] = display_quantity
            record["visible"] = visible
        else:
            visible = remaining
        own_totals[limit] = own_totals.get(limit, 0) + visible

    def _remove_from_book(self, order_id: str, record: dict[str, object]) -> None:
        """Remove a resting order's visible quantity from its price level."""
        own_book, own_totals = self._books(record["side"])
        price = record["price"]
        queue = own_book[price]
        queue.remove(order_id)
        # Book totals hold only the current visible slice of an iceberg; its
        # reserve leaves with the order but was never aggregated.
        visible = record.get("visible", record["remaining"])
        own_totals[price] -= visible
        if own_totals[price] == 0:
            del own_totals[price]
            del own_book[price]

    def _replace(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[str, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        order_id: str = obj["order_id"]
        record = self._orders.get(order_id)
        if record is None or record["status"] != RESTING:
            return event_id, REJECTED, UNKNOWN_ORDER, [], None

        side: str = record["side"]
        # The replacement trades under the target's account; the event itself
        # is forbidden from carrying account_id by the schema checks.
        account_id: str | None = record.get("account_id")
        is_iceberg = "display_quantity" in record
        display_quantity = obj.get("display_quantity")
        if display_quantity is None:
            # An omitted slice size keeps the target's original peak.
            display_quantity = record.get("display_quantity")

        # Removal, matching and re-resting are one indivisible state change:
        # the old remainder leaves the book before the replacement arrives,
        # so the new order can never trade against its own previous state.
        self._remove_from_book(order_id, record)

        price: int = obj["price"]
        remaining, trades, stp_maker = self._match(
            event_id, order_id, side, price, obj["quantity"], account_id
        )

        new_record: dict[str, object] = {"side": side, "price": price}
        if account_id is not None:
            new_record["account_id"] = account_id
        stp: dict[str, object] | None = None
        if stp_maker is not None:
            # The old remainder was already removed and is not restored; the
            # replacement is cancelled with whatever it had left.
            result = (
                PARTIALLY_FILLED_SELF_TRADE_PREVENTED if trades else SELF_TRADE_PREVENTED
            )
            new_record["remaining"] = remaining
            new_record["status"] = CANCELLED
            stp = {
                "maker_order_id": stp_maker,
                "taker_order_id": order_id,
                "cancelled_quantity": remaining,
            }
        elif remaining == 0:
            result = FILLED
            new_record["remaining"] = 0
            new_record["status"] = FILLED
        else:
            # The replacement is a fresh GTC arrival and always rests.
            result = PARTIALLY_FILLED_RESTING if trades else REPLACED
            new_record["remaining"] = remaining
            new_record["status"] = RESTING
            self._rest(
                order_id, side, price, remaining,
                display_quantity if is_iceberg else None, new_record,
            )

        # The order id stays occupied: the replacement keeps it and no later
        # ADD may reuse it.
        self._orders[order_id] = new_record
        return event_id, result, None, trades, stp

    def _cancel(
        self, event_id: str, order_id: str
    ) -> tuple[str, str, str | None, list[dict[str, object]]]:
        record = self._orders.get(order_id)
        if record is None or record["status"] != RESTING:
            return event_id, REJECTED, UNKNOWN_ORDER, []

        self._remove_from_book(order_id, record)
        record["status"] = CANCELLED
        record["remaining"] = 0
        return event_id, CANCELLED, None, []

    def _execution_report(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[
        str, str, str | None,
        list[dict[str, object]], dict[str, object] | None, dict[str, object] | None,
    ]:
        """Answer a cumulative execution query without touching any state.

        Every trade the order ever took part in is attributed to its original
        ``order_id``: replacements keep the id and iceberg replenishment slices
        trade under the same maker id, so the journal lookup alone gathers the
        full history, whether the order acted as maker or taker.
        """
        order_id: str = obj["order_id"]
        record = self._orders.get(order_id)
        if record is None:
            return event_id, REJECTED, UNKNOWN_ORDER, [], None, None

        side: str = record["side"]
        status: str = record["status"]
        # A resting order's remaining total includes the iceberg reserve; a
        # finished order has nothing open.
        open_quantity = record["remaining"] if status == RESTING else 0

        attribution: list[dict[str, object]] = []
        filled_quantity = 0
        executed_notional = 0
        for trade in self._trade_log:
            if trade["maker_order_id"] == order_id:
                role = MAKER
                counterparty = trade["taker_order_id"]
            elif trade["taker_order_id"] == order_id:
                role = TAKER
                counterparty = trade["maker_order_id"]
            else:
                continue
            filled_quantity += trade["quantity"]
            executed_notional += trade["price"] * trade["quantity"]
            attribution.append(
                {
                    "trade_id": trade["trade_id"],
                    "role": role,
                    "counterparty_order_id": counterparty,
                    "event_id": trade["event_id"],
                    "price": trade["price"],
                    "quantity": trade["quantity"],
                }
            )

        if filled_quantity:
            vwap: dict[str, int] | None = {
                "numerator": executed_notional,
                "denominator": filled_quantity,
            }
        else:
            vwap = None
        benchmark_price: int = obj["benchmark_price"]
        slippage = executed_notional - benchmark_price * filled_quantity
        if side == SELL:
            # Mirror the buy formula: negative always means improvement.
            slippage = -slippage

        analysis: dict[str, object] = {
            "side": side,
            "current_status": status,
            "open_quantity": open_quantity,
            "filled_quantity": filled_quantity,
            "executed_notional": executed_notional,
            "vwap": vwap,
            "slippage_notional": slippage,
            "trade_attribution": attribution,
        }
        return event_id, REPORTED, None, [], None, analysis

    def _account_report(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[
        str, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None,
    ]:
        """Answer a cumulative account query without touching any state.

        Every trade is attributed to an account through the order it was
        resting or arriving as: replacements keep the order id and its
        account, and iceberg replenishment slices trade under the same maker
        id, so the journal lookup alone gathers the full history. Orders
        without an ``account_id`` never contribute.
        """
        account_id: str = obj["account_id"]
        if account_id not in self._accounts:
            return event_id, REJECTED, UNKNOWN_ACCOUNT, [], None, None, None

        buy_quantity = 0
        sell_quantity = 0
        buy_notional = 0
        sell_notional = 0
        for trade in self._trade_log:
            for order_key in ("maker_order_id", "taker_order_id"):
                order = self._orders[trade[order_key]]
                if order.get("account_id") != account_id:
                    continue
                notional = trade["price"] * trade["quantity"]
                if order["side"] == BUY:
                    buy_quantity += trade["quantity"]
                    buy_notional += notional
                else:
                    sell_quantity += trade["quantity"]
                    sell_notional += notional

        net_position = buy_quantity - sell_quantity
        if buy_quantity:
            buy_vwap: dict[str, int] | None = {
                "numerator": buy_notional,
                "denominator": buy_quantity,
            }
        else:
            buy_vwap = None
        if sell_quantity:
            sell_vwap: dict[str, int] | None = {
                "numerator": sell_notional,
                "denominator": sell_quantity,
            }
        else:
            sell_vwap = None

        mark_price: int = obj["mark_price"]
        analysis: dict[str, object] = {
            "account_id": account_id,
            "mark_price": mark_price,
            "buy_quantity": buy_quantity,
            "sell_quantity": sell_quantity,
            "net_position": net_position,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "buy_vwap": buy_vwap,
            "sell_vwap": sell_vwap,
            "turnover_notional": buy_notional + sell_notional,
            "risk_exposure": abs(net_position) * mark_price,
            "mark_to_market_pnl": (
                sell_notional - buy_notional + net_position * mark_price
            ),
        }
        return event_id, REPORTED, None, [], None, None, analysis

    def _day_end_reconciliation(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[
        str, str, str | None,
        list[dict[str, object]], dict[str, object] | None,
        dict[str, object] | None, dict[str, object] | None, dict[str, object] | None,
    ]:
        """Compare the caller's cumulative records against engine state.

        A pure read over the same trade journal and account set the other
        queries use; it touches nothing. The actual trade set is the whole
        journal (replacements keep the order id and iceberg slices the maker
        id, so every historical trade is present exactly once). The actual
        account set is the accounts established by accepted ADDs, including
        accounts that never traded. Cash is sell proceeds minus buy cost over
        every trade the account took as maker or taker.
        """
        actual_trades: dict[int, dict[str, object]] = {}
        for trade in self._trade_log:
            actual_trades[trade["trade_id"]] = {
                "trade_id": trade["trade_id"],
                "maker_order_id": trade["maker_order_id"],
                "taker_order_id": trade["taker_order_id"],
                "price": trade["price"],
                "quantity": trade["quantity"],
            }

        actual_accounts: dict[str, dict[str, object]] = {}
        for account_id in self._accounts:
            net_position = 0
            cash_balance = 0
            for trade in self._trade_log:
                for order_key in ("maker_order_id", "taker_order_id"):
                    order = self._orders[trade[order_key]]
                    if order.get("account_id") != account_id:
                        continue
                    notional = trade["price"] * trade["quantity"]
                    if order["side"] == BUY:
                        net_position += trade["quantity"]
                        cash_balance -= notional
                    else:
                        net_position -= trade["quantity"]
                        cash_balance += notional
            actual_accounts[account_id] = {
                "account_id": account_id,
                "net_position": net_position,
                "cash_balance": cash_balance,
            }

        expected_trades: dict[int, dict[str, object]] = {
            trade["trade_id"]: trade for trade in obj["expected_trades"]
        }
        expected_accounts: dict[str, dict[str, object]] = {
            account["account_id"]: account for account in obj["expected_accounts"]
        }

        trade_breaks: list[dict[str, object]] = []
        for trade_id in sorted(set(expected_trades) | set(actual_trades)):
            expected = expected_trades.get(trade_id)
            actual = actual_trades.get(trade_id)
            if expected is None:
                reason = MISSING_EXPECTED
            elif actual is None:
                reason = MISSING_ACTUAL
            elif expected != actual:
                reason = FIELD_MISMATCH
            else:
                continue
            trade_breaks.append(
                {
                    "identifier": trade_id,
                    "expected": expected,
                    "actual": actual,
                    "reason": reason,
                }
            )

        account_breaks: list[dict[str, object]] = []
        for account_id in sorted(set(expected_accounts) | set(actual_accounts)):
            expected = expected_accounts.get(account_id)
            actual = actual_accounts.get(account_id)
            if expected is None:
                reason = MISSING_EXPECTED
            elif actual is None:
                reason = MISSING_ACTUAL
            elif expected != actual:
                reason = FIELD_MISMATCH
            else:
                continue
            account_breaks.append(
                {
                    "identifier": account_id,
                    "expected": expected,
                    "actual": actual,
                    "reason": reason,
                }
            )

        reconciliation = {
            "trade_breaks": trade_breaks,
            "account_breaks": account_breaks,
        }
        result = RECONCILED if not trade_breaks and not account_breaks else BREAKS_FOUND
        return event_id, result, None, [], None, None, None, reconciliation

    def snapshot(self) -> tuple[list[dict[str, int]], list[dict[str, int]]]:
        bids = [
            {"price": price, "quantity": self._bid_totals[price]}
            for price in sorted(self._bid_totals, reverse=True)
        ]
        asks = [
            {"price": price, "quantity": self._ask_totals[price]}
            for price in sorted(self._ask_totals)
        ]
        return bids, asks

    def level_totals(self) -> tuple[dict[int, int], dict[int, int]]:
        """Raw visible-quantity aggregates keyed by price (bid, then ask)."""
        return self._bid_totals, self._ask_totals

    def dump_state(self) -> dict[str, object]:
        """Export the complete mutable engine state for an authoritative owner.

        Containers are deep-copied so later processing cannot mutate the export.
        The state includes enough to preserve price-time queue priority, order
        remainders, iceberg slice/reserve bookkeeping, spent event/order ids,
        the cumulative trade journal and the next trade id counter.
        """
        return {
            "event_ids": set(self._event_ids),
            "order_ids": set(self._order_ids),
            "orders": copy.deepcopy(self._orders),
            "bids": {p: deque(q) for p, q in self._bids.items()},
            "asks": {p: deque(q) for p, q in self._asks.items()},
            "bid_totals": dict(self._bid_totals),
            "ask_totals": dict(self._ask_totals),
            "next_trade_id": self._next_trade_id,
            "accounts": set(self._accounts),
            "trade_log": copy.deepcopy(self._trade_log),
        }

    def load_state(self, state: dict[str, object]) -> Engine:
        """Replace this engine's whole state from an authoritative export."""
        self._event_ids = set(state["event_ids"])
        self._order_ids = set(state["order_ids"])
        self._orders = copy.deepcopy(state["orders"])
        self._bids = {p: deque(q) for p, q in state["bids"].items()}
        self._asks = {p: deque(q) for p, q in state["asks"].items()}
        self._bid_totals = dict(state["bid_totals"])
        self._ask_totals = dict(state["ask_totals"])
        self._next_trade_id = state["next_trade_id"]
        self._accounts = set(state["accounts"])
        self._trade_log = copy.deepcopy(state["trade_log"])
        return self
