"""Single-instrument order book with price-time priority matching.

The engine is deterministic: given the same sequence of event objects it
produces identical results, trade identifiers and book snapshots.
Rejected events never mutate engine state.
"""

from __future__ import annotations

import json
from collections import deque

ADD = "ADD"
CANCEL = "CANCEL"
REPLACE = "REPLACE"
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

INVALID_JSON = "INVALID_JSON"
INVALID_SCHEMA = "INVALID_SCHEMA"
DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
UNKNOWN_ORDER = "UNKNOWN_ORDER"

_ALL_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity", "account_id"}
)
_CANCEL_KEYS = frozenset({"event_id", "type", "order_id"})
_REQUIRED_ADD_KEYS = frozenset({"event_id", "type", "order_id", "side", "order_type", "quantity"})
_REQUIRED_REPLACE_KEYS = frozenset({"event_id", "type", "order_id", "quantity", "price"})
_ALLOWED_REPLACE_KEYS = _REQUIRED_REPLACE_KEYS | {"display_quantity"}


def _is_positive_int(value: object) -> bool:
    # ``bool`` is a subclass of ``int`` and must not be accepted.
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


class Engine:
    """Stateful replay engine processing events in arrival order."""

    def __init__(self) -> None:
        self._event_ids: set[str] = set()
        self._order_ids: set[str] = set()
        self._orders: dict[str, dict[str, object]] = {}
        self._bids: dict[int, deque[str]] = {}
        self._asks: dict[int, deque[str]] = {}
        self._bid_totals: dict[int, int] = {}
        self._ask_totals: dict[int, int] = {}
        self._next_trade_id = 1

    def handle_line(self, line: str) -> tuple[str | None, str, str | None, list[dict[str, object]]]:
        """Process one input line (without line terminator).

        Returns ``(event_id, result, reason, trades)`` where ``event_id`` is
        the event id when it can be obtained as a string, otherwise ``None``,
        and ``reason`` is set only for rejected events.
        """
        event_id, result, reason, trades, _stp = self.handle_line_full(line)
        return event_id, result, reason, trades

    def handle_line_full(
        self, line: str
    ) -> tuple[str | None, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        """Like :meth:`handle_line`, additionally returning the self-trade
        prevention descriptor (``None`` for every other result)."""
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None, REJECTED, INVALID_JSON, [], None
        return self.handle_object(obj)

    def handle_object(
        self, obj: object
    ) -> tuple[str | None, str, str | None, list[dict[str, object]], dict[str, object] | None]:
        if not isinstance(obj, dict):
            return None, REJECTED, INVALID_SCHEMA, [], None

        event_id = obj.get("event_id")
        event_id_out = event_id if isinstance(event_id, str) else None

        schema_error = self._schema_error(obj)
        if schema_error is not None:
            return event_id_out, REJECTED, schema_error, [], None

        if event_id in self._event_ids:
            return event_id, REJECTED, DUPLICATE_EVENT_ID, [], None
        if obj["type"] == REPLACE and "display_quantity" in obj:
            target = self._orders.get(obj["order_id"])
            if (
                target is not None
                and target["status"] == RESTING
                and "display_quantity" not in target
            ):
                # Only an iceberg target may be replaced with a display slice;
                # like every schema error this consumes no event id.
                return event_id, REJECTED, INVALID_SCHEMA, [], None
        # The event is well formed, so its id occupies the stream from here,
        # even if a later business rule rejects it.
        self._event_ids.add(event_id)

        if obj["type"] == CANCEL:
            event_id, result, reason, trades = self._cancel(event_id, obj["order_id"])
            return event_id, result, reason, trades, None
        if obj["type"] == REPLACE:
            return self._replace(event_id, obj)
        return self._add(event_id, obj)

    @staticmethod
    def _schema_error(obj: dict[str, object]) -> str | None:
        keys = set(obj)
        if not keys <= _ALL_KEYS:
            return INVALID_SCHEMA
        if not isinstance(obj.get("event_id"), str):
            return INVALID_SCHEMA

        event_type = obj.get("type")
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
            order_id, side, limit, remaining, account_id
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
                trades.append(
                    {
                        "trade_id": self._next_trade_id,
                        "maker_order_id": maker_id,
                        "taker_order_id": order_id,
                        "price": price,
                        "quantity": matched,
                    }
                )
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
            order_id, side, price, obj["quantity"], account_id
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
