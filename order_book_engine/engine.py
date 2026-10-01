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
PARTIALLY_FILLED_RESTING = "PARTIALLY_FILLED_RESTING"
PARTIALLY_FILLED_CANCELLED = "PARTIALLY_FILLED_CANCELLED"
UNFILLED_CANCELLED = "UNFILLED_CANCELLED"
CANCELLED = "CANCELLED"
REPLACED = "REPLACED"
REJECTED = "REJECTED"

INVALID_JSON = "INVALID_JSON"
INVALID_SCHEMA = "INVALID_SCHEMA"
DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
UNKNOWN_ORDER = "UNKNOWN_ORDER"

_ALL_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price",
     "time_in_force", "display_quantity"}
)
_CANCEL_KEYS = frozenset({"event_id", "type", "order_id"})
_REQUIRED_ADD_KEYS = frozenset({"event_id", "type", "order_id", "side", "order_type", "quantity"})
_REPLACE_KEYS = frozenset(
    {"event_id", "type", "order_id", "quantity", "price", "display_quantity"}
)
_REQUIRED_REPLACE_KEYS = frozenset({"event_id", "type", "order_id", "quantity", "price"})


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
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None, REJECTED, INVALID_JSON, []
        return self.handle_object(obj)

    def handle_object(
        self, obj: object
    ) -> tuple[str | None, str, str | None, list[dict[str, object]]]:
        if not isinstance(obj, dict):
            return None, REJECTED, INVALID_SCHEMA, []

        event_id = obj.get("event_id")
        event_id_out = event_id if isinstance(event_id, str) else None

        schema_error = self._schema_error(obj)
        if schema_error is not None:
            return event_id_out, REJECTED, schema_error, []

        if event_id in self._event_ids:
            return event_id, REJECTED, DUPLICATE_EVENT_ID, []
        # The event is well formed, so its id occupies the stream from here,
        # even if a later business rule rejects it.
        self._event_ids.add(event_id)

        if obj["type"] == CANCEL:
            return self._cancel(event_id, obj["order_id"])
        if obj["type"] == REPLACE:
            return self._replace(event_id, obj)
        return self._add(event_id, obj)

    def _schema_error(self, obj: dict[str, object]) -> str | None:
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
            if not keys <= _REPLACE_KEYS:
                # side, order_type, time_in_force and any other field are
                # inherited from the target and must not be carried.
                return INVALID_SCHEMA
            if not keys >= _REQUIRED_REPLACE_KEYS:
                return INVALID_SCHEMA
            if not isinstance(obj.get("order_id"), str):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("quantity")):
                return INVALID_SCHEMA
            if not _is_positive_int(obj.get("price")):
                return INVALID_SCHEMA
            display_quantity = obj.get("display_quantity")
            if "display_quantity" in obj and (
                not _is_positive_int(display_quantity)
                or display_quantity > obj["quantity"]
            ):
                return INVALID_SCHEMA
            # A peak size belongs to iceberg orders only. Carrying it on a
            # resting ordinary limit target is a structural error and must not
            # occupy the event id; the target's type is inherited, so it is
            # resolved from the live book here.
            target = self._orders.get(obj["order_id"])
            if (
                "display_quantity" in obj
                and target is not None
                and target["status"] == RESTING
                and "visible" not in target
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

    def _add(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[str, str, str | None, list[dict[str, object]]]:
        order_id: str = obj["order_id"]
        if order_id in self._order_ids:
            return event_id, REJECTED, DUPLICATE_ORDER_ID, []
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

        if side == BUY:
            opposite = self._asks
            totals = self._ask_totals

            def tradable(price: int) -> bool:
                return limit is None or price <= limit
        else:
            opposite = self._bids
            totals = self._bid_totals

            def tradable(price: int) -> bool:
                return limit is None or price >= limit

        # FOK must either match the whole quantity against the pre-event book
        # or do nothing at all: no trades, no book change, no trade ids spent.
        # A resting iceberg offers its full remaining quantity to this check,
        # including reserve that is not part of the visible book totals.
        if tif == FOK:
            available = 0
            for price, queue in opposite.items():
                if tradable(price):
                    for maker_id in queue:
                        available += self._orders[maker_id]["remaining"]
            if available < quantity:
                self._orders[order_id] = {
                    "side": side,
                    "price": limit,
                    "remaining": remaining,
                    "status": CANCELLED,
                }
                return event_id, UNFILLED_CANCELLED, None, []

        trades: list[dict[str, object]] = []
        while remaining > 0:
            candidates = [price for price in totals if tradable(price)]
            if not candidates:
                break
            price = min(candidates) if side == BUY else max(candidates)
            queue = opposite[price]
            while remaining > 0 and queue:
                maker_id = queue[0]
                maker = self._orders[maker_id]
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

        record: dict[str, object] = {"side": side, "price": limit}
        if remaining == 0:
            result = FILLED
            record["remaining"] = 0
            record["status"] = FILLED
        elif order_type in (LIMIT, ICEBERG) and tif == GTC:
            result = PARTIALLY_FILLED_RESTING if trades else RESTING
            record["remaining"] = remaining
            record["status"] = RESTING
            visible = self._rest(
                record, order_id, side, limit, remaining, display_quantity
            )
            own_totals = self._bid_totals if side == BUY else self._ask_totals
            own_totals[limit] = own_totals.get(limit, 0) + visible
        else:
            # IOC leftovers never enter the book; a successful FOK is always
            # fully filled by construction of the pre-event availability check.
            result = PARTIALLY_FILLED_CANCELLED if trades else UNFILLED_CANCELLED
            record["remaining"] = remaining
            record["status"] = CANCELLED

        self._orders[order_id] = record
        return event_id, result, None, trades

    def _rest(
        self,
        record: dict[str, object],
        order_id: str,
        side: str,
        limit: int | None,
        remaining: int,
        display_quantity: int | None,
    ) -> int:
        """Put the leftover of an arrived GTC order into its price queue.

        Returns the visible quantity added to the book totals; for an iceberg
        only the first slice is published and the rest stays as reserve.
        """
        own_book = self._bids if side == BUY else self._asks
        own_book.setdefault(limit, deque()).append(order_id)
        if display_quantity is not None:
            visible = min(display_quantity, remaining)
            record["display_quantity"] = display_quantity
            record["visible"] = visible
        else:
            visible = remaining
        return visible

    def _withdraw(self, order_id: str, record: dict[str, object]) -> None:
        """Remove a resting order's full size (visible slice plus reserve)."""
        price = record["price"]
        if record["side"] == BUY:
            own_book = self._bids
            own_totals = self._bid_totals
        else:
            own_book = self._asks
            own_totals = self._ask_totals

        queue = own_book[price]
        queue.remove(order_id)
        visible = record.get("visible", record["remaining"])
        own_totals[price] -= visible
        if own_totals[price] == 0:
            del own_totals[price]
            del own_book[price]

    def _replace(
        self, event_id: str, obj: dict[str, object]
    ) -> tuple[str, str, str | None, list[dict[str, object]]]:
        order_id: str = obj["order_id"]
        record = self._orders.get(order_id)
        # Only ordinary LIMIT and ICEBERG orders rest in the book; a filled,
        # cancelled or never-seen target cannot be replaced.
        if record is None or record["status"] != RESTING:
            return event_id, REJECTED, UNKNOWN_ORDER, []

        side: str = record["side"]
        is_iceberg = "visible" in record
        quantity: int = obj["quantity"]
        limit: int = obj["price"]
        if "display_quantity" in obj:
            display_quantity: int | None = obj["display_quantity"]
        elif is_iceberg:
            # Omitting the peak keeps the original iceberg's peak.
            display_quantity = record["display_quantity"]
        else:
            display_quantity = None

        # The replacement is indivisible: the whole resting target (visible
        # slice and reserve) leaves the book before the new order arrives, so
        # it can never match its own former state and always loses priority.
        self._withdraw(order_id, record)

        remaining = quantity
        if side == BUY:
            opposite = self._asks
            totals = self._ask_totals

            def tradable(price: int) -> bool:
                return price <= limit
        else:
            opposite = self._bids
            totals = self._bid_totals

            def tradable(price: int) -> bool:
                return price >= limit

        trades: list[dict[str, object]] = []
        while remaining > 0:
            candidates = [price for price in totals if tradable(price)]
            if not candidates:
                break
            price = min(candidates) if side == BUY else max(candidates)
            queue = opposite[price]
            while remaining > 0 and queue:
                maker_id = queue[0]
                maker = self._orders[maker_id]
                if maker.get("visible") is not None:
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

        new_record: dict[str, object] = {"side": side, "price": limit}
        if remaining == 0:
            result = FILLED
            new_record["remaining"] = 0
            new_record["status"] = FILLED
        else:
            # A successful replacement is a fresh GTC order; when nothing
            # trades the outcome is REPLACED rather than RESTING.
            result = PARTIALLY_FILLED_RESTING if trades else REPLACED
            new_record["remaining"] = remaining
            new_record["status"] = RESTING
            visible = self._rest(
                new_record, order_id, side, limit, remaining, display_quantity
            )
            own_totals = self._bid_totals if side == BUY else self._ask_totals
            own_totals[limit] = own_totals.get(limit, 0) + visible

        self._orders[order_id] = new_record
        return event_id, result, None, trades

    def _cancel(
        self, event_id: str, order_id: str
    ) -> tuple[str, str, str | None, list[dict[str, object]]]:
        record = self._orders.get(order_id)
        if record is None or record["status"] != RESTING:
            return event_id, REJECTED, UNKNOWN_ORDER, []

        self._withdraw(order_id, record)

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
