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
BUY = "BUY"
SELL = "SELL"
LIMIT = "LIMIT"
MARKET = "MARKET"

FILLED = "FILLED"
RESTING = "RESTING"
PARTIALLY_FILLED_RESTING = "PARTIALLY_FILLED_RESTING"
PARTIALLY_FILLED_CANCELLED = "PARTIALLY_FILLED_CANCELLED"
UNFILLED_CANCELLED = "UNFILLED_CANCELLED"
CANCELLED = "CANCELLED"
REJECTED = "REJECTED"

INVALID_JSON = "INVALID_JSON"
INVALID_SCHEMA = "INVALID_SCHEMA"
DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
UNKNOWN_ORDER = "UNKNOWN_ORDER"

_ALL_KEYS = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity", "price"}
)
_CANCEL_KEYS = frozenset({"event_id", "type", "order_id"})
_REQUIRED_ADD_KEYS = frozenset({"event_id", "type", "order_id", "side", "order_type", "quantity"})


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

        if event_type != ADD:
            return INVALID_SCHEMA
        if not keys >= _REQUIRED_ADD_KEYS:
            return INVALID_SCHEMA

        if not isinstance(obj.get("order_id"), str):
            return INVALID_SCHEMA
        if obj.get("side") not in (BUY, SELL):
            return INVALID_SCHEMA
        order_type = obj.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_SCHEMA
        if not _is_positive_int(obj.get("quantity")):
            return INVALID_SCHEMA

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
        remaining: int = obj["quantity"]
        limit: int | None = obj["price"] if order_type == LIMIT else None

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
                matched = min(remaining, maker["remaining"])
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
                if maker["remaining"] == 0:
                    queue.popleft()
                    maker["status"] = FILLED
            if not queue:
                del opposite[price]
                del totals[price]

        if remaining == 0:
            result = FILLED
            status = FILLED
        elif order_type == LIMIT:
            result = PARTIALLY_FILLED_RESTING if trades else RESTING
            status = RESTING
            own_book = self._bids if side == BUY else self._asks
            own_totals = self._bid_totals if side == BUY else self._ask_totals
            own_book.setdefault(limit, deque()).append(order_id)
            own_totals[limit] = own_totals.get(limit, 0) + remaining
        else:
            result = PARTIALLY_FILLED_CANCELLED if trades else UNFILLED_CANCELLED
            status = CANCELLED

        self._orders[order_id] = {
            "side": side,
            "price": limit,
            "remaining": remaining,
            "status": status,
        }
        return event_id, result, None, trades

    def _cancel(
        self, event_id: str, order_id: str
    ) -> tuple[str, str, str | None, list[dict[str, object]]]:
        record = self._orders.get(order_id)
        if record is None or record["status"] != RESTING:
            return event_id, REJECTED, UNKNOWN_ORDER, []

        price = record["price"]
        if record["side"] == BUY:
            own_book = self._bids
            own_totals = self._bid_totals
        else:
            own_book = self._asks
            own_totals = self._ask_totals

        queue = own_book[price]
        queue.remove(order_id)
        own_totals[price] -= record["remaining"]
        if own_totals[price] == 0:
            del own_totals[price]
            del own_book[price]

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
