"""Single-symbol order event replay with price-time priority matching.

The engine is pure in-memory state: feed it validated event dictionaries and
it returns statuses, trades and aggregated book snapshots. All stream I/O and
JSON (de)serialization lives in :mod:`order_book_engine.cli`.
"""

from __future__ import annotations

from collections import deque
from typing import Any

BUY = "BUY"
SELL = "SELL"
LIMIT = "LIMIT"
MARKET = "MARKET"

# ADD result statuses.
FILLED = "FILLED"
RESTING = "RESTING"
PARTIALLY_FILLED_RESTING = "PARTIALLY_FILLED_RESTING"
PARTIALLY_FILLED_CANCELLED = "PARTIALLY_FILLED_CANCELLED"
UNFILLED_CANCELLED = "UNFILLED_CANCELLED"
CANCELLED = "CANCELLED"

# Rejection reasons.
INVALID_JSON = "INVALID_JSON"
INVALID_SCHEMA = "INVALID_SCHEMA"
DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
UNKNOWN_ORDER = "UNKNOWN_ORDER"

_ADD_REQUIRED = frozenset(
    {"event_id", "type", "order_id", "side", "order_type", "quantity"}
)
_CANCEL_FIELDS = frozenset({"event_id", "type", "order_id"})


def _is_positive_int(value: Any) -> bool:
    # bool is a subclass of int but must not be accepted as a quantity/price.
    return not isinstance(value, bool) and isinstance(value, int) and value > 0


def validate_event(event: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Validate one parsed JSON value.

    Returns ``(normalized_event, None)`` for a well-formed ADD/CANCEL event,
    otherwise ``(None, "INVALID_SCHEMA")``.
    """
    if not isinstance(event, dict):
        return None, INVALID_SCHEMA

    event_type = event.get("type")
    if event_type not in ("ADD", "CANCEL"):
        return None, INVALID_SCHEMA
    if not isinstance(event.get("event_id"), str):
        return None, INVALID_SCHEMA

    if event_type == "CANCEL":
        if set(event) != _CANCEL_FIELDS:
            return None, INVALID_SCHEMA
        if not isinstance(event["order_id"], str):
            return None, INVALID_SCHEMA
        return (
            {
                "kind": "CANCEL",
                "event_id": event["event_id"],
                "order_id": event["order_id"],
            },
            None,
        )

    keys = set(event)
    if not _ADD_REQUIRED <= keys or keys - (_ADD_REQUIRED | {"price"}):
        return None, INVALID_SCHEMA
    if not isinstance(event["order_id"], str):
        return None, INVALID_SCHEMA
    if event["side"] not in (BUY, SELL):
        return None, INVALID_SCHEMA
    if event["order_type"] not in (LIMIT, MARKET):
        return None, INVALID_SCHEMA
    if not _is_positive_int(event["quantity"]):
        return None, INVALID_SCHEMA

    if event["order_type"] == LIMIT:
        if "price" not in keys or not _is_positive_int(event["price"]):
            return None, INVALID_SCHEMA
        price: int | None = event["price"]
    else:
        # MARKET orders must not carry a non-null price; an absent or null
        # price is allowed.
        if event.get("price", None) is not None:
            return None, INVALID_SCHEMA
        price = None

    return (
        {
            "kind": "ADD",
            "event_id": event["event_id"],
            "order_id": event["order_id"],
            "side": event["side"],
            "order_type": event["order_type"],
            "quantity": event["quantity"],
            "price": price,
        },
        None,
    )


class _Order:
    __slots__ = ("side", "price", "remaining")

    def __init__(self, side: str, price: int, remaining: int) -> None:
        self.side = side
        self.price = price
        self.remaining = remaining


class OrderBook:
    """Price-time priority order book for one symbol.

    Each side maps price to a FIFO deque of order ids; live order state lives
    in ``_orders``. Cancelled orders are dropped from ``_orders`` and pruned
    lazily from their price queue, which preserves time priority.
    """

    def __init__(self) -> None:
        self._orders: dict[str, _Order] = {}
        self._bids: dict[int, deque[str]] = {}
        self._asks: dict[int, deque[str]] = {}
        self.event_ids: set[str] = set()
        self.order_ids: set[str] = set()
        self.next_trade_id = 1

    def add(
        self,
        order_id: str,
        side: str,
        order_type: str,
        quantity: int,
        price: int | None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Process a validated ADD event; return ``(status, trades)``."""
        trades: list[dict[str, Any]] = []
        remaining = quantity
        levels = self._asks if side == BUY else self._bids

        while remaining > 0 and levels:
            if side == BUY:
                best_price = min(levels)
                if order_type == LIMIT and best_price > price:
                    break
            else:
                best_price = max(levels)
                if order_type == LIMIT and best_price < price:
                    break

            queue = levels[best_price]
            while queue and remaining > 0:
                maker_id = queue[0]
                maker = self._orders.get(maker_id)
                if maker is None:
                    # Cancelled order lingering at the queue head.
                    queue.popleft()
                    continue
                traded = min(remaining, maker.remaining)
                maker.remaining -= traded
                remaining -= traded
                trades.append(
                    {
                        "maker_order_id": maker_id,
                        "taker_order_id": order_id,
                        "price": maker.price,
                        "quantity": traded,
                    }
                )
                self.next_trade_id += 1
                if maker.remaining == 0:
                    queue.popleft()
                    del self._orders[maker_id]
            if not queue:
                del levels[best_price]

        if remaining > 0:
            if order_type == LIMIT:
                self._orders[order_id] = _Order(side, price, remaining)  # type: ignore[arg-type]
                own_levels = self._bids if side == BUY else self._asks
                own_levels.setdefault(price, deque()).append(order_id)  # type: ignore[arg-type]
                status = PARTIALLY_FILLED_RESTING if trades else RESTING
            else:
                status = PARTIALLY_FILLED_CANCELLED if trades else UNFILLED_CANCELLED
        else:
            status = FILLED

        return status, trades

    def cancel(self, order_id: str) -> str | None:
        """Cancel remaining quantity; ``None`` means the order is unknown."""
        if self._orders.pop(order_id, None) is None:
            return None
        return CANCELLED

    def snapshot(self) -> dict[str, Any]:
        """Aggregated book: bids by descending price, asks ascending.

        Cancelled orders lingering behind live queue entries are pruned here.
        """
        return {
            "bids": self._side_snapshot(self._bids, reverse=True),
            "asks": self._side_snapshot(self._asks, reverse=False),
        }

    def _side_snapshot(
        self, levels: dict[int, deque[str]], *, reverse: bool
    ) -> list[dict[str, int]]:
        out: list[dict[str, int]] = []
        for price in sorted(levels, reverse=reverse):
            queue = levels[price]
            for dead in [oid for oid in list(queue) if oid not in self._orders]:
                queue.remove(dead)
            total = sum(self._orders[oid].remaining for oid in queue)
            if total > 0:
                out.append({"price": price, "quantity": total})
            else:
                del levels[price]
        return out
