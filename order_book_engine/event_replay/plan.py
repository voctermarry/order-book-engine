"""Resumable parent-order plan state (TWAP, VWAP and POV algorithms)."""

from __future__ import annotations

from ..engine import SELL
from .constants import (
    ALGORITHM_POV,
    ALGORITHM_TWAP,
    ALGORITHM_VWAP,
    PLAN_ACTIVE,
)
from .validation import _allocate_slices


class ExecutionPlan:
    """Mutable state of one resumable TWAP, VWAP or POV parent order.

    All cumulative analytics (filled quantity, notional, cancelled quantity)
    are maintained incrementally, so a snapshot carries exactly the state a
    continued or resumed slice needs. TWAP and VWAP slice quantities are fixed
    at creation: a TWAP plan divides the total evenly across ``slice_count``
    slices and spreads the division remainder as one extra unit over the
    earliest slices, while a VWAP plan allocates one unit per bucket up front
    and distributes the rest proportionally to its volume weights (see
    :func:`_allocate_slices`). A POV plan has no fixed schedule: its release
    quantities are derived at run time from the cumulative market volume fed
    by POV_VOLUME events and the participation rate, so its slice list grows
    one entry per positive release.
    """

    __slots__ = (
        "plan_id", "side", "order_type", "benchmark_price", "price",
        "account_id", "algorithm", "volume_weights", "slice_quantities",
        "released", "released_quantity", "filled_quantity",
        "cancelled_quantity", "notional", "status",
        "participation_bps", "market_volume", "plan_total",
    )

    def __init__(
        self,
        plan_id: str,
        side: str,
        order_type: str,
        total_quantity: int,
        slice_count: int,
        benchmark_price: int,
        price: int | None,
        account_id: str | None,
        *,
        algorithm: str = ALGORITHM_TWAP,
        volume_weights: list[int] | None = None,
        participation_bps: int | None = None,
    ) -> None:
        self.plan_id = plan_id
        self.side = side
        self.order_type = order_type
        self.benchmark_price = benchmark_price
        self.price = price
        self.account_id = account_id
        self.algorithm = algorithm
        self.volume_weights = list(volume_weights) if volume_weights is not None else None
        # POV parameters: the participation rate in basis points and the
        # cumulative caller-fed market volume. TWAP/VWAP leave both unused.
        self.participation_bps = participation_bps
        self.market_volume = 0
        # POV keeps its total explicitly; the fixed-schedule plans derive
        # theirs from the slice list.
        self.plan_total: int | None = None
        if algorithm == ALGORITHM_VWAP:
            self.slice_quantities: list[int] = _allocate_slices(
                total_quantity, self.volume_weights
            )
        elif algorithm == ALGORITHM_POV:
            self.plan_total = total_quantity
            # One entry is appended per positive release, holding that
            # child order's original quantity.
            self.slice_quantities = []
        else:
            base, extra = divmod(total_quantity, slice_count)
            self.slice_quantities = [
                base + (1 if index < extra else 0)
                for index in range(slice_count)
            ]
        # Number of releases (child orders) already sent; also the index of
        # the next one for every algorithm.
        self.released = 0
        self.released_quantity = 0
        self.filled_quantity = 0
        self.cancelled_quantity = 0
        self.notional = 0
        self.status = PLAN_ACTIVE

    @property
    def slice_count(self) -> int:
        """Fixed schedule length for TWAP/VWAP; releases so far for POV.

        A POV plan's slice list only ever contains its released children, so
        its length is the current count of positive releases.
        """
        return len(self.slice_quantities)

    @property
    def child_count(self) -> int:
        """Number of derived child ids the plan owns.

        TWAP/VWAP own one fixed id per scheduled slice; POV owns one id per
        unit of its total quantity, of which only the released prefix is ever
        submitted (an unfinished POV release would itself carry more than one
        unit, so the release count never reaches the id count).
        """
        if self.algorithm == ALGORITHM_POV:
            return self.plan_total
        return len(self.slice_quantities)

    @property
    def total_quantity(self) -> int:
        if self.algorithm == ALGORITHM_POV:
            return self.plan_total
        return sum(self.slice_quantities)

    @property
    def remaining_slices(self) -> int:
        if self.status != PLAN_ACTIVE:
            return 0
        if self.algorithm == ALGORITHM_POV:
            # A POV release is driven by market volume rather than a fixed
            # bucket list; until the total is reached another release can
            # always occur.
            return 1 if self.released_quantity < self.total_quantity else 0
        return self.slice_count - self.released

    def unreleased_quantity(self) -> int:
        """Quantity still awaiting release (zero once completed or cancelled)."""
        return (
            self.total_quantity
            - self.released_quantity
            - self.cancelled_quantity
        )

    def child_order_id(self, slice_number: int) -> str:
        """The deterministic derived order id of release ``slice_number``.

        Release numbers are 1-based and joined with ``#``; the scheme is
        fixed, so the same input stream always derives the same identifiers.
        """
        return f"{self.plan_id}#{slice_number}"

    def child_ids(self) -> list[str]:
        """All derived ids reserved when the plan starts.

        TWAP/VWAP reserve one id per scheduled slice; POV reserves one id per
        unit of its total quantity (``plan_id#1 … plan_id#total_quantity``),
        of which only the released prefix is ever submitted. Up-front
        reservation keeps the external event/order-id protection identical
        across algorithms.
        """
        return [self.child_order_id(i + 1) for i in range(self.child_count)]

    def summary(
        self,
        *,
        slice_number: int | None = None,
        child_order_id: str | None = None,
        target_weight: int | None = None,
        scheduled_quantity: int | None = None,
        release_number: int | None = None,
        pov_volume: bool = False,
    ) -> dict[str, object]:
        """Build the ``execution_plan`` object echoed by plan responses."""
        vwap = (
            {"numerator": self.notional, "denominator": self.filled_quantity}
            if self.filled_quantity
            else None
        )
        slippage = self.notional - self.benchmark_price * self.filled_quantity
        if self.side == SELL:
            # Mirror the buy formula: negative always means improvement.
            slippage = -slippage
        if self.algorithm == ALGORITHM_POV:
            # POV reports the still-unreleased quantity instead of a fixed
            # remaining-slice count.
            plan: dict[str, object] = {
                "status": self.status,
                "released_quantity": self.released_quantity,
                "filled_quantity": self.filled_quantity,
                "cancelled_quantity": self.cancelled_quantity,
                "unreleased_quantity": self.unreleased_quantity(),
                "executed_notional": self.notional,
                "vwap": vwap,
                "slippage_notional": slippage,
                "algorithm": ALGORITHM_POV,
            }
            if pov_volume:
                # Every POV_VOLUME response names the child order id (null
                # when the event released nothing); the release ordinal
                # exists only for an actual positive release.
                plan["child_order_id"] = child_order_id
                if release_number is not None:
                    plan["release_number"] = release_number
            return plan
        plan = {
            "status": self.status,
            "released_quantity": self.released_quantity,
            "filled_quantity": self.filled_quantity,
            "cancelled_quantity": self.cancelled_quantity,
            "remaining_slices": self.remaining_slices,
            "executed_notional": self.notional,
            "vwap": vwap,
            "slippage_notional": slippage,
        }
        if self.algorithm == ALGORITHM_VWAP:
            plan["algorithm"] = ALGORITHM_VWAP
        if slice_number is not None:
            plan["slice_number"] = slice_number
        if child_order_id is not None:
            plan["child_order_id"] = child_order_id
        if target_weight is not None:
            plan["target_weight"] = target_weight
        if scheduled_quantity is not None:
            plan["scheduled_quantity"] = scheduled_quantity
        return plan

    def to_json(self) -> dict[str, object]:
        """Serialize the complete plan state for a snapshot."""
        data: dict[str, object] = {
            "plan_id": self.plan_id,
            "side": self.side,
            "order_type": self.order_type,
            "benchmark_price": self.benchmark_price,
            "price": self.price,
            "account_id": self.account_id,
            "slice_quantities": list(self.slice_quantities),
            "released": self.released,
            "released_quantity": self.released_quantity,
            "filled_quantity": self.filled_quantity,
            "cancelled_quantity": self.cancelled_quantity,
            "notional": self.notional,
            "status": self.status,
        }
        if self.algorithm == ALGORITHM_VWAP:
            # TWAP records keep their historical shape; VWAP records add the
            # algorithm label and the schedule the allocation derives from.
            data["algorithm"] = ALGORITHM_VWAP
            data["volume_weights"] = list(self.volume_weights)
        elif self.algorithm == ALGORITHM_POV:
            # POV records add the rate, the cumulative observed market volume
            # and the total the schedule converges to; slice_quantities holds
            # one original quantity per released child, in release order.
            data["algorithm"] = ALGORITHM_POV
            data["total_quantity"] = self.total_quantity
            data["participation_bps"] = self.participation_bps
            data["market_volume"] = self.market_volume
        return data
