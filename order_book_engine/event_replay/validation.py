"""Structural validation of event payloads and of the session configuration.

Every ``*_schema_error`` function mirrors the baseline contract: any field
problem (missing or extra field, wrong type, a non-positive integer where a
positive one is required, an empty identifier, ...) is an ``INVALID_EVENT``
and consumes neither the event id nor the sequence. These validators are
pure functions of the payload; the registry maps each event type to its
validator, so the orchestrator never dispatches on the type itself.
"""

from __future__ import annotations

from ..engine import (
    BUY,
    LIMIT,
    MARKET,
    SELL,
    _is_int,
    _is_non_empty_str,
    _is_positive_int,
)
from .constants import (
    INVALID_EVENT,
    POV_START,
    POV_VOLUME,
    TWAP_START,
    VWAP_START,
    _BOOK_RECONSTRUCTION_REPORT_KEYS,
    _EXECUTION_REPORT_KEYS,
    _IMPACT_REPORT_KEYS,
    _PLAN_TCA_REPORT_KEYS,
    _PORTFOLIO_REPORT_KEYS,
    _PORTFOLIO_STRESS_REPORT_KEYS,
    _POV_START_KEYS,
    _POV_START_REQUIRED,
    _POV_VOLUME_KEYS,
    _PRICE_LIMIT_UPDATE_KEYS,
    _SESSION_EXPECTED_ACCOUNT_KEYS,
    _SESSION_EXPECTED_TRADE_KEYS,
    _SESSION_RECONCILIATION_KEYS,
    _STRESS_SCENARIO_KEYS,
    _TWAP_PLAN_REF_KEYS,
    _TWAP_START_KEYS,
    _TWAP_START_REQUIRED,
    _VWAP_START_KEYS,
    _VWAP_START_REQUIRED,
)


def _allocate_slices(total_quantity: int, weights: list[int]) -> list[int]:
    """Split ``total_quantity`` across buckets proportionally to ``weights``.

    Every bucket receives one unit up front; the remaining units are
    distributed in proportion to the weights by integer quotient, and the
    units left over by that truncation go one each to the buckets with the
    largest division remainder, earlier buckets first on ties. The result
    always sums to exactly ``total_quantity``.
    """
    count = len(weights)
    remainder = total_quantity - count
    total_weight = sum(weights)
    quantities: list[int] = []
    residuals: list[int] = []
    for weight in weights:
        quotient, residual = divmod(remainder * weight, total_weight)
        quantities.append(1 + quotient)
        residuals.append(residual)
    leftover = remainder - (sum(quantities) - count)
    by_residual = sorted(range(count), key=lambda i: (-residuals[i], i))
    for index in by_residual[:leftover]:
        quantities[index] += 1
    return quantities


def _twap_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a TWAP command payload.

    Mirrors the baseline contract: any field problem (missing or extra field,
    wrong type, a non-positive integer where a positive one is required, an
    empty identifier, a LIMIT without a positive price or a MARKET carrying
    one, ...) is an ``INVALID_EVENT`` and consumes neither the event id nor the
    sequence.
    """
    event_type = payload["type"]
    if event_type == TWAP_START:
        keys = set(payload)
        if not keys >= _TWAP_START_REQUIRED or not keys <= _TWAP_START_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if payload.get("side") not in (BUY, SELL):
            return INVALID_EVENT
        order_type = payload.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_EVENT
        total_quantity = payload.get("total_quantity")
        slice_count = payload.get("slice_count")
        benchmark_price = payload.get("benchmark_price")
        if not _is_positive_int(total_quantity):
            return INVALID_EVENT
        if not _is_positive_int(slice_count):
            return INVALID_EVENT
        if total_quantity < slice_count:
            return INVALID_EVENT
        if not _is_positive_int(benchmark_price):
            return INVALID_EVENT
        if order_type == LIMIT:
            if not _is_positive_int(payload.get("price")):
                return INVALID_EVENT
        elif "price" in payload and payload["price"] is not None:
            # MARKET plans must not carry a non-null price; omission or an
            # explicit null is accepted.
            return INVALID_EVENT
        if "account_id" in payload and not _is_non_empty_str(payload.get("account_id")):
            return INVALID_EVENT
        return None
    # TWAP_SLICE / TWAP_CANCEL / TWAP_REPORT are pure plan references.
    if set(payload) != _TWAP_PLAN_REF_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    return None


def _vwap_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a VWAP command payload.

    Follows the TWAP contract exactly: any field problem (missing or extra
    field, wrong type, a non-positive integer where a positive one is
    required, an empty identifier, an empty or non-positive weight, a LIMIT
    without a positive price or a MARKET carrying one, ...) is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    event_type = payload["type"]
    if event_type == VWAP_START:
        keys = set(payload)
        if not keys >= _VWAP_START_REQUIRED or not keys <= _VWAP_START_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if payload.get("side") not in (BUY, SELL):
            return INVALID_EVENT
        order_type = payload.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_EVENT
        total_quantity = payload.get("total_quantity")
        volume_weights = payload.get("volume_weights")
        benchmark_price = payload.get("benchmark_price")
        if not _is_positive_int(total_quantity):
            return INVALID_EVENT
        if not (
            isinstance(volume_weights, list)
            and len(volume_weights) > 0
            and all(_is_positive_int(weight) for weight in volume_weights)
        ):
            return INVALID_EVENT
        # Every bucket receives one unit up front, so the total must cover
        # the bucket count.
        if total_quantity < len(volume_weights):
            return INVALID_EVENT
        if not _is_positive_int(benchmark_price):
            return INVALID_EVENT
        if order_type == LIMIT:
            if not _is_positive_int(payload.get("price")):
                return INVALID_EVENT
        elif "price" in payload and payload["price"] is not None:
            # MARKET plans must not carry a non-null price; omission or an
            # explicit null is accepted.
            return INVALID_EVENT
        if "account_id" in payload and not _is_non_empty_str(payload.get("account_id")):
            return INVALID_EVENT
        return None
    # VWAP_SLICE / VWAP_CANCEL / VWAP_REPORT are pure plan references.
    if set(payload) != _TWAP_PLAN_REF_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    return None


def _pov_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a POV command payload.

    POV_START mirrors the TWAP/VWAP contract; the schedule parameter is a
    participation rate in basis points, an integer from 1 to 10000. POV_VOLUME
    carries the plan id plus a positive integer market volume increment;
    POV_CANCEL and POV_REPORT are pure plan references. Any field problem is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    event_type = payload["type"]
    if event_type == POV_START:
        keys = set(payload)
        if not keys >= _POV_START_REQUIRED or not keys <= _POV_START_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if payload.get("side") not in (BUY, SELL):
            return INVALID_EVENT
        order_type = payload.get("order_type")
        if order_type not in (LIMIT, MARKET):
            return INVALID_EVENT
        total_quantity = payload.get("total_quantity")
        participation_bps = payload.get("participation_bps")
        benchmark_price = payload.get("benchmark_price")
        if not _is_positive_int(total_quantity):
            return INVALID_EVENT
        if not _is_int(participation_bps) or not (1 <= participation_bps <= 10000):
            # Booleans are not integers; zero and out-of-range rates are
            # rejected structurally.
            return INVALID_EVENT
        if not _is_positive_int(benchmark_price):
            return INVALID_EVENT
        if order_type == LIMIT:
            if not _is_positive_int(payload.get("price")):
                return INVALID_EVENT
        elif "price" in payload and payload["price"] is not None:
            # MARKET plans must not carry a non-null price; omission or an
            # explicit null is accepted.
            return INVALID_EVENT
        if "account_id" in payload and not _is_non_empty_str(payload.get("account_id")):
            return INVALID_EVENT
        return None
    if event_type == POV_VOLUME:
        if set(payload) != _POV_VOLUME_KEYS:
            return INVALID_EVENT
        if not _is_non_empty_str(payload.get("plan_id")):
            return INVALID_EVENT
        if not _is_positive_int(payload.get("market_volume_increment")):
            return INVALID_EVENT
        return None
    # POV_CANCEL / POV_REPORT are pure plan references.
    if set(payload) != _TWAP_PLAN_REF_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    return None


def _portfolio_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PORTFOLIO_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``account_id`` and
    ``mark_prices``; identifiers are non-empty strings and ``mark_prices`` is
    an object mapping non-empty symbol strings to positive integers (booleans
    do not count). The map may be empty structurally: for a known account it
    can never match that account's securities, so the empty map is classified
    as ``MARK_PRICE_MISMATCH`` (which consumes the id and the sequence) rather
    than as a schema error. Whether the account is known and whether the key
    set actually covers its securities are therefore business checks, not
    schema checks: an INVALID_EVENT consumes neither the event id nor the
    sequence, while the business rejections do.
    """
    if set(payload) != _PORTFOLIO_REPORT_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("account_id")):
        return INVALID_EVENT
    mark_prices = payload.get("mark_prices")
    if not isinstance(mark_prices, dict):
        return INVALID_EVENT
    for symbol, mark_price in mark_prices.items():
        if not _is_non_empty_str(symbol):
            return INVALID_EVENT
        if not _is_positive_int(mark_price):
            return INVALID_EVENT
    return None


def _price_map_error(value: object) -> str | None:
    """Structural validation of one symbol-to-price object.

    The map keys are non-empty symbol strings and every price is a positive
    integer (booleans do not count). An empty map is structurally valid: for
    a known account it can never match that account's securities, so it is
    classified as ``MARK_PRICE_MISMATCH`` (a business rejection that consumes
    the id and the sequence) rather than as a schema error.
    """
    if not isinstance(value, dict):
        return INVALID_EVENT
    for symbol, price in value.items():
        if not _is_non_empty_str(symbol):
            return INVALID_EVENT
        if not _is_positive_int(price):
            return INVALID_EVENT
    return None


def _portfolio_stress_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PORTFOLIO_STRESS_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``account_id``,
    ``mark_prices`` and ``scenarios``; the account id is a non-empty string,
    ``mark_prices`` is an object mapping non-empty symbol strings to positive
    integers, and ``scenarios`` is a non-empty array whose items each hold
    exactly ``name`` (a non-empty string, unique across the array) and
    ``prices`` (a symbol-to-price object of the same shape as
    ``mark_prices``). Any field problem — missing or extra fields, a wrong
    type, an empty identifier, a duplicate scenario name, an empty scenarios
    array or an illegal price — is an ``INVALID_EVENT`` and consumes neither
    the event id nor the sequence. Whether the account is known and whether
    each price object's key set exactly covers the account's securities are
    business checks (``UNKNOWN_ACCOUNT`` / ``MARK_PRICE_MISMATCH``), so they
    consume the id and the sequence instead.
    """
    if set(payload) != _PORTFOLIO_STRESS_REPORT_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("account_id")):
        return INVALID_EVENT
    error = _price_map_error(payload.get("mark_prices"))
    if error is not None:
        return error
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) == 0:
        return INVALID_EVENT
    seen_names: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict) or set(scenario) != _STRESS_SCENARIO_KEYS:
            return INVALID_EVENT
        name = scenario.get("name")
        if not _is_non_empty_str(name):
            return INVALID_EVENT
        if name in seen_names:
            return INVALID_EVENT
        seen_names.add(name)
        error = _price_map_error(scenario.get("prices"))
        if error is not None:
            return error
    return None


def _session_reconciliation_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a SESSION_RECONCILIATION query payload.

    The query carries exactly ``event_id``, ``type``, ``expected_trades`` and
    ``expected_accounts``; both records are arrays. A trade item holds exactly
    ``symbol`` and ``trade_id`` (its composite key), ``maker_order_id`` and
    ``taker_order_id`` (non-empty strings) and positive-integer ``price`` and
    ``quantity``; an account item holds exactly ``symbol`` and ``account_id``
    (its composite key) and integer ``net_position`` and ``cash_balance``.
    Symbols are non-empty strings, trade ids are positive integers (booleans
    do not count as integers anywhere), and the composite keys must be unique
    within their array. Any field problem — missing or extra fields, empty or
    duplicate identifiers, a non-array or non-object member, a non-positive
    trade value, a boolean masquerading as an integer — is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    if set(payload) != _SESSION_RECONCILIATION_KEYS:
        return INVALID_EVENT
    expected_trades = payload.get("expected_trades")
    expected_accounts = payload.get("expected_accounts")
    if not isinstance(expected_trades, list) or not isinstance(expected_accounts, list):
        return INVALID_EVENT
    seen_trade_keys: set[tuple[str, int]] = set()
    for item in expected_trades:
        if not isinstance(item, dict) or set(item) != _SESSION_EXPECTED_TRADE_KEYS:
            return INVALID_EVENT
        symbol = item.get("symbol")
        trade_id = item.get("trade_id")
        if not _is_non_empty_str(symbol):
            return INVALID_EVENT
        if not _is_positive_int(trade_id):
            return INVALID_EVENT
        key = (symbol, trade_id)
        if key in seen_trade_keys:
            return INVALID_EVENT
        seen_trade_keys.add(key)
        if not _is_non_empty_str(item.get("maker_order_id")):
            return INVALID_EVENT
        if not _is_non_empty_str(item.get("taker_order_id")):
            return INVALID_EVENT
        if not _is_positive_int(item.get("price")):
            return INVALID_EVENT
        if not _is_positive_int(item.get("quantity")):
            return INVALID_EVENT
    seen_account_keys: set[tuple[str, str]] = set()
    for item in expected_accounts:
        if not isinstance(item, dict) or set(item) != _SESSION_EXPECTED_ACCOUNT_KEYS:
            return INVALID_EVENT
        symbol = item.get("symbol")
        account_id = item.get("account_id")
        if not _is_non_empty_str(symbol) or not _is_non_empty_str(account_id):
            return INVALID_EVENT
        key = (symbol, account_id)
        if key in seen_account_keys:
            return INVALID_EVENT
        seen_account_keys.add(key)
        if not _is_int(item.get("net_position")):
            return INVALID_EVENT
        if not _is_int(item.get("cash_balance")):
            return INVALID_EVENT
    return None


def _execution_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of an EXECUTION_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``order_id`` and
    ``benchmark_price``; the order id is a non-empty string and the
    benchmark is a positive integer (booleans do not count). Any field
    problem (missing or extra field, wrong type, an empty order id) is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    if set(payload) != _EXECUTION_REPORT_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("order_id")):
        return INVALID_EVENT
    if not _is_positive_int(payload.get("benchmark_price")):
        return INVALID_EVENT
    return None


def _impact_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of an IMPACT_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``side``,
    ``quantity`` and ``benchmark_price``; ``side`` is ``BUY`` or
    ``SELL`` and the quantity and benchmark are positive integers
    (booleans do not count). Any field problem (missing or extra field,
    a non-string side, a non-positive quantity or benchmark) is an
    ``INVALID_EVENT`` and consumes neither the event id nor the
    sequence, exactly matching the baseline single-security contract.
    """
    if set(payload) != _IMPACT_REPORT_KEYS:
        return INVALID_EVENT
    if payload.get("side") not in (BUY, SELL):
        return INVALID_EVENT
    if not _is_positive_int(payload.get("quantity")):
        return INVALID_EVENT
    if not _is_positive_int(payload.get("benchmark_price")):
        return INVALID_EVENT
    return None


def _plan_tca_report_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PLAN_TCA_REPORT query payload.

    The query carries exactly ``event_id``, ``type``, ``plan_id`` and
    ``mark_price``; the plan id is a non-empty string and the evaluation
    price is a positive integer (booleans do not count). Any field
    problem (missing or extra field, wrong type, an empty plan id) is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    Whether the envelope symbol actually owns such a plan is a business
    check (``UNKNOWN_EXECUTION_PLAN``), so it consumes the id and the
    sequence instead.
    """
    if set(payload) != _PLAN_TCA_REPORT_KEYS:
        return INVALID_EVENT
    if not _is_non_empty_str(payload.get("plan_id")):
        return INVALID_EVENT
    if not _is_positive_int(payload.get("mark_price")):
        return INVALID_EVENT
    return None


def _book_reconstruction_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a BOOK_RECONSTRUCTION_REPORT query payload.

    The query carries exactly ``event_id``, ``type`` and
    ``target_sequence``; the target is a non-negative integer that is not a
    boolean (``True``/``False`` must not masquerade as ``1``/``0``). Any
    field problem — missing or extra field, a boolean, float, string, null
    or negative target — is an ``INVALID_EVENT`` and consumes neither the
    event id nor the sequence. Whether the target names a sequence this
    security has actually committed is a business check
    (``TARGET_SEQUENCE_NOT_FOUND``), so it consumes the id and the sequence
    instead.
    """
    if set(payload) != _BOOK_RECONSTRUCTION_REPORT_KEYS:
        return INVALID_EVENT
    target = payload.get("target_sequence")
    if not (_is_int(target) and target >= 0):
        return INVALID_EVENT
    return None


def _price_limit_update_schema_error(payload: dict[str, object]) -> str | None:
    """Structural validation of a PRICE_LIMIT_UPDATE command payload.

    The command carries exactly ``event_id``, ``type``, ``lower_price`` and
    ``upper_price``; both bounds are positive integers (booleans do not
    count) with ``lower_price <= upper_price``. Any field problem is an
    ``INVALID_EVENT`` and consumes neither the event id nor the sequence.
    """
    if set(payload) != _PRICE_LIMIT_UPDATE_KEYS:
        return INVALID_EVENT
    lower_price = payload.get("lower_price")
    upper_price = payload.get("upper_price")
    if not _is_positive_int(lower_price) or not _is_positive_int(upper_price):
        return INVALID_EVENT
    if lower_price > upper_price:
        return INVALID_EVENT
    return None


def _valid_timestamp(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    return isinstance(value, str) and value != ""


def _price_limit_error(price_limits: object) -> str | None:
    """Validate the optional static ``price_limits`` configuration block.

    The block maps each configured security name (a non-empty string) to an
    object holding exactly ``lower`` and ``upper``; both bounds are positive
    integers (booleans do not count) with ``lower <= upper``. An empty block
    (or an absent one) leaves every security at the baseline behaviour.
    Returns an error message when the block is malformed, otherwise ``None``.
    """
    if not isinstance(price_limits, dict):
        return "price_limits must be an object mapping symbols to bounds"
    for symbol, bounds in price_limits.items():
        if not _is_non_empty_str(symbol):
            return "price_limits keys must be non-empty symbol strings"
        if not isinstance(bounds, dict) or set(bounds) != {"lower", "upper"}:
            return (
                f"price_limits for {symbol!r} must be an object with exactly "
                "'lower' and 'upper'"
            )
        lower = bounds["lower"]
        upper = bounds["upper"]
        if not _is_positive_int(lower) or not _is_positive_int(upper):
            return f"price_limits for {symbol!r} must use positive integer bounds"
        if lower > upper:
            return f"price_limits for {symbol!r} require lower <= upper"
    return None


def _validate_config(config: dict[str, object]) -> None:
    """Raise ``ValueError`` before any event or snapshot is touched."""
    if "price_limits" in config:
        message = _price_limit_error(config["price_limits"])
        if message is not None:
            raise ValueError(message)
