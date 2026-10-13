"""Business rules the Avro contract cannot express.

Avro says ``amount_cents`` is a nullable long. It cannot say a payment needs one, or
that it must be positive. A record that decodes but breaks these rules goes to the
dead-letter topic with the rule it broke, like one that does not decode at all.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

CURRENCIES = frozenset({"USD", "EUR", "GBP"})
MAX_FUTURE_MS = 5 * 60 * 1000


def _ms(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(value)


def violations(event: dict[str, Any], now_ms: int | None = None) -> list[str]:
    """Every rule ``event`` breaks; empty when it is valid."""
    out: list[str] = []
    kind = event.get("event_type")
    if not event.get("event_id"):
        out.append("event_id_empty")
    if not event.get("session_id"):
        out.append("session_id_empty")
    if event.get("currency") not in CURRENCIES:
        out.append("currency_unknown")
    if now_ms is None:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
    if _ms(event["event_time"]) > now_ms + MAX_FUTURE_MS:
        out.append("event_time_in_future")

    if kind == "PAGE_VIEW" and not event.get("page"):
        out.append("page_view_without_page")
    if kind == "ADD_TO_CART":
        if not event.get("product_id"):
            out.append("cart_without_product")
        if (event.get("quantity") or 0) < 1:
            out.append("cart_quantity_not_positive")
    if kind in ("CHECKOUT_STARTED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED") and not event.get(
        "order_id"
    ):
        out.append("order_event_without_order_id")
    if kind in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED") and (event.get("amount_cents") or 0) <= 0:
        out.append("payment_amount_not_positive")
    return out
