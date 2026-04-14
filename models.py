"""Pydantic models for pipeline events and dead-letter payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    """Base schema shared by all modeled pipeline events."""

    event_id: UUID = Field(default_factory=uuid4)
    profile_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    event_type: str


class PlacedOrderEvent(EventBase):
    """Event emitted when a customer places an order."""

    event_type: Literal["placed_order"] = "placed_order"
    order_id: str
    amount: float
    items: list[str]


class ViewedProductEvent(EventBase):
    """Event emitted when a customer views a product."""

    event_type: Literal["viewed_product"] = "viewed_product"
    product_id: str
    category: str


class AbandonedCartEvent(EventBase):
    """Event emitted when a customer abandons a cart."""

    event_type: Literal["abandoned_cart"] = "abandoned_cart"
    cart_id: str
    items: list[str]
    cart_value: float


class ClickedEmailEvent(EventBase):
    """Event emitted when a customer clicks an email link."""

    event_type: Literal["clicked_email"] = "clicked_email"
    campaign_id: str
    link_url: str


class DeadLetterEvent(BaseModel):
    """Represents a payload and failure metadata captured in the DLQ."""

    original_payload: dict[str, Any]
    error_message: str
    failed_at: datetime
    retry_count: int = 0


AnyEvent = Union[
    PlacedOrderEvent,
    ViewedProductEvent,
    AbandonedCartEvent,
    ClickedEmailEvent,
]


def create_event(event_type: str, profile_id: str, **kwargs: Any) -> AnyEvent:
    """Create and return a concrete event model for the given event type."""
    event_model_map: dict[str, type[AnyEvent]] = {
        "placed_order": PlacedOrderEvent,
        "viewed_product": ViewedProductEvent,
        "abandoned_cart": AbandonedCartEvent,
        "clicked_email": ClickedEmailEvent,
    }
    event_model = event_model_map.get(event_type)
    if event_model is None:
        raise ValueError(f"Unknown event_type: {event_type}")
    return event_model(profile_id=profile_id, **kwargs)
