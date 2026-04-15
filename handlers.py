"""Asynchronous event handlers for analytics, automation, and DLQ flows."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from metrics import record_event_type
from models import AbandonedCartEvent, AnyEvent, DeadLetterEvent, PlacedOrderEvent

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    """Base class for pipeline handlers that process events asynchronously."""

    def __init__(self, metrics: dict[str, Any]) -> None:
        """Store shared metrics dictionary for handler-side updates."""
        self.metrics = metrics

    @abstractmethod
    async def handle(self, event: AnyEvent) -> bool:
        """Process one event and return True on success, False on failure."""

    @property
    def name(self) -> str:
        """Return the concrete handler class name."""
        return self.__class__.__name__


class AnalyticsHandler(BaseHandler):
    """Handler that records analytics for all supported event types."""

    def __init__(self, metrics: dict[str, Any]) -> None:
        """Initialize analytics handler with shared metrics and local counts."""
        super().__init__(metrics)
        self._counts: dict[str, int] = {}

    async def handle(self, event: AnyEvent) -> bool:
        """Record per-type analytics counts and latency for any incoming event."""
        _ = asyncio.get_running_loop()
        started = time.monotonic()
        record_event_type(self.metrics, event.event_type)
        self._counts[event.event_type] = self._counts.get(event.event_type, 0) + 1
        self.metrics["processed_total"] = self.metrics.get("processed_total", 0) + 1
        self.metrics.setdefault("handler_latency_ms", []).append((time.monotonic() - started) * 1000)
        logger.debug("Analytics: recorded %s for profile %s", event.event_type, event.profile_id)
        return True

    @property
    def event_counts(self) -> dict[str, int]:
        """Return local per-event-type analytics counts."""
        return dict(self._counts)


class AutomationHandler(BaseHandler):
    """Handler that triggers only for placed order and abandoned cart events."""

    def __init__(self, metrics: dict[str, Any]) -> None:
        """Initialize automation handler with trigger counter."""
        super().__init__(metrics)
        self._triggered_count: int = 0

    async def handle(self, event: AnyEvent) -> bool:
        """Trigger automation flows for supported event types and track latency."""
        if event.event_type not in ("placed_order", "abandoned_cart"):
            return True

        started = time.monotonic()
        if isinstance(event, AbandonedCartEvent):
            logger.info(
                "TRIGGER: abandoned_cart flow for profile %s, cart_value=%s",
                event.profile_id,
                event.cart_value,
            )
        if isinstance(event, PlacedOrderEvent):
            logger.info(
                "TRIGGER: post-purchase flow for profile %s, order=%s",
                event.profile_id,
                event.order_id,
            )

        self._triggered_count += 1
        self.metrics["processed_total"] = self.metrics.get("processed_total", 0) + 1
        self.metrics.setdefault("handler_latency_ms", []).append((time.monotonic() - started) * 1000)
        return True

    @property
    def triggered_count(self) -> int:
        """Return how many automation flows were triggered."""
        return self._triggered_count


class DeadLetterHandler(BaseHandler):
    """Handler that records dead-letter events for later inspection."""

    def __init__(self, metrics: dict[str, Any]) -> None:
        """Initialize dead-letter handler storage."""
        super().__init__(metrics)
        self._dead_letters: list[DeadLetterEvent] = []

    async def handle(self, event: Any) -> bool:
        """Store dead-letter events and update DLQ metrics."""
        if not isinstance(event, DeadLetterEvent):
            return True

        logger.debug(
            "DLQ: %s | payload keys: %s",
            event.error_message,
            list(event.original_payload.keys()),
        )
        self.metrics["dlq_total"] = self.metrics.get("dlq_total", 0) + 1
        self._dead_letters.append(event)
        return True

    @property
    def dead_letter_count(self) -> int:
        """Return the number of dead-letter events captured."""
        return len(self._dead_letters)

    @property
    def dead_letters(self) -> list[DeadLetterEvent]:
        """Return captured dead-letter events."""
        return list(self._dead_letters)
