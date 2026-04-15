"""Event producer that validates events, applies backpressure, and enqueues work."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from models import AnyEvent, DeadLetterEvent, create_event

logger = logging.getLogger(__name__)

BACKPRESSURE_THRESHOLD: int = 1000
THROTTLE_DELAY_SECONDS: float = 0.05


class EventProducer:
    """Creates events, enqueues valid events, and routes invalid ones to the DLQ."""

    def __init__(self, queue: asyncio.Queue, dlq: asyncio.Queue, metrics: dict) -> None:
        """Store queue dependencies and metrics tracking references."""
        self.queue: asyncio.Queue = queue
        self.dlq: asyncio.Queue = dlq
        self.metrics: dict = metrics
        self._throttle_count: int = 0

    async def emit(self, event_type: str, profile_id: str, **kwargs: Any) -> bool:
        """Create and enqueue one event, or send unknown types to the DLQ."""
        try:
            event: AnyEvent = create_event(event_type=event_type, profile_id=profile_id, **kwargs)
        except ValueError as exc:
            dead_letter_event = DeadLetterEvent(
                original_payload={
                    "event_type": event_type,
                    "profile_id": profile_id,
                    **kwargs,
                },
                error_message=str(exc),
                failed_at=datetime.now(UTC),
                retry_count=0,
            )
            await self.dlq.put(dead_letter_event)
            # dlq_total is incremented when DeadLetterHandler records the item (avoids double count on drain).
            logger.debug("Unknown event type routed to DLQ: %s", event_type)
            return False

        while self.queue.qsize() >= BACKPRESSURE_THRESHOLD:
            await asyncio.sleep(THROTTLE_DELAY_SECONDS)
            self._throttle_count += 1
            self.metrics["backpressure_triggers"] = self.metrics.get("backpressure_triggers", 0) + 1
            logger.debug("Backpressure triggered at queue size %s", self.queue.qsize())

        await self.queue.put(event)
        self.metrics["emitted_total"] = self.metrics.get("emitted_total", 0) + 1
        return True

    async def emit_batch(self, events: list[dict]) -> dict:
        """Emit a batch of input dictionaries and return success/failure totals."""
        success_count = 0
        failed_count = 0

        for index, event_data in enumerate(events):
            if not isinstance(event_data, dict):
                failed_count += 1
                logger.warning("Skipping non-dict batch entry at index %s", index)
                continue

            payload = dict(event_data)
            event_type = payload.pop("event_type", None)
            profile_id = payload.pop("profile_id", None)

            if not isinstance(event_type, str) or not isinstance(profile_id, str):
                failed_count += 1
                logger.warning(
                    "Skipping malformed batch entry at index %s: missing/invalid event_type or profile_id",
                    index,
                )
                continue

            try:
                emitted = await self.emit(event_type=event_type, profile_id=profile_id, **payload)
            except Exception:  # noqa: BLE001
                failed_count += 1
                logger.exception("Unexpected emit failure for batch entry at index %s", index)
                continue

            if emitted:
                success_count += 1
            else:
                failed_count += 1

        return {"success": success_count, "failed": failed_count}

    @property
    def throttle_count(self) -> int:
        """Return how many times producer throttling has been applied."""
        return self._throttle_count
