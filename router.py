"""Router implementing fan-out dispatch where each event is sent to all handlers."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from handlers import BaseHandler, DeadLetterHandler
from models import DeadLetterEvent

logger = logging.getLogger(__name__)


class EventRouter:
    """Consumes queued events and fans them out to every registered handler."""

    def __init__(self, queue: asyncio.Queue, dlq: asyncio.Queue, metrics: dict) -> None:
        """Store router dependencies and initialize runtime state."""
        self.queue: asyncio.Queue = queue
        self.dlq: asyncio.Queue = dlq
        self.metrics: dict = metrics
        self._handlers: list[BaseHandler] = []
        self._running: bool = False
        self._processed: int = 0

    def register_handler(self, handler: BaseHandler) -> None:
        """Register a handler for fan-out event dispatching."""
        self._handlers.append(handler)
        logger.info("Registered handler: %s", handler.name)

    async def _dispatch(self, event: Any) -> None:
        """Dispatch one event to all handlers and isolate per-handler failures.

        Dead-letter payloads are only sent to :class:`DeadLetterHandler`.
        Regular pipeline events are fanned out to every registered handler.
        """
        if isinstance(event, DeadLetterEvent):
            handlers_to_run = [h for h in self._handlers if isinstance(h, DeadLetterHandler)]
            if not handlers_to_run:
                logger.warning("DeadLetterEvent received but no DeadLetterHandler is registered")
                self._processed += 1
                return
        else:
            handlers_to_run = self._handlers

        tasks = [handler.handle(event) for handler in handlers_to_run]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for handler, result in zip(handlers_to_run, results):
            if isinstance(result, Exception):
                logger.warning("Handler %s failed: %s", handler.name, result)
                self.metrics["handler_errors"] = self.metrics.get("handler_errors", 0) + 1

                dead_letter = DeadLetterEvent(
                    original_payload={
                        "event_repr": repr(event),
                        "handler": handler.name,
                    },
                    error_message=str(result),
                    failed_at=datetime.now(UTC),
                    retry_count=0,
                )
                await self.dlq.put(dead_letter)

                dead_letter_handler = next(
                    (registered for registered in self._handlers if isinstance(registered, DeadLetterHandler)),
                    None,
                )
                if isinstance(dead_letter_handler, DeadLetterHandler):
                    await dead_letter_handler.handle(dead_letter)

        self._processed += 1

    async def run(self) -> None:
        """Run continuously and dispatch events from the queue until cancelled."""
        self._running = True
        logger.info("EventRouter started")
        try:
            while True:
                event = await self.queue.get()
                await self._dispatch(event)
                self.queue.task_done()
        finally:
            self._running = False

    async def run_until_empty(self) -> None:
        """Dispatch all events currently queued and then stop processing."""
        while not self.queue.empty():
            event = await self.queue.get()
            await self._dispatch(event)
            self.queue.task_done()
        logger.info("EventRouter processed %s events, stopping", self._processed)

    async def run_until_empty_concurrent(self) -> None:
        """Drain queue continuously, stopping only when queue is empty
        AND no item is currently being processed.
        Uses queue.join() pattern with a stop-when-idle approach."""
        logger.info("Router started in concurrent mode")
        while True:
            try:
                # Wait up to 0.01s for an item — if nothing arrives, check if done
                event = await asyncio.wait_for(self.queue.get(), timeout=0.01)
                await self._dispatch(event)
                self.queue.task_done()
            except asyncio.TimeoutError:
                # No item arrived within timeout — if queue is empty, we're done
                if self.queue.empty():
                    break
        logger.info("Router drained — processed %d events", self._processed)

    @property
    def processed(self) -> int:
        """Return total number of events dispatched by this router."""
        return self._processed

    @property
    def is_running(self) -> bool:
        """Return whether the router run loop is currently active."""
        return self._running
