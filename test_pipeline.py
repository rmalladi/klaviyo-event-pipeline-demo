"""Tests for models and producer behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
import pytest_asyncio  # noqa: F401

from models import (
    AbandonedCartEvent,
    ClickedEmailEvent,
    DeadLetterEvent,
    PlacedOrderEvent,
    ViewedProductEvent,
    create_event,
)
from producer import BACKPRESSURE_THRESHOLD, EventProducer


def test_models_base_fields_auto_generated() -> None:
    """Event factory auto-generates UUID event_id and timestamp fields."""
    event = create_event(
        "placed_order",
        "profile_1",
        order_id="order_1",
        amount=9.99,
        items=["sku_1"],
    )
    assert isinstance(event.event_id, UUID)
    assert isinstance(event.timestamp, datetime)


def test_models_placed_order_valid() -> None:
    """PlacedOrderEvent accepts valid required fields."""
    event = PlacedOrderEvent(
        profile_id="profile_1",
        order_id="order_1",
        amount=12.5,
        items=["sku_1", "sku_2"],
    )
    assert event.event_type == "placed_order"
    assert event.profile_id == "profile_1"
    assert event.order_id == "order_1"
    assert event.amount == 12.5
    assert event.items == ["sku_1", "sku_2"]


def test_models_create_event_factory() -> None:
    """Factory returns the correct subclass for each supported event type."""
    placed = create_event(
        "placed_order",
        "p1",
        order_id="o1",
        amount=1.0,
        items=["sku_1"],
    )
    viewed = create_event("viewed_product", "p1", product_id="prod_1", category="shirts")
    abandoned = create_event(
        "abandoned_cart",
        "p1",
        cart_id="cart_1",
        items=["sku_1"],
        cart_value=3.0,
    )
    clicked = create_event(
        "clicked_email",
        "p1",
        campaign_id="cmp_1",
        link_url="https://example.com",
    )

    assert isinstance(placed, PlacedOrderEvent)
    assert isinstance(viewed, ViewedProductEvent)
    assert isinstance(abandoned, AbandonedCartEvent)
    assert isinstance(clicked, ClickedEmailEvent)


def test_models_create_event_unknown_type() -> None:
    """Factory raises ValueError for unknown event types."""
    with pytest.raises(ValueError):
        create_event("unknown", "p1")


def test_models_dead_letter_event() -> None:
    """DeadLetterEvent defaults retry_count to zero."""
    dead_letter = DeadLetterEvent(
        original_payload={"event_type": "unknown"},
        error_message="boom",
        failed_at=datetime.now(UTC),
    )
    assert dead_letter.retry_count == 0


@pytest.mark.asyncio
async def test_producer_emit_success() -> None:
    """Valid emit enqueues one event and increments emitted metric."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    emitted = await producer.emit(
        "placed_order",
        "p1",
        order_id="o1",
        amount=10.0,
        items=["sku_1"],
    )

    assert emitted is True
    assert queue.qsize() == 1
    assert metrics["emitted_total"] == 1


@pytest.mark.asyncio
async def test_producer_emit_invalid_type() -> None:
    """Unknown event type is routed to DLQ and counted."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    emitted = await producer.emit("unknown", "p1")

    assert emitted is False
    assert dlq.qsize() == 1
    assert metrics["dlq_total"] == 1


@pytest.mark.asyncio
async def test_producer_backpressure_triggers() -> None:
    """Producer increments backpressure metric when threshold is reached."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    for _ in range(BACKPRESSURE_THRESHOLD):
        await queue.put("seed")

    async def drain_one() -> None:
        await asyncio.sleep(0.06)
        await queue.get()
        queue.task_done()

    drain_task = asyncio.create_task(drain_one())
    await producer.emit(
        "placed_order",
        "p1",
        order_id="o2",
        amount=5.0,
        items=["sku_1"],
    )
    await drain_task

    assert metrics["backpressure_triggers"] >= 1


@pytest.mark.asyncio
async def test_producer_emit_batch_mixed() -> None:
    """Batch emit reports mixed valid/invalid outcomes."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    result = await producer.emit_batch(
        [
            {"event_type": "placed_order", "profile_id": "p1", "order_id": "o1", "amount": 1.0, "items": ["a"]},
            {"event_type": "viewed_product", "profile_id": "p1", "product_id": "prod_1", "category": "cat"},
            {
                "event_type": "abandoned_cart",
                "profile_id": "p1",
                "cart_id": "c1",
                "items": ["a"],
                "cart_value": 4.0,
            },
            {"event_type": "unknown", "profile_id": "p1"},
        ]
    )

    assert result == {"success": 3, "failed": 1}


@pytest.mark.asyncio
async def test_producer_throttle_count_increments() -> None:
    """Producer throttle_count increases when backpressure loop runs."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    for _ in range(BACKPRESSURE_THRESHOLD):
        await queue.put("seed")

    async def drain_one() -> None:
        await asyncio.sleep(0.06)
        await queue.get()
        queue.task_done()

    drain_task = asyncio.create_task(drain_one())
    await producer.emit(
        "clicked_email",
        "p1",
        campaign_id="cmp_1",
        link_url="https://example.com",
    )
    await drain_task

    assert producer.throttle_count > 0
