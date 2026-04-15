"""Tests for models, metrics, producer, router, and handlers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio  # noqa: F401
from pydantic import ValidationError

from handlers import (
    AnalyticsHandler,
    AutomationHandler,
    BaseHandler,
    DeadLetterHandler,
)
from metrics import make_metrics, record_event_type, summary
from models import (
    AbandonedCartEvent,
    AnyEvent,
    ClickedEmailEvent,
    DeadLetterEvent,
    PlacedOrderEvent,
    ViewedProductEvent,
    create_event,
)
from producer import BACKPRESSURE_THRESHOLD, EventProducer
from router import EventRouter


class FailingHandler(BaseHandler):
    """Test-only handler that always raises to exercise router error paths."""

    async def handle(self, event: AnyEvent) -> bool:
        """Raise unconditionally so the router records a handler failure."""
        raise RuntimeError(f"forced failure for {event.event_type}")


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
    """Unknown event type is routed to DLQ; dlq_total increments when DeadLetterHandler records it."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = {"emitted_total": 0, "dlq_total": 0, "backpressure_triggers": 0}
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    emitted = await producer.emit("unknown", "p1")

    assert emitted is False
    assert dlq.qsize() == 1
    assert metrics["dlq_total"] == 0

    dead = await dlq.get()
    await DeadLetterHandler(metrics).handle(dead)
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


def test_models_create_event_validation_error_missing_fields() -> None:
    """Known event_type with invalid kwargs raises Pydantic ValidationError."""
    with pytest.raises(ValidationError):
        create_event("placed_order", "p1", order_id="o1")


def test_models_create_event_value_error_message_contains_type() -> None:
    """ValueError from factory includes the unknown event type."""
    with pytest.raises(ValueError, match="not_a_real_type"):
        create_event("not_a_real_type", "p1")


def test_models_placed_order_explicit_event_id() -> None:
    """Explicit event_id is preserved when constructing an event."""
    custom_id = uuid4()
    event = PlacedOrderEvent(
        event_id=custom_id,
        profile_id="p1",
        order_id="o1",
        amount=1.0,
        items=["sku"],
    )
    assert event.event_id == custom_id


def test_models_dead_letter_explicit_retry_count() -> None:
    """DeadLetterEvent honors a non-default retry_count."""
    dead = DeadLetterEvent(
        original_payload={},
        error_message="retry",
        failed_at=datetime.now(UTC),
        retry_count=3,
    )
    assert dead.retry_count == 3


def test_metrics_make_metrics_returns_independent_dicts() -> None:
    """Each make_metrics() call must not share nested mutable defaults."""
    first = make_metrics()
    second = make_metrics()
    first["events_by_type"]["placed_order"] = 99
    first["handler_latency_ms"].append(1.0)
    assert second["events_by_type"] == {}
    assert second["handler_latency_ms"] == []


def test_metrics_record_event_type_increments_and_initializes() -> None:
    """record_event_type creates missing keys and increments counts."""
    metrics = make_metrics()
    record_event_type(metrics, "viewed_product")
    record_event_type(metrics, "viewed_product")
    assert metrics["events_by_type"]["viewed_product"] == 2


def test_metrics_summary_empty_latencies() -> None:
    """summary reports zero latency aggregates when no samples exist."""
    result = summary(make_metrics())
    assert result["avg_latency_ms"] == 0.0
    assert result["p99_latency_ms"] == 0.0
    assert "throughput_note" in result


def test_metrics_summary_p99_with_many_samples() -> None:
    """p99 uses sorted latencies (nearest-rank style)."""
    metrics = make_metrics()
    metrics["handler_latency_ms"] = [float(i) for i in range(100)]
    result = summary(metrics)
    assert result["avg_latency_ms"] == 49.5
    assert result["p99_latency_ms"] == 98.0


@pytest.mark.asyncio
async def test_producer_emit_batch_empty_returns_zeros() -> None:
    """emit_batch on an empty list returns zero success and zero failed."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)
    assert await producer.emit_batch([]) == {"success": 0, "failed": 0}


@pytest.mark.asyncio
async def test_producer_emit_batch_skips_non_dict_and_missing_keys() -> None:
    """emit_batch tolerates bad rows without aborting the rest."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    result = await producer.emit_batch(
        [
            "not-a-dict",
            {"event_type": "placed_order"},
            {
                "event_type": "viewed_product",
                "profile_id": "p1",
                "product_id": "x",
                "category": "y",
            },
        ]
    )

    assert result == {"success": 1, "failed": 2}
    assert metrics["emitted_total"] == 1


@pytest.mark.asyncio
async def test_producer_emit_invalid_payload_caught_as_valueerror_subclass() -> None:
    """ValidationError subclasses ValueError; producer routes invalid schema to DLQ (same branch as unknown type)."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    producer = EventProducer(queue=queue, dlq=dlq, metrics=metrics)

    emitted = await producer.emit("clicked_email", "p1", campaign_id="only_one_field")

    assert emitted is False
    assert queue.qsize() == 0
    assert dlq.qsize() == 1
    dead: DeadLetterEvent = await dlq.get()
    assert isinstance(dead, DeadLetterEvent)
    assert "link_url" in dead.error_message or "validation" in dead.error_message.lower()


@pytest.mark.asyncio
async def test_router_dispatch_dead_letter_only_hits_dlq_handler() -> None:
    """DeadLetterEvent must not be sent to AnalyticsHandler (no event_type)."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    router = EventRouter(queue, dlq, metrics)
    analytics = AnalyticsHandler(metrics)
    dlq_handler = DeadLetterHandler(metrics)
    router.register_handler(analytics)
    router.register_handler(dlq_handler)

    dead = DeadLetterEvent(
        original_payload={"reason": "test"},
        error_message="synthetic",
        failed_at=datetime.now(UTC),
    )
    await router._dispatch(dead)

    assert analytics.event_counts == {}
    assert dlq_handler.dead_letter_count == 1
    assert metrics["dlq_total"] == 1
    assert router.processed == 1


@pytest.mark.asyncio
async def test_router_dispatch_dead_letter_without_dlq_handler_increments_processed() -> None:
    """Router warns and still counts dispatch when no DeadLetterHandler is registered."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    router = EventRouter(queue, dlq, metrics)
    router.register_handler(AnalyticsHandler(metrics))

    dead = DeadLetterEvent(
        original_payload={},
        error_message="orphan",
        failed_at=datetime.now(UTC),
    )
    await router._dispatch(dead)

    assert router.processed == 1


@pytest.mark.asyncio
async def test_router_run_until_empty_drains_all() -> None:
    """run_until_empty processes every queued item."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    router = EventRouter(queue, dlq, metrics)
    router.register_handler(AnalyticsHandler(metrics))

    for _ in range(3):
        await queue.put(
            create_event("viewed_product", "p1", product_id="prod", category="c")
        )

    await router.run_until_empty()

    assert queue.empty()
    assert router.processed == 3
    assert metrics["processed_total"] == 3


@pytest.mark.asyncio
async def test_router_gather_handler_failure_increments_errors_and_dlq() -> None:
    """One failing handler does not prevent others; failure is recorded."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    router = EventRouter(queue, dlq, metrics)
    router.register_handler(FailingHandler(metrics))
    router.register_handler(AnalyticsHandler(metrics))
    router.register_handler(DeadLetterHandler(metrics))

    event = create_event("placed_order", "p1", order_id="o1", amount=1.0, items=["a"])
    await router._dispatch(event)

    assert metrics["handler_errors"] == 1
    assert dlq.qsize() == 1
    assert metrics["processed_total"] == 1


@pytest.mark.asyncio
async def test_analytics_handler_event_counts_match_emitted_types() -> None:
    """AnalyticsHandler local counts mirror handled event types."""
    metrics = make_metrics()
    handler = AnalyticsHandler(metrics)
    await handler.handle(create_event("clicked_email", "p1", campaign_id="c", link_url="https://x"))
    await handler.handle(create_event("clicked_email", "p1", campaign_id="c2", link_url="https://y"))
    assert handler.event_counts == {"clicked_email": 2}


@pytest.mark.asyncio
async def test_automation_handler_skips_non_automation_event_types() -> None:
    """AutomationHandler returns True without triggering for unrelated types."""
    metrics = make_metrics()
    handler = AutomationHandler(metrics)
    await handler.handle(create_event("viewed_product", "p1", product_id="p", category="c"))
    assert handler.triggered_count == 0
    assert metrics["processed_total"] == 0


@pytest.mark.asyncio
async def test_dead_letter_handler_skips_pipeline_events() -> None:
    """DeadLetterHandler ignores normal pipeline events."""
    metrics = make_metrics()
    handler = DeadLetterHandler(metrics)
    await handler.handle(create_event("abandoned_cart", "p1", cart_id="c", items=["i"], cart_value=1.0))
    assert handler.dead_letter_count == 0
    assert metrics["dlq_total"] == 0


@pytest.mark.asyncio
async def test_router_cancel_run_loop_sets_is_running_false() -> None:
    """Cancelled run() clears is_running in finally."""
    queue: asyncio.Queue = asyncio.Queue()
    dlq: asyncio.Queue = asyncio.Queue()
    metrics = make_metrics()
    router = EventRouter(queue, dlq, metrics)
    router.register_handler(AnalyticsHandler(metrics))

    task = asyncio.create_task(router.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert router.is_running is False
