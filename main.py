"""Demo runner for the klaviyo event pipeline simulation."""

from __future__ import annotations

import asyncio
import logging
import random
import time

from handlers import AnalyticsHandler, AutomationHandler, DeadLetterHandler
from metrics import make_metrics, summary
from producer import EventProducer
from router import EventRouter

logging.basicConfig(level=logging.WARNING)  # suppress DEBUG/INFO for clean output
logger = logging.getLogger(__name__)


async def main() -> None:
    """Run the pipeline demo from event generation through summary output."""
    metrics = make_metrics()
    queue = asyncio.Queue()
    dlq = asyncio.Queue()
    producer = EventProducer(queue, dlq, metrics)
    router = EventRouter(queue, dlq, metrics)

    router.register_handler(AnalyticsHandler(metrics))
    router.register_handler(AutomationHandler(metrics))
    router.register_handler(DeadLetterHandler(metrics))

    event_types = [
        "placed_order",
        "viewed_product",
        "abandoned_cart",
        "clicked_email",
        "invalid_type",
    ]
    event_type_weights = [0.30, 0.35, 0.20, 0.10, 0.05]
    event_specs: list[dict] = []

    # ~5% "invalid_type" is intentional: exercises producer -> DLQ path for unknown event_type.
    for index in range(10_000):
        selected_type = random.choices(event_types, weights=event_type_weights, k=1)[0]
        profile_id = f"profile_{index % 250}"
        kwargs: dict = {}

        if selected_type == "placed_order":
            kwargs = {
                "order_id": f"order_{index}",
                "amount": round(random.uniform(10, 500), 2),
                "items": [f"sku_{index % 50}", f"sku_{(index + 1) % 50}"],
            }
        elif selected_type == "viewed_product":
            kwargs = {
                "product_id": f"product_{index % 100}",
                "category": random.choice(["apparel", "accessories", "home", "beauty"]),
            }
        elif selected_type == "abandoned_cart":
            kwargs = {
                "cart_id": f"cart_{index}",
                "items": [f"sku_{index % 50}", f"sku_{(index + 2) % 50}"],
                "cart_value": round(random.uniform(15, 700), 2),
            }
        elif selected_type == "clicked_email":
            kwargs = {
                "campaign_id": f"campaign_{index % 40}",
                "link_url": f"https://example.com/campaign/{index % 40}?click={index}",
            }
        else:
            # Unknown type: kwargs are ignored by create_event; payload is only for DLQ audit.
            kwargs = {}

        event_specs.append(
            {
                "event_type": selected_type,
                "profile_id": profile_id,
                "kwargs": kwargs,
            }
        )


    start_time = time.monotonic()
    
    # ── Run producer and router concurrently ──────────────────────────────
    start = time.monotonic()

    async def emit_all():
        for spec in event_specs:
            await producer.emit(
                spec["event_type"],
                spec["profile_id"],
                **spec["kwargs"],
            )

    # Producer fills the queue; router drains it at the same time.
    # producer signals done by setting a sentinel or we join after gather.
    await asyncio.gather(
        emit_all(),
        router.run_until_empty_concurrent(),  # see router fix below
    )

    # Drain DLQ
    while not dlq.empty():
        dead = await dlq.get()
        await router._dispatch(dead)
        dlq.task_done()

    emit_duration = time.monotonic() - start_time
    logger.info("Emitting events took %s seconds", emit_duration)
    
    
    
    s = summary(metrics)
    total_time = time.monotonic() - start_time
    events_per_sec = int(metrics["emitted_total"] / emit_duration) if emit_duration > 0 else 0

    print("\n=== Klaviyo Event Pipeline Demo ===")
    print(f"Events emitted:          {s['emitted_total']}  (valid events enqueued only)")
    print(
        f"Events processed:        {s['processed_total']}  "
        "(handler completions; can exceed emitted when multiple handlers run per event)"
    )
    print(f"DLQ (invalid/failed):    {s['dlq_total']}")
    print(f"Backpressure triggers:   {s['backpressure_triggers']}")
    print(f"Handler errors:          {s['handler_errors']}")
    print(f"Emit throughput:         ~{events_per_sec} events/sec")
    print(f"Avg handler latency:     {s['avg_latency_ms']:.3f} ms")
    print(f"p99 handler latency:     {s['p99_latency_ms']:.3f} ms")
    print(f"Total wall time:         {total_time:.2f}s")
    print("\nEvents by type:")
    for etype, count in sorted(s["events_by_type"].items()):
        print(f"  {etype:<20} {count}")


if __name__ == "__main__":
    asyncio.run(main())
