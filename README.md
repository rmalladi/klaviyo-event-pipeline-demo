# klaviyo-event-pipeline-demo

A Python event pipeline simulator demonstrating real-time event
ingestion, fan-out routing, backpressure, and dead-letter queue
handling — the core patterns in Klaviyo's events platform.

Built using an AI-assisted development workflow: Cursor agent mode
for scaffolding, Claude for design review, human review on every
change before merge.

## What this demonstrates

**Distributed systems patterns**
- Typed event schemas with Pydantic validation
- Fan-out routing: one event dispatched to N independent consumers
- Dead-letter queue for failed or invalid events
- Backpressure: producer throttles when consumer queue depth exceeds threshold
- In-memory metrics: throughput, error rate, queue depth, handler latency

**AI-assisted development (L3 maturity)**
- Cursor agent mode used to scaffold each module
- Every generated line reviewed in diff view before accepting
- PR descriptions document AI vs. human decisions (see PROMPTS.md)

## Event types

| Event | Payload fields | Downstream consumers |
|-------|---------------|---------------------|
| placed_order | order_id, amount, items | Analytics, Automation |
| viewed_product | product_id, category | Analytics |
| abandoned_cart | cart_id, items, value | Automation (high priority) |
| clicked_email | campaign_id, link_url | Analytics |

## Architecture

```
EventProducer
     │  validates schema (Pydantic)
     │  throttles on backpressure signal
     ▼
  [queue]  ← queue depth monitored by MetricsCollector
     │
EventRouter
     ├──► AnalyticsHandler   (all event types)
     ├──► AutomationHandler  (placed_order, abandoned_cart)
     └──► DeadLetterHandler  (validation failures, handler errors)
```

## Running it

```bash
uv run python main.py
```

Expected output:
```
Events emitted:    1000
Events processed:  987
DLQ depth:         13
Throughput:        ~2400 events/sec
p99 latency:       < 2ms
Backpressure triggers: 3
```

## Running tests

```bash
uv run pytest test_pipeline.py -v
```

## AI workflow log

See [PROMPTS.md](./PROMPTS.md) for a full log of prompts used,
what Cursor generated, and what I changed in review.

## Connection to Klaviyo's events platform

Klaviyo processes billions of events daily across Profiles, Objects,
and Events — their core data entities. This project demonstrates
the same architectural patterns at small scale:

- **Fan-out**: Klaviyo's single event stream dispatches to
  segmentation, flows, analytics, and ML features simultaneously
- **Backpressure**: critical at Black Friday scale when one
  large customer can generate 10x normal event volume
- **DLQ**: Klaviyo needs every event accounted for —
  invalid events must be quarantined, not silently dropped
- **Schema validation**: with hundreds of event types and
  thousands of customers, strict schemas prevent downstream corruption

## Stack

- Python 3.12
- Pydantic v2 (schema validation)
- asyncio (concurrency)
- pytest (testing)
- uv (dependency management)
- Built with: Cursor (code gen) + Claude (design review)
