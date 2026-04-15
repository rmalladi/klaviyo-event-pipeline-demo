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
- AI caught: schema validation edge cases in Pydantic v2 model config
- I caught: missing `await` on `asyncio.sleep()` in backpressure logic —
  silent bug that would have made throttling do nothing under load
- I caught: producer and router must run as concurrent `asyncio` tasks —
  sequential execution caused the producer to deadlock in the backpressure
  `while` loop with nothing draining the queue
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
     │  throttles on backpressure signal (while queue.qsize() >= threshold)
     ▼
  [queue]  ← shared asyncio.Queue; depth monitored per emit cycle
     │
EventRouter  ── runs concurrently with producer via asyncio.gather()
     │          asyncio.gather(return_exceptions=True) per dispatch
     ├──► AnalyticsHandler    (all 4 event types)
     ├──► AutomationHandler   (placed_order, abandoned_cart only)
     └──► DeadLetterHandler   (invalid/failed events from DLQ)
```

> **Why `return_exceptions=True`?** One slow or failing handler must
> not block the others. At Klaviyo's scale a flaky ML feature-store
> writer cannot be allowed to stall segmentation or automation flows
> that share the same event stream.

> **Why concurrent tasks?** `asyncio` is single-threaded. If producer
> and router run sequentially the producer's backpressure sleep never
> yields to the router — the queue stays full and the producer deadlocks.
> `asyncio.gather(emit_all(), router.run_concurrent())` lets the event
> loop switch between them: producer sleeps → router drains → producer
> resumes. This mirrors the Kafka model where producer and consumer group
> run as independent processes sharing only the broker.

## Benchmark results

### Run 1 — 1,000 events (baseline)

```
=== Klaviyo Event Pipeline Demo ===
Events emitted:          964   (valid events enqueued only)
Events processed:        1480  (handler completions — exceeds emitted
                                because 3 handlers run per event)
DLQ (invalid/failed):    36
Backpressure triggers:   0
Handler errors:          0
Emit throughput:         ~200,277 events/sec
Avg handler latency:     0.000 ms
p99 handler latency:     0.001 ms
Total wall time:         0.06s

Events by type:
  abandoned_cart         226
  clicked_email          108
  placed_order           290
  viewed_product         340
```

No backpressure at this scale — the queue drains as fast as it fills.

### Run 2 — 10,000 events (backpressure active)

```
=== Klaviyo Event Pipeline Demo ===
Events emitted:          9500  (valid events enqueued only)
Events processed:        14450 (handler completions — exceeds emitted
                                because 3 handlers run per event)
DLQ (invalid/failed):    500
Backpressure triggers:   10
Handler errors:          0
Emit throughput:         ~16,080 events/sec
Avg handler latency:     0.000 ms
p99 handler latency:     0.001 ms
Total wall time:         0.59s

Events by type:
  abandoned_cart         2000
  clicked_email          986
  placed_order           2950
  viewed_product         3564
```

Backpressure triggered 10 times — the producer correctly throttled
when queue depth hit the threshold, then resumed as the router drained.
Throughput drops from ~200K to ~16K events/sec at this scale because
the concurrent task-switching overhead grows with queue contention.

## Reading the numbers

| Metric | What it means |
|--------|---------------|
| `emitted < attempted` | Invalid event types caught at schema validation and routed to DLQ before entering the queue |
| `processed > emitted` | Each valid event dispatches to up to 3 handlers concurrently — AnalyticsHandler receives all 4 types, AutomationHandler receives 2 |
| `DLQ ~5% of attempts` | Matches the intentional 5% `invalid_type` weight in the event mix |
| `Backpressure triggers: 10` | Producer hit the queue-depth threshold 10 times and throttled; confirms concurrent task fix is working |
| `~16K events/sec at 10K scale` | In-memory asyncio; a Kafka-backed system is network/IO bound — typically 50K–500K events/sec per partition depending on payload size |
| `p99 latency: 0.001ms` | In-process handler calls; production latency includes network round-trips to downstream stores |

## Key engineering decisions

| Decision | Rationale |
|----------|-----------|
| `asyncio.gather(return_exceptions=True)` | Handler isolation — one failure must not cancel sibling handlers |
| Concurrent producer + router tasks | Backpressure only works when consumer runs simultaneously; sequential execution causes producer deadlock |
| Pydantic v2 `Literal` on `event_type` | Type-safe dispatch; invalid types rejected at schema validation, not silently passed downstream |
| Separate DLQ from main queue | Failed events need independent retry/replay path without blocking valid event flow |
| `time.monotonic()` for latency | Correct for duration measurement — immune to system clock adjustments |
| `BACKPRESSURE_THRESHOLD` as a constant | Tunable per deployment; in production derived from consumer lag metrics |

## Running it

```bash
uv run python main.py
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
- **Backpressure**: critical at Black Friday scale when one large
  customer can generate 10x normal event volume — the producer must
  slow down rather than overwhelming downstream consumers
- **DLQ**: every event must be accounted for — invalid events are
  quarantined and replayable, not silently dropped
- **Schema validation**: with hundreds of event types and thousands
  of customers, strict schemas at ingestion prevent downstream
  corruption in the feature store and ML pipelines
- **Concurrent producer/consumer**: mirrors the Kafka model where
  producer and consumer group run independently, coordinating only
  through the broker (queue)

## Stack

- Python 3.12
- Pydantic v2 (schema validation)
- asyncio (concurrency)
- pytest (testing)
- uv (dependency management)
- Built with: Cursor (code generation) + Claude (design review)
