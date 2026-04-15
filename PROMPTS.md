# AI Workflow Log — klaviyo-event-pipeline-demo

##  models.py + producer.py

### models.py

**Tool used:** Cursor Agent mode  
**Prompt summary:** Asked Cursor to generate Pydantic v2 event schemas
for 4 event types + DeadLetterEvent + `create_event()` factory.

**What Cursor generated correctly:**

- Literal `event_type` on each subclass aligned with the four fixed types.
- `create_event()` factory mapping `event_type` → concrete model and `ValueError` for unknown types.
- Auto-generated `event_id` (UUID) and `timestamp` (timezone-aware UTC via `datetime.now(UTC)`).

**What I changed in review:**

- Ensured timestamp defaults use aware UTC datetimes (not naive `utcnow()` / invalid helpers).
- Kept `DeadLetterEvent` as its own `BaseModel` (not extending `EventBase`) so DLQ payloads stay explicit.

**Why it matters:**  
Mixing naive and aware datetimes or using incorrect defaults breaks comparisons and serialization; using DLQ as a separate model avoids pretending that invalid payloads are first-class pipeline events.

---

### producer.py

**Tool used:** Cursor Agent mode  
**Prompt summary:** Asked Cursor to generate `EventProducer` with
async `emit()`, `emit_batch()`, backpressure throttling, and DLQ routing.

**What Cursor generated correctly:**

- `asyncio.Queue` usage, `logging.getLogger(__name__)`, and `emit_batch()` success/failed counts.
- Fault-tolerant `emit_batch()` (malformed rows do not abort the whole batch).

**What I changed in review:**

- Replaced one-shot backpressure sleep with a `**while` loop** so the producer keeps throttling until queue depth drops below the threshold (avoids enqueueing immediately after a single sleep while still saturated).
- **DLQ metric ownership:** stopped incrementing `metrics["dlq_total"]` inside the producer when enqueueing unknown-type `DeadLetterEvent`s. Only `DeadLetterHandler` increments `dlq_total` when it records an item, so draining the DLQ does not **double-count** the same failure.

**Design question I asked Claude:**  
"What happens if the consumer stops reading from the queue entirely?"

**Claude's answer summary:**  
With an unbounded queue the producer does not deadlock but memory grows; with a bounded queue `put` blocks. Throttling reduces enqueue rate but does not cap total backlog without a max queue or drop policy.

**What I decided NOT to change and why:**  
Kept demo-oriented throttle constants; production would add bounded queues, quotas, and explicit overflow/DLQ policies.

---

### Tests (initial)

**Tests written:** 10 (5 for models, 5 for producer)  
**Tests passing:** 10/10  
**Failures and root cause:** None after aligning `test_producer_emit_invalid_type` with `dlq_total` being updated only when `DeadLetterHandler` processes a DLQ item.

---

## metrics, handlers, router, main + expanded tests

### modules added or filled in

- `**metrics.py`** — `make_metrics()`, `record_event_type()`, `summary()` with avg / p99 latency (stdlib only, no numpy).
- `**handlers.py**` — `AnalyticsHandler`, `AutomationHandler`, `DeadLetterHandler`; shared metrics dict and per-handler latency appends.
- `**router.py**` — `EventRouter` with `asyncio.gather(..., return_exceptions=True)` for fan-out; DLQ-only routing for `DeadLetterEvent` (see bugs section).
- `**main.py**` — demo runner: weighted random event mix, emit phase, `run_until_empty()`, DLQ drain.

### Tests expanded (`test_pipeline.py`)

**Current count:** 29 tests, all passing.

**Added coverage:**

- **Models:** validation errors on missing fields; explicit `event_id` / `retry_count`; unknown-type error message.
- **Metrics:** independent dicts per `make_metrics()`; `record_event_type` increments; `summary()` empty latencies and p99 on a known sample set.
- **Producer:** empty `emit_batch`; malformed batch rows (non-dict, missing keys); **invalid schema vs DLQ** — Pydantic `ValidationError` subclasses `ValueError`, so `emit()`’s `except ValueError` currently routes schema failures to the DLQ the same way as unknown `event_type` (documented in tests, not “silent success”).
- **Router:** `DeadLetterEvent` only hits `DeadLetterHandler`; no DLQ handler registered still increments `processed`; `run_until_empty` drains queue; failing handler + `gather` isolation increments `handler_errors` and enqueues DLQ artifact; cancelled `run()` clears `is_running`.
- **Handlers:** analytics `event_counts`; automation skips non–placed_order/abandoned_cart; DLQ handler skips pipeline events.

**Test helper:** `FailingHandler` (raises on every `handle`) to exercise router failure paths without production code changes.

---

## Bugs fixed (router, handlers, main, metrics)

These showed up when running the full demo (`main.py`) end-to-end.

### 1. DLQ items fanned out to all handlers (`router.py`)

**Symptom:** Warnings like `'DeadLetterEvent' object has no attribute 'event_type'` from `AnalyticsHandler` / `AutomationHandler` when `main.py` drained the DLQ and called `router._dispatch(dead)`.

**Root cause:** `_dispatch` sent every payload—including `DeadLetterEvent`—to every registered handler. Only pipeline events (`AnyEvent`) expose `event_type` / `profile_id` in the shape those handlers expect.

**Fix:** In `_dispatch`, if the payload is a `DeadLetterEvent`, only run handlers that are instances of `DeadLetterHandler`. Regular events still fan out to all handlers.

---

### 2. Double-counting `dlq_total` (`producer.py` + `handlers.py`)

**Symptom:** Printed `DLQ (invalid/failed)` roughly **2×** the number of producer-side unknown types (e.g. ~5% invalid in the demo, but DLQ total looked ~2× that).

**Root cause:** Producer incremented `dlq_total` when putting a `DeadLetterEvent` for unknown `event_type`, then `DeadLetterHandler` incremented again when the same item was drained and processed.

**Fix:** Producer enqueues to the DLQ only; `DeadLetterHandler` is the single place that increments `dlq_total` when recording.

---

### 3. Noisy DLQ logs for intentionally invalid events (`handlers.py`)

**Symptom:** Many `WARNING` lines for `Unknown event_type: invalid_type` during the demo even though ~5% invalid types are **by design** for DLQ coverage.

**Fix:** `DeadLetterHandler` logs DLQ ingestion at **DEBUG** instead of **WARNING**, so `logging.basicConfig(level=logging.WARNING)` keeps terminal output clean; the printed summary still reports DLQ totals.

---

### 4. Demo event volume and misleading payloads (`main.py`)

**Symptom:** Loop used `range(100)` while the README described ~1000 events; `invalid_type` rows carried an extra `unexpected_field` that added noise to DLQ payloads without affecting validation.

**Fix:** Restored `range(1000)` for the demo mix; `invalid_type` specs use empty `kwargs` where appropriate; clarified printed lines that `**emitted_total`** counts only valid enqueues and `**processed_total**` can exceed it when multiple handlers count per event.

---

### 5. Debug noise in router (`router.py`)

**Symptom:** `logger.info` dumping coroutine/task objects during dispatch.

**Fix:** Removed that debug log line.
---

## Backpressure deadlock fix (1,500 → 10,000 events)

### Bug: producer deadlocks when event volume exceeds threshold

**Symptom:** Running `main.py` with 1,500+ events caused the process
to hang indefinitely at the emit loop with no output, even after 30+ minutes.
Logging revealed the producer was stuck in the backpressure `while` loop.

**Root cause:** `asyncio` is single-threaded. The producer and router
were running sequentially — emit all events first, then drain. When the
queue depth hit `BACKPRESSURE_THRESHOLD` (1,000), `emit()` entered the
`while queue.qsize() >= BACKPRESSURE_THRESHOLD` loop and called
`await asyncio.sleep(THROTTLE_DELAY_SECONDS)`. The `await` yielded
control back to the event loop — but no other task was registered to
run. The router had not started yet. So the sleep resolved, the queue
was still at 1,000 (nothing had drained it), and the producer slept
again. Indefinitely.

```python
# What was happening — sequential, broken for large volumes
emit_start = time.monotonic()
for spec in event_specs:
    await producer.emit(...)          # hangs here at event 1001
emit_duration = time.monotonic() - emit_start

await router.run_until_empty()        # never reached
```

**Fix:** Run producer and router as concurrent `asyncio` tasks using
`asyncio.gather()`. When the producer's backpressure sleep yields,
the event loop switches to the router task, which drains the queue.
Queue depth drops below the threshold, the producer's `while` condition
becomes false, and it resumes emitting.

```python
# Fix — concurrent tasks, backpressure works correctly
async def emit_all():
    for spec in event_specs:
        await producer.emit(
            spec["event_type"],
            spec["profile_id"],
            **spec["kwargs"],
        )

await asyncio.gather(
    emit_all(),
    router.run_until_empty_concurrent(),  # drains while producer fills
)
```

Added `run_until_empty_concurrent()` to `EventRouter` — uses
`asyncio.wait_for(queue.get(), timeout=0.01)` in a loop so the
router checks for new items continuously and exits cleanly once
the queue is empty and the producer has finished.

**Why this matters at Klaviyo's scale:**
This is the exact reason Kafka separates producers and consumers
into independent processes. A Kafka producer never waits for
consumers — it writes to the broker and moves on. Consumer groups
read at their own pace. Backpressure is signalled via consumer lag
metrics and external throttling policies, not by blocking the producer
in-process. This asyncio fix is the correct local analogue of that model.

**Result after fix (10,000 events):**

```
Events emitted:          9500
Events processed:        14450
DLQ (invalid/failed):    500
Backpressure triggers:   10      ← confirms throttling now works
Handler errors:          0
Emit throughput:         ~16,080 events/sec
Total wall time:         0.59s
```

Backpressure triggered 10 times — the producer correctly throttled
and resumed as the router drained. Throughput drops from ~200K
(1K sequential run) to ~16K (10K concurrent run) due to task-switching
overhead under queue contention — expected and acceptable.

**What I decided NOT to change:**
Kept `BACKPRESSURE_THRESHOLD = 1000` and `THROTTLE_DELAY_SECONDS = 0.05`
as module-level constants. In production these would be derived from
consumer lag telemetry (e.g. Kafka consumer group lag metrics) and
adjusted dynamically. The constant approach is correct for a demo
with a known event volume.
