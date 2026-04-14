# AI Workflow Log — klaviyo-event-pipeline-demo

## Day 2: models.py + producer.py

### models.py

**Tool used:** Cursor Agent mode
**Prompt summary:** Asked Cursor to generate Pydantic v2 event schemas
for 4 event types + DeadLetterEvent + create_event() factory.

**What Cursor generated correctly:**
- [e.g. Correct Literal types on each subclass event_type field]
- [e.g. create_event() factory with correct union dispatch]
- [e.g. Auto-generated UUID and timestamp defaults]

**What I changed in review:**
- [e.g. Changed datetime.now() to datetime.utcnow for timezone consistency]
- [e.g. Fixed DeadLetterEvent — Cursor made it extend EventBase, I removed that]
- [e.g. Added missing ValueError message to create_event() for clarity]

**Why it matters:**
[1-2 sentences on why the thing you caught could have caused a real bug]

---

### producer.py

**Tool used:** Cursor Agent mode
**Prompt summary:** Asked Cursor to generate EventProducer class with
async emit(), emit_batch(), backpressure throttling, and DLQ routing.

**What Cursor generated correctly:**
- [e.g. Correct asyncio.Queue usage throughout]
- [e.g. Proper logging setup with getLogger(__name__)]
- [e.g. emit_batch() returning success/failed counts]

**What I changed in review:**
- [e.g. Added while loop to backpressure check — Cursor only slept once,
  queue could still grow unbounded if consumer stalled]
- [e.g. Fixed missing await on asyncio.sleep() call]

**Design question I asked Claude:**
"What happens if the consumer stops reading from the queue entirely?"

**Claude's answer summary:**
[What Claude said + what you did about it]

**What I decided NOT to change and why:**
- [e.g. Kept THROTTLE_DELAY_SECONDS at 0.05 — Cursor suggested 0.1
  but 50ms is a better balance for demo throughput]

---

### Tests

**Tests written:** 10 (5 for models, 5 for producer)
**Tests passing:** [X/10]
**Failures and root cause:** [if any]