"""Utilities for pipeline metrics creation, updates, and reporting."""

from __future__ import annotations

import math
from typing import Any


def make_metrics() -> dict[str, Any]:
    """Create and return a fresh metrics dictionary with default counters."""
    return {
        "emitted_total": 0,
        "processed_total": 0,
        "dlq_total": 0,
        "backpressure_triggers": 0,
        "handler_errors": 0,
        "events_by_type": {},
        "handler_latency_ms": [],
    }


def record_event_type(metrics: dict[str, Any], event_type: str) -> None:
    """Increment the per-event-type counter in the provided metrics dictionary."""
    events_by_type = metrics.setdefault("events_by_type", {})
    events_by_type[event_type] = events_by_type.get(event_type, 0) + 1


def summary(metrics: dict[str, Any]) -> dict[str, Any]:
    """Return a human-readable metrics summary with average and p99 latency."""
    latencies_raw = metrics.get("handler_latency_ms", [])
    latencies: list[float] = [float(value) for value in latencies_raw]

    if latencies:
        avg_latency_ms = sum(latencies) / len(latencies)
        sorted_latencies = sorted(latencies)
        p99_index = max(0, math.ceil(0.99 * len(sorted_latencies)) - 1)
        p99_latency_ms = sorted_latencies[p99_index]
    else:
        avg_latency_ms = 0.0
        p99_latency_ms = 0.0

    return {
        "emitted_total": int(metrics.get("emitted_total", 0)),
        "processed_total": int(metrics.get("processed_total", 0)),
        "dlq_total": int(metrics.get("dlq_total", 0)),
        "backpressure_triggers": int(metrics.get("backpressure_triggers", 0)),
        "handler_errors": int(metrics.get("handler_errors", 0)),
        "events_by_type": dict(metrics.get("events_by_type", {})),
        "avg_latency_ms": float(avg_latency_ms),
        "p99_latency_ms": float(p99_latency_ms),
        "throughput_note": "see main.py for events/sec calculation",
    }
