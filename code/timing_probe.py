#!/usr/bin/env python3
"""One-off: measure actual wall-clock gap between quax_results messages."""
import json, time
from kafka import KafkaConsumer

c = KafkaConsumer(
    "quax_results", bootstrap_servers="localhost:9092",
    auto_offset_reset="latest", value_deserializer=lambda v: json.loads(v),
)
print("probe listening", flush=True)
start = None
last = None
n = 0
for msg in c:
    now = time.time()
    if start is None:
        start = now
    gap = (now - last) if last is not None else 0.0
    last = now
    n += 1
    print(f"result {n}: t={now - start:.3f}s gap={gap:.3f}s", flush=True)
    if n >= 6:
        break
print("probe done", flush=True)
