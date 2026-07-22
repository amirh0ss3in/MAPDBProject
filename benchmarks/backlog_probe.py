#!/usr/bin/env python3
"""Logs Kafka offsets for quax_stream every 2s, producing the raw data behind
benchmarks/benchmark_{8,16,32}mbps.log and, via plot_benchmark.py, benchmark.png.

For the full 0.5x/1x/2x benchmark, just run ./run_benchmark.sh instead of any
of this by hand — it does all of the below for all three rates automatically.

Manual usage (run alongside a pipeline at a given rate, save its output under
the matching name) — useful for a single rate or if you want to see each step:

    # terminal 1: reset the consumer group for a clean run, then start the probe
    ~/kafka_2.13-3.7.0/bin/kafka-consumer-groups.sh \\
        --bootstrap-server localhost:9092 --delete --group quax-processor
    python3 backlog_probe.py > benchmark_8mbps.log &

    # terminal 2: run the pipeline at the matching rate
    cd ~/Project && ./run.sh start --rate 0.5 --n-pairs 20   # 0.5x -> 8 MB/s

    # once the producer finishes and backlog drains back to 0, stop both:
    ./run.sh stop
    kill %1   # or: pkill -f backlog_probe.py

Repeat per rate (0.5 -> benchmark_8mbps.log, 1.0 -> benchmark_16mbps.log,
2.0 -> benchmark_32mbps.log), resetting the quax-processor group between runs
so each log starts from a clean backlog of 0. Then regenerate the figure:

    python3 plot_benchmark.py
"""
import time
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

TP = TopicPartition("quax_stream", 0)
GROUP = "quax-processor"

c = KafkaConsumer(bootstrap_servers="localhost:9092")
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

start = time.time()
print("probe listening", flush=True)
while True:
    end = c.end_offsets([TP])[TP]
    committed = admin.list_consumer_group_offsets(group_id=GROUP)
    processed = committed[TP].offset if TP in committed else end
    lag = max(0, end - processed)
    print(f"t={time.time() - start:.1f}s end={end} committed={processed} lag={lag}", flush=True)
    time.sleep(2)
