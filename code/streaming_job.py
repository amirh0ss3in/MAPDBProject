#!/usr/bin/env python3
"""Spark processor: consumes quax_stream, distributes the FFT work across
worker1/worker2, merges per-file partial sums on the driver, and publishes
finished spectra to quax_results (plus per-chunk routing telemetry)."""
import json, os, socket, subprocess, time

PYTHON_BIN = "/home/ubuntu/pyvenv/bin/python3"
os.environ["PYSPARK_PYTHON"] = PYTHON_BIN
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_BIN
# Never attach to a stale gateway JVM leaked into this shell by a previous crashed run.
os.environ.pop("PYSPARK_GATEWAY_PORT", None)
os.environ.pop("PYSPARK_GATEWAY_SECRET", None)

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from physics import NBINS, SCANS_PER_FILE, FREQS, chunk_stats, finalize
from producer import ensure_kafka

MB = 1024 * 1024
SPARK_HOME = os.path.dirname(__import__("pyspark").__file__)
WORKERS = ["worker1", "worker2"]


def port_open(host, port, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_spark():
    if not port_open("master", 7077):
        subprocess.run(
            f"{SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master "
            f"--host master --port 7077 --webui-port 8080 "
            f">> /home/ubuntu/spark-master.log 2>&1 &", shell=True)
        for _ in range(20):
            if port_open("master", 7077):
                break
            time.sleep(1)
    for w in WORKERS:
        running = subprocess.run(f"ssh {w} pgrep -f org.apache.spark.deploy.worker.Worker",
                                  shell=True, capture_output=True).stdout.strip()
        if not running:
            subprocess.run(
                f"ssh {w} \"PYSPARK_PYTHON={PYTHON_BIN} {SPARK_HOME}/bin/spark-class "
                f"org.apache.spark.deploy.worker.Worker spark://master:7077 "
                f"--webui-port 8081 --cores 2 --memory 1500m "
                f">> /home/ubuntu/spark-worker.log 2>&1 &\"", shell=True)


def process_chunk(item):
    file_id, chunk_idx, value = item
    s, sq, n = chunk_stats(value)
    return file_id, chunk_idx, socket.gethostname(), s.tolist(), sq.tolist(), n


def main():
    ensure_kafka("localhost:9092")
    ensure_spark()
    spark = (
        SparkSession.builder.appName("quax-processor")
        .master("spark://master:7077")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "physics.py"))

    consumer = KafkaConsumer(
        "quax_stream", bootstrap_servers="localhost:9092", auto_offset_reset="latest",
        fetch_max_bytes=64 * MB, max_partition_fetch_bytes=64 * MB, consumer_timeout_ms=500,
    )
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                              value_serializer=lambda v: json.dumps(v).encode())

    acc = {}  # file_id -> (sum, sumsq, count)
    print("Listening for chunks...", flush=True)

    # Cap the data shipped per Spark job: chunks ride inside task closures through the
    # driver JVM, so an uncapped drain (e.g. a fast producer or a backlog after a stall)
    # can put hundreds of MB on a 1g driver heap at once and OOM it.
    max_chunks_per_job = 8

    while True:
        batch = []
        t_wait = time.time()
        for msg in consumer:
            headers = dict(msg.headers)
            batch.append((headers["file_id"].decode(), int(headers["chunk_idx"].decode()), msg.value))
            if len(batch) >= max_chunks_per_job:
                break
        if not batch:
            continue

        t0 = time.time()
        results = sc.parallelize(batch, numSlices=len(batch)).map(process_chunk).collect()
        print(f"[{time.time():.3f}] batch of {len(batch)} chunk(s): waited {t0 - t_wait:.2f}s, spark job {time.time() - t0:.2f}s", flush=True)
        for file_id, chunk_idx, host, s, sq, n in results:
            producer.send("quax_telemetry", {"file_id": file_id, "chunk_idx": chunk_idx, "host": host})
            total, total_sq, count = acc.get(file_id, (np.zeros(NBINS), np.zeros(NBINS), 0))
            acc[file_id] = (total + s, total_sq + sq, count + n)
            if acc[file_id][2] >= SCANS_PER_FILE:
                mean, rms = finalize(*acc[file_id])
                producer.send("quax_results", {"average": {
                    "frequency": FREQS.tolist(), "value": mean.tolist(), "rms": rms.tolist()}})
                del acc[file_id]


if __name__ == "__main__":
    main()
