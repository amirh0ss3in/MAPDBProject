# QUAX Experiment: Distributed Real-Time Data Pipeline

## Overview

This project implements a streaming data pipeline for the QUAX experiment. It fetches high-frequency IQ radio data from a CloudVeneto S3 bucket, processes the Fast Fourier Transform (FFT) across a distributed cluster, and visualizes the resulting power spectrum in real-time.

The distributed processing step is implemented on **two independent engines — Dask and Apache Spark** — which are then compared head-to-head in a dedicated benchmark.

## Architecture

- **Producer:** Pushes "work orders" (S3 filenames) to Apache Kafka to simulate a continuous DAQ stream.
- **Processor:** A Kafka consumer that delegates data downloading and FFT math to a distributed cluster. Processed results are pushed to a second Kafka topic. Two interchangeable implementations exist:
  - `processor.py` — Dask (multi-node `SSHCluster`)
  - `processor_spark.py` — Apache Spark (Approach 1: Spark distributes whole batches; the FFT itself runs in one shot with NumPy, exactly as in the Dask worker)
- **Dashboard:** A Streamlit application that consumes the processed data and updates a live frequency plot.

## Infrastructure Setup

The pipeline was built and tested on 3 CloudVeneto Virtual Machines (Ubuntu 24.04).

- 1 Master Node (runs Kafka, Streamlit, and the Dask Scheduler)
- 2 Worker Nodes (Dask Workers)

*Note: Passwordless SSH was configured between the master and worker nodes.*

## Prerequisites

Before running, Apache Kafka must be active on the master node, and S3 credentials must be exported to the environment:

```bash
export S3_ACCESS_KEY="your_access_key"
export S3_SECRET_KEY="your_secret_key"
```

## How to Run the Pipeline

The pipeline requires three separate terminals running on the Master node.

1. **Start the Processor** (choose one engine):

```bash
source ~/pyvenv/bin/activate
python3 code/processor.py          # Dask version
# or
python3 code/processor_spark.py    # Spark version
```

2. **Start the Kafka Producer:**

```bash
source ~/pyvenv/bin/activate
python3 code/producer.py
```

3. **Start the Live Dashboard:**

```bash
source ~/pyvenv/bin/activate
streamlit run code/dashboard.py
```

(View the dashboard by port-forwarding port 8501 to your local machine.)

## Spark Implementation

A second, independent implementation of the processor was built on Apache Spark (`code/processor_spark.py`) so that the two engines could be compared fairly. Everything is kept identical between the two versions — the same Kafka topics, the same S3 source, and the same NumPy FFT — so that **only the distributed-processing engine changes**.

**Design decision (Approach 1):** Spark distributes *whole batches* across the cluster and runs the FFT once with NumPy inside each task, mirroring exactly what the Dask worker does. Spark is deliberately *not* used to parallelize a single FFT internally, because that would compare the *algorithm* rather than the *engine* and make the benchmark misleading.

## Benchmark: Spark vs Dask

Both engines were benchmarked on 20 identical batches of real QUAX data, run synchronously (submit one batch, wait for its result, record the time). For every batch the processing time, CPU %, and memory % were logged with `psutil`.

**Files:**
- `code/benchmark_spark.py` — instrumented Spark processor
- `code/benchmark_dask.py` — instrumented Dask processor
- `code/benchmark_spark_results.csv` — raw Spark results
- `code/benchmark_dask_results.csv` — raw Dask results

**Key findings (warm state, after the first batch):**

| Metric              | Spark   | Dask    |
| ------------------- | ------- | ------- |
| Cold start          | 3.58 s  | 2.40 s  |
| Warm avg / batch    | 1.42 s  | 1.44 s  |
| Timing stability    | stdev 0.08 | stdev 0.25 |
| Avg CPU             | ~26%    | ~2%     |
| Avg memory          | ~61%    | ~50%    |

**Interpretation:** Steady-state speed is essentially tied, because the NumPy FFT inside each task is identical. The engines differ in *character*: Spark gives more stable, predictable per-batch latency but is heavier on resources and slower to start (JVM warm-up); Dask is lightweight and starts faster (pure Python) but shows more timing variance. There is no single "winner" — it is a trade-off depending on what you optimize for.

*Note on configuration:* in this run, Spark was executed in `local[*]` mode (single node) while Dask used a 3-node `SSHCluster`. Because each batch is a small FFT, distributing it across machines provided no speed-up (network overhead exceeds the compute gain — Dask on 3 nodes matched a single node), so the speed comparison is presented with that caveat. The resource-usage and stability findings reflect intrinsic engine characteristics (JVM vs. pure Python) and are independent of node count.

## Presentation

`quax_presentation.html` is a self-contained visual presentation of the full project (problem, infrastructure, Kafka, design decisions, Spark implementation, and the benchmark with charts). Open it in any web browser.

## Tests

The `tests/` directory contains helper scripts used during development:
- `cluster_check.py` — verifies the Dask cluster is reachable
- `explore_s3.py` — lists/inspects objects in the S3 bucket
- `test_keys.py` — checks that S3 credentials are correctly loaded
