# QUAX Experiment: Distributed Real-Time Data Pipeline

## Overview

This project implements a streaming data pipeline for the QUAX experiment. It fetches high-frequency IQ radio data from a CloudVeneto S3 bucket, computes the Fast Fourier Transform (FFT) of each batch, and visualizes the resulting power spectrum in real time.

The processing step is implemented on **two independent distributed engines — Dask and Apache Spark** — which are compared head-to-head, and against a **single-process baseline with no distribution engine at all**, in a dedicated benchmark.

For the full build log — infrastructure, design decisions, and the reasoning behind them — see **[what_we_did.md](what_we_did.md)**.

## Project Layout

```
Project/
├── README.md              — this file
├── what_we_did.md          — full reproducible build guide
├── requirements.txt        — Python dependencies
├── code/                   — all pipeline and benchmark scripts
│   ├── producer.py         — Kafka work-order emitter
│   ├── processor.py        — Dask processor (live pipeline)
│   ├── processor_spark.py  — Spark processor (live pipeline)
│   ├── dashboard.py        — Streamlit live dashboard
│   ├── benchmark_baseline.py
│   ├── benchmark_dask.py
│   ├── benchmark_spark.py
│   └── summarize_benchmarks.py
├── results/                — raw benchmark output (CSV)
│   ├── benchmark_baseline_results.csv
│   ├── benchmark_dask_results.csv
│   └── benchmark_spark_results.csv
└── tests/                  — development helper scripts
    ├── cluster_check.py
    ├── explore_s3.py
    └── test_keys.py
```

## Architecture

- **Producer:** Pushes "work orders" (S3 filenames) to Apache Kafka to simulate a continuous DAQ stream.
- **Processor:** A Kafka consumer that, for each batch, downloads the IQ data from S3 and computes the FFT, then pushes the processed result to a second Kafka topic. Two interchangeable implementations exist:
  - `code/processor.py` — Dask
  - `code/processor_spark.py` — Apache Spark
- **Dashboard:** A Streamlit application that consumes the processed data and updates a live frequency plot.

Both processors treat one batch as one unit of work: the batch is scheduled as a single task, and the FFT is computed in one shot with NumPy inside that task. This keeps the two engines directly comparable — the FFT math is identical, and only the engine that schedules the work differs.

## Infrastructure

The pipeline was developed and run on CloudVeneto Virtual Machines (Ubuntu 24.04):

- 1 Master Node (runs Kafka, Streamlit, and the processor)
- 2 Worker Nodes

The Dask processor connects to the two worker nodes over SSH (`SSHCluster`), so the Dask runs use all three machines. Passwordless SSH is configured between the master and the workers.

For the benchmark, Spark is run the same way: as a Spark Standalone cluster with the master daemon on `master` and worker daemons on `worker1` and `worker2` (`spark://master:7077`), so both engines use all three machines symmetrically. `processor_spark.py` (the live pipeline) uses Spark's default local mode instead, since the live demo only needs one machine — see [what_we_did.md](what_we_did.md) (Step 7) for how the cluster is brought up.

## Prerequisites

Before running, Apache Kafka must be active on the master node, and S3 credentials must be exported to the environment:

```bash
export S3_ACCESS_KEY="your_access_key"
export S3_SECRET_KEY="your_secret_key"
```

## How to Run the Pipeline

The pipeline requires three separate terminals running on the master node.

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

## Benchmark: Baseline vs Dask vs Spark

Three configurations were benchmarked on 20 identical batches of real QUAX data, run synchronously (submit one batch, wait for its result, record the time). For every batch the processing time, CPU %, and memory % were logged with `psutil`.

- **Baseline** — no distribution engine at all; `process_physics_data` is called directly, in-process, on the master node. The reference point that Dask and Spark are measured against.
- **Dask** — the 3-node `SSHCluster` (`master` + `worker1` + `worker2`).
- **Spark** — a 3-node Spark Standalone cluster (`master` + `worker1` + `worker2`) — the same infrastructure as Dask.

To reproduce: run one benchmark script at a time from `code/` (each needs the Kafka consumer up before `producer.py` starts sending), then run `summarize_benchmarks.py` to regenerate the table below from `results/`.

**Results (warm state, i.e. after the first batch):**

| Metric            | Baseline | Dask   | Spark  |
| ----------------- | -------- | ------ | ------ |
| Cold start        | 2.54 s   | 2.64 s | 4.35 s |
| Warm avg / batch  | 1.39 s   | 1.32 s | 1.54 s |
| Timing stdev      | 0.11     | 0.19   | 0.29   |
| Avg CPU           | ~23%     | ~2%    | ~8%    |
| Avg memory        | ~59%     | ~60%   | ~68%   |

**Interpretation:** All three are close in steady-state speed (1.3–1.5 s/batch), because the NumPy FFT inside each task is identical and dominates the time — network/scheduling overhead is a small fraction of it. For a workload this small, **neither distribution engine buys a speed-up over doing the work in-process**, since only one batch is ever in flight at a time in this synchronous benchmark. Where the three differ is overhead and character:

- **Baseline** has the lowest timing variance of the three and no cluster machinery to set up, but ties up the master node's own CPU/network for every batch and doesn't scale if batches arrive faster than one at a time.
- **Dask** is lightest on CPU/memory (pure Python, lower overhead) and about as fast as the baseline, but shows more timing variance.
- **Spark** is the slowest to cold-start (JVM warm-up) and heaviest on CPU/memory of the three, and also the most variable — the extra hop through a second machine adds a small, variable network cost on top of the identical FFT.

There is no single "winner" for this workload — the benchmark shows that the choice of engine matters more for *how you scale* (many concurrent batches, fault tolerance, mixed workloads) than for raw per-batch latency at this batch size. Full methodology is in [what_we_did.md](what_we_did.md) (Step 8).

## Tests

The `tests/` directory contains helper scripts used during development:
- `cluster_check.py` — verifies the Dask cluster is reachable
- `explore_s3.py` — lists/inspects objects in the S3 bucket
- `test_keys.py` — checks that S3 credentials are correctly loaded

## Known Limitations

- S3 credentials passed to Dask workers via `worker_options={"env": {...}}` end up visible in `ps aux` output on the worker VMs (any local user on `worker1`/`worker2` could read them). Hardening this (e.g. Dask's `Security` config, or a secrets file instead of inline env) is a good follow-up.
