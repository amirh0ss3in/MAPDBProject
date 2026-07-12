# QUAX Experiment: Distributed Real-Time Data Pipeline

## Overview

This project implements a streaming data pipeline for the QUAX experiment. It fetches high-frequency IQ radio data from a CloudVeneto S3 bucket, computes the Fast Fourier Transform (FFT) of each batch, and visualizes the resulting power spectrum in real time.

The processing step is implemented on **two independent distributed engines — Dask and Apache Spark** — plus a **single-process baseline with no distribution engine at all**. The three are compared in two separate benchmarks that ask two different questions:

- **Per-task overhead** — what does one batch cost, engine vs. no engine? (repeated trials, statistically real)
- **Throughput under load** — when batches pile up faster than one machine can process them, does distribution actually help?

For the full build log — infrastructure, design decisions, and the reasoning behind them — see **[what_we_did.md](what_we_did.md)**. For a visual walkthrough of the whole story (including the flawed first benchmark and how it got fixed), open **[quax_story-2.html](quax_story-2.html)** in a browser.

## Project Layout

```
Project/
├── README.md              — this file
├── what_we_did.md          — full reproducible build guide
├── quax_story-2.html       — interactive presentation deck
├── requirements.txt        — Python dependencies
├── .gitignore
├── code/
│   ├── producer.py                    — Kafka work-order emitter (live pipeline; --batches/--interval for benchmark use)
│   ├── processor.py                   — Dask processor (live pipeline)
│   ├── processor_spark.py             — Spark processor (live pipeline, local mode)
│   ├── dashboard.py                   — Streamlit live dashboard
│   │
│   ├── benchmark_baseline.py          — Experiment A: no engine, one trial
│   ├── benchmark_dask.py              — Experiment A: 3-node Dask SSHCluster, one trial
│   ├── benchmark_spark.py             — Experiment A: 3-node Spark Standalone, one trial
│   ├── run_trials.sh                  — drives N repeated trials of one engine
│   ├── summarize_overhead.py          — aggregates results/overhead/ into the table below
│   │
│   ├── load_producer.py               — fires batches uncapped for the load test (cycles the 31 real files)
│   ├── load_baseline_sequential.py    — Experiment B: no engine, one batch at a time
│   ├── load_baseline_concurrent.py    — Experiment B: no engine, ThreadPoolExecutor on one machine
│   ├── load_dask.py                   — Experiment B: 3-node Dask, non-blocking submit
│   ├── load_spark.py                  — Experiment B: 3-node Spark, concurrent driver threads
│   ├── run_load.sh                    — drives one load-test configuration end to end
│   ├── analyze_load.py                — aggregates results/load/ into the table below
│   │
│   └── run_with_env.sh                — loads S3 keys from ~/.bashrc without printing them (used by the run_* scripts)
├── results/
│   ├── overhead/           — Experiment A: {baseline,dask,spark}_trial{1..5}.csv
│   └── load/               — Experiment B: {baseline_sequential,baseline_concurrent,dask,spark}.csv
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

The pipeline was developed and run on CloudVeneto Virtual Machines (Ubuntu 24.04, 2 vCPU / 4GB each):

- 1 Master Node (runs Kafka, Streamlit, and the processor)
- 2 Worker Nodes

The Dask processor connects to the two worker nodes over SSH (`SSHCluster`), so the Dask runs use all three machines. For both benchmarks below, Spark is run the same way — a Spark Standalone cluster with the master daemon on `master` and worker daemons on `worker1`/`worker2` (`spark://master:7077`) — so both engines use all three machines symmetrically. `processor_spark.py` (the live pipeline) uses Spark's default local mode instead, since the live demo only needs one machine. See [what_we_did.md](what_we_did.md) for the exact commands to bring the Spark Standalone cluster up.

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

## Benchmark A: Per-Task Overhead

**What it measures:** the cost of one batch, submitted synchronously (submit, wait for the result, then submit the next) — cold start, steady-state processing time, timing stability, CPU/memory. Only one unit of work is ever in flight, so this cannot show any benefit from an engine's ability to schedule many tasks concurrently — that's what Benchmark B is for.

**Why 5 repeated trials, not 1:** a single 20-batch run makes claims like "engine X has more variance" fragile — one unlucky batch (a GC pause, a slow S3 fetch) can dominate a 20-sample stdev. Each configuration was run **5 independent trials** of 20 batches, and the table below reports the mean and the spread *across trials*, not just one run's internal noise. Reproduce with `code/run_trials.sh <baseline|dask|spark> <n_trials>`, then `code/summarize_overhead.py`.

**Results (mean ± stdev across 5 trials):**

| Metric | Baseline | Dask | Spark |
| --- | --- | --- | --- |
| Cold start (s) | 1.98 ± 0.70 | 1.68 ± 0.20 | 3.24 ± 0.39 |
| Warm avg / batch (s) | 1.35 ± 0.09 | 1.45 ± 0.06 | 1.54 ± 0.05 |
| Timing stdev (within a trial) | 0.19 ± 0.10 | 0.18 ± 0.05 | 0.33 ± 0.08 |
| Avg CPU | ~52% | ~52% | ~58% |
| Avg memory | ~59% | ~57% | ~68% |

**Interpretation:** steady-state speed is close across all three (1.35–1.54s/batch) — the identical NumPy FFT dominates regardless of engine. Two findings *do* hold up across all 5 trials, not just one:

- **Spark is consistently slower to cold-start** (JVM warm-up: ~3.2s vs. ~1.7–2.0s) — every one of its 5 trials is slower than every baseline/Dask trial.
- **Spark is consistently more variable batch-to-batch** (stdev ~0.33 vs. ~0.18–0.19) — every one of its 5 trials has higher internal stdev than every baseline/Dask trial. (An earlier single-run version of this benchmark showed the same pattern, but it turned out to be driven almost entirely by one outlier batch — repeating the trial is what turns that from a fragile claim into a real one.)

Dask is the lightest on CPU/memory. None of this says anything about *speed at scale* — for that, see Benchmark B.

## Benchmark B: Throughput Under Load

**What it measures:** what happens when batches arrive faster than one machine can keep up — the regime Benchmark A's one-at-a-time design structurally cannot show. 100 batches (cycling through the 31 real I/Q file pairs) are fired at each configuration as fast as Kafka accepts them, with no artificial delay, and the resulting backlog is processed however each configuration can. Four configurations:

- **baseline (sequential)** — no engine, strictly one batch at a time (same as Benchmark A, just without gaps between batches)
- **baseline (concurrent)** — no engine, but a `ThreadPoolExecutor` (8 threads) on the single master node — isolates whether the fix is *any* concurrency, or specifically a multi-machine cluster
- **Dask** — 3-node cluster, non-blocking `client.submit` (matches how the live `processor.py` actually schedules work)
- **Spark** — 3-node cluster, multiple driver threads concurrently submitting jobs to the same `SparkContext`

Reproduce with `code/run_load.sh <config> <n_batches>`, then `code/analyze_load.py`.

**Results (100 batches, uncapped arrival):**

| Config | Throughput (batches/s) | p50 latency | p95 latency | p99 latency |
| --- | --- | --- | --- | --- |
| Baseline (sequential) | 0.80 | 64.2s | 119.4s | 124.2s |
| Baseline (concurrent) | 1.13 | 50.5s | 84.5s | 87.5s |
| Dask | 1.20 | 43.5s | 79.8s | 82.8s |
| Spark | 1.21 | 44.8s | 80.2s | 82.4s |

**Interpretation:** this is the regime Benchmark A couldn't speak to, and here distribution genuinely earns its keep. Under a real backlog, both Dask and Spark cut p50 latency by ~30% and lift sustained throughput by ~50% versus doing nothing but processing one batch at a time. But look at where most of that gain comes from: simply adding concurrency on the *same single machine* (threads, no cluster at all) gets you from 0.80 to 1.13 batches/s — most of the way there. The extra jump from "concurrent on one box" to "an actual 3-node cluster" is real (1.13 → 1.20–1.21) but modest for this workload. Dask and Spark are statistically indistinguishable from each other here.

**Bottom line across both benchmarks:** for a single batch processed in isolation, engine choice barely matters and adds overhead (Spark's JVM cold-start and jitter, in particular, buy you nothing at this scale). For a backlog of many batches, distribution helps — but a large share of that benefit is just "stop blocking on one thing at a time," which you get from ordinary concurrency before you ever need a cluster.

## Tests

The `tests/` directory contains helper scripts used during development:
- `cluster_check.py` — verifies the Dask cluster is reachable
- `explore_s3.py` — lists/inspects objects in the S3 bucket
- `test_keys.py` — checks that S3 credentials are correctly loaded

## Known Limitations

- S3 credentials passed to Dask workers via `worker_options={"env": {...}}` end up visible in `ps aux` output on the worker VMs (any local user on `worker1`/`worker2` could read them). Hardening this (e.g. Dask's `Security` config, or a secrets file instead of inline env) is a good follow-up.
- Both benchmarks ran on small 2 vCPU / 4GB VMs. Benchmark B's absolute throughput numbers (and the size of the "concurrency vs. cluster" gap) are specific to this hardware and to this I/O-bound, sub-2-second-per-batch workload; a heavier per-batch computation or bigger machines could shift where the crossover point is.
