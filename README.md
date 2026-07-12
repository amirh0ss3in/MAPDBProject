# QUAX Experiment: Distributed Real-Time Data Pipeline

## Overview

This project implements a streaming data pipeline for the QUAX experiment. It fetches high-frequency IQ radio data from a CloudVeneto S3 bucket, computes the Fast Fourier Transform (FFT) of each batch, and visualizes the resulting power spectrum in real-time.

The processing step is implemented on **two independent distributed engines — Dask and Apache Spark** — which are compared head-to-head, and against a **single-process baseline with no distribution engine at all**, in a dedicated benchmark.

## Architecture

- **Producer:** Pushes "work orders" (S3 filenames) to Apache Kafka to simulate a continuous DAQ stream.
- **Processor:** A Kafka consumer that, for each batch, downloads the IQ data from S3 and computes the FFT, then pushes the processed result to a second Kafka topic. Two interchangeable implementations exist:
  - `processor.py` — Dask
  - `processor_spark.py` — Apache Spark
- **Dashboard:** A Streamlit application that consumes the processed data and updates a live frequency plot.

Both processors treat one batch as one unit of work: the batch is scheduled as a single task, and the FFT is computed in one shot with NumPy inside that task. This keeps the two engines directly comparable — the FFT math is identical, and only the engine that schedules the work differs.

## Infrastructure

The pipeline was developed and run on CloudVeneto Virtual Machines (Ubuntu 24.04):

- 1 Master Node (runs Kafka, Streamlit, and the processor)
- 2 Worker Nodes

The Dask processor connects to the two worker nodes over SSH (`SSHCluster`), so the Dask runs used all three machines. Passwordless SSH was configured between the master and the workers.

For the benchmark, Spark is run the same way: as a Spark Standalone cluster with the master daemon on `master` and worker daemons on `worker1` and `worker2` (`spark://master:7077`), so both engines use all three machines symmetrically.

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

A second implementation of the processor was built on Apache Spark (`code/processor_spark.py`) so the two engines could be compared. Everything is kept identical between the two versions — the same Kafka topics, the same S3 source, and the same NumPy FFT — so that only the processing engine changes.

**Design decision:** each batch is handled as a single Spark task, and the FFT runs once with NumPy inside that task, mirroring what the Dask version does. Spark is deliberately *not* used to split a single FFT across cores, because that would compare the *algorithm* instead of the *engine* and make the benchmark misleading.

### Running Spark as a 3-node cluster (for the benchmark)

`processor_spark.py` (the live pipeline) uses Spark's default local mode, since the live demo only needs one machine. The benchmark, however, connects to a real Spark Standalone cluster spanning all three nodes. Java 17 and `pyspark` (matching version) must be installed on all three nodes, then:

```bash
# On master — start the Spark master daemon
source ~/pyvenv/bin/activate
SPARK_HOME=$(python -c "import pyspark,os; print(os.path.dirname(pyspark.__file__))")
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host master --port 7077 --webui-port 8080 &

# On worker1 and worker2 — start a Spark worker daemon pointing at the master
source ~/pyvenv/bin/activate
SPARK_HOME=$(python -c "import pyspark,os; print(os.path.dirname(pyspark.__file__))")
export PYSPARK_PYTHON=~/pyvenv/bin/python
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://master:7077 --webui-port 8081 &
```

`benchmark_spark.py` then connects with `.master("spark://master:7077")` instead of the default local mode.

## Benchmark: Baseline vs Dask vs Spark

Three configurations were benchmarked on 20 identical batches of real QUAX data, run synchronously (submit one batch, wait for its result, record the time). For every batch the processing time, CPU %, and memory % were logged with `psutil`:

- **Baseline** — no distribution engine at all; `process_physics_data` is called directly, in-process, on the master node. This is the reference point that Dask and Spark are measured against.
- **Dask** — the 3-node `SSHCluster` (`master` + `worker1` + `worker2`).
- **Spark** — a 3-node Spark Standalone cluster (`master` + `worker1` + `worker2`), so it is now run on the *same* infrastructure as Dask (see note below).

**Files:**
- `code/benchmark_baseline.py` — instrumented, undistributed processor
- `code/benchmark_dask.py` — instrumented Dask processor
- `code/benchmark_spark.py` — instrumented Spark processor
- `code/benchmark_baseline_results.csv` / `benchmark_dask_results.csv` / `benchmark_spark_results.csv` — raw results
- `code/summarize_benchmarks.py` — recomputes the table below from the raw CSVs

**Results (warm state, i.e. after the first batch):**

| Metric            | Baseline | Dask   | Spark  |
| ----------------- | -------- | ------ | ------ |
| Cold start        | 2.54 s   | 2.64 s | 4.35 s |
| Warm avg / batch   | 1.39 s   | 1.32 s | 1.54 s |
| Timing stdev       | 0.11     | 0.19   | 0.29   |
| Avg CPU            | ~23%     | ~2%    | ~8%    |
| Avg memory         | ~59%     | ~60%   | ~68%   |

**Interpretation:** All three are close in steady-state speed (1.3–1.5 s/batch), because the NumPy FFT inside each task is identical and dominates the time — network/scheduling overhead is a small fraction of it. This is itself the key finding: for a workload this small, **neither distribution engine buys a speed-up over doing the work in-process**; the network/serialization cost of shipping a batch to a remote worker roughly cancels out any parallelism benefit, since only one batch is ever in flight at a time in this synchronous benchmark. Where the engines *do* differ is overhead and character:

- **Baseline** has the lowest timing variance of the three and no cluster machinery to set up, but ties up the master node's own CPU/network for every batch and doesn't scale if batches arrive faster than one at a time.
- **Dask** is lightest on CPU/memory (pure Python, lower overhead) and about as fast as the baseline, but shows more timing variance.
- **Spark** is the slowest to cold-start (JVM warm-up) and heaviest on CPU/memory of the three, and also the most variable here — the extra hop through a second machine (task goes to whichever of worker1/worker2 Spark schedules it to) adds a small, variable network cost on top of the identical FFT.

There is no single "winner" for this workload — the benchmark shows that the choice of engine matters more for *how you scale* (many concurrent batches, fault tolerance, mixed workloads) than for raw per-batch latency at this batch size.

**Note on configuration.** Earlier runs of this benchmark had a known asymmetry: Spark ran in `local[*]` mode on the master only, while Dask used the 3-node `SSHCluster`. This has since been fixed — a real Spark Standalone cluster now runs across all three nodes (master + worker1 + worker2), matching Dask's configuration, and a baseline (no engine) run was added for reference. The numbers above are from this corrected, symmetric setup.

## Presentation

`quax_presentation.html` is a self-contained visual presentation of the full project (problem, infrastructure, Kafka, design decisions, Spark implementation, and the benchmark with charts). Open it in any web browser.

## Tests

The `tests/` directory contains helper scripts used during development:
- `cluster_check.py` — verifies the Dask cluster is reachable
- `explore_s3.py` — lists/inspects objects in the S3 bucket
- `test_keys.py` — checks that S3 credentials are correctly loaded
