# Reproducible Guide: Spark Implementation & Spark-vs-Dask Benchmark

## Overview
This guide documents the second half of the QUAX pipeline work: re-implementing the processor on **Apache Spark** and running a controlled **benchmark against the existing Dask version**. It builds directly on the Dask pipeline described in `what_we_did.md` — the producer, Kafka setup, S3 source, and dashboard are all reused unchanged. Only the processing engine is swapped.

> **Goal:** Answer a concrete question — for this workload, does the choice of distributed engine (Dask vs. Spark) actually matter, and how do the two compare on speed, resource usage, and stability?

---

## Step 1: Installing Spark on the Cluster

> **Why?** The Dask environment was already in place. Spark runs on the Java Virtual Machine (JVM) and ships as the `pyspark` package, so we needed both a JDK and `pyspark` inside the existing `pyvenv`.

Java 17 was already present on the master node. We then tried to install `pyspark`, but hit a problem: the shared `pyvenv` had been built **without pip** (`--without-pip`), so `pip` was missing. We bootstrapped it manually:

```bash
# Bootstrap pip into the venv
curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py

# Install PySpark
python -m pip install pyspark
```

This installed `pyspark 4.1.2`. A quick check confirmed Spark ran locally on the master (a trivial `parallelize([1,2,3]).map(...).collect()` returned the expected result).

> **Note (local development on macOS):** When testing PySpark on a Mac laptop, the driver and workers must use the *same* Python interpreter, or the job dies with a `PythonException`. Setting both `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `sys.executable` at the top of the script fixed it. This is not needed on the cluster, where a single interpreter is used.

---

## Step 2: The Spark Processor (`processor_spark.py`)

> **Design Choice:** We kept **everything** identical to the Dask processor — the same Kafka topics (`quax_raw` → `quax_processed`), the same S3 download, and the *same* NumPy FFT function. Only the engine that schedules the work changes. This is what makes the later benchmark a fair, apples-to-apples comparison.

> **Design Choice (Approach 1 — distribute batches, not the FFT):** Spark *could* be used to split a single batch's FFT across cores. We deliberately did **not** do that. Dask computes the whole FFT in one shot with NumPy; if Spark instead split the FFT, the benchmark would be comparing *algorithms*, not *engines*. So in our Spark version, each batch is handled as **one task**, and the FFT runs once with NumPy inside it — exactly mirroring the Dask worker.

The worker function is byte-for-byte the same physics as the Dask version. The Spark-specific parts are the session setup and how a batch is submitted:

```python
from pyspark.sql import SparkSession

# --- SESSION SETUP ---
# S3 keys are injected into the executors' environment, the same secure
# pattern as Dask's worker_options={"env": ...}. Keys never appear as task
# arguments and are never hardcoded.
spark = SparkSession.builder \
    .appName("QUAX-Spark-Processor") \
    .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
    .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
    .getOrCreate()
sc = spark.sparkContext

# --- PER-BATCH SUBMISSION (inside the Kafka poll loop) ---
# Hand one whole batch to Spark: it ships the work_order to an executor,
# runs process_physics_data there, and brings the result back.
rdd = sc.parallelize([work_order], numSlices=1)
result_json = rdd.map(process_physics_data).collect()[0]
```

> **Note:** This is intentionally **synchronous** — one batch in, one result out — which gives clean, directly comparable per-batch timings for the benchmark.

---

## Step 3: The Benchmark

> **Why?** "It runs" is not the same as "it's the right tool." To compare Dask and Spark objectively, we instrumented both processors identically and ran them on the same data.

### 3.1 Methodology

> **Design Choice (keep it fair):** The measurement had to change only one thing at a time. So:
> - **Same workload:** the same producer streamed the same 20 batches of real QUAX data to both engines (one batch every 5 s).
> - **Same timing method:** both processors were run **synchronously** (submit one batch, block until its result, record the time). The Dask version — which is asynchronous by default — was made synchronous for the benchmark so the two are measured the same way.
> - **Same metrics:** for every batch we logged processing time, CPU %, and memory % using `psutil`, and wrote them to a CSV.

Benchmark files:
- `code/benchmark_spark.py` — instrumented Spark processor
- `code/benchmark_dask.py` — instrumented Dask processor (made synchronous)
- `code/benchmark_spark_results.csv` — raw Spark results (20 batches)
- `code/benchmark_dask_results.csv` — raw Dask results (20 batches)

> **Reading the numbers correctly:** the meaningful metric is *per-batch processing time after warm-up*. Total wall-clock time (~100–160 s) is dominated by the producer's deliberate 5 s gap between batches, **not** by the engines, so it is not a measure of speed.

### 3.2 Results (warm state, i.e. after the first batch)

| Metric            | Spark      | Dask       |
| ----------------- | ---------- | ---------- |
| Cold start        | 3.58 s     | 2.40 s     |
| Warm avg / batch  | 1.42 s     | 1.44 s     |
| Timing stability  | stdev 0.08 | stdev 0.25 |
| Avg CPU           | ~26%       | ~2%        |
| Avg memory        | ~61%       | ~50%       |

### 3.3 Interpretation

Steady-state speed is essentially **tied** (1.42 s vs. 1.44 s), which makes sense: the NumPy FFT inside each task is identical, so once both engines are warm they do the same work in the same time. The real differences are in *character*, not raw throughput:

- **Spark** is very **stable** (low timing variance) and predictable, but heavier on CPU and memory and slower to start (JVM warm-up).
- **Dask** is **lightweight** (much lower CPU/memory) and starts faster (pure Python), but shows more timing variance.

There is no single "winner" — it is a trade-off. Spark suits workloads that value predictable latency; Dask suits resource-constrained, Python-native environments.

---

## Step 4: Note on Cluster Configuration (Honest Limitation)

The two engines were **not** run on the same number of machines: the Dask benchmark used the 3-node `SSHCluster`, while the Spark benchmark was run in `local[*]` mode on the master node only.

> **Why does this not invalidate the results?** Because the Dask data itself shows that node count barely affected this workload. Each batch is a small FFT — small enough that spreading it across three machines gave no speed-up over one, since the network cost of distribution outweighs the compute gain. (This is itself a meaningful finding: for small per-batch tasks, more machines don't help.)

So the **speed** comparison is presented with this caveat. The **resource-usage** and **stability** findings, however, come from the intrinsic nature of each engine (JVM vs. pure Python) and are independent of the number of nodes.

*Attempting a fully symmetric 3-node Spark run was blocked by infrastructure: at the time of writing, the two worker VMs were powered off and unreachable (`ping` → Destination Host Unreachable), so a Spark Standalone cluster across all three nodes could not be brought up.*

---

## Step 5: Presentation

The full project, including this Spark work and the benchmark charts, is summarized in `quax_presentation.html` — a self-contained visual presentation that opens in any web browser.
