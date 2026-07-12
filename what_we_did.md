# Reproducible Guide: QUAX Distributed Real-Time Pipeline

## Overview

This guide documents the full build of the QUAX pipeline: a streaming ETL system that fetches high-frequency IQ radio data from S3, computes the FFT, and visualizes the power spectrum in real time — plus the two distributed-engine implementations (Dask, Apache Spark) and the two benchmarks that compare them against each other and against a no-distribution baseline.

> **Goal:** Build the pipeline, then answer a concrete question — for this workload, does the choice of distributed engine (Dask vs. Spark) actually matter, and how does either compare to just not distributing at all? The answer turned out to depend entirely on *how* you ask it — see Steps 8-10.

---

## Step 1: Cloud Infrastructure & Networking

We deployed the cluster on the CloudVeneto OpenStack infrastructure.

### 1.1 VM Provisioning
We provisioned three Ubuntu 24.04 Virtual Machines using the `medium` flavor (2 vCPUs, 4GB RAM) and attached them to the `pod-students` security group.
* `master`: 10.67.22.111
* `worker1`: 10.67.22.246
* `worker2`: 10.67.22.248

### 1.2 Local SSH Configuration (ProxyJump)
Because CloudVeneto VMs sit behind a private network, we configured our local laptop's `~/.ssh/config` file. This allows us to tunnel through the gate server securely in a single command.
```text
Host master
    HostName 10.67.22.111
    User ubuntu
    IdentityFile ~/mapd_key.pem
    ProxyJump arezaeig@gate.cloudveneto.it
```

### 1.3 Internal Networking
> **Why?** Dask and Spark both require nodes to communicate seamlessly. The hosts file allows nodes to resolve each other by name.

On **all three VMs**, we appended the cluster IPs to `/etc/hosts`:
```bash
sudo nano /etc/hosts

# Added to bottom:
10.67.22.111 master
10.67.22.246 worker1
10.67.22.248 worker2
```

### 1.4 Passwordless SSH Authentication
> **Why?** The master node needs the authority to silently spawn worker processes on remote machines without human intervention (typing passwords).

On the `master` node, we generated an RSA keypair and copied the public key:
```bash
ssh-keygen -t rsa  # Press Enter for all prompts
cat ~/.ssh/id_rsa.pub
```
We then pasted that output into the `~/.ssh/authorized_keys` file on `master`, `worker1`, and `worker2`.

---

## Step 2: Software Environment Setup

> **Why?** In a distributed system, if the master sends a NumPy task to a worker that lacks NumPy, the task fails. We used `uv` (a high-speed Rust-based Python package installer) to guarantee identical environments across the cluster instantly.

We ran this exact chain of commands on **all three nodes**:
```bash
sudo apt update && sudo apt upgrade -y
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
uv venv ~/pyvenv
source ~/pyvenv/bin/activate
uv pip install "dask[complete]" asyncssh notebook matplotlib numpy boto3 kafka-python-ng streamlit pandas
```

Java 17 and `pyspark` were added to all three nodes later, when the Spark cluster was built (Step 7).

---

## Step 3: Message Broker Setup (Apache Kafka)

> **Why?** Kafka acts as an asynchronous buffer. If data downloads from S3 faster than the processing engine can compute the FFTs, the system would crash. Kafka decouples the scripts: the producer drops data into a queue, and the processor pulls it out at its own pace.

We installed Kafka natively on the **master node**:
```bash
# Install Java
sudo apt install openjdk-17-jre-headless -y

# Download and extract Kafka
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# Start background services
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1 &
bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &

# Create communication topics
bin/kafka-topics.sh --create --topic quax_raw --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic quax_processed --bootstrap-server localhost:9092
```

---

## Step 4: The Python Pipeline Code

We created a `Project/code` directory on the master node and wrote the microservices below.

### 4.1 Security Credentials (Persistent Environment Variables)
> **Why?** Hardcoding S3 keys directly into Python scripts is a security vulnerability. Instead, we injected our CloudVeneto S3 keys directly into the master node's Linux `~/.bashrc` file. When we log in via SSH, Linux automatically loads these keys into the system environment *before* we even activate the Python virtual environment. The venv inherits them, keeping the keys out of source control while allowing every script to pull them via `os.environ.get()`.

```bash
echo 'export S3_ACCESS_KEY="your_access_key"' >> ~/.bashrc
echo 'export S3_SECRET_KEY="your_secret_key"' >> ~/.bashrc
```

Because non-interactive scripted SSH commands don't source `~/.bashrc` by default, the benchmark/orchestration scripts (Steps 8-10) load just the two `S3_*` export lines via a small wrapper, `code/run_with_env.sh`, instead of the whole file — so the keys are never printed or logged anywhere.

### 4.2 The Producer (`code/producer.py`)
> **Design Choice:** We do *not* download the 64MB binary files and push them through Kafka. Pushing heavy binary data causes severe network bottlenecks. Instead, we send lightweight JSON "work orders" containing the S3 filenames, by default one every 5 seconds to simulate the real QUAX hardware's DAQ rate. It also accepts `--batches`/`--interval` overrides, used by the benchmarks in Steps 8-10 to send batches faster than real hardware would.

### 4.3 The Processor — Dask Engine (`code/processor.py`)
> **Design Choice:** We securely inject the S3 keys into the remote Dask workers using `worker_options={"env": ...}` so they don't leak into the Dask task graph, and use non-blocking Kafka polling so the master can continually submit tasks without waiting for previous ones to finish.

The cluster is a `dask.distributed.SSHCluster` spanning `master`, `worker1`, and `worker2` — Dask genuinely uses all three machines.

> **Known limitation:** `worker_options={"env": {...}}` passes the S3 keys to each worker's `Nanny` process as part of its spec, which ends up visible in the worker's own `ps aux` output on the remote VM. This is a pre-existing pattern in the original design; hardening it (e.g. via Dask's `Security` config or a secrets file instead of inline env) is a good follow-up but out of scope here.

### 4.4 The Dashboard (`code/dashboard.py`)
> **Design Choice:** Streamlit wraps a Pandas DataFrame to render real-time UI updates consumed directly from the `quax_processed` Kafka topic.

---

## Step 5: Execution & Port Forwarding

Because the Streamlit dashboard runs on port `8501` of a private cloud VM, we used SSH local port forwarding from our laptop to securely tunnel the web traffic.

**On the local laptop terminal:**
```bash
ssh -L 8501:localhost:8501 master
```

**Inside the master node (using 3 terminals):**
```bash
# Terminal 1
source ~/pyvenv/bin/activate
python3 code/processor.py

# Terminal 2
source ~/pyvenv/bin/activate
python3 code/producer.py

# Terminal 3
source ~/pyvenv/bin/activate
streamlit run code/dashboard.py
```
*Result: navigating to `http://localhost:8501` on the local laptop displays the live-updating frequency spectrum.*

---

## Step 6: The Spark Processor (`code/processor_spark.py`)

> **Goal:** Re-implement the processor on Apache Spark so the two engines can be compared. Everything is kept identical to the Dask version — the same Kafka topics, the same S3 download, the *same* NumPy FFT function. Only the engine that schedules the work changes.

> **Design Choice (distribute batches, not the FFT):** Spark *could* be used to split a single batch's FFT across cores. We deliberately did **not** do that — Dask computes the whole FFT in one shot with NumPy, so if Spark instead split the FFT, the benchmark would be comparing *algorithms*, not *engines*. Each batch is handled as **one task**, and the FFT runs once with NumPy inside it, exactly mirroring the Dask worker.

The worker function is byte-for-byte the same physics as the Dask version. The Spark-specific parts are the session setup and how a batch is submitted:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QUAX-Spark-Processor") \
    .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
    .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
    .getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([work_order], numSlices=1)
result_json = rdd.map(process_physics_data).collect()[0]
```

`processor_spark.py` (the live pipeline script) deliberately uses Spark's default local mode — the live demo only needs one machine. The benchmarks need a real cluster to be a fair comparison against Dask; that's built next.

> **Note (installing PySpark):** the shared `pyvenv` was created without pip, so `pip` had to be bootstrapped once on `master` (`curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python get-pip.py`) before `pip install pyspark` would work. `pyspark==4.1.2` was installed this way on `master`; it was later installed on `worker1`/`worker2` too (Step 7) using `uv pip install pyspark==4.1.2` directly, which needs no such bootstrap.

---

## Step 7: A Real 3-Node Spark Cluster

> **Why?** For the benchmarks to be a fair comparison with Dask's 3-node `SSHCluster`, Spark also needs to actually run across all three machines — not just the master.

Java 17 and `pyspark==4.1.2` (matching the master's version) were installed on `worker1` and `worker2`:
```bash
sudo apt update && sudo apt install -y openjdk-17-jre-headless
source ~/pyvenv/bin/activate
uv pip install pyspark==4.1.2
```

A Spark Standalone cluster is then started — the master daemon on `master`, and a worker daemon on each of `worker1`/`worker2` registering against it. This is only needed while a Spark benchmark is actually running; it's not left up permanently.

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

Every Spark benchmark script connects with `.master("spark://master:7077")` (instead of the implicit local default that `processor_spark.py` uses), with `spark.pyspark.python` pinned to the shared venv so executors have the same NumPy/boto3 environment as the driver.

---

## Step 8: First Benchmark Attempt — and Why It Was Flawed

The first version of this benchmark ran `benchmark_baseline.py`, `benchmark_dask.py`, and `benchmark_spark.py` once each, synchronously (submit one batch, block until its result, submit the next), for 20 batches, and reported cold start / warm average / timing stdev / CPU / memory. It found all three engines essentially tied on speed, with Spark showing the highest timing variance.

Two problems surfaced on closer inspection:

1. **Statistical fragility.** Spark's "highest variance" claim rested on a single run of 20 batches. One outlier batch (a 2.65s spike against a typical 1.25–1.68s band) was doing almost all the work of that claim — remove it and Spark's stdev collapsed to match the others. A single run isn't enough evidence to call something a repeatable engine characteristic.
2. **Structural flaw.** The benchmark is synchronous by construction: only one batch is ever in flight, on any engine. Dask and Spark's actual value proposition — scheduling many concurrent tasks across workers — was never exercised. Combined with the workload being I/O-bound (S3 download dominates a FFT that's only a few ms of real compute), "no speedup over baseline" was close to guaranteed by the test's own construction, not a discovery.

This led to a redesign: keep the synchronous benchmark (it's still the right tool for isolating pure per-task overhead), but run it properly — repeated trials instead of one — and add a second, different benchmark that can actually show a difference if one exists: concurrent load. Steps 9 and 10 are that redesign.

---

## Step 9: Benchmark A — Per-Task Overhead, Done Properly

**Fix for the statistical fragility:** run **5 independent trials** of 20 batches per engine instead of 1, and report mean and spread *across trials* — not a single run's internal stdev.

**Fix for Kafka's offset semantics across trials:** each `KafkaConsumer` in the benchmark scripts uses `auto_offset_reset='latest'` with no consumer group, so it only sees messages sent *after* it starts polling. `code/run_trials.sh <engine> <n_trials>` drives this correctly for each trial: start the benchmark script in the background, poll its log for "Listening for work orders" (the readiness signal) before firing the producer, run `producer.py --batches 20 --interval 0.1` (fast — no need to simulate real 5s hardware cadence for an overhead benchmark), wait for the benchmark process to exit, then move to the next trial.

> **Gotcha hit during implementation:** Python fully buffers stdout when it isn't attached to a terminal (as it isn't here, redirected to a log file). The readiness-polling loop above only works if the benchmark scripts are invoked with `python3 -u` (unbuffered) — without it, "Listening for work orders" doesn't actually appear in the log file until the process exits, making every readiness check time out uselessly (the trial still completes correctly underneath, just ~60s slower per trial for no reason).

Each benchmark script gained a `--trial N` argument and writes to `results/overhead/<engine>_trialN.csv` with a `trial` column. `code/summarize_overhead.py` reads all 15 files and rolls them up.

**Results (mean ± stdev across 5 trials):**

| Metric | Baseline | Dask | Spark |
| --- | --- | --- | --- |
| Cold start (s) | 1.98 ± 0.70 | 1.68 ± 0.20 | 3.24 ± 0.39 |
| Warm avg / batch (s) | 1.35 ± 0.09 | 1.45 ± 0.06 | 1.54 ± 0.05 |
| Timing stdev (within a trial) | 0.19 ± 0.10 | 0.18 ± 0.05 | 0.33 ± 0.08 |
| Avg CPU | ~52% | ~52% | ~58% |
| Avg memory | ~59% | ~57% | ~68% |

**The fragile claim, re-tested:** every one of Spark's 5 trials (0.283, 0.311, 0.360, 0.237, 0.458) has a higher within-trial stdev than every one of baseline's or Dask's trials. The original finding — Spark is less stable batch-to-batch — turned out to be *true*, it just wasn't properly supported by a single run. Repetition is what turned a fragile-looking result into a real one. Spark's slow cold start (JVM warm-up) is similarly consistent across all 5 trials.

What this benchmark still cannot tell us: whether any of this matters when more than one batch is queued up at a time. That needs a different test.

---

## Step 10: Benchmark B — Throughput Under Concurrent Load

**Why this benchmark exists:** Benchmark A is deliberately synchronous, so Dask and Spark never get to schedule more than one task at once — the one thing a distributed engine is actually for. This benchmark removes that constraint: fire batches at each configuration faster than it can keep up, and see what happens to throughput and latency once a backlog forms.

**Load generation:** only 31 real S3 file pairs exist. `code/load_producer.py` cycles through them with fresh `batch_id`s to generate as many batches as needed — still real physics data reprocessed, no synthetic data required. It fires batches with no delay (or an optional rate cap).

**Four configurations, one question each:**
- `code/load_baseline_sequential.py` — no engine, strictly one batch at a time (the natural ceiling with zero concurrency)
- `code/load_baseline_concurrent.py` — no engine, but a `ThreadPoolExecutor` (8 threads) on the single master node — isolates whether the fix is *any* concurrency, or specifically needs a multi-machine cluster
- `code/load_dask.py` — 3-node cluster, non-blocking `client.submit` per batch as it arrives (this mirrors how the live `processor.py` actually works, unlike Benchmark A's blocking version)
- `code/load_spark.py` — 3-node cluster; since a single `sc.parallelize(...).collect()` call is itself blocking, concurrent submission is achieved with multiple driver threads each making their own `collect()` call against the same shared `SparkContext` (a supported pattern — Spark schedules jobs from multiple threads onto the cluster's executors)

Each script logs `send_time` (when `load_producer.py` sent it), `start_time`/`end_time` (when this configuration actually processed it) per batch to `results/load/<config>.csv`. `code/run_load.sh <config> <n_batches>` drives one configuration end to end (same readiness-polling pattern as Step 9); `code/analyze_load.py` computes throughput and p50/p95/p99 latency across all four.

**Results (100 batches, uncapped arrival):**

| Config | Throughput (batches/s) | p50 latency | p95 latency | p99 latency |
| --- | --- | --- | --- | --- |
| Baseline (sequential) | 0.80 | 64.2s | 119.4s | 124.2s |
| Baseline (concurrent) | 1.13 | 50.5s | 84.5s | 87.5s |
| Dask | 1.20 | 43.5s | 79.8s | 82.8s |
| Spark | 1.21 | 44.8s | 80.2s | 82.4s |

**Interpretation:** this is the regime Benchmark A structurally couldn't speak to, and here distribution clearly earns its keep — both Dask and Spark cut p50 latency by roughly 30% and lift sustained throughput by roughly 50% over sequential baseline. But the more interesting result is *where the gain comes from*: switching from sequential to merely concurrent (8 threads, still one machine, no cluster) already gets from 0.80 to 1.13 batches/s — most of the total improvement. Going from "concurrent on one box" to "an actual 3-node cluster" adds a further, real but modest gain (1.13 → 1.20–1.21). Dask and Spark are statistically indistinguishable from each other under load.

## Combined Conclusion

Two honestly-scoped benchmarks, two different (and both true) findings:

- **In isolation, one batch at a time (Benchmark A):** engine choice barely matters for speed, and Spark's overhead (slow JVM cold start, more per-batch jitter) is a real, repeatable cost with no offsetting benefit at this scale.
- **Under a real backlog (Benchmark B):** distribution helps — but a large share of that benefit is simply "don't process things one at a time," which ordinary concurrency on a single machine already gets you most of the way to, before a multi-machine cluster is ever needed.

The right takeaway isn't "distributed computing doesn't matter" (Benchmark A alone would have wrongly suggested that) or "distributed computing obviously wins" (an unfairly-designed load test could have oversold that) — it's that the two questions are genuinely different, and a benchmark answers exactly the question it was built to ask.

---

## Presentation

The full project — including both benchmarks and their real numbers — is told in `quax_story-2.html`, a self-contained interactive slide deck. Open it in any web browser.

## Tests

The `tests/` directory contains helper scripts used during development:
- `cluster_check.py` — verifies the Dask cluster is reachable
- `explore_s3.py` — lists/inspects objects in the S3 bucket
- `test_keys.py` — checks that S3 credentials are correctly loaded
