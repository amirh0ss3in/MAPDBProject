# Reproducible Guide: QUAX Distributed Real-Time Pipeline

## Overview

This guide documents the full build of the QUAX pipeline: a streaming ETL system that fetches high-frequency IQ radio data from S3, computes the FFT, and visualizes the power spectrum in real time — plus the two distributed-engine implementations (Dask, Apache Spark) and the benchmark that compares them against each other and against a no-distribution baseline.

> **Goal:** Build the pipeline, then answer a concrete question — for this workload, does the choice of distributed engine (Dask vs. Spark) actually matter, and how does either compare to just not distributing at all?

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

### 4.2 The Producer (`code/producer.py`)
> **Design Choice:** We do *not* download the 64MB binary files and push them through Kafka. Pushing heavy binary data causes severe network bottlenecks. Instead, we send lightweight JSON "work orders" containing the S3 filenames, one every 5 seconds to simulate the real QUAX hardware's DAQ rate.

### 4.3 The Processor — Dask Engine (`code/processor.py`)
> **Design Choice:** We securely inject the S3 keys into the remote Dask workers using `worker_options={"env": ...}` so they don't leak into the Dask task graph, and use non-blocking Kafka polling so the master can continually submit tasks without waiting for previous ones to finish.

The cluster is a `dask.distributed.SSHCluster` spanning `master`, `worker1`, and `worker2` — Dask genuinely uses all three machines.

> **Known limitation:** `worker_options={"env": {...}}` passes the S3 keys to each worker's `Nanny` process as part of its spec, which ends up visible in the worker's own `ps aux` output on the remote VM (i.e. any local user on `worker1`/`worker2` could read them). This is a pre-existing pattern in the original design; hardening it (e.g. via Dask's `Security` config or a secrets file instead of inline env) is a good follow-up but out of scope here.

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

# S3 keys are injected into the executors' environment, the same pattern
# as Dask's worker_options={"env": ...}.
spark = SparkSession.builder \
    .appName("QUAX-Spark-Processor") \
    .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
    .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
    .getOrCreate()
sc = spark.sparkContext

# Hand one whole batch to Spark: it ships the work_order to an executor,
# runs process_physics_data there, and brings the result back.
rdd = sc.parallelize([work_order], numSlices=1)
result_json = rdd.map(process_physics_data).collect()[0]
```

`processor_spark.py` (the live pipeline script) deliberately uses Spark's default local mode — the live demo only needs one machine to keep up with a batch every 5 seconds. The benchmark, however, needs a real cluster to be a fair comparison against Dask; that's built next.

> **Note (installing PySpark):** the shared `pyvenv` was created without pip, so `pip` had to be bootstrapped once on `master` (`curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python get-pip.py`) before `pip install pyspark` would work. `pyspark==4.1.2` was installed this way on `master`; it was later installed on `worker1`/`worker2` too (Step 7) using `uv pip install pyspark==4.1.2` directly, which needs no such bootstrap.

---

## Step 7: A Real 3-Node Spark Cluster

> **Why?** For the benchmark to be a fair comparison with Dask's 3-node `SSHCluster`, Spark also needs to actually run across all three machines — not just the master.

Java 17 and `pyspark==4.1.2` (matching the master's version) were installed on `worker1` and `worker2`:
```bash
sudo apt update && sudo apt install -y openjdk-17-jre-headless
source ~/pyvenv/bin/activate
uv pip install pyspark==4.1.2
```

A Spark Standalone cluster is then started — the master daemon on `master`, and a worker daemon on each of `worker1`/`worker2` registering against it:

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

`code/benchmark_spark.py` connects to this cluster with `.master("spark://master:7077")` (instead of the implicit local default that `processor_spark.py` uses), with `spark.pyspark.python` pinned to the shared venv so executors have the same NumPy/boto3 environment as the driver.

---

## Step 8: The Benchmark — Baseline vs Dask vs Spark

> **Why?** "It runs" is not the same as "it's the right tool," and neither is meaningful without knowing what you get over not distributing at all. Three configurations were instrumented identically and run on the same data:

- **Baseline** (`code/benchmark_baseline.py`) — no distribution engine at all; `process_physics_data` is called directly, in-process, on the master node. This is the reference point.
- **Dask** (`code/benchmark_dask.py`) — the 3-node `SSHCluster` (`master` + `worker1` + `worker2`).
- **Spark** (`code/benchmark_spark.py`) — the 3-node Spark Standalone cluster from Step 7.

### 8.1 Methodology

- **Same workload:** the same producer streamed the same 20 batches of real QUAX data to each engine (one batch every 5s).
- **Same timing method:** each processor runs **synchronously** — submit one batch, block until its result, record the time. (Dask's `processor.py` is asynchronous by default; the benchmark version blocks on each `future.result()` instead, so all three are measured the same way.)
- **Same metrics:** for every batch, processing time, CPU %, and memory % were logged with `psutil` and written to `results/benchmark_<engine>_results.csv`.
- **Same physics:** `process_physics_data` is byte-for-byte identical across all three scripts, so the benchmark measures engine overhead, not algorithm differences.

> **Reading the numbers correctly:** the meaningful metric is *per-batch processing time after warm-up*. Total wall-clock time is dominated by the producer's deliberate 5s gap between batches, not by the engines, so it isn't a measure of speed.

**Files:**
- `code/benchmark_baseline.py`, `code/benchmark_dask.py`, `code/benchmark_spark.py` — the three instrumented processors
- `results/benchmark_baseline_results.csv`, `results/benchmark_dask_results.csv`, `results/benchmark_spark_results.csv` — raw per-batch results (20 batches each)
- `code/summarize_benchmarks.py` — recomputes the table below from the raw CSVs

### 8.2 Results (warm state, i.e. after the first batch)

| Metric            | Baseline | Dask   | Spark  |
| ----------------- | -------- | ------ | ------ |
| Cold start        | 2.54 s   | 2.64 s | 4.35 s |
| Warm avg / batch  | 1.39 s   | 1.32 s | 1.54 s |
| Timing stdev      | 0.11     | 0.19   | 0.29   |
| Avg CPU           | ~23%     | ~2%    | ~8%    |
| Avg memory        | ~59%     | ~60%   | ~68%   |

### 8.3 Interpretation

All three configurations land in the same 1.3–1.5s/batch range, because the identical NumPy FFT dominates the time regardless of what schedules it. This is itself the key finding: **for a workload this small and this synchronous (one batch in flight at a time), neither Dask nor Spark buys a speed-up over just running the function locally** — the network/serialization cost of shipping a batch to a remote worker roughly cancels out any parallelism benefit.

Where the three differ is overhead and character, not raw throughput:

- **Baseline** has the lowest timing variance and no cluster machinery to set up, but ties up the master node's own CPU/network for every batch and doesn't scale if batches arrive faster than one at a time.
- **Dask** is lightest on CPU/memory (pure Python, low overhead) and about as fast as the baseline, but shows more timing variance.
- **Spark** is the slowest to cold-start (JVM warm-up) and heaviest on CPU/memory of the three, and also the most variable — the extra hop through a second machine (task goes to whichever of worker1/worker2 Spark schedules it to) adds a small, variable network cost on top of the identical FFT.

There is no single "winner" for this workload — the benchmark shows that the choice of engine matters more for *how you scale* (many concurrent batches, fault tolerance, mixed workloads) than for raw per-batch latency at this batch size.
