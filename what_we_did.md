# Reproducible Guide: QUAX Distributed Real-Time Pipeline

## Overview
This project implements a streaming ETL (Extract, Transform, Load) pipeline for the QUAX experiment. It continuously fetches high-frequency IQ radio data from S3, distributes Fast Fourier Transform (FFT) computations across a Dask cluster, and visualizes the power spectrum in real-time.

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
> **Why?** Dask requires nodes to communicate seamlessly. The hosts file allows nodes to resolve each other by name.
On **all three VMs**, we appended the cluster IPs to `/etc/hosts`:
```bash
sudo nano /etc/hosts

# Added to bottom:
10.67.22.111 master
10.67.22.246 worker1
10.67.22.248 worker2
```

### 1.4 Passwordless SSH Authentication
> **Why?** The Dask Master needs the authority to silently spawn worker processes on remote machines without human intervention (typing passwords).
On the `master` node, we generated an RSA keypair and copied the public key:
```bash
ssh-keygen -t rsa  # Press Enter for all prompts
cat ~/.ssh/id_rsa.pub
```
We then pasted that output into the `~/.ssh/authorized_keys` file on `master`, `worker1`, and `worker2`.

---

## Step 2: Software Environment Setup
> **Why?** In a distributed system, if the Master sends a NumPy task to a Worker that lacks NumPy, the task fails. We used `uv` (a high-speed Rust-based Python package installer) to guarantee identical environments across the cluster instantly.

We ran this exact chain of commands on **all three nodes**:
```bash
sudo apt update && sudo apt upgrade -y
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
uv venv ~/pyvenv
source ~/pyvenv/bin/activate
uv pip install "dask[complete]" asyncssh notebook matplotlib numpy boto3 kafka-python-ng streamlit pandas
```

---

## Step 3: Message Broker Setup (Apache Kafka)
> **Why?** Kafka acts as an asynchronous buffer. If data downloads from S3 faster than the Dask cluster can compute the FFTs, the system would crash. Kafka decouples the scripts: the Producer drops data into a queue, and the Dask Processor pulls it out at its own pace.

We installed Kafka natively on the **Master node**:
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
We created a `Project/code` directory on the Master node and wrote three microservices.



### 4.1 Security Credentials (Persistent Environment Variables)
> **Why?** Hardcoding AWS/S3 passwords directly into Python scripts is a major security vulnerability. Instead, we injected our CloudVeneto S3 keys directly into the Master node's Linux `~/.bashrc` file. When we log via SSH, Linux automatically loads these keys into the system environment *before* we even activate our Python virtual environment. The virtual environment inherits them, keeping the keys secure while allowing `processor.py` to pull them via `os.environ.get()` and safely inject them into the remote Dask workers.

To permanently save the credentials to the Master node's Linux profile, we ran:
```bash
echo 'export S3_ACCESS_KEY="your_access_key"' >> ~/.bashrc
echo 'export S3_SECRET_KEY="your_secret_key"' >> ~/.bashrc
```
Because of this setup, we never need to export the keys manually again. They are automatically present in the background every time we log in, allowing us to simply activate the virtual environment and start the pipeline.


### 4.2 The Producer (`producer.py`)
> **Design Choice:** We do *not* download the 64MB binary files and push them through Kafka. Pushing heavy binary data causes severe network bottlenecks. Instead, we send lightweight JSON "Work Orders" containing the S3 filenames.

```python
import time, json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for i in range(31):
    file_idx = f"{i:05d}"
    work_order = {
        "batch_id": i,
        "i_file": f"duck_i_{file_idx}.dat",
        "q_file": f"duck_q_{file_idx}.dat"
    }
    producer.send('quax_raw', value=work_order)
    producer.flush()
    time.sleep(5) # Simulates hardware DAQ frequency
```

### 4.3 The Processor & Dask Engine (`processor.py`)
> **Design Choice:** We securely injected the S3 keys into the remote workers using `worker_options={"env": ...}` so they don't leak into the Dask task graph. We also used non-blocking Kafka polling so the Master can continually submit tasks without waiting for previous ones to finish.

```python
import os, json, time, urllib3
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from dask.distributed import Client, SSHCluster

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- WORKER PAYLOAD (Physics Math) ---
def process_physics_data(work_order):
    import os, boto3, numpy as np, urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    s3 = boto3.client('s3',
        endpoint_url='https://cloud-areapd.pd.infn.it:5210',
        aws_access_key_id=os.environ.get('S3_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('S3_SECRET_KEY'),
        verify=False
    )
    
    # Download directly to RAM
    obj_i = s3.get_object(Bucket='quax', Key=work_order['i_file'])
    data_i = np.frombuffer(obj_i['Body'].read(), dtype='<f4')
    
    obj_q = s3.get_object(Bucket='quax', Key=work_order['q_file'])
    data_q = np.frombuffer(obj_q['Body'].read(), dtype='<f4')
    
    # Combine, reshape, FFT, and Power Spectrum
    signal = data_i + 1j * data_q
    spectra = np.fft.fftshift(np.fft.fft(signal.reshape(-1, 2048), axis=1), axes=1)
    power = np.abs(spectra)**2
    
    return {
        "batch_id": work_order['batch_id'],
        "frequency": np.linspace(-1, 1, 2048).tolist(),
        "average": np.mean(power, axis=0).tolist(),
        "std": np.std(power, axis=0).tolist()
    }

# --- MASTER EVENT LOOP ---
if __name__ == "__main__":
    cluster = SSHCluster(
        ["master", "worker1", "worker2"],
        connect_options={"known_hosts": None},
        worker_options={"env": {"S3_ACCESS_KEY": os.environ.get('S3_ACCESS_KEY'), "S3_SECRET_KEY": os.environ.get('S3_SECRET_KEY')}}
    )
    client = Client(cluster)

    consumer = KafkaConsumer('quax_raw', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    active_futures = []
    
    while True:
        # Non-blocking poll
        for tp, messages in consumer.poll(timeout_ms=100).items():
            for msg in messages:
                future = client.submit(process_physics_data, msg.value)
                active_futures.append({"future": future, "batch": msg.value['batch_id'], "start": time.time()})

        # Check for completed tasks
        for i in range(len(active_futures) - 1, -1, -1):
            task = active_futures[i]
            if task["future"].status == 'finished':
                producer.send('quax_processed', value=task["future"].result())
                producer.flush()
                print(f"Batch {task['batch']} finished in {time.time() - task['start']:.2f}s")
                active_futures.pop(i)
```

### 4.4 The Dashboard (`dashboard.py`)
> **Design Choice:** Streamlit easily wraps Pandas dataframes to render real-time UI updates consumed directly from Kafka.

```python
import streamlit as st, pandas as pd, json
from kafka import KafkaConsumer

st.set_page_config(page_title="QUAX Monitor", layout="wide")
st.title("📡 QUAX Experiment: Live Data Monitor")
chart_placeholder = st.empty()

consumer = KafkaConsumer('quax_processed', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    data = message.value
    df = pd.DataFrame({'Frequency (MHz)': data['frequency'], 'Power Spectrum': data['average']})
    chart_placeholder.line_chart(df.set_index('Frequency (MHz)'))
```

---

## Step 5: Execution & Port Forwarding
Because the Streamlit dashboard runs on port `8501` of a private Cloud VM, we used SSH Local Port Forwarding from our laptop to securely tunnel the web traffic.

**On the local laptop terminal:**
```bash
ssh -L 8501:localhost:8501 master
```

**Inside the Master node (using 3 terminals):**
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
*Result: Navigating to `http://localhost:8501` on the local laptop displays the live-updating frequency spectrum.*
