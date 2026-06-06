# QUAX Experiment: Distributed Real-Time Data Pipeline

## Overview
This project implements a streaming data pipeline for the QUAX experiment. It fetches high-frequency IQ radio data from a CloudVeneto S3 bucket, processes the Fast Fourier Transform (FFT) across a distributed Dask cluster, and visualizes the resulting power spectrum in real-time.

## Architecture
- **Producer:** Pushes "work orders" (S3 filenames) to Apache Kafka to simulate a continuous DAQ stream.
- **Processor:** A Kafka consumer that delegates data downloading and FFT math to a multi-node Dask SSHCluster. Processed results are pushed to a second Kafka topic.
- **Dashboard:** A Streamlit application that consumes the processed data and updates a live frequency plot.

## Infrastructure Setup
The pipeline was built and tested on 3 CloudVeneto Virtual Machines (Ubuntu 24.04).
- 1 Master Node (runs Kafka, Streamlit, and Dask Scheduler)
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

1. **Start the Dask Processor:**
```bash
source ~/pyvenv/bin/activate
python3 code/processor.py
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
(View the dashboard by port-forwarding port 8501 to your local machine).

## Benchmarks
Performance scaling tests (Execution Time vs. Number of Workers) are located in the `/benchmarks` directory.
