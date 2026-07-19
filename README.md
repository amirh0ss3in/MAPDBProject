# QUAX streaming pipeline

Real-time FFT power-spectrum monitoring for the QUAX experiment: S3 → Kafka → Spark (distributed across `master`/`worker1`/`worker2`) → Kafka → live dashboard.

## Run

```bash
ssh master
cd Project
./run.sh start --rate 1.0   # starts processor, producer, dashboard
./run.sh status             # check what's running
./run.sh stop                # stop everything
```

Logs: `/tmp/quax_logs/{streaming_job,producer,bokeh}.log`.

From your laptop, forward the dashboard port: `ssh -L 5006:localhost:5006 master`, then open `http://localhost:5006/dashboard`.

## Flags

Passed through to `producer.py` via `run.sh start`:
- `--rate` — multiplier on the real 16MB/s DAQ throughput (default 1.0; e.g. `--rate 2.0` streams at 32MB/s)
- `--chunk-mb` — per-channel chunk size sent per Kafka message (default 1)
- `--n-pairs` — stop after N file-pairs instead of looping forever

## Dashboard

Ingestion throughput, processing backlog (is Spark keeping pace with the producer, or falling behind?), live power spectrum (latest batch + cumulative run average), and a pipeline topology diagram — pulled Kafka/Spark health plus pushed per-chunk worker routing, updated live.

## Processing

`streaming_job.py` micro-batches chunks off `quax_stream` (up to 8 chunks or a short wait, whichever comes first) into one Spark job per batch, so work actually fans out across both workers instead of one chunk at a time. Its Kafka consumer group is `quax-processor` — its committed offset vs. the topic's latest offset is what the dashboard's backlog graph reads.

## Validated throughput

Confirmed sustaining the target 16MB/s and holding at 32MB/s (2x) with backlog recovering to zero after each run; `--rate 0.5` (8MB/s) is comfortably idle.
