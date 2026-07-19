# QUAX streaming pipeline

Real-time FFT power-spectrum monitoring for the QUAX experiment: S3 → Kafka → Spark (distributed across `master`/`worker1`/`worker2`) → Kafka → live dashboard.

## Run

```bash
ssh master
cd Project
./run.sh start --rate 1.0 --chunk-mb 8   # starts processor, producer, dashboard
./run.sh status                          # check what's running
./run.sh stop                            # stop everything
```

Logs: `/tmp/quax_logs/{streaming_job,producer,bokeh}.log`.

From your laptop, forward the dashboard port: `ssh -L 5006:localhost:5006 master`, then open `http://localhost:5006/dashboard`.

## Flags

Passed through to `producer.py` via `run.sh start`:
- `--rate` — speed multiplier on the real ~5s/pair DAQ cadence (default 1.0)
- `--chunk-mb` — per-channel chunk size sent per Kafka message (default 8)
- `--n-pairs` — stop after N file-pairs instead of looping forever
