# QUAX streaming pipeline

Real-time FFT power-spectrum monitoring for the QUAX experiment: S3 → Kafka → Spark (distributed across `master`/`worker1`/`worker2`) → Kafka → live dashboard.

## Run (3 terminals, in order)

**1. Processor** — starts Kafka + the Spark cluster automatically.
```bash
ssh master
cd Project && source ~/pyvenv/bin/activate
python3 code/streaming_job.py
```
Wait for `Listening for chunks...` before continuing.

**2. Producer** — streams real QUAX data from S3 into Kafka.
```bash
ssh master
cd Project && source ~/pyvenv/bin/activate
python3 code/producer.py --rate 1.0 --chunk-mb 8
```

**3. Dashboard** — from your laptop, forward the port first:
```bash
ssh -L 5006:localhost:5006 master
cd Project && source ~/pyvenv/bin/activate
bokeh serve --show code/dashboard.py --allow-websocket-origin=localhost:5006
```
Open `http://localhost:5006/dashboard`.

## Flags

- `producer.py --rate` — speed multiplier on the real ~5s/pair DAQ cadence (default 1.0)
- `producer.py --chunk-mb` — per-channel chunk size sent per Kafka message (default 8)
- `producer.py --n-pairs` — stop after N file-pairs instead of looping forever
