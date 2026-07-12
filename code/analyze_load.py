import os
import csv
import statistics

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "load")
CONFIGS = ["baseline_sequential", "baseline_concurrent", "dask", "spark"]


def percentile(data, p):
    s = sorted(data)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


print(f"{'config':<22}{'batches':<10}{'span(s)':<10}{'throughput/s':<14}{'p50 lat':<10}{'p95 lat':<10}{'p99 lat':<10}")
for cfg in CONFIGS:
    path = os.path.join(RESULTS_DIR, f"{cfg}.csv")
    if not os.path.exists(path):
        print(f"{cfg:<22} (no data)")
        continue
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        continue
    starts = [float(r["start_time"]) for r in rows]
    ends = [float(r["end_time"]) for r in rows]
    lat = [float(r["latency"]) for r in rows]
    span = max(ends) - min(starts)
    throughput = len(rows) / span if span > 0 else float("nan")
    print(f"{cfg:<22}{len(rows):<10}{span:<10.1f}{throughput:<14.2f}"
          f"{percentile(lat,0.50):<10.2f}{percentile(lat,0.95):<10.2f}{percentile(lat,0.99):<10.2f}")
