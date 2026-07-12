import csv
import statistics

files = {
    "baseline": "benchmark_baseline_results.csv",
    "dask": "benchmark_dask_results.csv",
    "spark": "benchmark_spark_results.csv",
}

print(f"{'Metric':<18}{'Baseline':<12}{'Dask':<12}{'Spark':<12}")
rows = {}
for engine, fname in files.items():
    with open(fname) as f:
        r = list(csv.DictReader(f))
    times = [float(x["calc_time"]) for x in r]
    cpus = [float(x["cpu_percent"]) for x in r]
    mems = [float(x["mem_percent"]) for x in r]
    cold = times[0]
    warm = times[1:]
    rows[engine] = {
        "cold": cold,
        "warm_avg": statistics.mean(warm),
        "warm_std": statistics.stdev(warm),
        "cpu_avg": statistics.mean(cpus[1:]),
        "mem_avg": statistics.mean(mems[1:]),
    }

for metric, label in [("cold", "Cold start (s)"), ("warm_avg", "Warm avg/batch (s)"),
                       ("warm_std", "Timing stdev"), ("cpu_avg", "Avg CPU %"), ("mem_avg", "Avg mem %")]:
    print(f"{label:<18}" + "".join(f"{rows[e][metric]:<12.2f}" for e in ["baseline", "dask", "spark"]))
