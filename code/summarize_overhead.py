import os
import csv
import glob
import statistics

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "overhead")
ENGINES = ["baseline", "dask", "spark"]


def load_trial(engine, trial):
    path = os.path.join(RESULTS_DIR, f"{engine}_trial{trial}.csv")
    with open(path) as f:
        rows = list(csv.DictReader(f))
    times = [float(r["calc_time"]) for r in rows]
    cpus = [float(r["cpu_percent"]) for r in rows]
    mems = [float(r["mem_percent"]) for r in rows]
    warm = times[1:]
    return {
        "cold": times[0],
        "warm_avg": statistics.mean(warm),
        "warm_stdev": statistics.stdev(warm),
        "cpu_avg": statistics.mean(cpus[1:]),
        "mem_avg": statistics.mean(mems[1:]),
    }


print("=== Per-trial detail ===")
print(f"{'engine':<10}{'trial':<7}{'cold':<8}{'warm_avg':<10}{'warm_stdev':<12}{'cpu%':<7}{'mem%':<7}")
per_engine = {e: [] for e in ENGINES}
for engine in ENGINES:
    for trial in range(1, 6):
        stats = load_trial(engine, trial)
        per_engine[engine].append(stats)
        print(f"{engine:<10}{trial:<7}{stats['cold']:<8.2f}{stats['warm_avg']:<10.3f}"
              f"{stats['warm_stdev']:<12.3f}{stats['cpu_avg']:<7.1f}{stats['mem_avg']:<7.1f}")

print("\n=== Rollup across 5 trials (mean +/- stdev-across-trials) ===")
print(f"{'engine':<10}{'cold':<16}{'warm_avg':<18}{'within-trial stdev':<20}{'cpu%':<10}{'mem%':<10}")
for engine in ENGINES:
    trials = per_engine[engine]
    cold = [t["cold"] for t in trials]
    warm = [t["warm_avg"] for t in trials]
    wstd = [t["warm_stdev"] for t in trials]
    cpu = [t["cpu_avg"] for t in trials]
    mem = [t["mem_avg"] for t in trials]
    print(f"{engine:<10}"
          f"{statistics.mean(cold):.2f}+/-{statistics.stdev(cold):<9.2f}"
          f"{statistics.mean(warm):.3f}+/-{statistics.stdev(warm):<11.3f}"
          f"{statistics.mean(wstd):.3f}+/-{statistics.stdev(wstd):<13.3f}"
          f"{statistics.mean(cpu):<10.1f}{statistics.mean(mem):<10.1f}")
