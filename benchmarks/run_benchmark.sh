#!/usr/bin/env bash
# Runs the full 0.5x/1x/2x benchmark end-to-end, unattended.
# For each rate: resets the quax-processor consumer group, starts the offset
# probe (backlog_probe.py), runs the pipeline for 20 file-pairs, waits for it
# to finish sending and drain, stops everything, then moves to the next rate.
# Regenerates benchmark.png at the end.
#
# Run from Project/benchmarks/ on master (takes ~6 minutes total):
#   ./run_benchmark.sh
set -uo pipefail
cd "$(dirname "$0")"
KAFKA_HOME=~/kafka_2.13-3.7.0
PROJECT=..

source ~/pyvenv/bin/activate

run_one() {
    local rate=$1 label=$2
    echo "=== rate=$rate -> $label ==="

    (cd "$PROJECT" && ./run.sh stop) >/dev/null 2>&1

    # the consumer group can take a few seconds to become deletable after stop
    for _ in $(seq 1 10); do
        "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 \
            --delete --group quax-processor >/dev/null 2>&1 && break
        sleep 2
    done

    python3 backlog_probe.py > "benchmark_${label}.log" 2>&1 &
    local probe_pid=$!
    sleep 1

    (cd "$PROJECT" && ./run.sh start --rate "$rate" --n-pairs 20)

    echo "waiting for producer to finish sending..."
    while pgrep -f '[p]roducer\.py' >/dev/null; do sleep 3; done

    echo "draining backlog..."
    sleep 15

    (cd "$PROJECT" && ./run.sh stop) >/dev/null 2>&1
    kill "$probe_pid" 2>/dev/null
    echo "=== $label done -> benchmark_${label}.log ==="
    echo
}

run_one 0.5 8mbps
run_one 1.0 16mbps
run_one 2.0 32mbps

echo "=== regenerating benchmark.png ==="
python3 plot_benchmark.py
echo "Done. benchmark_{8,16,32}mbps.log and benchmark.png are up to date."
