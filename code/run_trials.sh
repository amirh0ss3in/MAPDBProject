#!/bin/bash
# Usage: run_trials.sh <engine: baseline|dask|spark> <num_trials> [start_trial]
set -uo pipefail
cd ~/Project/code || exit 1
PY=~/pyvenv/bin/python3
LOGDIR=~/exp_logs
mkdir -p "$LOGDIR"

ENGINE="$1"
TRIALS="$2"
START="${3:-1}"
SCRIPT="benchmark_${ENGINE}.py"

wait_ready () {
  local logfile="$1"
  local max="$2"
  local i=0
  while [ "$i" -lt "$max" ]; do
    if grep -q "Listening for work orders" "$logfile" 2>/dev/null; then
      return 0
    fi
    i=$((i+1))
    sleep 1
  done
  echo "WARNING: $logfile never became ready after ${max}s"
  return 1
}

wait_exit () {
  local pattern="$1"
  local max_iters="$2"
  local i=0
  while [ "$i" -lt "$max_iters" ]; do
    if ! pgrep -f "$pattern" > /dev/null; then
      return 0
    fi
    i=$((i+1))
    sleep 2
  done
  echo "WARNING: $pattern still running after $((max_iters*2))s, killing"
  pkill -f "$pattern"
  return 1
}

for t in $(seq "$START" "$TRIALS"); do
  echo "=== $ENGINE trial $t/$TRIALS ==="
  LOGFILE="$LOGDIR/${ENGINE}_trial${t}.log"
  ~/run_with_env.sh "$PY" -u "$SCRIPT" --trial "$t" > "$LOGFILE" 2>&1 &
  wait_ready "$LOGFILE" 45
  "$PY" -u producer.py --batches 20 --interval 0.1 > "$LOGDIR/producer_${ENGINE}_trial${t}.log" 2>&1
  wait_exit "$SCRIPT --trial $t" 90
  echo "=== $ENGINE trial $t done ==="
  sleep 3
done

echo "ALL TRIALS DONE for $ENGINE"
