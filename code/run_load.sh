#!/bin/bash
# Usage: run_load.sh <config: baseline_sequential|baseline_concurrent|dask|spark> <num_batches>
set -uo pipefail
cd ~/Project/code || exit 1
PY=~/pyvenv/bin/python3
LOGDIR=~/exp_logs
mkdir -p "$LOGDIR"

CONFIG="$1"
BATCHES="$2"
SCRIPT="load_${CONFIG}.py"

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

echo "=== load test: $CONFIG ($BATCHES batches) ==="
LOGFILE="$LOGDIR/load_${CONFIG}.log"
~/run_with_env.sh "$PY" -u "$SCRIPT" --batches "$BATCHES" > "$LOGFILE" 2>&1 &
wait_ready "$LOGFILE" 45
"$PY" -u load_producer.py --batches "$BATCHES" > "$LOGDIR/load_producer_${CONFIG}.log" 2>&1
wait_exit "$SCRIPT --batches $BATCHES" 180
echo "=== load test $CONFIG done ==="
