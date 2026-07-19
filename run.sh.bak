#!/usr/bin/env bash
# Launch/stop the QUAX pipeline (processor + producer + dashboard) as background
# processes, instead of juggling 3 terminals. Usage:
#   ./run.sh start [producer.py args...]   e.g. ./run.sh start --rate 1.0 --chunk-mb 8
#   ./run.sh stop
#   ./run.sh status
set -uo pipefail
cd "$(dirname "$0")"
source ~/pyvenv/bin/activate

PIDFILE=/tmp/quax_pids
LOGDIR=/tmp/quax_logs
mkdir -p "$LOGDIR"

start() {
    if [ -f "$PIDFILE" ]; then
        echo "Already running (or left over) — run '$0 stop' first."
        exit 1
    fi
    : > "$PIDFILE"
    eval "$(grep '^export S3_' ~/.bashrc | sort -u)"

    nohup python3 code/streaming_job.py > "$LOGDIR/streaming_job.log" 2>&1 &
    echo $! >> "$PIDFILE"
    echo "Waiting for processor..."
    for _ in $(seq 1 30); do
        grep -q "Listening for chunks" "$LOGDIR/streaming_job.log" 2>/dev/null && break
        sleep 1
    done
    if ! grep -q "Listening for chunks" "$LOGDIR/streaming_job.log" 2>/dev/null; then
        echo "Processor failed to start — see $LOGDIR/streaming_job.log"
        stop
        exit 1
    fi

    nohup python3 code/producer.py "$@" > "$LOGDIR/producer.log" 2>&1 &
    echo $! >> "$PIDFILE"

    nohup bokeh serve --show code/dashboard.py --allow-websocket-origin=localhost:5006 \
        > "$LOGDIR/bokeh.log" 2>&1 &
    echo $! >> "$PIDFILE"

    echo "Started. Logs in $LOGDIR — dashboard: http://localhost:5006/dashboard"
}

stop() {
    if [ -f "$PIDFILE" ]; then
        while read -r pid; do kill "$pid" 2>/dev/null; done < "$PIDFILE"
        rm -f "$PIDFILE"
    fi
    # Safety net: a crashed run can leave the Spark driver JVM orphaned.
    pkill -f 'SparkSubmit.*[q]uax-processor' 2>/dev/null
    echo "Stopped."
}

status() {
    if [ ! -f "$PIDFILE" ]; then
        echo "Not running."
        return
    fi
    while read -r pid; do
        ps -p "$pid" -o pid,cmd --no-headers || echo "$pid (dead)"
    done < "$PIDFILE"
}

case "${1:-}" in
    start) shift; start "$@" ;;
    stop) stop ;;
    status) status ;;
    *) echo "Usage: $0 {start [producer.py args...]|stop|status}"; exit 1 ;;
esac
