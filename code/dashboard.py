#!/usr/bin/env python3
"""Live QUAX ingestion + cluster-routing monitor. Run with: bokeh serve --show dashboard.py"""
import json, time
from kafka import KafkaConsumer
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, Div
from bokeh.layouts import column, row

MB = 1024 * 1024
WINDOW = 100
NODES = {"mapd-master": "Master", "mapd-master1": "Worker 1", "mapd-master2": "Worker 2"}

stream_consumer = KafkaConsumer(
    "quax_stream", bootstrap_servers="localhost:9092",
    auto_offset_reset="latest", consumer_timeout_ms=1,
    fetch_max_bytes=64 * MB, max_partition_fetch_bytes=64 * MB,
)
telemetry_consumer = KafkaConsumer(
    "quax_telemetry", bootstrap_servers="localhost:9092",
    auto_offset_reset="latest", consumer_timeout_ms=1,
    value_deserializer=lambda v: json.loads(v),
)
results_consumer = KafkaConsumer(
    "quax_results", bootstrap_servers="localhost:9092",
    auto_offset_reset="latest", consumer_timeout_ms=1,
)

src = ColumnDataSource(dict(t=[], rate=[]))
state = dict(start=time.time(), last_t=None, n=0)
status = Div(text="Waiting for data...", styles={"color": "#ccc", "font-size": "16px"})

p = figure(title="QUAX stream ingestion", x_axis_label="Time (s)", y_axis_label="Current throughput (MB/s)",
           width=850, height=350, background_fill_color="#111318", border_fill_color="#111318")
p.title.text_color = p.xaxis.axis_label_text_color = p.yaxis.axis_label_text_color = "#ddd"
p.xaxis.major_label_text_color = p.yaxis.major_label_text_color = "#aaa"
p.grid.grid_line_color = "#2a2d34"
p.line("t", "rate", source=src, line_width=2, color="#4fc3f7")
p.circle("t", "rate", source=src, size=6, color="#4fc3f7")


def poll_stream():
    for msg in stream_consumer:
        now = time.time()
        dt = now - state["last_t"] if state["last_t"] else None
        state["last_t"], state["n"] = now, state["n"] + 1
        rate = (len(msg.value) / MB) / dt if dt else 0
        src.stream(dict(t=[now - state["start"]], rate=[rate]), rollover=WINDOW)
        file_id = dict(msg.headers).get("file_id", b"?").decode() if msg.headers else "?"
        status.text = f"Chunks received: {state['n']} | Current rate: {rate:.1f} MB/s | Latest: {file_id}"


node_counts = {host: 0 for host in NODES}
node_active = {host: 0 for host in NODES}
result_count = [0]
node_boxes = {
    host: Div(width=270, height=90, styles={
        "border": "1px solid #2a2d34", "border-radius": "12px", "padding": "12px",
        "background-color": "#111318", "color": "#ccc", "font-size": "14px", "text-align": "center",
    })
    for host in NODES
}


def render_node(host):
    label = NODES[host]
    lit = node_active[host] > 0
    color = "#4fc3f7" if lit else "#2a2d34"
    role = "Kafka + Spark driver" if host == "mapd-master" else "Spark executor"
    extra = f"results published: {result_count[0]}" if host == "mapd-master" else f"chunks processed: {node_counts[host]}"
    node_boxes[host].styles["border"] = f"2px solid {color}"
    node_boxes[host].text = f"<b>{label}</b><br>{role}<br>{extra}"


def poll_telemetry():
    for host in node_active:
        node_active[host] = max(0, node_active[host] - 1)
    for msg in telemetry_consumer:
        host = msg.value["host"]
        if host in node_counts:
            node_counts[host] += 1
            node_active[host] = 3
    for host in NODES:
        render_node(host)


def poll_results():
    for _ in results_consumer:
        result_count[0] += 1


def poll():
    poll_stream()
    poll_telemetry()
    poll_results()


for host in NODES:
    render_node(host)

curdoc().add_periodic_callback(poll, 500)
curdoc().title = "QUAX Stream Monitor"
curdoc().add_root(column(
    status, p,
    row(node_boxes["mapd-master"], node_boxes["mapd-master1"], node_boxes["mapd-master2"]),
))
