#!/usr/bin/env python3
"""Live QUAX ingestion monitor. Run with: bokeh serve --show dashboard.py"""
import time
from kafka import KafkaConsumer
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, Div
from bokeh.layouts import column

MB = 1024 * 1024
WINDOW = 100
consumer = KafkaConsumer(
    "quax_stream", bootstrap_servers="localhost:9092",
    auto_offset_reset="latest", consumer_timeout_ms=1,
    fetch_max_bytes=64 * MB, max_partition_fetch_bytes=64 * MB,
)

src = ColumnDataSource(dict(t=[], rate=[]))
state = dict(start=time.time(), last_t=None, n=0)
status = Div(text="Waiting for data...", styles={"color": "#ccc", "font-size": "16px"})

p = figure(title="QUAX stream ingestion", x_axis_label="Time (s)", y_axis_label="Current throughput (MB/s)",
           width=850, height=400, background_fill_color="#111318", border_fill_color="#111318")
p.title.text_color = p.xaxis.axis_label_text_color = p.yaxis.axis_label_text_color = "#ddd"
p.xaxis.major_label_text_color = p.yaxis.major_label_text_color = "#aaa"
p.grid.grid_line_color = "#2a2d34"
p.line("t", "rate", source=src, line_width=2, color="#4fc3f7")
p.circle("t", "rate", source=src, size=6, color="#4fc3f7")


def poll():
    for msg in consumer:
        now = time.time()
        dt = now - state["last_t"] if state["last_t"] else None
        state["last_t"], state["n"] = now, state["n"] + 1
        rate = (len(msg.value) / MB) / dt if dt else 0
        src.stream(dict(t=[now - state["start"]], rate=[rate]), rollover=WINDOW)
        file_id = dict(msg.headers).get("file_id", b"?").decode() if msg.headers else "?"
        status.text = f"Chunks received: {state['n']} | Current rate: {rate:.1f} MB/s | Latest: {file_id}"


curdoc().add_periodic_callback(poll, 500)
curdoc().title = "QUAX Stream Monitor"
curdoc().add_root(column(status, p))
