#!/usr/bin/env python3
"""Live QUAX ingestion + spectrum + cluster-routing monitor. Run with: bokeh serve --show dashboard.py"""
import json, time
from kafka import KafkaConsumer
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, Band, Div
from bokeh.layouts import column, row
from bokeh.palettes import Category10

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
    value_deserializer=lambda v: json.loads(v),
)


def caption(text):
    return Div(text=f'<div style="color:#8a8f98;font-size:13px;max-width:850px;padding:2px 0 10px 0">{text}</div>')


def styled(title, x_label, y_label, height=350, y_axis_type="linear"):
    p = figure(title=title, x_axis_label=x_label, y_axis_label=y_label, y_axis_type=y_axis_type,
               width=850, height=height, background_fill_color="#111318", border_fill_color="#111318")
    p.title.text_color = p.xaxis.axis_label_text_color = p.yaxis.axis_label_text_color = "#ddd"
    p.xaxis.major_label_text_color = p.yaxis.major_label_text_color = "#aaa"
    p.grid.grid_line_color = "#2a2d34"
    return p


# --- stream ingestion throughput ---
rate_src = ColumnDataSource(dict(t=[], rate=[]))
rate_state = dict(start=time.time(), last_poll=time.time(), bytes=0, n=0, file_id="?")
status = Div(text="Waiting for data...", styles={"color": "#ccc", "font-size": "16px"})

p_rate = styled("QUAX stream ingestion", "Time (s)", "Current throughput (MB/s)")
p_rate.line("t", "rate", source=rate_src, line_width=2, color="#4fc3f7")
p_rate.circle("t", "rate", source=rate_src, size=6, color="#4fc3f7")


def poll_stream():
    for msg in stream_consumer:
        rate_state["bytes"] += len(msg.value)
        rate_state["n"] += 1
        if msg.headers:
            rate_state["file_id"] = dict(msg.headers).get("file_id", b"?").decode()
    now = time.time()
    dt = now - rate_state["last_poll"]
    rate = (rate_state["bytes"] / MB) / dt if dt else 0
    rate_src.stream(dict(t=[now - rate_state["start"]], rate=[rate]), rollover=WINDOW)
    status.text = f"Chunks received: {rate_state['n']} | Current rate: {rate:.1f} MB/s | Latest: {rate_state['file_id']}"
    rate_state["bytes"], rate_state["last_poll"] = 0, now


# --- power spectrum: latest batch + cumulative average ---
latest_src = ColumnDataSource(dict(freq=[], value=[], lo=[], hi=[]))
cum_src = ColumnDataSource(dict(freq=[], value=[]))
cum_state = dict(n=0, sum=None, freq=None)
result_count = [0]

p_latest = styled("Latest batch — power spectrum", "Frequency (Hz)", "Power", y_axis_type="log")
p_latest.add_layout(Band(base="freq", lower="lo", upper="hi", source=latest_src,
                          fill_alpha=0.25, fill_color=Category10[3][0]))
p_latest.line("freq", "value", source=latest_src, line_width=2, color=Category10[3][0])

p_cum = styled("Cumulative run average", "Frequency (Hz)", "Power", y_axis_type="log")
p_cum.line("freq", "value", source=cum_src, line_width=2, color=Category10[3][1])


def poll_results():
    for msg in results_consumer:
        result_count[0] += 1
        d = msg.value["average"]
        freq, value, rms = d["frequency"], d["value"], d["rms"]
        latest_src.data = dict(freq=freq, value=value,
                                lo=[v - r for v, r in zip(value, rms)],
                                hi=[v + r for v, r in zip(value, rms)])
        cum_state["sum"] = value if cum_state["sum"] is None else [s + v for s, v in zip(cum_state["sum"], value)]
        cum_state["freq"], cum_state["n"] = freq, cum_state["n"] + 1
        cum_src.data = dict(freq=cum_state["freq"], value=[s / cum_state["n"] for s in cum_state["sum"]])


# --- cluster routing ---
node_counts = {host: 0 for host in NODES}
node_active = {host: 0 for host in NODES}
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


def poll():
    poll_stream()
    poll_results()
    poll_telemetry()


for host in NODES:
    render_node(host)

curdoc().add_periodic_callback(poll, 500)
curdoc().title = "QUAX Stream Monitor"
curdoc().add_root(column(
    status,
    p_rate,
    caption("Each point is one raw chunk landing from the DAQ producer, plotted as its arrival speed in MB/s. "
            "This is the pipeline's pulse — how fast IQ samples are streaming in right now, not physics yet."),
    p_latest,
    caption("Blue line: this batch's power spectrum — ~4096 scans FFT'd and averaged for one pair of DAQ files. "
            "Grey band: ±1 std-dev spread across those scans, i.e. where the noise lives. "
            "A real axion signal would show up as a steady bump poking above that noise."),
    p_cum,
    caption("The same spectrum, averaged over every batch since the run started. Random noise cancels out as more "
            "batches pile in, while any real steady feature keeps adding up — so this curve should get cleaner, "
            "and any true signal sharper, the longer you watch."),
    row(node_boxes["mapd-master"], node_boxes["mapd-master1"], node_boxes["mapd-master2"]),
))
