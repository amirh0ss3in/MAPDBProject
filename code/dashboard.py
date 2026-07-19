#!/usr/bin/env python3
"""Live QUAX ingestion + spectrum + cluster-routing monitor. Run with: bokeh serve --show dashboard.py"""
import json, time, urllib.request, urllib.error
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, Band, Div
from bokeh.layouts import column
from bokeh.palettes import Category10

MB = 1024 * 1024
WINDOW = 100
NODES = {"mapd-master": "Master", "mapd-master1": "Worker 1", "mapd-master2": "Worker 2"}
# Spark's master JSON API reports workers by IP, not hostname, so routing telemetry
# (keyed by socket.gethostname()) and pulled cluster health (keyed by IP) need this bridge.
WORKER_IP = {"mapd-master1": "10.67.22.246", "mapd-master2": "10.67.22.248"}
SPARK_MASTER_API = "http://master:8080/json/"
# Must match streaming_job.py's KafkaConsumer(group_id=...) on quax_stream.
CONSUMER_GROUP = "quax-processor"
STREAM_TP = TopicPartition("quax_stream", 0)

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
kafka_rates = {"quax_stream": 0.0, "quax_telemetry": 0.0, "quax_results": 0.0}

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
    kafka_rates["quax_stream"] = rate
    rate_state["bytes"], rate_state["last_poll"] = 0, now


# --- processing backlog: is Spark keeping pace with the producer, or falling behind? ---
# p_rate above measures producer->Kafka arrival rate; it stays a healthy-looking number
# even while Spark falls hopelessly behind, since it never looks at whether anything got
# consumed. Backlog = (latest quax_stream offset) - (streaming_job's committed offset) is
# the one number that actually answers "can the pipeline withstand this throughput".
backlog_src = ColumnDataSource(dict(t=[], backlog=[]))
backlog_state = dict(start=time.time())

p_backlog = styled("Processing backlog", "Time (s)", "Chunks behind")
p_backlog.line("t", "backlog", source=backlog_src, line_width=2, color="#ff6b6b")
p_backlog.circle("t", "backlog", source=backlog_src, size=6, color="#ff6b6b")


def poll_backlog():
    try:
        # A fresh AdminClient each tick mirrors poll_cluster_health's KafkaProducer probe:
        # both eagerly connect/probe API versions at construction, so building one once at
        # import time would crash dashboard.py's startup if Kafka isn't up yet.
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092", api_version_auto_timeout_ms=1000)
        try:
            end_offset = stream_consumer.end_offsets([STREAM_TP])[STREAM_TP]
            committed = admin.list_consumer_group_offsets(group_id=CONSUMER_GROUP)
            processed = committed[STREAM_TP].offset if STREAM_TP in committed else end_offset
            lag = max(0, end_offset - processed)
        finally:
            admin.close()
    except NoBrokersAvailable:
        return
    backlog_src.stream(dict(t=[time.time() - backlog_state["start"]], backlog=[lag]), rollover=WINDOW)


# --- power spectrum: latest batch + cumulative average ---
latest_src = ColumnDataSource(dict(freq=[], value=[], lo=[], hi=[]))
cum_src = ColumnDataSource(dict(freq=[], value=[]))
cum_state = dict(n=0, sum=None, freq=None)
result_count = [0]
results_rate_state = dict(last=time.time(), n=0)

p_latest = styled("Latest batch — power spectrum", "Frequency (Hz)", "Power", y_axis_type="log")
p_latest.add_layout(Band(base="freq", lower="lo", upper="hi", source=latest_src,
                          fill_alpha=0.25, fill_color=Category10[3][0]))
p_latest.line("freq", "value", source=latest_src, line_width=2, color=Category10[3][0])

p_cum = styled("Cumulative run average", "Frequency (Hz)", "Power", y_axis_type="log")
p_cum.line("freq", "value", source=cum_src, line_width=2, color=Category10[3][1])


def poll_results():
    for msg in results_consumer:
        result_count[0] += 1
        results_rate_state["n"] += 1
        d = msg.value["average"]
        freq, value, rms = d["frequency"], d["value"], d["rms"]
        latest_src.data = dict(freq=freq, value=value,
                                lo=[v - r for v, r in zip(value, rms)],
                                hi=[v + r for v, r in zip(value, rms)])
        cum_state["sum"] = value if cum_state["sum"] is None else [s + v for s, v in zip(cum_state["sum"], value)]
        cum_state["freq"], cum_state["n"] = freq, cum_state["n"] + 1
        cum_src.data = dict(freq=cum_state["freq"], value=[s / cum_state["n"] for s in cum_state["sum"]])
    now = time.time()
    dt = now - results_rate_state["last"]
    kafka_rates["quax_results"] = results_rate_state["n"] / dt if dt else 0
    results_rate_state["n"], results_rate_state["last"] = 0, now


# --- cluster health (pulled: Spark master JSON API + Kafka connection state) ---
cluster_health = {"kafka_up": False, "spark_up": False, "spark_status": "?", "workers": {}}


def poll_cluster_health():
    # bootstrap_connected() is a false-negative trap: kafka-python drops the seed
    # connection once it's discovered real brokers via metadata, so it reads "down"
    # even mid-stream. A short-lived probe (same pattern producer.py's ensure_kafka
    # already relies on) is what actually reflects broker reachability.
    try:
        KafkaProducer(bootstrap_servers="localhost:9092", api_version_auto_timeout_ms=1000).close()
        cluster_health["kafka_up"] = True
    except NoBrokersAvailable:
        cluster_health["kafka_up"] = False
    try:
        with urllib.request.urlopen(SPARK_MASTER_API, timeout=1) as r:
            info = json.loads(r.read())
        cluster_health["spark_up"] = True
        cluster_health["spark_status"] = info.get("status", "?")
        # De-dupe by IP: a crashed run can leave orphaned Worker daemons registered
        # alongside a fresh one, which would otherwise look like extra live workers.
        cluster_health["workers"] = {w["host"]: w for w in info.get("workers", [])}
    except (urllib.error.URLError, OSError, ValueError):
        cluster_health["spark_up"] = False
        cluster_health["workers"] = {}


# --- cluster routing (pushed: per-chunk host attribution from the Spark job) ---
node_counts = {host: 0 for host in NODES}
node_active = {host: 0 for host in NODES}
telemetry_rate_state = dict(last=time.time(), n=0)


def poll_telemetry():
    for host in node_active:
        node_active[host] = max(0, node_active[host] - 1)
    for msg in telemetry_consumer:
        telemetry_rate_state["n"] += 1
        host = msg.value["host"]
        if host in node_counts:
            node_counts[host] += 1
            node_active[host] = 3
    now = time.time()
    dt = now - telemetry_rate_state["last"]
    kafka_rates["quax_telemetry"] = telemetry_rate_state["n"] / dt if dt else 0
    telemetry_rate_state["n"], telemetry_rate_state["last"] = 0, now


# --- pipeline topology: one live diagram. Master is a single physical box (it hosts
# the Kafka broker, the Spark driver, and this dashboard process) with edges fanning
# out to the two worker machines — matching the real 3-machine layout, not a 5-hop chain. ---
TOPO_W, TOPO_H = 900, 300
dash_state = {"phase": 0}

ARROW_DEFS = """<defs>
  <marker id="arr-blue" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#4fc3f7"/></marker>
  <marker id="arr-green" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#2ecc71"/></marker>
  <marker id="arr-gray" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#3a3f4a"/></marker>
  <marker id="arr-red" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#e05c5c"/></marker>
</defs>"""

topology_box = Div(width=TOPO_W, height=TOPO_H + 10)


def edge_line(x1, y1, x2, y2, color, marker, width, flowing):
    dash = f'stroke-dasharray="9 7" stroke-dashoffset="{-dash_state["phase"]}"' if flowing else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{width}" marker-end="url(#arr-{marker})" {dash}/>')


def worker_box(x, y, w_px, h_px, host, label):
    w = cluster_health["workers"].get(WORKER_IP[host])
    alive = w is not None and w.get("state") == "ALIVE"
    color = "#2ecc71" if alive else "#e05c5c"
    detail = f"{w['coresused']}/{w['cores']} cores &middot; {w['memoryused']}/{w['memory']}MB" if alive else "unreachable"
    title = (f"{label} ({WORKER_IP[host]}): ALIVE, {w['coresused']}/{w['cores']} cores used, "
             f"{w['memoryused']}/{w['memory']}MB used" if alive else
             f"{label} ({WORKER_IP[host]}): not registered with Spark master")
    return (f'<div style="position:absolute;left:{x}px;top:{y}px;width:{w_px}px;height:{h_px}px;'
            f'border:2px solid {color};border-radius:12px;background:#111318;color:#ccc;'
            f'font-size:13px;text-align:center;padding:10px;box-sizing:border-box" title="{title}">'
            f'<b>{label}</b><br><span style="color:{color}">● {"ALIVE" if alive else "unreachable"}</span><br>'
            f'<span style="font-size:12px">{detail}</span><br>'
            f'<span style="font-size:12px">chunks processed: {node_counts[host]}</span></div>')


def worker_edge(host, x1, y1, x2, y2):
    w = cluster_health["workers"].get(WORKER_IP[host])
    alive = w is not None and w.get("state") == "ALIVE"
    if not alive:
        return edge_line(x1, y1, x2, y2, "#e05c5c", "red", 3, False)
    if node_active[host] > 0:
        return edge_line(x1, y1, x2, y2, "#2ecc71", "green", 4, True)
    return edge_line(x1, y1, x2, y2, "#3a3f4a", "gray", 2, False)


def render_topology():
    dash_state["phase"] = (dash_state["phase"] + 3) % 16

    stream_rate = kafka_rates["quax_stream"]
    s3_flowing = stream_rate > 0.02
    s3_color, s3_marker = ("#4fc3f7", "blue") if s3_flowing else ("#3a3f4a", "gray")
    s3_width = 2 + min(stream_rate, 8) * 1.2

    kafka_up = cluster_health["kafka_up"]
    spark_up = cluster_health["spark_up"] and cluster_health["spark_status"] == "ALIVE"
    kafka_color = "#2ecc71" if kafka_up else "#e05c5c"
    spark_color = "#2ecc71" if spark_up else "#e05c5c"
    master_border = "#2ecc71" if (kafka_up and spark_up) else "#e05c5c"

    s3_box = (
        '<div style="position:absolute;left:10px;top:105px;width:140px;height:90px;'
        'border:1px solid #2a2d34;border-radius:12px;background:#111318;color:#ccc;'
        'font-size:13px;text-align:center;padding:10px;box-sizing:border-box" '
        'title="Raw DAQ file pairs land in S3 before being streamed into Kafka.">'
        '<b>S3 bucket</b><br><span style="color:#8a8f98">raw DAQ files (source)</span></div>'
    )

    master_title = (f"master (Kafka broker + Spark driver + this dashboard process). "
                     f"Kafka: {'connected' if kafka_up else 'unreachable'}. Spark: {cluster_health['spark_status']}.")
    master_box = (
        f'<div style="position:absolute;left:260px;top:55px;width:260px;height:190px;'
        f'border:2px solid {master_border};border-radius:14px;background:#111318;color:#ccc;'
        f'font-size:13px;text-align:center;padding:12px;box-sizing:border-box" title="{master_title}">'
        f'<b>Master</b> <span style="color:#8a8f98;font-size:11px">Kafka + Spark driver</span><br>'
        f'<span style="color:{kafka_color}">● Kafka {"up" if kafka_up else "down"}</span><br>'
        f'<span style="font-size:11.5px">stream {stream_rate:.1f}MB/s &middot; '
        f'telemetry {kafka_rates["quax_telemetry"]:.1f}/s &middot; results {kafka_rates["quax_results"]:.1f}/s</span><br>'
        f'<span style="color:{spark_color}">● Spark {"ALIVE" if spark_up else "unreachable"}</span><br>'
        f'<span style="font-size:12px">results published: {result_count[0]}</span><br>'
        f'<span style="color:#4fc3f7;font-size:11px">◆ this dashboard runs here</span></div>'
    )

    w1_box = worker_box(650, 15, 230, 110, "mapd-master1", "Worker 1")
    w2_box = worker_box(650, 175, 230, 110, "mapd-master2", "Worker 2")

    edges = (
        edge_line(150, 150, 260, 150, s3_color, s3_marker, s3_width, s3_flowing)
        + worker_edge("mapd-master1", 520, 110, 650, 70)
        + worker_edge("mapd-master2", 520, 190, 650, 230)
    )

    topology_box.text = (
        f'<div style="position:relative;width:{TOPO_W}px;height:{TOPO_H}px">'
        f'<svg width="{TOPO_W}" height="{TOPO_H}" style="position:absolute;top:0;left:0">{ARROW_DEFS}{edges}</svg>'
        f'{s3_box}{master_box}{w1_box}{w2_box}</div>'
    )


def poll():
    poll_stream()
    poll_results()
    poll_telemetry()
    render_topology()


poll_cluster_health()
poll_backlog()
render_topology()

curdoc().add_periodic_callback(poll, 500)
curdoc().add_periodic_callback(poll_cluster_health, 2000)
curdoc().add_periodic_callback(poll_backlog, 2000)
curdoc().title = "QUAX Stream Monitor"
curdoc().add_root(column(
    status,
    p_rate,
    caption("Each point is one raw chunk landing from the DAQ producer, plotted as its arrival speed in MB/s. "
            "This is the pipeline's pulse — how fast IQ samples are streaming in right now, not physics yet. "
            "This measures the producer's send rate into Kafka, not whether Spark can keep up with it — "
            "see Processing backlog below for that."),
    p_backlog,
    caption("Chunks sent minus chunks Spark has confirmed processing (via streaming_job.py's committed Kafka "
            "offset). Healthy looks like a sawtooth — rising between micro-batches, then dropping back toward "
            "zero as each one completes, never climbing higher over time. A baseline that keeps climbing and "
            "stops returning to zero means chunks are piling up faster than Spark can drain them — the "
            "throughput above is too high to sustain."),
    p_latest,
    caption("Blue line: this batch's power spectrum — ~4096 scans FFT'd and averaged for one pair of DAQ files. "
            "Grey band: ±1 std-dev spread across those scans, i.e. where the noise lives. "
            "A real axion signal would show up as a steady bump poking above that noise."),
    p_cum,
    caption("The same spectrum, averaged over every batch since the run started. Random noise cancels out as more "
            "batches pile in, while any real steady feature keeps adding up — so this curve should get cleaner, "
            "and any true signal sharper, the longer you watch."),
    caption("Pipeline topology, live. Master is one physical box — it runs the Kafka broker, the Spark "
            "driver, and this dashboard — so its health/rates are pulled straight from Kafka's connection "
            "state and Spark's master API every 2s. Edges to the workers turn green and animate when that "
            "worker just did FFT work (pushed per-chunk telemetry); a worker missing from Spark's API turns "
            "its edge and box red. Hover any node for detail."),
    topology_box,
))
