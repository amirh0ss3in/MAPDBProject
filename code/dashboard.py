import streamlit as st
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import json

st.set_page_config(page_title="QUAX Monitor", layout="wide")

st.markdown("""
<style>
[data-testid="metric-container"] { background-color: #1a1a2e; padding: 15px; border-radius: 10px; border: 1px solid #00ff88; }
</style>
""", unsafe_allow_html=True)

st.title("🔭 QUAX Experiment: Live Signal Monitor")

col1, col2, col3, col4 = st.columns(4)
batches_metric = col1.empty()
progress_metric = col2.empty()
peak_metric = col3.empty()
speed_metric = col4.empty()

alert_placeholder = st.empty()

st.markdown("### 📊 Frequency Spectrum")
chart_placeholder = st.empty()

st.markdown("### ⏱ Processing Time per Batch")
time_chart_placeholder = st.empty()

st.markdown("### 📋 Overall Statistics")
stats_placeholder = st.empty()

st.markdown("### 📜 Batch History")
history_placeholder = st.empty()

consumer = KafkaConsumer(
    'quax_processed',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

TOTAL_BATCHES = 31
seen_batches = set()
times = []
history = []

for message in consumer:
    data = message.value
    batch_id = data['batch_id']

    if batch_id in seen_batches:
        continue
    seen_batches.add(batch_id)

    calc_time = data.get('calc_time', 0)
    if calc_time > 0:
        times.append(calc_time)

    freqs = data['frequency']
    avg = data['average']
    peak_idx = int(np.argmax(avg))
    peak_freq = round(freqs[peak_idx], 6)
    peak_power = round(avg[peak_idx], 4)
    batch_count = len(seen_batches)

    batches_metric.metric("Batches Processed", f"{batch_count}/{TOTAL_BATCHES}")
    progress_metric.metric("Progress", f"{round(batch_count/TOTAL_BATCHES*100)}%")
    peak_metric.metric("Peak Frequency", peak_freq)
    speed_metric.metric("Processing Time", f"{calc_time}s" if calc_time > 0 else "N/A")

    if abs(peak_freq) > 0.01:
        alert_placeholder.error(f"⚠️ Unusual signal detected! Peak at {peak_freq}")
    else:
        alert_placeholder.success(f"✅ Signal normal - Batch {batch_id}")

    df = pd.DataFrame({
        'Average Power': avg,
        'Std Dev': data['std']
    }, index=freqs)
    chart_placeholder.line_chart(df, color=["#00ff88", "#ff6600"])

    history.append({'Batch': batch_id, 'Time (s)': calc_time, 'Peak Freq': peak_freq, 'Peak Power': peak_power})
    history_df = pd.DataFrame(history)

    if len(times) > 0:
        time_chart_placeholder.line_chart(
            history_df.set_index('Batch')['Time (s)'],
            color=["#ffaa00"]
        )
        stats_placeholder.table(pd.DataFrame({
            'Metric': ['Fastest', 'Slowest', 'Average'],
            'Value': [f"{min(times)}s", f"{max(times)}s", f"{round(sum(times)/len(times), 2)}s"]
        }))

    history_placeholder.dataframe(history_df, use_container_width=True)

    if batch_count >= TOTAL_BATCHES:
        alert_placeholder.success("🎉 All batches processed successfully!")
        break
