import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer

st.set_page_config(page_title="QUAX Monitor", layout="wide")
st.title("📡 QUAX Experiment: Live Data Monitor")

# Create placeholders on the web page that we will overwrite
status_text = st.empty()
chart_placeholder = st.empty()

# Connect to the processed Kafka topic
consumer = KafkaConsumer(
    'quax_processed',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

status_text.info("Listening for processed data... (Waiting for processor.py)")

# Continuously read from Kafka and update the graph
for message in consumer:
    data = message.value
    batch_id = data['batch_id']
    
    status_text.success(f"✅ Currently displaying: Batch {batch_id}")
    
    # Format the data for the chart
    df = pd.DataFrame({
        'Frequency (MHz)': data['frequency'],
        'Power Spectrum': data['average']
    })
    
    # Draw the line chart (X-axis = Frequency, Y-axis = Power)
    chart_placeholder.line_chart(df.set_index('Frequency (MHz)'))
