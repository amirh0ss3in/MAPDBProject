import os
import csv
import json
import time
import argparse
import urllib3
import boto3
import numpy as np
from kafka import KafkaConsumer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')
if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

parser = argparse.ArgumentParser()
parser.add_argument("--batches", type=int, default=100)
args = parser.parse_args()

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "load")
RESULTS_FILE = os.path.join(RESULTS_DIR, "baseline_sequential.csv")


# Same physics as the overhead benchmark - no engine, one batch at a time,
# no artificial delay, processed as fast as this single process can go.
def process_physics_data(work_order):
    s3 = boto3.client('s3',
        endpoint_url='https://cloud-areapd.pd.infn.it:5210',
        aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET, verify=False)
    obj_i = s3.get_object(Bucket='quax', Key=work_order['i_file'])
    data_i = np.frombuffer(obj_i['Body'].read(), dtype='<f4')
    obj_q = s3.get_object(Bucket='quax', Key=work_order['q_file'])
    data_q = np.frombuffer(obj_q['Body'].read(), dtype='<f4')
    signal = data_i + 1j * data_q
    spectra = np.fft.fftshift(np.fft.fft(signal.reshape(-1, 2048), axis=1), axes=1)
    power = np.abs(spectra) ** 2
    return {"batch_id": work_order['batch_id']}


if __name__ == "__main__":
    print(f"Load test: baseline-sequential, {args.batches} batches, uncapped, one at a time.")

    consumer = KafkaConsumer(
        'quax_raw', bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )

    results = []
    processed = 0
    print("Listening for work orders... (Run load_producer.py!)")

    while processed < args.batches:
        msg_pack = consumer.poll(timeout_ms=100)
        for tp, messages in msg_pack.items():
            for message in messages:
                if processed >= args.batches:
                    break
                work_order = message.value
                start = time.time()
                process_physics_data(work_order)
                end = time.time()
                results.append({
                    "config": "baseline_sequential",
                    "batch_id": work_order['batch_id'],
                    "send_time": work_order['send_time'],
                    "start_time": start,
                    "end_time": end,
                    "calc_time": round(end - start, 3),
                    "latency": round(end - work_order['send_time'], 3),
                })
                processed += 1
                if processed % 10 == 0:
                    print(f"[{processed}/{args.batches}] done")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["config", "batch_id", "send_time", "start_time",
                                               "end_time", "calc_time", "latency"])
        writer.writeheader()
        writer.writerows(results)

    total_span = results[-1]["end_time"] - results[0]["start_time"]
    print(f"\nDONE. {len(results)} batches in {total_span:.2f}s => {len(results)/total_span:.2f} batches/s")
    print(f"Results saved to: {RESULTS_FILE}")
