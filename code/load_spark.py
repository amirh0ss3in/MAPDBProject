import os
import csv
import json
import time
import argparse
import urllib3
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')
if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

parser = argparse.ArgumentParser()
parser.add_argument("--batches", type=int, default=100)
parser.add_argument("--workers", type=int, default=8, help="driver-side thread pool size for concurrent job submission")
args = parser.parse_args()

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "load")
RESULTS_FILE = os.path.join(RESULTS_DIR, "spark.csv")


def process_physics_data(work_order):
    import os
    import boto3
    import numpy as np
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    access_key = os.environ.get('S3_ACCESS_KEY')
    secret_key = os.environ.get('S3_SECRET_KEY')
    s3 = boto3.client('s3',
        endpoint_url='https://cloud-areapd.pd.infn.it:5210',
        aws_access_key_id=access_key, aws_secret_access_key=secret_key, verify=False)
    obj_i = s3.get_object(Bucket='quax', Key=work_order['i_file'])
    data_i = np.frombuffer(obj_i['Body'].read(), dtype='<f4')
    obj_q = s3.get_object(Bucket='quax', Key=work_order['q_file'])
    data_q = np.frombuffer(obj_q['Body'].read(), dtype='<f4')
    signal = data_i + 1j * data_q
    spectra = np.fft.fftshift(np.fft.fft(signal.reshape(-1, 2048), axis=1), axes=1)
    power = np.abs(spectra) ** 2
    return work_order['batch_id']


def run_task(sc, work_order):
    start = time.time()
    rdd = sc.parallelize([work_order], numSlices=1)
    rdd.map(process_physics_data).collect()
    end = time.time()
    return work_order, start, end


if __name__ == "__main__":
    print(f"Load test: Spark (3-node Standalone, {args.workers} concurrent driver threads submitting jobs), "
          f"{args.batches} batches, uncapped.")

    spark = SparkSession.builder \
        .appName("QUAX-Spark-LoadTest") \
        .master("spark://master:7077") \
        .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
        .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
        .config("spark.pyspark.python", "/home/ubuntu/pyvenv/bin/python") \
        .config("spark.pyspark.driver.python", "/home/ubuntu/pyvenv/bin/python") \
        .getOrCreate()
    sc = spark.sparkContext
    print(f"Spark session ready! Executors: {len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1}")

    consumer = KafkaConsumer(
        'quax_raw', bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )

    results = []
    submitted = 0
    completed = 0
    print("Listening for work orders... (Run load_producer.py!)")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {}
        while completed < args.batches:
            if submitted < args.batches:
                msg_pack = consumer.poll(timeout_ms=100)
                for tp, messages in msg_pack.items():
                    for message in messages:
                        if submitted >= args.batches:
                            break
                        work_order = message.value
                        fut = pool.submit(run_task, sc, work_order)
                        pending[fut] = work_order
                        submitted += 1

            done_now = [f for f in list(pending.keys()) if f.done()]
            for fut in done_now:
                work_order, start, end = fut.result()
                results.append({
                    "config": "spark",
                    "batch_id": work_order['batch_id'],
                    "send_time": work_order['send_time'],
                    "start_time": start,
                    "end_time": end,
                    "calc_time": round(end - start, 3),
                    "latency": round(end - work_order['send_time'], 3),
                })
                del pending[fut]
                completed += 1
                if completed % 10 == 0:
                    print(f"[{completed}/{args.batches}] done")

            if not done_now and submitted >= args.batches:
                time.sleep(0.05)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["config", "batch_id", "send_time", "start_time",
                                               "end_time", "calc_time", "latency"])
        writer.writeheader()
        writer.writerows(sorted(results, key=lambda r: r["batch_id"]))

    total_span = max(r["end_time"] for r in results) - min(r["start_time"] for r in results)
    print(f"\nDONE. {len(results)} batches in {total_span:.2f}s => {len(results)/total_span:.2f} batches/s")
    print(f"Results saved to: {RESULTS_FILE}")

    spark.stop()
