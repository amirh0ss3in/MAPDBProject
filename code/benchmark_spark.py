import os
import csv
import json
import time
import argparse
import urllib3
import psutil
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')

if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

parser = argparse.ArgumentParser()
parser.add_argument("--trial", type=int, default=1, help="trial number, for repeated-trial statistics")
args = parser.parse_args()

# How many batches to process before stopping the benchmark
N_BATCHES = 20
# Where to save the benchmark results (one file per trial, in results/overhead/)
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "overhead")
RESULTS_FILE = os.path.join(RESULTS_DIR, f"spark_trial{args.trial}.csv")


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
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )

    obj_i = s3.get_object(Bucket='quax', Key=work_order['i_file'])
    data_i = np.frombuffer(obj_i['Body'].read(), dtype='<f4')

    obj_q = s3.get_object(Bucket='quax', Key=work_order['q_file'])
    data_q = np.frombuffer(obj_q['Body'].read(), dtype='<f4')

    signal = data_i + 1j * data_q
    signal_2d = signal.reshape(-1, 2048)

    spectra = np.fft.fftshift(np.fft.fft(signal_2d, axis=1), axes=1)

    power = np.abs(spectra) ** 2
    avg_power = np.mean(power, axis=0)
    std_power = np.std(power, axis=0)

    freqs = np.linspace(-1, 1, 2048)

    return {
        "batch_id": work_order['batch_id'],
        "frequency": freqs.tolist(),
        "average": avg_power.tolist(),
        "std": std_power.tolist()
    }


if __name__ == "__main__":
    print("Starting Spark Session (BENCHMARK mode, 3-node Standalone cluster)...")

    # Connect to the Spark Standalone cluster (master + worker1 + worker2),
    # matching the Dask benchmark's 3-node SSHCluster for a fair comparison.
    spark = SparkSession.builder \
        .appName("QUAX-Spark-Benchmark") \
        .master("spark://master:7077") \
        .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
        .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
        .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", "")) \
        .config("spark.pyspark.python", "/home/ubuntu/pyvenv/bin/python") \
        .config("spark.pyspark.driver.python", "/home/ubuntu/pyvenv/bin/python") \
        .getOrCreate()
    sc = spark.sparkContext
    print(f"Spark session ready! Executors: {len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1}")

    consumer = KafkaConsumer(
        'quax_raw',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    # Prepare the results file with a header row
    results = []
    print(f"Benchmark will stop after {N_BATCHES} batches.")
    print("Listening for work orders... (Run producer.py!)")

    processed = 0
    benchmark_start = time.time()

    try:
        while processed < N_BATCHES:
            msg_pack = consumer.poll(timeout_ms=100)

            for tp, messages in msg_pack.items():
                for message in messages:
                    if processed >= N_BATCHES:
                        break
                    work_order = message.value

                    # --- read resource usage BEFORE processing ---
                    cpu_before = psutil.cpu_percent(interval=None)
                    mem_before = psutil.virtual_memory().percent

                    start = time.time()
                    rdd = sc.parallelize([work_order], numSlices=1)
                    result_json = rdd.map(process_physics_data).collect()[0]
                    calc_time = time.time() - start

                    # --- read resource usage AFTER processing ---
                    cpu_after = psutil.cpu_percent(interval=None)
                    mem_after = psutil.virtual_memory().percent

                    result_json['calc_time'] = round(calc_time, 2)
                    producer.send('quax_processed', value=result_json)
                    producer.flush()

                    # save one row of benchmark data for this batch
                    results.append({
                        "engine": "spark",
                        "trial": args.trial,
                        "batch_id": work_order['batch_id'],
                        "order": processed,
                        "calc_time": round(calc_time, 3),
                        "cpu_percent": round(max(cpu_before, cpu_after), 1),
                        "mem_percent": round(max(mem_before, mem_after), 1),
                    })

                    processed += 1
                    print(f"[{processed}/{N_BATCHES}] Batch {work_order['batch_id']} "
                          f"done in {calc_time:.2f}s "
                          f"(CPU {max(cpu_before, cpu_after):.0f}%, "
                          f"MEM {max(mem_before, mem_after):.0f}%)")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    total_time = time.time() - benchmark_start

    # write all results to a CSV file
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["engine", "trial", "batch_id", "order",
                                               "calc_time", "cpu_percent", "mem_percent"])
        writer.writeheader()
        writer.writerows(results)

    print("\n========== BENCHMARK DONE ==========")
    print(f"Engine:          Spark")
    print(f"Trial:           {args.trial}")
    print(f"Batches:         {len(results)}")
    print(f"Total time:      {total_time:.2f}s")
    if results:
        times = [r['calc_time'] for r in results]
        print(f"First batch:     {times[0]:.2f}s (cold start)")
        if len(times) > 1:
            warm = times[1:]
            print(f"Avg after first: {sum(warm)/len(warm):.2f}s")
    print(f"Results saved to: {RESULTS_FILE}")

    spark.stop()
