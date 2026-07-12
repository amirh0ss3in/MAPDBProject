import os
import csv
import json
import time
import urllib3
import psutil
import boto3
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')

if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

# How many batches to process before stopping the benchmark
N_BATCHES = 20
# Where to save the benchmark results
RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "benchmark_baseline_results.csv")


# =================================================================
# NO ENGINE AT ALL - runs in-process on the master node.
# Byte-for-byte the same physics as the Dask/Spark worker functions,
# so this measures the engines' overhead, not a different algorithm.
# =================================================================
def process_physics_data(work_order):
    s3 = boto3.client('s3',
        endpoint_url='https://cloud-areapd.pd.infn.it:5210',
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
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
    print("Starting Baseline benchmark (BENCHMARK mode, no distribution engine)...")

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
                    result_json = process_physics_data(work_order)
                    calc_time = time.time() - start

                    # --- read resource usage AFTER processing ---
                    cpu_after = psutil.cpu_percent(interval=None)
                    mem_after = psutil.virtual_memory().percent

                    result_json['calc_time'] = round(calc_time, 2)
                    producer.send('quax_processed', value=result_json)
                    producer.flush()

                    results.append({
                        "engine": "baseline",
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

    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["engine", "batch_id", "order",
                                               "calc_time", "cpu_percent", "mem_percent"])
        writer.writeheader()
        writer.writerows(results)

    print("\n========== BENCHMARK DONE ==========")
    print(f"Engine:          Baseline (no distribution)")
    print(f"Batches:         {len(results)}")
    print(f"Total time:      {total_time:.2f}s")
    if results:
        times = [r['calc_time'] for r in results]
        print(f"First batch:     {times[0]:.2f}s (cold start)")
        if len(times) > 1:
            warm = times[1:]
            print(f"Avg after first: {sum(warm)/len(warm):.2f}s")
    print(f"Results saved to: {RESULTS_FILE}")
