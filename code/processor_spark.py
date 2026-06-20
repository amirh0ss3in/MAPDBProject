import os
import json
import time
import urllib3
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Pull keys securely from environment variables (same as the Dask version)
S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')

if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

# =================================================================
# WORKER FUNCTION (runs on a Spark executor)
# This is IDENTICAL to the Dask version - the physics does not change.
# =================================================================
def process_physics_data(work_order):
    import os
    import boto3
    import numpy as np
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Executors pull credentials from their own injected environment
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

# =================================================================
# MAIN LOOP (runs on the driver)
# =================================================================
if __name__ == "__main__":
    print("Starting Spark Session...")

    # Inject the S3 keys into every executor.
    # This is the Spark equivalent of Dask's worker_options={"env": worker_env}
    spark = SparkSession.builder \
        .appName("QUAX-Spark-Processor") \
        .config("spark.executorEnv.S3_ACCESS_KEY", S3_ACCESS) \
        .config("spark.executorEnv.S3_SECRET_KEY", S3_SECRET) \
        .getOrCreate()
    sc = spark.sparkContext
    print("Spark session ready!")

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

    print("Listening for work orders... (Run producer.py!)")

    try:
        while True:
            # 1. Grab any new messages from Kafka (non-blocking)
            msg_pack = consumer.poll(timeout_ms=100)

            for tp, messages in msg_pack.items():
                for message in messages:
                    work_order = message.value
                    print(f"[{time.strftime('%H:%M:%S')}] Sending Batch {work_order['batch_id']} to Spark...")

                    start = time.time()

                    # 2. Hand this batch to Spark: it ships the work_order to an
                    #    executor, runs process_physics_data there, brings result back.
                    rdd = sc.parallelize([work_order], numSlices=1)
                    result_json = rdd.map(process_physics_data).collect()[0]

                    calc_time = time.time() - start
                    result_json['calc_time'] = round(calc_time, 2)
                    print(f"   -> Batch {work_order['batch_id']} done in {calc_time:.2f}s! Pushing to Kafka...")

                    # 3. Send the result back to Kafka
                    producer.send('quax_processed', value=result_json)
                    producer.flush()

    except KeyboardInterrupt:
        print("\nShutting down processor...")
        spark.stop()
