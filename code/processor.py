import os
import json
import time
import urllib3
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from dask.distributed import Client, SSHCluster, wait

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Pull keys securely from Linux environment variables
S3_ACCESS = os.environ.get('S3_ACCESS_KEY')
S3_SECRET = os.environ.get('S3_SECRET_KEY')

if not S3_ACCESS or not S3_SECRET:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

# =================================================================
# WORKER FUNCTION (Runs remotely)
# =================================================================
def process_physics_data(work_order):
    import os
    import boto3
    import numpy as np
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Workers pull credentials securely from their own injected environment
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
    
    power = np.abs(spectra)**2
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
# MAIN ASYNC LOOP (Runs on Master)
# =================================================================
if __name__ == "__main__":
    print("Starting Dask Cluster...")
    
    # Securely inject environment variables into all workers
    worker_env = {
        "S3_ACCESS_KEY": S3_ACCESS,
        "S3_SECRET_KEY": S3_SECRET
    }
    
    cluster = SSHCluster(
        ["master", "worker1", "worker2"],
        connect_options={"known_hosts": None},
        scheduler_options={"port": 8786, "dashboard_address": ":8797"},
        worker_options={"env": worker_env}  # <-- Secure injection
    )
    client = Client(cluster)
    print(f"Cluster connected with {len(client.scheduler_info()['workers'])} workers!")

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
    
    active_futures = []

    try:
        while True:
            # 1. Grab any new messages from Kafka (Non-blocking)
            msg_pack = consumer.poll(timeout_ms=100)
            
            for tp, messages in msg_pack.items():
                for message in messages:
                    work_order = message.value
                    print(f"[{time.strftime('%H:%M:%S')}] Pushing Batch {work_order['batch_id']} to cluster...")
                    
                    # Submit task and instantly keep moving
                    future = client.submit(process_physics_data, work_order)
                    active_futures.append({
                        "future": future, 
                        "batch": work_order['batch_id'], 
                        "start": time.time()
                    })

            # 2. Check the status of active tasks
            # We loop backwards so we can safely remove finished tasks from the list
            for i in range(len(active_futures) - 1, -1, -1):
                task = active_futures[i]
                f = task["future"]
                
                if f.status == 'finished':
                    result_json = f.result()
                    calc_time = time.time() - task["start"]
                    result_json['calc_time'] = round(calc_time, 2)
                    print(f"   -> Batch {task['batch']} math finished in {calc_time:.2f}s! Pushing to Kafka...")
                    
                    producer.send('quax_processed', value=result_json)
                    producer.flush()
                    active_futures.pop(i)
                    
                elif f.status == 'error':
                    print(f"   -> ERROR on Batch {task['batch']}: {f.exception()}")
                    active_futures.pop(i)

    except KeyboardInterrupt:
        print("\nShutting down processor...")
