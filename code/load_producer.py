import time
import json
import argparse
from kafka import KafkaProducer

# Only 31 real I/Q file pairs exist in the S3 bucket (00000-00030).
# For a load test we cycle through them with fresh batch_ids - still real
# physics data reprocessed, no synthetic data needed.
N_FILES = 31

parser = argparse.ArgumentParser()
parser.add_argument("--batches", type=int, default=100, help="number of batches to fire")
parser.add_argument("--rate", type=float, default=0.0, help="batches/sec cap; 0 = fire as fast as possible")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(f"Load producer: firing {args.batches} batches"
      + (f" at {args.rate}/s" if args.rate > 0 else " uncapped (as fast as possible)"))

start = time.time()
for i in range(args.batches):
    file_idx = f"{i % N_FILES:05d}"
    work_order = {
        "batch_id": i,
        "i_file": f"duck_i_{file_idx}.dat",
        "q_file": f"duck_q_{file_idx}.dat",
        "send_time": time.time(),
    }
    producer.send('quax_raw', value=work_order)
    if args.rate > 0:
        time.sleep(1.0 / args.rate)

producer.flush()
elapsed = time.time() - start
print(f"Sent {args.batches} batches in {elapsed:.2f}s ({args.batches/elapsed:.1f} batches/s send rate)")
