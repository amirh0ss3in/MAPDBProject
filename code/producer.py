import time
import json
import argparse
from kafka import KafkaProducer

# There are only 31 real I/Q file pairs in the S3 bucket (00000-00030).
# For benchmark/load-test use, batches beyond 31 cycle back through the
# same real files with a fresh batch_id - still real physics data, just reused.
N_FILES = 31

parser = argparse.ArgumentParser()
parser.add_argument("--batches", type=int, default=31, help="number of batches to send")
parser.add_argument("--interval", type=float, default=5.0, help="seconds to sleep between batches")
args = parser.parse_args()

# 1. Connect to the Kafka Server
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Starting QUAX Data Stream Emulator...")
print(f"Sending {args.batches} batches, {args.interval}s apart.")
print("Press Ctrl+C to stop.\n")

try:
    for i in range(args.batches):
        # Format the number to have 5 digits (e.g. 00004), cycling through the 31 real files
        file_idx = f"{i % N_FILES:05d}"

        i_filename = f"duck_i_{file_idx}.dat"
        q_filename = f"duck_q_{file_idx}.dat"

        # Create the message payload
        work_order = {
            "batch_id": i,
            "i_file": i_filename,
            "q_file": q_filename
        }

        # 2. Send the message to the 'quax_raw' topic
        print(f"[{time.strftime('%H:%M:%S')}] Emitting -> {i_filename} & {q_filename}")
        producer.send('quax_raw', value=work_order)

        # Flush ensures it sends immediately
        producer.flush()

        if args.interval > 0:
            time.sleep(args.interval)

except KeyboardInterrupt:
    print("\nStream stopped by user.")

print("\nFinished sending all data!")
