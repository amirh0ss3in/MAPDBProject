import time
import json
from kafka import KafkaProducer

# 1. Connect to the Kafka Server
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Starting QUAX Data Stream Emulator...")
print("Press Ctrl+C to stop.\n")

# 2. Loop through the 31 file pairs (00000 to 00030)
try:
    for i in range(31):
        # Format the number to have 5 digits (e.g. 00004)
        file_idx = f"{i:05d}"
        
        i_filename = f"duck_i_{file_idx}.dat"
        q_filename = f"duck_q_{file_idx}.dat"
        
        # Create the message payload
        work_order = {
            "batch_id": i,
            "i_file": i_filename,
            "q_file": q_filename
        }
        
        # 3. Send the message to the 'quax_raw' topic
        print(f"[{time.strftime('%H:%M:%S')}] Emitting -> {i_filename} & {q_filename}")
        producer.send('quax_raw', value=work_order)
        
        # Flush ensures it sends immediately
        producer.flush() 
        
        # 4. Sleep for 5 seconds to simulate the real QUAX hardware speed
        time.sleep(5)
        
except KeyboardInterrupt:
    print("\nStream stopped by user.")

print("\nFinished sending all data!")
