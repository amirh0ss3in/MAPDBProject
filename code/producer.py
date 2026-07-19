#!/usr/bin/env python3
"""DAQ emulator: streams QUAX i/q pairs from S3 into Kafka in tunable-size chunks."""
import argparse, os, re, subprocess, threading, time
from queue import Queue
import boto3
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

FILE_SIZE = 32 * 1024 * 1024
BUCKET = "quax"
S3_ENDPOINT = "https://cloud-areapd.pd.infn.it:5210"
KAFKA_HOME = os.path.expanduser("~/kafka_2.13-3.7.0")


def list_pairs(s3):
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    idx = lambda k: re.search(r"(\d+)\.dat$", k).group(1)
    i_by_idx = {idx(k): k for k in keys if k.startswith("duck_i_")}
    q_by_idx = {idx(k): k for k in keys if k.startswith("duck_q_")}
    return [(i_by_idx[n], q_by_idx[n]) for n in sorted(i_by_idx)]


def prefetch(s3, pairs, n_pairs, q):
    sent, cycle = 0, 0
    while n_pairs is None or sent < n_pairs:
        for i_key, q_key in pairs:
            i_bytes = s3.get_object(Bucket=BUCKET, Key=i_key)["Body"].read()
            q_bytes = s3.get_object(Bucket=BUCKET, Key=q_key)["Body"].read()
            q.put((f"{i_key}#{cycle}", i_bytes, q_bytes))
            sent += 1
            if n_pairs is not None and sent >= n_pairs:
                break
        cycle += 1
    q.put(None)


def cap_quax_stream(bootstrap_servers):
    # quax_stream carries raw multi-MB chunks; without a retention cap it fills the disk
    # within a long/high-rate run since Kafka never expires data just because it's been read.
    subprocess.run(f"{KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server {bootstrap_servers} "
                   f"--create --if-not-exists --topic quax_stream --partitions 1 --replication-factor 1", shell=True)
    subprocess.run(f"{KAFKA_HOME}/bin/kafka-configs.sh --bootstrap-server {bootstrap_servers} --alter "
                   f"--entity-type topics --entity-name quax_stream --add-config "
                   f"retention.bytes=209715200,segment.bytes=67108864,retention.ms=600000", shell=True)


def ensure_kafka(bootstrap_servers):
    try:
        KafkaProducer(bootstrap_servers=bootstrap_servers, api_version_auto_timeout_ms=3000).close()
        cap_quax_stream(bootstrap_servers)
        return
    except NoBrokersAvailable:
        pass
    subprocess.run(f"{KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon {KAFKA_HOME}/config/zookeeper.properties", shell=True)
    time.sleep(5)
    subprocess.run(f"{KAFKA_HOME}/bin/kafka-server-start.sh -daemon {KAFKA_HOME}/config/server.properties", shell=True)
    for _ in range(30):
        try:
            KafkaProducer(bootstrap_servers=bootstrap_servers, api_version_auto_timeout_ms=3000).close()
            cap_quax_stream(bootstrap_servers)
            return
        except NoBrokersAvailable:
            time.sleep(2)
    raise RuntimeError("Kafka did not come up in time")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=1.0, help="multiplier on the real ~5s/pair DAQ cadence")
    ap.add_argument("--chunk-mb", type=float, default=8.0, help="per-channel chunk size sent per Kafka message")
    ap.add_argument("--n-pairs", type=int, default=None)
    ap.add_argument("--topic", default="quax_stream")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    args = ap.parse_args()
    ensure_kafka(args.bootstrap_servers)

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                       aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                       aws_secret_access_key=os.environ["S3_SECRET_KEY"])
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, max_request_size=41943040, buffer_memory=83886080)
    q = Queue(maxsize=2)
    threading.Thread(target=prefetch, args=(s3, list_pairs(s3), args.n_pairs, q), daemon=True).start()

    chunk = int(args.chunk_mb * 1024 * 1024)
    n_chunks = -(-FILE_SIZE // chunk)
    interval = (5.0 / args.rate) / n_chunks
    while (item := q.get()) is not None:
        file_id, i_bytes, q_bytes = item
        for chunk_idx in range(n_chunks):
            sl = slice(chunk_idx * chunk, (chunk_idx + 1) * chunk)
            producer.send(args.topic, key=file_id.encode(), value=i_bytes[sl] + q_bytes[sl],
                          headers=[("file_id", file_id.encode()), ("chunk_idx", str(chunk_idx).encode())]).get(timeout=30)
            time.sleep(interval)
    producer.flush()


if __name__ == "__main__":
    main()
