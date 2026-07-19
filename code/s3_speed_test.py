#!/usr/bin/env python3
"""One-off: measure S3 download throughput and enumerate dataset size."""
import os, time
import boto3
from producer import BUCKET, S3_ENDPOINT, list_pairs

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                   aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                   aws_secret_access_key=os.environ["S3_SECRET_KEY"])
pairs = list_pairs(s3)
print(f"pairs in bucket: {len(pairs)}", flush=True)

for trial in range(2):
    t0 = time.time()
    b = s3.get_object(Bucket=BUCKET, Key=pairs[trial][0])["Body"].read()
    dt = time.time() - t0
    print(f"trial {trial}: {len(b)/1e6:.0f}MB in {dt:.2f}s = {len(b)/1e6/dt:.1f} MB/s", flush=True)
