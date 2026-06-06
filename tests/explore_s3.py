import os
import boto3
import urllib3

# Suppress the annoying SSL warnings since verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Pull keys securely from the Linux environment
access_key = os.environ.get('S3_ACCESS_KEY')
secret_key = os.environ.get('S3_SECRET_KEY')

if not access_key or not secret_key:
    raise ValueError("ERROR: S3 keys not found in environment variables!")

# Connect to the CloudVeneto S3 Bucket
s3_client = boto3.client('s3',
    endpoint_url='https://cloud-areapd.pd.infn.it:5210',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key, 
    verify=False
)

print("Connected! Peeking inside the 'quax' bucket...")

# Get the list of files
response = s3_client.list_objects_v2(Bucket='quax')
files = response.get('Contents', [])

print(f"Found {len(files)} files total.")
print("Here are the first 10 files:")

for obj in files[:10]:
    print(f" - {obj['Key']} ({obj['Size'] / 1024 / 1024:.2f} MB)")