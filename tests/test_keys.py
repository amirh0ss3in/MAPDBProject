import os
import boto3
import urllib3

# Suppress the SSL warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. Check if the environment variables exist
access_key = os.environ.get('S3_ACCESS_KEY')
secret_key = os.environ.get('S3_SECRET_KEY')

if not access_key or not secret_key:
    print("❌ ERROR: Python cannot find the S3_ACCESS_KEY or S3_SECRET_KEY environment variables!")
    exit(1)

print(f"✅ Found Access Key starting with: {access_key[:5]}...")

# 2. Try to unlock the bucket
print("Testing connection to CloudVeneto S3...")
try:
    s3 = boto3.client('s3',
        endpoint_url='https://cloud-areapd.pd.infn.it:5210',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    
    # Just try to grab the name of a single file to prove we have access
    response = s3.list_objects_v2(Bucket='quax', MaxKeys=1)
    
    if 'Contents' in response:
        print("✅ SUCCESS! The keys work flawlessly. You are ready to process data.")
    else:
        print("❓ Connected, but the bucket seems empty?")
        
except Exception as e:
    print(f"❌ FAILED! Connection refused. Error: {e}")
