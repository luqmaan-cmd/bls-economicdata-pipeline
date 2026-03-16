import os
import glob
from google.cloud import storage
from datetime import datetime
from .config import GCS_CONFIG

def get_gcs_client():
    creds_path = os.path.abspath(GCS_CONFIG["credentials_path"])
    return storage.Client.from_service_account_json(creds_path)

def upload_file(local_path, bucket_name=None, gcs_prefix="raw"):
    bucket_name = bucket_name or GCS_CONFIG["bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    filename = os.path.basename(local_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_path = f"{gcs_prefix}/{timestamp}/{filename}"
    
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    
    print(f"Uploaded {local_path} -> gs://{bucket_name}/{gcs_path}")
    return f"gs://{bucket_name}/{gcs_path}"

def upload_directory(local_dir, bucket_name=None, gcs_prefix="raw", pattern="*.csv"):
    bucket_name = bucket_name or GCS_CONFIG["bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uploaded = []
    
    for local_path in glob.glob(os.path.join(local_dir, pattern)):
        filename = os.path.basename(local_path)
        gcs_path = f"{gcs_prefix}/{timestamp}/{filename}"
        
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        print(f"Uploaded {filename} -> {gcs_uri}")
        uploaded.append(gcs_uri)
    
    return uploaded

def list_bucket_files(bucket_name=None, prefix=""):
    bucket_name = bucket_name or GCS_CONFIG["bucket"]
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    blobs = client.list_blobs(bucket, prefix=prefix)
    return [blob.name for blob in blobs]

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upload files to GCS")
    parser.add_argument("--file", "-f", help="Single file to upload")
    parser.add_argument("--dir", "-d", help="Directory to upload")
    parser.add_argument("--list", "-l", action="store_true", help="List bucket files")
    parser.add_argument("--prefix", "-p", default="", help="GCS prefix for listing")
    args = parser.parse_args()
    
    if args.file:
        upload_file(args.file)
    elif args.dir:
        upload_directory(args.dir)
    elif args.list:
        files = list_bucket_files(prefix=args.prefix)
        for f in files:
            print(f)
    else:
        parser.print_help()
