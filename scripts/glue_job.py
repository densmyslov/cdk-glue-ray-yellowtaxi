import os
import sys
import pandas as pd
from datetime import datetime
from time import time
import urllib.request
import argparse
import logging

import boto3

# Define bucket names based on environment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Python sys.path: {sys.path}")

try:
    import psutil
    logger.info(f"psutil version: {psutil.__version__}")
except ModuleNotFoundError as e:
    logger.error(f"Failed to import psutil: {e}")

def parse_arguments():
    """Get environment and bucket name from stack arguments."""
    parser = argparse.ArgumentParser(description="AWS Glue Job Arguments")

    # Define the expected arguments
    parser.add_argument("--ENV_NAME", type=str, default="dev", help="Environment name (e.g., dev, stage, prod)")
    parser.add_argument("--BUCKET_NAME", type=str, default="default-bucket-name", help="S3 bucket name")

    # Parse the arguments
    args, unknown = parser.parse_known_args()

    # Extract values
    env_name = args.ENV_NAME
    bucket_name = args.BUCKET_NAME

    print(f"Environment: {env_name}")
    print(f"Bucket Name: {bucket_name}")

    return env_name, bucket_name

def create_dir_if_not_exists(path):
    """Create directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)

def download_data(year: int, month: int) -> str:
    """Download yellow taxi data for given year and month."""
    tmp_dir = "/tmp/yellow_tripdata"
    create_dir_if_not_exists(tmp_dir)
    
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join(tmp_dir, filename)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    if not os.path.exists(local_path):
        try:
            print(f"Downloading => {local_path}")
            urllib.request.urlretrieve(url, local_path)
        except Exception as e:
            print(f"Data not available for {year}-{month:02d}: {e}")
            return None
    else:
        print(f"File already exists: {local_path}")

    return local_path

def upload_to_s3(local_path: str, bucket_name: str, s3_path: str):
    """Upload a local file to S3."""
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(local_path, bucket_name, s3_path)
        print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Error uploading file {local_path} to S3: {e}")

def process_data(year: int, month: int, bucket_name: str) -> int:
    """Download, process, and upload data; return file size."""
    local_path = download_data(year, month)
    if not local_path:
        return 0

    try:
        # Read the parquet file
        print(f"Reading parquet file from {local_path}")
        df = pd.read_parquet(local_path)

        # Add tip rate calculation
        df["tip_rate"] = df["tip_amount"] / df["total_amount"]

        # Drop unnecessary columns
        columns_to_drop = [
            "payment_type",
            "fare_amount",
            "extra",
            "tolls_amount",
            "improvement_surcharge"
        ]
        df = df.drop(columns=columns_to_drop)

        # Define output path
        s3_path = f"glue/python_shell/output/yellow_tripdata_{year}-{month:02d}.parquet"

        # Save transformed data locally
        transformed_local_path = f"/tmp/yellow_tripdata_transformed_{year}-{month:02d}.parquet"
        df.to_parquet(transformed_local_path, index=False)

        # Upload to S3
        upload_to_s3(transformed_local_path, bucket_name, s3_path)

        # Calculate file size
        file_size = os.path.getsize(transformed_local_path)
        print(f"Ingested {year}-{month:02d}, File Size: {file_size / (1024 * 1024):.2f} MB")

        cpu_usage = psutil.cpu_percent(interval=1)
        print(f"CPU Usage: {cpu_usage}%")
        
        return file_size
    except Exception as e:
        print(f"Error processing data for {year}-{month:02d}: {e}")
        return 0

def ingest_data_for_last_10_years(bucket_name: str):
    """Ingest data for the last 10 years."""
    current_year = datetime.now().year
    current_month = datetime.now().month
    start_year = current_year - 1

    total_file_size = 0

    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            # Skip future months
            if year == current_year and month >= current_month:
                break
            
            # Process data for each year and month
            file_size = process_data(year, month, bucket_name)
            total_file_size += file_size

    # Print total file size
    print(f"Total Ingested File Size: {total_file_size / (1024 * 1024):.2f} MB")

def main():
    try:
        print("Job arguments:", sys.argv)

        env_name, bucket_name = parse_arguments()

        print(f"Environment: {env_name}")
        print(f"Bucket Name: {bucket_name}")

        print(f"Running in environment: {env_name}")
        print(f"Using bucket: {bucket_name}")
        
        # Ingest data for the last 10 years
        start_time = time()
        ingest_data_for_last_10_years(bucket_name)
        print(f"Data ingestion completed in {time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error in job execution: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
