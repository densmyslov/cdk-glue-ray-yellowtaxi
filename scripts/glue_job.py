import os
import sys
import pandas as pd
from datetime import datetime
from time import time
import urllib.request
import argparse

# Define bucket names based on environment
# BUCKET_MAPPING = {
#     "stage": "bergena-yellow-taxi-stage",
#     "prod": "bergena-yellow-taxi-prod"
# }

# def get_argument_value(arg_name, default_value=None):
#     """Fetch the value of a Glue job argument."""
#     args_dict = dict(arg.split('=', 1) for arg in sys.argv[1:] if '=' in arg)
#     return args_dict.get(arg_name, default_value)


# def get_environment():
#     """Get environment from AWS Glue job arguments."""
#     if len(sys.argv) > 1:
#         args_dict = dict(arg.split('=', 1) for arg in sys.argv[1:] if '=' in arg)
#         return args_dict.get('--ENV', 'stage')
#     return os.environ.get('ENV', 'stage')

def parse_arguments():
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
            raise ValueError(f"Data not available for {year}-{month:02d}: {e}")
    else:
        print(f"File already exists: {local_path}")

    return local_path

def find_latest_available_data():
    """Find the most recent available data."""
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month - 1  # Previous month's data

    # Handle January case
    if month == 0:
        year -= 1
        month = 12

    while month > 0:
        try:
            download_data(year, month)
            return year, month
        except ValueError:
            month -= 1
            if month == 0:
                year -= 1
                month = 12

    raise ValueError("No available data found")

def main():
    try:
        print("Job arguments:", sys.argv)
        # print("Environment:", get_environment())

        # # Retrieve environment and bucket name
        # env_name = get_argument_value("--ENV_NAME", "stage")
        # bucket_name = get_argument_value("--BUCKET_NAME", "default-bucket-name")

        # env_name = os.environ.get("ENV_NAME", "stage")
        # bucket_name = os.environ.get("BUCKET_NAME", "default-bucket-name")
        env_name, bucket_name = parse_arguments()

        print(f"Environment: {env_name}")
        print(f"Bucket Name: {bucket_name}")

        print(f"Running in environment: {env_name}")
        print(f"Using bucket: {bucket_name}")
        
        # Find and download latest available data
        year, month = find_latest_available_data()
        
        start_time = time()
        local_dataset_path = download_data(year, month)
        print(f"Data downloaded in {time() - start_time:.2f} seconds")

        # Read the parquet file
        print(f"Reading parquet file from {local_dataset_path}")
        df = pd.read_parquet(local_dataset_path)

        # Add tip rate calculation
        print("Calculating tip rates...")
        df["tip_rate"] = df["tip_amount"] / df["total_amount"]

        # Drop unnecessary columns
        columns_to_drop = [
            "payment_type",
            "fare_amount",
            "extra",
            "tolls_amount",
            "improvement_surcharge"
        ]
        print(f"Dropping columns: {columns_to_drop}")
        df = df.drop(columns=columns_to_drop)

        # Define output path and save to S3
        output_path = f"s3://{bucket_name}/glue/python_shell/output/yellow_tripdata_transformed.parquet"
        print(f"Writing transformed data to {output_path}")
        
        df.to_parquet(
            output_path,
            index=False
        )

        print(f"Job completed successfully. Data written to {output_path}")
        
    except Exception as e:
        print(f"Error in job execution: {str(e)}")
        raise e

if __name__ == "__main__":
    main()