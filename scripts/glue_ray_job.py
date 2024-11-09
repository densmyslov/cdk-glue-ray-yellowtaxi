import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from pathlib import Path
from datetime import datetime
from time import time
import urllib.request
import s3fs
import json

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
job.init(args['JOB_NAME'], args)

# Define bucket names based on environment
BUCKET_MAPPING = {
    "stage": "bergena-yellow-taxi-stage",
    "prod": "bergena-yellow-taxi-prod"
}

env = args.get('ENV', 'stage')  # Default to "stage" if not provided
bucket_name = BUCKET_MAPPING.get(env)
if not bucket_name:
    raise ValueError(f"Unknown environment: {env}")

# Define paths
MOUNT_PATH = Path("/tmp/yellow_tripdata")
YELLOW_TAXI_DATA_PATH = MOUNT_PATH

def download_data(year: int, month: int) -> str:
    """Download yellow taxi data for given year and month."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    s3_path = MOUNT_PATH / filename

    # Skip downloading if file exists
    if not s3_path.exists():
        if not YELLOW_TAXI_DATA_PATH.exists():
            YELLOW_TAXI_DATA_PATH.mkdir(parents=True, exist_ok=True)
        try:
            print(f"Downloading => {s3_path}")
            urllib.request.urlretrieve(url, s3_path)
        except Exception as e:
            raise ValueError(f"Data not available for {year}-{month:02d}: {e}")
    else:
        print(f"File already exists: {s3_path}")

    return s3_path.as_posix()

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
        # Find and download latest available data
        year, month = find_latest_available_data()
        
        start_time = time()
        dataset_path = download_data(year, month)
        print(f"Data downloaded in {time() - start_time:.2f} seconds")

        # Read dataset using pandas (for smaller datasets)
        # For larger datasets, consider using Spark instead
        df = pd.read_parquet(dataset_path)
        
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

        # Define output path in S3
        output_path = f"s3://{bucket_name}/glue/python_shell/output/yellow_tripdata_transformed.parquet"

        # Write to S3 using s3fs
        print(f"Writing transformed data to {output_path}")
        s3 = s3fs.S3FileSystem()
        df.to_parquet(
            output_path,
            engine="pyarrow",
            filesystem=s3,
            index=False
        )

        print(f"Data successfully written to {output_path}")
        job.commit()
    except Exception as e:
        print(f"Error in job execution: {str(e)}")
        job.commit()
        raise e

if __name__ == "__main__":
    main()