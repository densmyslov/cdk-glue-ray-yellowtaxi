import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from time import time
import urllib.request
import s3fs
import json

# Load environment settings from cdk.json
with open("cdk.json") as f:
    cdk_config = json.load(f)

# Define AWS environment details
env = os.environ.get("ENV", "stage")  # Default to "stage" if not provided

# Extract bucket names from cdk.json based on environment
if env == "stage":
    bucket_name = cdk_config["env"]["stage"]["bucket_name"]
elif env == "prod":
    bucket_name = cdk_config["env"]["prod"]["bucket_name"]
else:
    raise ValueError(f"Unknown environment: {env}")

if not bucket_name:
    raise ValueError(f"Bucket name not specified for environment: {env}")

# Define paths
MOUNT_PATH = Path("/tmp/yellow_tripdata")
YELLOW_TAXI_DATA_PATH = MOUNT_PATH

# Function to download data
def download_data(year: int, month: int) -> str:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    s3_path = MOUNT_PATH / filename

    # Skip downloading if file exists.
    if not s3_path.exists():
        if not YELLOW_TAXI_DATA_PATH.exists():
            YELLOW_TAXI_DATA_PATH.mkdir(parents=True, exist_ok=True)
        try:
            print(f"downloading => {s3_path}")
            urllib.request.urlretrieve(url, s3_path)
        except Exception as e:
            raise ValueError(f"Data not available for {year}-{month:02d}: {e}")
    else:
        print(f"File already exists: {s3_path}")

    return s3_path.as_posix()

# Determine the latest available year and month
def find_latest_available_data():
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month - 1

    # If the current month is January, set to previous year December
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

    raise ValueError("No available data found.")

# Find the latest available data
year, month = find_latest_available_data()

s = time()
# Download data for the latest available month
dataset_path = download_data(year, month)
print(f"Data downloaded in {time() - s} seconds.")

# Read dataset using pandas
df = pd.read_parquet(dataset_path)

# Add the given new column to the dataset
df["tip_rate"] = df["tip_amount"] / df["total_amount"]

# Dropping few columns from the underlying DataFrame
df = df.drop(columns=["payment_type", "fare_amount", "extra", "tolls_amount", "improvement_surcharge"])

# Write the transformed dataset to the specified S3 bucket
output_path = f"s3://{bucket_name}/glue/python_shell/output/yellow_tripdata_transformed.parquet"

# Use boto3 and s3fs to write to S3
s3 = s3fs.S3FileSystem()
df.to_parquet(output_path, engine="pyarrow", filesystem=s3)

print(f"Data written to {output_path}")
