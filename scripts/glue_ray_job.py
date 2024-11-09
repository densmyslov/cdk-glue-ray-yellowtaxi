import ray
import os
import requests
from pathlib import Path
from datetime import datetime

# Initialize Ray cluster
ray.init('auto')

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
        with requests.get(url, stream=True) as r:
            if r.status_code == 200:
                print(f"downloading => {s3_path}")
                # Writing locally (in this case, to /tmp directory)
                with open(s3_path, "wb") as file:
                    for chunk in r.iter_content(chunk_size=8192):
                        file.write(chunk)
            else:
                raise ValueError(f"Data not available for {year}-{month:02d}")
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

# Download data for the latest available month
dataset_path = download_data(year, month)

# Read dataset using Ray
# Load the data from the downloaded file
ds = ray.data.read_parquet(dataset_path)

# Add the given new column to the dataset and show the sample record after adding a new column
ds = ds.add_column("tip_rate", lambda df: df["tip_amount"] / df["total_amount"])

# Dropping few columns from the underlying Dataset
ds = ds.drop_columns(["payment_type", "fare_amount", "extra", "tolls_amount", "improvement_surcharge"])

# Write the transformed dataset to the specified S3 bucket
bucket_name = os.environ.get("bucket_name")
ds.write_parquet(f"s3://{bucket_name}/ray/tutorial/output/")
