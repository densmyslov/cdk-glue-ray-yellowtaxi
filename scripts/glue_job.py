import os
import sys
import pandas as pd
from datetime import datetime
from time import time
import urllib.request
import argparse

# Define bucket names based on environment

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

def ingest_data_for_last_10_years():
    """Ingest data for the last 10 years."""
    current_year = datetime.now().year
    current_month = datetime.now().month
    start_year = current_year - 10

    all_data = []

    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            # Skip future months
            if year == current_year and month >= current_month:
                break
            
            try:
                local_path = download_data(year, month)
                if local_path:
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

                    all_data.append(df)
            except Exception as e:
                print(f"Error processing data for {year}-{month:02d}: {e}")

    # Concatenate all data
    if all_data:
        print("Concatenating all data...")
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        raise ValueError("No data was downloaded or processed successfully.")

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
        combined_df = ingest_data_for_last_10_years()
        print(f"Data ingested in {time() - start_time:.2f} seconds")

        # Define output path and save to S3
        output_path = f"s3://{bucket_name}/glue/python_shell/output/yellow_tripdata_10_years_transformed.parquet"
        print(f"Writing transformed data to {output_path}")
        
        combined_df.to_parquet(
            output_path,
            index=False
        )

        print(f"Job completed successfully. Data written to {output_path}")
        
    except Exception as e:
        print(f"Error in job execution: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
