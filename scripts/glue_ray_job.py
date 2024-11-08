import ray
import pandas
from ray import data
import os

ray.init('auto')

bucket_name = os.environ.get("bucket_name")

ds = ray.data.read_csv("s3://nyc-tlc/opendata_repo/opendata_webconvert/yellow/yellow_tripdata_2022-01.csv")

# Add the given new column to the dataset and show the sample record after adding a new column
ds = ds.add_column( "tip_rate", lambda df: df["tip_amount"] / df["total_amount"])

# Dropping few columns from the underlying Dataset 
ds = ds.drop_columns(["payment_type", "fare_amount", "extra", "tolls_amount", "improvement_surcharge"])

ds.write_parquet("fs3://{bucket_name}/ray/tutorial/output/")