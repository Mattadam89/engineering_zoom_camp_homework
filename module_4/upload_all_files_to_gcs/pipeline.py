import os
import pandas as pd
from google.cloud import storage



bucket_name = "zoom_camp_module_4"
services = ['green', 'yellow', 'fhv']
years = [2019, 2020]


def upload_to_gcs(bucket_name, month, file_name, filepath):

    #initialise the google cloud storage client with credentials
    storage_client = storage.Client()

    #get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # print statement to check progress
    print(f"uploading ny taxi data from 2022 for month {month} to bucket")

    #upload the file to bucket
    blob = bucket.blob(filepath)
    blob.upload_from_filename(file_name)

def send_csv_to_gcs(service, month, year):
    # print statement to check progress
    print(f"downloading ny taxi data for {service} taxi service for {month}/{year}\n")

    # set file name
    file_name = f'{service}_tripdata_{year}-{month}.csv.gz'

    #set url of download
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{file_name}"

    #download file
    os.system(f"wget '{url}' -O {file_name}")

    #read into data frame
    print("Converting to parquet...")
    df = pd.read_csv(file_name, compression='gzip')
    file_name = file_name.replace('.csv.gz', '.parquet')
    df.to_parquet(file_name, engine='pyarrow')
    print(f"Parquet: {file_name}")

    upload_to_gcs(bucket_name, month, file_name, f"{service}/{file_name}")








def main():
    """
    script to download a parquet file from a URL and write that parquet
    file to GCS bucket
    """
    for service in services:
        for year in years:
            for month in range(1,13):
                ### change month variable for correct month to download files sequentially
                month = f'{month:02d}'
                send_csv_to_gcs(service, month,year)

main()