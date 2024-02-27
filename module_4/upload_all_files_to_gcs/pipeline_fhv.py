import os
import pandas as pd
from google.cloud import storage



bucket_name = "zoom_camp_module_4"
years = [2019]


def upload_to_gcs(bucket_name, month, file_name, filepath):

    #initialise the google cloud storage client with credentials
    storage_client = storage.Client()

    #get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # print statement to check progress
    print(f"uploading ny taxi data to bucket")

    #upload the file to bucket
    blob = bucket.blob(filepath)
    blob.upload_from_filename(file_name)

def send_csv_to_gcs(month, year):
    # print statement to check progress
    print(f"downloading ny taxi data for FHV taxi service for {month}/{year}\n")

    # set file name
    file_name = f'FHV_tripdata_{year}-{month}.csv.gz'

    #set url of download
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file_name}"
    
    #download file
    os.system(f"wget '{url}' -O {file_name}")

    #read into data frame
    print("Converting to parquet...")
    df = pd.read_csv(file_name, compression='gzip')

    #set types
    df["PUlocationID"] = df["PUlocationID"].astype('Int64')
    df["DOlocationID"] = df["DOlocationID"].astype('Int64')

    #convert to paquet
    file_name = file_name.replace('.csv.gz', '.parquet')
    df.to_parquet(file_name, engine='pyarrow')
    print(f"Parquet: {file_name}")

    #upload to gcs
    upload_to_gcs(bucket_name, month, file_name, f"fhv/{file_name}")








def main():
    """
    script to download a parquet file from a URL and write that parquet
    file to GCS bucket
    """
    for year in years:
        for month in range(1,13):
            ### change month variable for correct month to download files sequentially
            month = f'{month:02d}'
            send_csv_to_gcs(month,year)

main()