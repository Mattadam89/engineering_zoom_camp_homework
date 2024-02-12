import os
from google.cloud import storage


bucket_name = os.environ["BUCKET_NAME"]



def upload_to_gcs(bucket_name, month):
    #initialise the google cloud storage client with credentials
    storage_client = storage.Client()

    #get the target bucket
    bucket = storage_client.bucket(bucket_name)

    #file to be uploaded
    file_name = f'green_taxi_2022_{month}.parquet'

    # print statement to check progress
    print(f"uploading ny taxi data from 2022 for month {month} to bucket")

    #upload the file to bucket
    blob = bucket.blob(f'ny_green_taxi_2022_{month}.parquet')
    blob.upload_from_filename(file_name)

def download_file(month):
    # print statement to check progress
    print(f"downloading ny taxi data from 2022 for month {month}\n")
    # set output file name of downloaded file
    file_name = f'green_taxi_2022_{month}.parquet'
    #set url of download
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet"
    #download file
    os.system(f"wget '{url}' -O {file_name}")


def main():
    """
    script to download a parquet file from a URL and write that parquet
    file to GCS bucket
    """

    for x in range(1,13):
        ### change month variable for correct month to download files sequentially
        month = f'{x:02d}'
        download_file(month)
        upload_to_gcs(bucket_name, month)

main()

        