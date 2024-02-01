import pyarrow as pa 
import pyarrow.parquet as pq 
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/eng-zoom-camp-4a5a6167c740.json"

bucket_name = 'mage-zoomcamp-homework'
project_id = 'eng-zoom-camp'
object_key = 'green_taxi_homework.parquet'

table_name = 'green_taxi_homework'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem = gcs
    )
    


