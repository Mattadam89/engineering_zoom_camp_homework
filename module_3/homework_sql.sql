/* Setup - create the external table */
CREATE OR REPLACE EXTERNAL TABLE eng-zoom-camp.ny_taxi.external_green_trips_2022
OPTIONS (
  format = 'Parquet',
  uris = ['gs://zoom_camp_homework_three/ny_green_taxi_2022_*.parquet']
)

/* Setup - create the materialized table */
CREATE OR REPLACE TABLE eng-zoom-camp.ny_taxi.green_trips_2022_non_partitioned AS
SELECT * FROM eng-zoom-camp.ny_taxi.external_green_trips_2022

/* Question 1 - count records */
SELECT count(*) 
FROM `eng-zoom-camp.ny_taxi.green_trips_2022_non_partitioned`

/* Question 2 - estimated data */
SELECT distinct PULocationID 
FROM eng-zoom-camp.ny_taxi.external_green_trips_2022

SELECT distinct PULocationID 
FROM eng-zoom-camp.ny_taxi.green_trips_2022_non_partitioned

/* question 3 - count rows where fare amount = 0 */
select count(*)
from eng-zoom-camp.ny_taxi.green_trips_2022_non_partitioned
where fare_amount = 0

/* question 4 - partitioning and clustering strategy*/
CREATE OR REPLACE TABLE eng-zoom-camp.ny_taxi.green_trips_2022_partitioned_clustered
Partition by DATE (lpep_pickup_datetime) 
Cluster by PUlocationID AS
SELECT * FROM eng-zoom-camp.ny_taxi.external_green_trips_2022

/* question 5 - bytes processed*/

select distinct PULocationID
from `eng-zoom-camp.ny_taxi.green_trips_2022_non_partitioned`
where lpep_pickup_datetime >= '2022-06-01' AND lpep_pickup_datetime <= '2022-06-30'

select distinct PULocationID
from `eng-zoom-camp.ny_taxi.green_trips_2022_partitioned_clustered`
where lpep_pickup_datetime >= '2022-06-01' AND lpep_pickup_datetime <= '2022-06-30'