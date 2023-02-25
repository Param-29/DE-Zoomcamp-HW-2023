# fhv_19
CREATE OR REPLACE EXTERNAL TABLE `quick-ray-375906.ny_rides.fhv_2019_external`
  OPTIONS (
    format ="csv",
    uris = ['gs://prefect-dee/data/fhv/fhv_tripdata_*']
    );


CREATE or REPLACE TABLE `quick-ray-375906.ny_rides.fhv_2019`
AS 
SELECT * FROM `quick-ray-375906.ny_rides.fhv_2019_external`;



SELECT COUNT(*) FROM `quick-ray-375906.ny_rides.fhv_2019`;

# yellow_19_20

CREATE OR REPLACE EXTERNAL TABLE `quick-ray-375906.ny_rides.external_data_yellow_19_20`
  OPTIONS (
    format ="csv",
    uris = [
    'gs://prefect-dee/data/yellow/yellow_tripdata_2019*.csv.gz', 
    'gs://prefect-dee/data/yellow/yellow_tripdata_2020*.csv.gz' 
    ]
    );

CREATE TABLE `quick-ray-375906.ny_rides.data_yellow_19_20`
AS 
SELECT * FROM `quick-ray-375906.ny_rides.external_data_yellow_19_20`;

SELECT COUNT(*) FROM `quick-ray-375906.ny_rides.data_yellow_19_20`;


# green_19_20

CREATE OR REPLACE EXTERNAL TABLE `quick-ray-375906.ny_rides.external_data_green_19_20`
  OPTIONS (
    format ="csv",
    uris = [
    'gs://prefect-dee/data/green/green_tripdata_2019*.csv.gz', 
    'gs://prefect-dee/data/green/green_tripdata_2020*.csv.gz' 
    ]
    );

CREATE TABLE `quick-ray-375906.ny_rides.data_green_19_20`
AS 
SELECT * FROM `quick-ray-375906.ny_rides.external_data_green_19_20`;

SELECT COUNT(*) FROM `quick-ray-375906.ny_rides.data_green_19_20`;
