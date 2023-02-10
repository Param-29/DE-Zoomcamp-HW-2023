1. Create an external table

```sql
CREATE EXTERNAL TABLE `quick-ray-375906.ny_rides.external_data`
  OPTIONS (
    format ="parquet",
    uris = ['gs://prefect-dee/data/yellow/yellow_tripdata_2019-02.parquet']
    );
```

2. Create a partitioned table 

```sql
CREATE TABLE `quick-ray-375906.ny_rides.part_external_data`
PARTITION BY DATE(tpep_pickup_datetime)
 AS
  SELECT *
   
  FROM quick-ray-375906.ny_rides.external_data;

```

2. Create a cluster & partitioned table 

```sql
CREATE TABLE `quick-ray-375906.ny_rides.part_external_data`
PARTITION BY DATE(tpep_pickup_datetime)
 AS
  SELECT *
   
  FROM quick-ray-375906.ny_rides.external_data;

```