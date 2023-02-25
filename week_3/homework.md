1. use `week_to_gcs_fhv.py` to push data to google cloud storage 

```sql

CREATE OR REPLACE EXTERNAL TABLE `quick-ray-375906.ny_rides.external_data_hw`
  OPTIONS (
    format ="csv",
    uris = ['gs://prefect-dee/data/fhv/fhv_tripdata_*']
    );

CREATE TABLE `quick-ray-375906.ny_rides.external_data_hw_copy`
AS 
  SELECT * FROM `quick-ray-375906.ny_rides.external_data_hw`;

SELECT COUNT(*) FROM `quick-ray-375906.ny_rides.external_data_hw`;

```
ANS1: 43,244,696

```
SELECT COUNT(Affiliated_base_number) FROM `quick-ray-375906.ny_rides.external_data_hw_copy`;
```

ANS2: 0 MB for the External Table and 317.94MB for the BQ Table


```sql
SELECT COUNT(*) FROM `quick-ray-375906.ny_rides.external_data_hw_copy`
WHERE 
  PUlocationID is NULL AND
  DOlocationID is NULL
;

```

ANS3: 717748

ANS4: Partition by pickup_datetime Cluster on affiliated_base_number


```sql
CREATE OR REPLACE TABLE `quick-ray-375906.ny_rides.external_data_hw_copy_part_cluster`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS 
  SELECT * FROM `quick-ray-375906.ny_rides.external_data_hw_copy`;

SELECT DISTINCT(affiliated_base_number) 
FROM `quick-ray-375906.ny_rides.external_data_hw_copy_part_cluster`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

```

ANS5: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

ANS6: Google storage 

ANS7: Thinking, mostly false (insertion time in database increases if cluster is present vs not present)
