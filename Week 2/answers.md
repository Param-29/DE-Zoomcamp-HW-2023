Q1: 447770 

```
dtype: object
17:25:59.919 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime store_and_fwd_flag  RatecodeID  ...  improvement_surcharge  total_amount  payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39                  N         1.0  ...                    0.3          4.81           1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39                  N         5.0  ...                    0.3         24.36           1.0        2.0                   0.0

[2 rows x 20 columns]
17:25:59.920 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
```

Q2: 0 5 1 * *

```
At 05:00 AM on day 1 of the month
```

Q3: 14851920

```
18:57:16.182 | INFO    | prefect.engine - Created flow run 'phenomenal-wapiti' for flow 'etl-gcs-to-bq'
18:57:17.059 | INFO    | Flow run 'phenomenal-wapiti' - Created task run 'extract_from_gcs_modified-8d260a40-0' for task 'extract_from_gcs_modified'
18:57:17.062 | INFO    | Flow run 'phenomenal-wapiti' - Executing 'extract_from_gcs_modified-8d260a40-0' immediately...
18:57:17.927 | INFO    | Task run 'extract_from_gcs_modified-8d260a40-0' - Downloading blob named data/yellow/yellow_tripdata_2019-02.parquet from the prefect-dee bucket to ../data/data/yellow/yellow_tripdata_2019-02.parquet
18:57:39.601 | INFO    | Task run 'extract_from_gcs_modified-8d260a40-0' - Downloading blob named data/yellow/yellow_tripdata_2019-03.parquet from the prefect-dee bucket to ../data/data/yellow/yellow_tripdata_2019-03.parquet
18:58:03.225 | INFO    | Task run 'extract_from_gcs_modified-8d260a40-0' - Finished in state Completed()
18:58:03.378 | INFO    | Flow run 'phenomenal-wapiti' - Created task run 'transform_all-75ed646f-0' for task 'transform_all'
18:58:03.380 | INFO    | Flow run 'phenomenal-wapiti' - Executing 'transform_all-75ed646f-0' immediately...
18:58:05.958 | INFO    | Task run 'transform_all-75ed646f-0' - pre: missing passenger count: 11030387
18:58:05.965 | INFO    | Task run 'transform_all-75ed646f-0' - post: missing passenger count: 11030387
18:58:07.588 | INFO    | Task run 'transform_all-75ed646f-0' - pre: missing passenger count: 12337664
18:58:07.597 | INFO    | Task run 'transform_all-75ed646f-0' - post: missing passenger count: 12337664
18:58:28.970 | INFO    | Task run 'transform_all-75ed646f-0' - Finished in state Completed()
18:58:29.456 | INFO    | Flow run 'phenomenal-wapiti' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
18:58:29.459 | INFO    | Flow run 'phenomenal-wapiti' - Executing 'write_bq-b366772c-0' immediately...
19:00:24.724 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
19:00:24.964 | INFO    | Flow run 'phenomenal-wapiti' - Finished in state Completed('All states completed.')
```
