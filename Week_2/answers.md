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

Q4: 88605

```
(de-week-2) paramm@paramm-X556UQK:~/projects/test$ prefect agent start -q 'test'
Starting v2.7.10 agent with ephemeral API...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): test...
19:11:43.731 | INFO    | prefect.agent - Submitting flow run 'a97eb138-de5c-44e2-abd9-db6f5d109115'
19:11:44.513 | INFO    | prefect.infrastructure.process - Opening process 'heavenly-mongoose'...
19:11:44.628 | INFO    | prefect.agent - Completed submission of flow run 'a97eb138-de5c-44e2-abd9-db6f5d109115'
/home/param/anaconda3/envs/de-week-2/lib/python3.10/runpy.py:126: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
19:11:48.077 | INFO    | Flow run 'heavenly-mongoose' - Downloading flow code from storage at '/home/param/projects/test/DE-Zoomcamp-HW-2023'
19:11:49.930 | INFO    | Flow run 'heavenly-mongoose' - Created task run 'fetch-ba00c645-0' for task 'fetch'
19:11:49.932 | INFO    | Flow run 'heavenly-mongoose' - Executing 'fetch-ba00c645-0' immediately...
19:11:50.089 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
19:11:51.020 | INFO    | Flow run 'heavenly-mongoose' - Created task run 'clean-2c6af9f6-0' for task 'clean'
19:11:51.026 | INFO    | Flow run 'heavenly-mongoose' - Executing 'clean-2c6af9f6-0' immediately...
19:11:51.264 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0       2.0  2020-11-01 00:08:23  ...       1.0                 2.75
1       2.0  2020-11-01 00:23:32  ...       1.0                 0.00

[2 rows x 20 columns]
19:11:51.266 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
19:11:51.267 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605

```


Q5: 514392

```
19:43:21.643 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392
19:43:21.795 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
19:43:23.733 | INFO    | Flow run 'sapphire-termite' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
19:43:23.748 | INFO    | Flow run 'sapphire-termite' - Executing 'write_local-09e9d2b8-0' immediately...
19:43:28.291 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
19:43:28.541 | INFO    | Flow run 'sapphire-termite' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
19:43:28.547 | INFO    | Flow run 'sapphire-termite' - Executing 'write_gcs-67f8f48e-0' immediately...
19:43:31.541 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'prefect-dee'.
19:43:32.172 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/green/green_tripdata_2019-04.parquet') to the bucket 'prefect-dee' path 'data/green/green_tripdata_2019-04.parquet'.
19:43:34.647 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
19:43:35.009 | INFO    | Flow run 'sapphire-termite' - Finished in state Completed('All states completed.')
19:43:37.101 | INFO    | prefect.infrastructure.process - Process 'sapphire-termite' exited cleanly.


```

Q6: 8