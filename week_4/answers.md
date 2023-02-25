Ans1 61.6M
```
(de-week-2) paramm@paramm-X556UQK:~/projects/dbt_hello_taxi/dbt_ny_taxi_hello$ dbt run --var 'is_test_run: false'
19:05:01  Running with dbt=1.4.3
19:05:01  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
There are 2 unused configuration paths:
- models.dbt_ny_taxi_hello.example
- seeds.taxi_rides_ny.taxi_zone_lookup
19:05:01  Found 5 models, 11 tests, 0 snapshots, 0 analyses, 522 macros, 0 operations, 1 seed file, 3 sources, 0 exposures, 0 metrics
19:05:01  
19:05:04  Concurrency: 1 threads (target='dev')
19:05:04  
19:05:04  1 of 5 START sql table model ny_dbt_dataset.dim_zones .......................... [RUN]
19:05:08  1 of 5 OK created sql table model ny_dbt_dataset.dim_zones ..................... [CREATE TABLE (265.0 rows, 12.1 KB processed) in 3.88s]
19:05:08  2 of 5 START sql view model ny_dbt_dataset.stg_green_tripdata .................. [RUN]
19:05:12  2 of 5 OK created sql view model ny_dbt_dataset.stg_green_tripdata ............. [CREATE VIEW (0 processed) in 3.65s]
19:05:12  3 of 5 START sql view model ny_dbt_dataset.stg_yellow_tripdata ................. [RUN]
19:05:13  3 of 5 OK created sql view model ny_dbt_dataset.stg_yellow_tripdata ............ [CREATE VIEW (0 processed) in 1.15s]
19:05:13  4 of 5 START sql table model ny_dbt_dataset.fact_trips ......................... [RUN]
19:05:24  4 of 5 OK created sql table model ny_dbt_dataset.fact_trips .................... [CREATE TABLE (61.6m rows, 14.8 GB processed) in 10.82s]
19:05:24  5 of 5 START sql table model ny_dbt_dataset.dm_monthly_zone_revenue ............ [RUN]
19:05:28  5 of 5 OK created sql table model ny_dbt_dataset.dm_monthly_zone_revenue ....... [CREATE TABLE (12.0k rows, 13.4 GB processed) in 4.07s]
19:05:28  
19:05:28  Finished running 3 table models, 2 view models in 0 hours 0 minutes and 26.29 seconds (26.29s).
19:05:28  
19:05:28  Completed successfully
19:05:28  
19:05:28  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5

```

ANS2: 89.8:10.1

ANS3: 43244696

ANS4: 22998722


ANS5: JAN