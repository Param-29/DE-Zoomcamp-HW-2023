1. Ans:      `--iidfile string          Write the image ID to the file`
2. Ans: 3


Codes
```markdown

### Q1

root@paramm-X556UQK:/home/param/projects/ML-Everything/DE-Zoomcamp-HW-2023# docker build --help

Usage:  docker build [OPTIONS] PATH | URL | -

Build an image from a Dockerfile

Options:
      --add-host list           Add a custom host-to-IP mapping (host:ip)
      --build-arg list          Set build-time variables
      --cache-from strings      Images to consider as cache sources
      --cgroup-parent string    Optional parent cgroup for the container
      --compress                Compress the build context using gzip
      --cpu-period int          Limit the CPU CFS (Completely Fair
                                Scheduler) period
      --cpu-quota int           Limit the CPU CFS (Completely Fair
                                Scheduler) quota
  -c, --cpu-shares int          CPU shares (relative weight)
      --cpuset-cpus string      CPUs in which to allow execution (0-3, 0,1)
      --cpuset-mems string      MEMs in which to allow execution (0-3, 0,1)
      --disable-content-trust   Skip image verification (default true)
  -f, --file string             Name of the Dockerfile (Default is
                                'PATH/Dockerfile')
      --force-rm                Always remove intermediate containers
      --iidfile string          Write the image ID to the file
      --isolation string        Container isolation technology
      --label list              Set metadata for an image
  -m, --memory bytes            Memory limit
      --memory-swap bytes       Swap limit equal to memory plus swap:
                                '-1' to enable unlimited swap
      --network string          Set the networking mode for the RUN
                                instructions during build (default "default")
      --no-cache                Do not use cache when building the image
      --pull                    Always attempt to pull a newer version of
                                the image
  -q, --quiet                   Suppress the build output and print image
                                ID on success
      --rm                      Remove intermediate containers after a
                                successful build (default true)
      --security-opt strings    Security options
      --shm-size bytes          Size of /dev/shm
  -t, --tag list                Name and optionally a tag in the
                                'name:tag' format
      --target string           Set the target build stage to build.
      --ulimit ulimit           Ulimit options (default [])


### Q2 
root@paramm-X556UQK:/home/param/projects/ML-Everything/DE-Zoomcamp-HW-2023# docker run --entrypoint=bash -it python:3.9
Unable to find image 'python:3.9' locally
3.9: Pulling from library/python
bbeef03cda1f: Already exists 
f049f75f014e: Already exists 
56261d0e6b05: Already exists 
9bd150679dbd: Already exists 
5b282ee9da04: Already exists 
03f027d5e312: Already exists 
3c8304b923fa: Pull complete 
1f510f0937ac: Pull complete 
cb0f5bf32016: Pull complete 
Digest: sha256:4b7ee97f021e0d1f5749ea3ad6d1bae1ca15a9b42cdebcf40269502d54f56666
Status: Downloaded newer image for python:3.9
root@3fb9b426ac59:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
WARNING: You are using pip version 22.0.4; however, version 22.3.1 is available.
You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.
root@3fb9b426ac59:/# ^C
root@3fb9b426ac59:/# exit
exit


```

### Q3
Ans: 20530
```
SELECT * from green_taxi_trips
WHERE 
	lpep_pickup_datetime >= to_timestamp('01/15/2019', 'MM/DD/YYYY') AND
	lpep_pickup_datetime < to_timestamp('01/16/2019', 'MM/DD/YYYY') AND
	lpep_dropoff_datetime >= to_timestamp('01/15/2019', 'MM/DD/YYYY') AND
	lpep_dropoff_datetime < to_timestamp('01/16/2019', 'MM/DD/YYYY')
	;
```


### Q4

ANs: 2019-01-15
```
SELECT * from green_taxi_trips
ORDER BY trip_distance DESC;
```
Or max query 


### q5

> 2. 1282, 3. 254


```sql
SELECT *  
FROM green_taxi_trips
WHERE green_taxi_trips.passenger_count = 2 AND
	lpep_pickup_datetime >= to_timestamp('01/01/2019', 'MM/DD/YYYY') AND
	lpep_pickup_datetime < to_timestamp('01/02/2019', 'MM/DD/YYYY');
```

### q6

```
|        146 | Queens        | Long Island City/Queens Plaza                 | Boro Zone    |

```

```sql
SELECT *  
FROM green_taxi_trips
WHERE "PULocationID" = 7
ORDER BY tip_amount DESC ;
```



Terraform HW

```
(base) param@week1-de:~/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ terraform apply
var.project
  Your GCP Project ID

  Enter a value: quick-ray-375906


Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + labels                     = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "quick-ray-375906"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + routine {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + routine_id = (known after apply)
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_quick-ray-375906"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }

      + website {
          + main_page_suffix = (known after apply)
          + not_found_page   = (known after apply)
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/quick-ray-375906/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 5s [id=dtc_data_lake_quick-ray-375906]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```


Discussion highlights 


* ETL, system-designing etc