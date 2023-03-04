# Homework.md

Get into the vm 

```
ssh de-zoomcamp
```

Note: 
1. Shut down the machine when not in use 
2. Remember to change IP in config file and login 

```bash
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"

```

Q1: `3.3.2`


Creating a Structure 
```
StructType([
	StructField('dispatching_base_num', types.StringType(), True), 
	StructField('pickup_datetime', types.TimestampType(), True), 
	StructField('dropoff_datetime', types.TimestampType(), True), 
	StructField('PULocationID', types.LongType(), True), 
	StructField('DOLocationID', types.LongType(), True), 
	StructField('SR_Flag', types.BooleanType(), True), 
	StructField('Affiliated_base_number', types.StringType(), True)
])
```

Repartitioning 
```
# Applying custom schema to data frame
df = spark.read.format( "csv") \
    .schema(schema) \
    .option("header", True) \
    .load("data/fhvhv_tripdata_2021-06.csv.gz") \
    .repartition(12) \
    .write.parquet("data/parquet3") # folder location where files need to be written

```

Spark tutorials 
> https://sparkbyexamples.com/pyspark-tutorial/

Q2: 24MB
Q3: 452474, 452472, 452468
Q4: 66.87889
Q5: 4040
Q6: Crown Heights North

