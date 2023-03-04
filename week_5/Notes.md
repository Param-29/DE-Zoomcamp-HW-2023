# Week5: batch processing using PySpark

## Different types of Processing data (/ deploying models)

* batch 
* Streaming 

## Batch processing

* Processing data (from database/datasource) in regular predefined intervals (say every day, every 5 min, etc etc etc)
* Processing chunks of data after a pre-specified timeframe 
	- vs Streaming which is real time processing 
	
### Advantages
* Easy to manage 
* Retries 
* Easier to mantain and log
* Scale 
### Disadvantages
* Delay

(Note: ~80% of jobs are in batch processingdd)


### Batch jobs
* Weekly
* Monthly
* Daily
* Hourly

### Tech used

* Py Scripts 
* SQL
* Spark 
* Py Scripts 

If something can be expressed in SQL, (in data pipeline) use SQL Athena, else use SPARK

## PySpark
* DATA Processing engine 
* Work one in a cluster of engine 
* Multi-language engine 
	- Java, Scala 
	- Python

Note to have following env variables before using pyspark
```bash 
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH" 
# look for right py4j version and add it above
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"

```

### When to use it?

**(WHEN PREPROCESSING CANNOT BE REPRESENTED IN SQL)**

* When custom/business logic is easier to be represented (and tested) in functions, compared to SQL 
* When you need to execute SQL on files (say in Data-lake) instead of database(or datawarehouse)
	- Other alternates for this are
		+ HIVE
		+ PRESTO 
		+ ATHENA


**Spark dataframe on 4040**

### PySpark dataframes
```py
df = spark.read.parquet('locations')
df.preintSchema()
```

### Actions vs transformations

1. Actions (TDB right now)
2. transformations on data: (lazy execution), executed on `.show()`
	- Check backend execution on 4040 
	
### Inbuilt functions

`from pyspark.sql import functions as f`

### Userdefined functions

* For complex logics (that cannot be done in SQL easily) but can be written in python functions

**Note:** Gzip is good because pandas and pyspark can read it without any unpacking :) 

`gzip {TOTAL_FILE_LOCATION} #deletes file and stores .gz compressed version`
TO print contents of zipped file 
`zcat {FILE_NAME}.gz`





Practicing 
* 5.1 to 5.3


## Spark internals (or Spark cluster)



```
Driver ---> cluster_master ---> (executor)^n
```


## SQL in dataframe

Where
GroupBy 
OrderBy
Join


## Resilient Distributed Datasets

### Mapreduce functions

1. Distributed way of working with batch files 

# PySpark

Starting pyspark: Documentation: Installation guide on linux: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md

    Recommended to be done on a remote VM


```
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
```

## Starting PySpark

```
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

```

Check logs running on port 4040

## Reading from a csv

```python
df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')
```
Note
* Doesnot take header by default

## `df.show()` to print dataframe

## Write back to parquet `df.write.parquet('zones')`
* Name of the folder specified in `zones`

## `df_pyspark.schema()` Prints schema, also `df.printSchema()`


## `df.repartition(number)` Repartitions dataframe into _number_ of partitoins (for distrubuted computing) across workers
* Lazy Operation 
* Compute heavy Operation