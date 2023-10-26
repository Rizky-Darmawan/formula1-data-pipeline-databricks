# Databricks notebook source
# QUESTIONS ASKED
# 1. Who are the top 20 drivers of all time in terms of number of wins, also show their average quali results
# 2. Show me top 5 best teams and 5 worst teams (in terms of grid position)
# 3. Show me teams from 2016-2023 and their respective statistics of their pit stops time

# COMMAND ----------

# SET UP THE CONNECTION TO STORAGE ACCOUNT
spark.conf.set(
    "fs.azure.account.key.formula1datapipeline.dfs.core.windows.net",
    dbutils.secrets.get(scope="formula1-data-pipeline", key="storage-account-key"))

# COMMAND ----------

# LOAD ALL RAW DATA TO DATAFRAME
constructors_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/constructors.csv")

drivers_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/drivers.csv")

pit_stops_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/pit_stops.csv")

qualifying_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/qualifying.csv")

races_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/races.csv")

results_df = spark \
    .read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .format('csv') \
    .load("abfss://formula1-data-pipeline@formula1datapipeline.dfs.core.windows.net/raw-data/results.csv")

# COMMAND ----------

# CREATE TEMP VIEW FOR SQL QUERY USAGE
constructors_df.createOrReplaceTempView('constructors')
drivers_df.createOrReplaceTempView('drivers')
pit_stops_df.createOrReplaceTempView('pit_stops')
qualifying_df.createOrReplaceTempView('qualifying')
races_df.createOrReplaceTempView('races')
results_df.createOrReplaceTempView('results')

# COMMAND ----------

# TEST QUERYING VIEW
test = spark.sql(
    '''
    WITH stg1 AS (
        SELECT * FROM constructors
    )
    SELECT * FROM stg1 WHERE nationality = "British"
    '''
)

display(test)

# COMMAND ----------

display(spark.sql('SELECT * FROM results LIMIT 10;'))

# COMMAND ----------

display(spark.sql('SELECT * FROM drivers LIMIT 10;'))

# COMMAND ----------

display(spark.sql('SELECT * FROM qualifying LIMIT 10;'))

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# GET TOP 20 DRIVERS OF ALL TIME IN TERMS OF NUMBER OF WINS AND SHOW THEIR AVERAGE AND MEDIAN QUALIFYING POSITION
top_20_drivers = spark.sql(
    '''
    WITH stg1 AS (
        SELECT * FROM results WHERE positionOrder = 1
    ),

    stg2 AS (
        SELECT driverId, COUNT(*) AS winCount FROM stg1 GROUP BY driverId ORDER BY winCount DESC LIMIT 20
    ),

    stg3 AS (
        SELECT
            a.driverId,
            b.forename,
            b.surname,
            b.dob,
            b.nationality,
            a.winCount 
        FROM stg2 AS a
        JOIN drivers AS b
        ON a.driverId = b.driverId
        ORDER BY a.winCount DESC
    ),

    stg4 AS (
        SELECT 
            driverId, 
            ROUND(AVG(position), 0) AS avgPosition,
            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY position) AS medianPosition
        FROM qualifying 
        GROUP BY driverId
    )

    SELECT 
        a.*,
        b.avgPosition,
        b.medianPosition
    FROM stg3 AS a
    JOIN stg4 AS b
    ON a.driverId = b.driverId;
    '''
)

display(top_20_drivers)

# COMMAND ----------


