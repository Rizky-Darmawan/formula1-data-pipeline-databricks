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

# GET TOP 20 DRIVERS OF ALL TIME IN TERMS OF NUMBER OF WINS AND SHOW THEIR AVERAGE AND MEDIAN QUALIFYING POSITION
top_20_drivers = spark.sql(
    '''
    WITH get_drivers_that_have_won AS (
        SELECT * FROM results WHERE positionOrder = 1
    ),

    count_number_of_wins AS (
        SELECT driverId, COUNT(*) AS winCount FROM get_drivers_that_have_won GROUP BY driverId ORDER BY winCount DESC LIMIT 20
    ),

    get_details_of_drivers AS (
        SELECT
            a.driverId,
            b.forename,
            b.surname,
            b.dob,
            b.nationality,
            a.winCount 
        FROM count_number_of_wins AS a
        JOIN drivers AS b
        ON a.driverId = b.driverId
        ORDER BY a.winCount DESC
    ),

    get_average_and_median_qualifying_results AS (
        SELECT 
            driverId, 
            ROUND(AVG(position), 0) AS avgPosition,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY position), 0) AS medianPosition
        FROM qualifying 
        GROUP BY driverId
    )

    SELECT 
        a.*,
        b.avgPosition,
        b.medianPosition
    FROM get_details_of_drivers AS a
    JOIN get_average_and_median_qualifying_results AS b
    ON a.driverId = b.driverId;
    '''
)

display(top_20_drivers)

# COMMAND ----------

# GET TOP 5 BEST TEAMS AND 5 WORST TEAMS (IN TERMS OF AVERAGE GRID POSITION)
best_and_worst_teams = spark.sql(
    '''
    WITH count_average_position AS (
        SELECT constructorId, ROUND(AVG(position), 0) AS avgPosition FROM qualifying GROUP BY constructorId
    ),
    
    get_5_best_teams AS (
        SELECT *, 'best' AS flag FROM count_average_position ORDER BY avgPosition LIMIT 5
    ),

    get_5_worst_teams AS (
        SELECT *, 'worst' AS flag FROM count_average_position ORDER BY avgPosition DESC LIMIT 5
    ),

    union_best_and_worst AS (
        SELECT * FROM get_5_best_teams UNION ALL SELECT * FROM get_5_worst_teams
    )

    SELECT 
        a.constructorId,
        b.name,
        b.nationality,
        a.flag,
        a.avgPosition
    FROM union_best_and_worst AS a
    JOIN constructors AS b
    ON a.constructorId = b.constructorId
    ORDER BY avgPosition;
    '''
)

display(best_and_worst_teams)

# COMMAND ----------

# GET TEAMS FROM 2016-2023 AND THEIR RESPECTIVE STATISTICS OF THEIR PIT STOPS TIME
teams_pit_time_2016_2023 = spark.sql(
    '''
    WITH filter_races AS (
        SELECT * FROM races WHERE date BETWEEN "2016-01-01" AND "2023-12-31"
    ),
    
    get_race_date(
        SELECT
            a.*,
            b.date
        FROM results AS a
        JOIN filter_races AS b
        ON a.raceId = b.raceId
    ),
    
    get_constructors_id AS (
        SELECT
            a.*,
            b.constructorId
        FROM pit_stops AS a
        JOIN get_race_date AS b
        ON a.driverId = b.driverId AND a.raceId = b.raceId
    ),

    count_average_pit_time AS (
        SELECT 
            constructorId, 
            ROUND(AVG(milliseconds) / 1000, 0) AS avgPitTime,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY milliseconds) / 1000, 0) AS medianPitTime,
            ROUND(MIN(milliseconds) / 1000, 0) AS fastestPitTime,
            ROUND(MAX(milliseconds) / 1000, 0) AS longestPitTime
        FROM get_constructors_id 
        GROUP BY constructorId
    )

    SELECT
        a.constructorId,
        b.name,
        b.nationality,
        a.avgPitTime,
        a.medianPitTime,
        a.fastestPitTime,
        a.longestPitTime
    FROM count_average_pit_time AS a
    JOIN constructors AS b
    ON a.constructorId = b.constructorId
    '''
)

display(teams_pit_time_2016_2023)

# COMMAND ----------

from pyspark.sql import *
import pandas as pd

jdbcHostname = "formula1-dwh.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "formula1-dwh"
properties = {
 "user" : "formula1",
 "password" : dbutils.secrets.get(scope="formula1-data-pipeline", key="azure-sql-password") }

url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

top_20_drivers_write = DataFrameWriter(top_20_drivers)
best_and_worst_teams_write = DataFrameWriter(best_and_worst_teams)
teams_pit_time_2016_2023_write = DataFrameWriter(teams_pit_time_2016_2023)

top_20_drivers_write.jdbc(url = url, table = "top_20_drivers", mode ="overwrite", properties = properties)
best_and_worst_teams_write.jdbc(url = url, table = "best_and_worst_teams", mode ="overwrite", properties = properties)
teams_pit_time_2016_2023_write.jdbc(url = url, table = "teams_pit_time_2016_2023", mode ="overwrite", properties = properties)

# COMMAND ----------


