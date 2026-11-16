# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5a848d1d-c899-496e-81db-b9757457af41",
# META       "default_lakehouse_name": "lakehouse_silver",
# META       "default_lakehouse_workspace_id": "f74538c4-e1c0-4843-8fd1-9e35f938bafa",
# META       "known_lakehouses": [
# META         {
# META           "id": "5a848d1d-c899-496e-81db-b9757457af41"
# META         },
# META         {
# META           "id": "d6dfd742-9000-4d7b-9a4c-adb901a08cea"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F 
from pyspark.sql.types import DecimalType

BRONZE_TAXI_PATH = "abfss://FabricCourseProject@onelake.dfs.fabric.microsoft.com/lakehouse_bronze.Lakehouse/Files/taxi"
SILVER_TAXI_TABLE = "lakehouse_silver.dbo.silver_taxi_trips"
SILVER_QUARANTINE_TABLE = "lakehouse_silver.dbo.silver_taxi_quarantine"
ALLOWED_MONTHS = ["2024-01", "2025-01", "2026-01"]
MAX_TRIP_DISTANCE = 500
MAX_FARE = 5000
MAX_DURATION_SECONDS = 86400

df_raw_taxi = spark.read.option("mergeSchema", "true").parquet(BRONZE_TAXI_PATH)
df_raw_taxi.printSchema()
print(df_raw_taxi.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_raw_taxi.summary().toPandas().set_index("summary").T.reset_index())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

money_columns = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee"
]

non_snake_case_columns_replacement_dict = {
    "VendorID": "vendor_id",
    "RatecodeID" : "ratecode_id",
    "PULocationID" : "pu_location_id",
    "DOLocationID" : "do_location_id",
    "Airport_fee" : "airport_fee"
}

def cast_money_columns(df, columns):
    changes = {}
    for c in columns:
        changes[c] = F.col(c).cast(DecimalType(10,2))
    return df.withColumns(changes)

def rename_columns(df, rename_dict):
   return df.withColumnsRenamed(rename_dict)



df_taxi_silver = cast_money_columns(df_raw_taxi, money_columns)
df_taxi_silver = rename_columns(df_taxi_silver,non_snake_case_columns_replacement_dict)
df_taxi_silver.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] Quality Gate

# CELL ********************

TAXI_DQ_RULES = [ F.when((F.col("fare_amount") < 0) | (F.col("total_amount") < 0), "NEGATIVE_FARE"),
        F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), "INVALID_TIME"),
        F.when(~F.date_format(F.col("tpep_pickup_datetime"),"yyyy-MM").isin(ALLOWED_MONTHS), "INVALID_DATE"),
        F.when(F.col("trip_distance")>MAX_TRIP_DISTANCE,"INVALID_DISTANCE"),
        F.when(F.col("fare_amount")>MAX_FARE,"EXTREME_FARE"),
        F.when(F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime")) > MAX_DURATION_SECONDS,"EXTREME_DURATION")]

def flag_taxi_quality(df):
    errors = F.concat_ws(
        ", ", *TAXI_DQ_RULES)
    return df.withColumn("dq_error_reason", F.when(errors == "", "OK").otherwise(errors))


df_taxi_silver = flag_taxi_quality(df_taxi_silver)
total = df_taxi_silver.count()
summary = df_taxi_silver.groupBy("dq_error_reason").count()
summary = summary.withColumn("percent", F.round(F.col("count") / total * 100, 2))
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_clean = df_taxi_silver.filter(F.col("dq_error_reason") == "OK")
df_taxi_quarantine = df_taxi_silver.filter(F.col("dq_error_reason") != "OK")
print(df_taxi_clean.count())
print(df_taxi_quarantine.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] DEDUPLICATION

# CELL ********************

DEDUPLICATE_KEYS = ["vendor_id","tpep_pickup_datetime", "tpep_dropoff_datetime", "pu_location_id", "do_location_id", "trip_distance", "total_amount"]
before_count = df_taxi_clean.count()
df_taxi_clean = df_taxi_clean.dropDuplicates(subset = DEDUPLICATE_KEYS)
after_count = df_taxi_clean.count()
print(f"Dropped duplicates: {before_count - after_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [4] ENRICHMENT

# CELL ********************

df_taxi_clean = df_taxi_clean.withColumn("pickup_date",F.to_date(F.col("tpep_pickup_datetime")))
df_taxi_clean = df_taxi_clean.drop("dq_error_reason")
df_taxi_clean = df_taxi_clean.withColumn(
    "trip_duration_minutes",
    F.round(
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        2,
    ),
)
display(df_taxi_clean)
print(df_taxi_clean.count())
df_taxi_clean.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [5] WRITE DELTA

# CELL ********************

df_taxi_clean.select("fare_amount", "total_amount", "trip_distance", "trip_duration_minutes", "pickup_date", "pu_location_id", "do_location_id").summary("count", "min", "max").show()
df_taxi_clean.select(F.count("pickup_date"),F.min("pickup_date"),F.max("pickup_date")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df_taxi_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_TAXI_TABLE))

(df_taxi_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_QUARANTINE_TABLE))

spark.sql("SHOW TABLES IN lakehouse_silver.dbo").show()
print(spark.table(SILVER_TAXI_TABLE).count())
print(spark.table(SILVER_QUARANTINE_TABLE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
