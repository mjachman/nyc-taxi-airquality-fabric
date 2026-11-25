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

# # Silver Transform: NYC Taxi Trips

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

import re

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_TAXI_PATH = (
    "abfss://FabricCourseProject@onelake.dfs.fabric.microsoft.com/"
    "lakehouse_bronze.Lakehouse/Files/taxi"
)
SILVER_TAXI_TABLE = "lakehouse_silver.dbo.silver_taxi_trips"
SILVER_QUARANTINE_TABLE = "lakehouse_silver.dbo.silver_taxi_quarantine"

MAX_TRIP_DISTANCE = 500
MAX_FARE = 5000
MAX_DURATION_SECONDS = 86400
SECONDS_PER_MINUTE = 60

INT_COLUMNS = [
    "vendor_id",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "ratecode_id",
    "payment_type",
]
RENAMED_BRONZE = {
    "VendorID": "vendor_id",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}


def cast_int_columns(df, columns):
    for col_name in columns:
        df = df.withColumn(col_name, F.col(col_name).cast("bigint"))
    return df


def read_taxi_file(file_info):
    df = spark.read.parquet(file_info.path).withColumnsRenamed(RENAMED_BRONZE)
    df = cast_int_columns(df, INT_COLUMNS)
    m = re.search(r"\d{4}-\d{2}", file_info.name)
    return df.withColumn("expected_month", F.lit(m.group(0) if m else ""))


df_raw = None

for file_info in mssparkutils.fs.ls(BRONZE_TAXI_PATH):
    if file_info.name.endswith(".parquet"):
        df_part = read_taxi_file(file_info)
        if df_raw is None:
            df_raw = df_part
        else:
            df_raw = df_raw.unionByName(
                df_part,
                allowMissingColumns=True,
            )

df_raw.printSchema()
print(df_raw.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw.summary().show(vertical=True)
df_raw.agg(
    F.min("tpep_pickup_datetime"),
    F.max("tpep_pickup_datetime"),
    F.min("tpep_dropoff_datetime"),
    F.max("tpep_dropoff_datetime"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

MONEY_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


def cast_money_columns(df, columns):
    for c in columns:
        val = F.col(c) if c in df.columns else F.lit(0)
        df = df.withColumn(c, val.cast(DecimalType(10, 2)))
    return df


def convert_ntz_to_timestamp(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "timestamp_ntz":
            df = df.withColumn(field.name, F.col(field.name).cast("timestamp"))
    return df


df_schema = cast_money_columns(df_raw, MONEY_COLUMNS)
df_schema = convert_ntz_to_timestamp(df_schema)
df_schema = df_schema.fillna({"cbd_congestion_fee": 0, "airport_fee": 0})
df_schema.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

TAXI_DQ_RULES = [
    F.when(
        F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | F.col("pu_location_id").isNull()
        | F.col("do_location_id").isNull()
        | F.col("fare_amount").isNull()
        | F.col("total_amount").isNull()
        | F.col("trip_distance").isNull(),
        "NULL_CRITICAL_FIELD",
    ),
    F.when(
        (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
        "NEGATIVE_FARE",
    ),
    F.when(
        F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
        "INVALID_TIME",
    ),
    F.when(F.col("expected_month") == "", "MISSING_FILE_MONTH"),
    F.when(
        (F.col("expected_month") != "")
        & (
            F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM")
            != F.col("expected_month")
        ),
        "INVALID_DATE",
    ),
    F.when(
        (F.col("trip_distance") < 0)
        | (F.col("trip_distance") > MAX_TRIP_DISTANCE),
        "INVALID_DISTANCE",
    ),
    F.when(F.col("fare_amount") > MAX_FARE, "EXTREME_FARE"),
    F.when(
        (
            F.unix_timestamp(F.col("tpep_dropoff_datetime"))
            - F.unix_timestamp(F.col("tpep_pickup_datetime"))
        )
        > MAX_DURATION_SECONDS,
        "EXTREME_DURATION",
    ),
]


def flag_taxi_quality(df):
    errors = F.concat_ws(", ", *TAXI_DQ_RULES)
    tag = F.when(errors == "", "OK").otherwise(errors)
    return df.withColumn("dq_error_reason", tag)


df_dq = flag_taxi_quality(df_schema)
total = df_dq.count()
summary = df_dq.groupBy("dq_error_reason").count()
summary = summary.withColumn(
    "percent",
    F.round(F.col("count") / total * 100, 2),
)
summary.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = df_dq.filter(F.col("dq_error_reason") == "OK")
df_quarantine = df_dq.filter(F.col("dq_error_reason") != "OK")
clean = df_clean.count()
dirty = df_quarantine.count()
clean_pct = clean / total * 100
print(
    f"Total: {total} | Clean: {clean} | Quarantine: {dirty} | "
    f"Clean %: ({clean_pct:.2f}%) | Diff: {total - (clean + dirty)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] DEDUPLICATE

# CELL ********************

DEDUPLICATE_KEYS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "trip_distance",
    "total_amount",
]
before_count = df_clean.count()
df_deduped = df_clean.dropDuplicates(subset=DEDUPLICATE_KEYS)
after_count = df_deduped.count()
print(f"Dropped duplicates: {before_count - after_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [4] ENRICH

# CELL ********************

duration_seconds = (
    F.unix_timestamp("tpep_dropoff_datetime")
    - F.unix_timestamp("tpep_pickup_datetime")
)
df_final = (
    df_deduped
    .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        F.round(duration_seconds / SECONDS_PER_MINUTE, 2),
    )
    .drop("dq_error_reason", "expected_month")
)
print(df_final.count())
df_final.printSchema()
df_final.select(
    "fare_amount",
    "total_amount",
    "trip_distance",
    "trip_duration_minutes",
    "pu_location_id",
    "do_location_id",
).summary("count", "min", "max").show()
df_final.select(
    F.count("pickup_date"),
    F.min("pickup_date"),
    F.max("pickup_date"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [5] WRITE DELTA

# CELL ********************

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TAXI_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_QUARANTINE_TABLE)
)

print(spark.table(SILVER_TAXI_TABLE).count())
print(spark.table(SILVER_QUARANTINE_TABLE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
