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

# # Silver Transform: OpenAQ Measurements

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

import re

from pyspark.sql import functions as F

BRONZE_MEASUREMENTS_TABLE = "lakehouse_bronze.dbo.bronze_openaq_measurements"
SILVER_MEASUREMENTS_TABLE = (
    "lakehouse_silver.dbo.silver_openaq_measurements"
)
SILVER_LOCATIONS_TABLE = "lakehouse_silver.dbo.silver_openaq_locations"
SILVER_QUARANTINE_TABLE = (
    "lakehouse_silver.dbo.silver_openaq_measurements_quarantine"
)

df_raw = spark.table(BRONZE_MEASUREMENTS_TABLE)
df_raw.printSchema()
df_raw.agg(
    F.min("utc"),
    F.max("utc"),
    F.min("local"),
    F.max("local"),
).show(truncate=False)

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************


def remove_dots(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.replace(".", "_"))
    return df


def to_snake_case(df):
    pattern = r"([a-z0-9])([A-Z])"
    for c in df.columns:
        df = df.withColumnRenamed(c, re.sub(pattern, r"\1_\2", c).lower())
    return df


df_schema = remove_dots(df_raw)
df_schema = to_snake_case(df_schema)
df_schema.printSchema()

# CELL ********************

df_openaq_locations = spark.table(SILVER_LOCATIONS_TABLE)
df_schema.filter(F.col("local").isNull()).show(truncate=False)
df_schema.groupBy("has_flags").count().show()
(
    df_schema
    .join(df_openaq_locations, on="sensor_id", how="left")
    .groupBy("parameter")
    .agg(F.count("value"), F.min("value"), F.max("value"))
    .show(truncate=False)
)

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

OPENAQ_DQ_RULES = [
    F.when(F.col("value").isNull() | F.col("utc").isNull(), "NULL_VALUE"),
    F.when(
        F.col("local").isNull()
        | F.to_date(F.substring(F.col("local"), 1, 10)).isNull(),
        "NULL_LOCAL_TIME",
    ),
    F.when(F.col("sensor_id").isNull(), "NULL_KEY"),
    F.when(F.col("parameter").isNull(), "UNKNOWN_SENSOR"),
    F.when(F.col("has_flags"), "HAS_FLAGS"),
    F.when(
        (F.col("parameter") != "temperature") & (F.col("value") < 0),
        "NEGATIVE_POLLUTANT",
    ),
]


def validate_openaq_quality(df, rules):
    raw_tags = F.concat_ws(", ", *rules)
    final_tag = F.when(raw_tags == "", "OK").otherwise(raw_tags)
    return df.withColumn("dq_error_reason", final_tag)


df_joined = df_schema.join(
    df_openaq_locations.select("sensor_id", "parameter"),
    on="sensor_id",
    how="left",
)
df_dq = validate_openaq_quality(
    df_joined,
    OPENAQ_DQ_RULES,
)

df_clean = df_dq.filter(
    F.col("dq_error_reason") == "OK"
)
df_quarantine = df_dq.filter(
    F.col("dq_error_reason") != "OK"
)

total = df_dq.count()
clean = df_clean.count()
dirty = df_quarantine.count()
clean_pct = clean / total * 100

print(
    f"Total: {total} | Clean: {clean} | Quarantine: {dirty} | "
    f"Clean %: ({clean_pct:.2f}%) | Diff: {total - (clean + dirty)}"
)

summary = df_dq.groupBy("dq_error_reason").count()
summary = summary.withColumn(
    "percent",
    F.round(F.col("count") / total * 100, 2),
)
summary.show(truncate=False)

# MARKDOWN ********************

# ## [3] DEDUPLICATE

# CELL ********************

df_final = (
    df_clean
    .withColumn(
        "measurement_date",
        F.to_date(F.substring(F.col("local"), 1, 10)),
    )
    .dropDuplicates(["sensor_id", "measurement_date"])
    .drop("dq_error_reason", "parameter")
)
print(f"Before: {df_clean.count()} | After: {df_final.count()}")

# MARKDOWN ********************

# ## [4] WRITE DELTA

# CELL ********************

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_MEASUREMENTS_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_QUARANTINE_TABLE)
)

print(spark.table(SILVER_MEASUREMENTS_TABLE).count())
print(spark.table(SILVER_QUARANTINE_TABLE).count())

