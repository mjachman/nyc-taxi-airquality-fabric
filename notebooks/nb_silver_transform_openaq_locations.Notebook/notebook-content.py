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

# # Silver Transform: OpenAQ Locations

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F

BRONZE_LOCATIONS_TABLE = "lakehouse_bronze.dbo.bronze_openaq_locations"
SILVER_LOCATIONS_TABLE = "lakehouse_silver.dbo.silver_openaq_locations"
SILVER_LOCATIONS_QUARANTINE = (
    "lakehouse_silver.dbo.silver_openaq_locations_quarantine"
)

df_raw = spark.table(BRONZE_LOCATIONS_TABLE)
df_raw.printSchema()
print(df_raw.count())
df_raw.summary("count", "min", "max").show()

print(df_raw.select("sensor_id").distinct().count())
print(df_raw.select("id").distinct().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw.select("parameter", "units").distinct().show(truncate=False)
df_raw.agg(
    F.max("datetime_first_utc"),
    F.max("datetime_last_utc"),
    F.min("datetime_first_utc"),
    F.min("datetime_last_utc"),
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

COLUMN_RENAMES = {
    "id": "location_id",
    "isMonitor": "is_monitor",
}


def rename_columns(df, rename_dict):
    return df.withColumnsRenamed(rename_dict)


df_schema = rename_columns(df_raw, COLUMN_RENAMES)
df_schema = df_schema.fillna({"locality": "Unknown"})
df_schema.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

LOCATIONS_DQ_RULES = [
    F.when(F.col("sensor_id").isNull(), "NULL_SENSOR_ID"),
    F.when(F.col("location_id").isNull(), "NULL_LOCATION_ID"),
    F.when(F.col("parameter").isNull(), "NULL_PARAMETER"),
    F.when(F.col("units").isNull(), "NULL_UNITS"),
]


def flag_locations_quality(df):
    errors = F.concat_ws(", ", *LOCATIONS_DQ_RULES)
    tag = F.when(errors == "", "OK").otherwise(errors)
    return df.withColumn("dq_error_reason", tag)


df_dq = flag_locations_quality(df_schema)
total = df_dq.count()
df_clean = df_dq.filter(F.col("dq_error_reason") == "OK")
df_quarantine = df_dq.filter(F.col("dq_error_reason") != "OK")
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] DEDUPLICATE

# CELL ********************

df_final = df_clean.dropDuplicates(
    subset=["sensor_id"]
).drop("dq_error_reason")
print(f"Before: {clean} | After: {df_final.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [4] WRITE DELTA

# CELL ********************

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_LOCATIONS_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_LOCATIONS_QUARANTINE)
)

print(spark.table(SILVER_LOCATIONS_TABLE).count())
print(spark.table(SILVER_LOCATIONS_QUARANTINE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
