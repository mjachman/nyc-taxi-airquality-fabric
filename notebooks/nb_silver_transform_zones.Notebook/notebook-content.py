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
# META           "id": "d6dfd742-9000-4d7b-9a4c-adb901a08cea"
# META         },
# META         {
# META           "id": "5a848d1d-c899-496e-81db-b9757457af41"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver Transform: NYC Taxi Zones

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F

BRONZE_ZONES_TABLE = "lakehouse_bronze.dbo.taxi_zone_lookup"
SILVER_ZONES_TABLE = "lakehouse_silver.dbo.silver_taxi_zones"
SILVER_ZONES_QUARANTINE = (
    "lakehouse_silver.dbo.silver_taxi_zones_quarantine"
)

df_raw = spark.table(BRONZE_ZONES_TABLE)
df_raw.printSchema()
df_raw.summary("count", "min", "max").show(truncate=False)
print(df_raw.select("LocationID").distinct().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

COLUMN_RENAMES = {
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
}


def rename_columns(df, rename_dict):
    return df.withColumnsRenamed(rename_dict)


df_schema = df_raw.withColumn(
    "LocationID",
    F.col("LocationID").cast("int"),
)
df_schema = rename_columns(df_schema, COLUMN_RENAMES)
df_schema.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

ZONES_DQ_RULES = [
    F.when(F.col("location_id").isNull(), "NULL_LOCATION_ID"),
    F.when(F.col("borough").isNull(), "NULL_BOROUGH"),
    F.when(F.col("zone").isNull(), "NULL_ZONE"),
]


def flag_zones_quality(df):
    errors = F.concat_ws(", ", *ZONES_DQ_RULES)
    tag = F.when(errors == "", "OK").otherwise(errors)
    return df.withColumn("dq_error_reason", tag)


df_dq = flag_zones_quality(df_schema)
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
    subset=["location_id"]
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
    .saveAsTable(SILVER_ZONES_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ZONES_QUARANTINE)
)

print(spark.table(SILVER_ZONES_TABLE).count())
print(spark.table(SILVER_ZONES_QUARANTINE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
