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
import re
BRONZE_MEASUREMENTS_TABLE = "lakehouse_bronze.dbo.bronze_openaq_measurements"
SILVER_MEASUREMENTS_TABLE = "lakehouse_silver.dbo.silver_openaq_measurements"
SILVER_LOCATIONS_TABLE = "lakehouse_silver.dbo.silver_openaq_locations"
SILVER_QUARANTINE_TABLE = "lakehouse_silver.dbo.silver_openaq_measurements_quarantine"

df_mesurements_raw = spark.table(BRONZE_MEASUREMENTS_TABLE)
df_mesurements_raw.printSchema()
# df_mesurements_raw.orderBy("sensor_id").show(
#     df_mesurements_raw.count(),
#     truncate=False
# )
# print(df_mesurements_raw.count())
# display(df_mesurements_raw.summary())

# df_locations_distinct = df_mesurements_raw.select("sensor_id").distinct()
# df_locations_distinct.show(1000,truncate=False)
# print(df_locations_distinct.count())
# print(df_mesurements_raw.select("id").distinct().count())
# df_mesurements_raw.summary().show()

# MARKDOWN ********************

# ## [1] PROFILE

# CELL ********************

def remove_dots(df):
    changes = {}
    for c in df.columns:
        changes[c] = c.replace(".","_")       
    return df.withColumnsRenamed(changes)

def to_snake_case(df):
    changes = {}
    for c in df.columns:
            changes[c] = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", c).lower()     
    return df.withColumnsRenamed(changes)

df_measurements_silver = remove_dots(df_mesurements_raw)
df_measurements_silver = to_snake_case(df_measurements_silver)
df_measurements_silver.printSchema()
display(df_measurements_silver.summary())
df_measurements_silver.summary().show(truncate=False)

# CELL ********************

df_measurements_silver.filter(F.col("local").isNull()).show(truncate=False)
df_measurements_silver.groupBy("has_flags").count().show()
(df_measurements_silver.
 join(spark.table("lakehouse_silver.dbo.silver_openaq_locations"), on="sensor_id", how ="left").
groupBy("parameter").agg(F.count("value"), F.min("value"), F.max("value")).show(truncate=False))

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

OPENAQ_DQ_RULES = [
    F.when(F.col("value").isNull() | F.col("utc").isNull(), "NULL_VALUE"),
    F.when(F.col("sensor_id").isNull(), "NULL_KEY"),
    F.when(F.col("has_flags"), "HAS_FLAGS"),
    F.when((F.col("parameter") != "temperature") & (F.col("value") < 0),"NEGATIVE_POLLUTANT")
]

def validate_openaq_quality(df, rules):
    raw_tags = F.concat_ws(", ", *rules)
    final_tag = F.when(raw_tags == "", "OK").otherwise(raw_tags)
    return df.withColumn("dq_error_reason", final_tag)

df_openaq_locations = spark.table(SILVER_LOCATIONS_TABLE)

df_measurements_joined = df_measurements_silver.join(df_openaq_locations.select("sensor_id", "parameter"), on = "sensor_id", how = "left")
df_measurements_validated = validate_openaq_quality(df_measurements_joined,OPENAQ_DQ_RULES)

df_measurements_clean = df_measurements_validated.filter(F.col("dq_error_reason") == "OK")
df_measurements_quarantine = df_measurements_validated.filter(F.col("dq_error_reason") != "OK")

total, clean, dirty = df_measurements_validated.count(), df_measurements_clean.count(), df_measurements_quarantine.count()

print(f"Total: {total} | Clean: {clean} | Dirty: {dirty} | Clean %: ({clean / total * 100:.2f}%) | Total - (Clean + Dirty): {total - (clean + dirty)}")


df_measurements_validated.groupBy("dq_error_reason").count().show(truncate=False)

df_measurements_clean.filter(F.col("value") < 0).select("parameter").distinct().show(truncate=False)

df_measurements_silver.summary().show(truncate=False)




# MARKDOWN ********************

# ## [3] DEDUPLICATION

# CELL ********************

df_deduped = df_measurements_clean.dropDuplicates(["sensor_id","utc"])
print(f"Before: {df_measurements_clean.count()} | After: {df_deduped.count()}")

# MARKDOWN ********************

# ## [4] ENRICHMENT

# CELL ********************

df_enriched = (df_deduped
    .withColumn("measurement_date", F.to_date(F.substring(F.col("local"), 1, 10)))
    .drop("dq_error_reason", "parameter"))

df_enriched.show()

# MARKDOWN ********************

# ## [5] WRITE DELTA

# CELL ********************

print(spark.table(SILVER_LOCATIONS_TABLE).columns)
spark.sql(f"DROP TABLE IF EXISTS {SILVER_MEASUREMENTS_TABLE}")
(df_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_MEASUREMENTS_TABLE))

spark.sql(f"DROP TABLE IF EXISTS {SILVER_QUARANTINE_TABLE}")
(df_measurements_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_QUARANTINE_TABLE))

spark.sql("SHOW TABLES IN lakehouse_silver.dbo").show()


print(spark.table(SILVER_MEASUREMENTS_TABLE).count())
print(spark.table(SILVER_QUARANTINE_TABLE).count())

