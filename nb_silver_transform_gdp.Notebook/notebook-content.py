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

BRONZE_ECB_FX_TABLE = "lakehouse_bronze.dbo.bronze_ecb_fx"
SILVER_ECB_FX_TABLE = "lakehouse_silver.dbo.silver_ecb_fx"
SILVER_ECB_FX_QUARANTINE = "lakehouse_silver.dbo.silver_ecb_fx_quarantine"

df_fx_raw = spark.table(BRONZE_ECB_FX_TABLE)
df_fx_raw.printSchema()
df_fx_raw.summary().show(vertical = True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

RENAMED_COLUMNS = {
"TIME_PERIOD" : "rate_date",
"CURRENCY" : "currency",
"CURRENCY_DENOM" : "currency_denom",
"OBS_VALUE" : "exchange_rate",
"OBS_STATUS" : "obs_status"
}
df_fx_silver = df_fx_raw.select("TIME_PERIOD","CURRENCY","CURRENCY_DENOM","OBS_VALUE","OBS_STATUS")
df_fx_silver = df_fx_silver.withColumnsRenamed(RENAMED_COLUMNS)
df_fx_silver = df_fx_silver.withColumn("exchange_rate",F.col("exchange_rate").cast(DecimalType(10,4)))
df_fx_silver.printSchema()
df_fx_silver.show(df_fx_silver.count(),truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

FX_DQ_RULES = [F.when((F.col("rate_date").isNull()), "INVALID_DATE"),
               F.when(F.col("exchange_rate").isNull() | (F.col("exchange_rate") <= 0), "INVALID_EXCHANGE_RATE")]

def flag_fx_quality(df):
    errors = F.concat_ws(
        ", ", *FX_DQ_RULES)
    return df.withColumn("dq_error_reason", F.when(errors == "", "OK").otherwise(errors))


df_fx_total= flag_fx_quality(df_fx_silver)
total = df_fx_total.count()

summary = df_fx_total.groupBy("dq_error_reason").count()
summary = summary.withColumn("percent", F.round(F.col("count") / total * 100, 2))

df_fx_clean = df_fx_total.filter(F.col("dq_error_reason") == "OK").drop("dq_error_reason","obs_status")

df_fx_quarantine = df_fx_total.filter(F.col("dq_error_reason") != "OK")
summary.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] DEDUPLICATE

# CELL ********************

df_fx_dedup = df_fx_clean.dropDuplicates(["rate_date"])
df_fx_dedup.show()
print(f"{df_fx_clean.count()} | {df_fx_dedup.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [4] WRITE DELTA

# CELL ********************

(df_fx_dedup.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_ECB_FX_TABLE))

(df_fx_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_ECB_FX_QUARANTINE))

spark.sql("SHOW TABLES IN lakehouse_silver.dbo").show()
print(spark.table(SILVER_ECB_FX_TABLE).count())
print(spark.table(SILVER_ECB_FX_QUARANTINE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
