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

# # Silver Transform: ECB FX Rates

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_ECB_FX_TABLE = "lakehouse_bronze.dbo.bronze_ecb_fx"
SILVER_ECB_FX_TABLE = "lakehouse_silver.dbo.silver_ecb_fx"
SILVER_ECB_FX_QUARANTINE = "lakehouse_silver.dbo.silver_ecb_fx_quarantine"

df_raw = spark.table(BRONZE_ECB_FX_TABLE)
df_raw.printSchema()
df_raw.summary().show(vertical=True)
df_raw.agg(
    F.min("TIME_PERIOD"),
    F.max("TIME_PERIOD"),
    F.count("TIME_PERIOD"),
).show()


# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

RENAMED_COLUMNS = {
    "TIME_PERIOD": "rate_date",
    "CURRENCY": "currency",
    "CURRENCY_DENOM": "currency_denom",
    "OBS_VALUE": "exchange_rate",
    "OBS_STATUS": "obs_status",
}
df_schema = df_raw.select(list(RENAMED_COLUMNS))
df_schema = df_schema.withColumnsRenamed(RENAMED_COLUMNS)
df_schema = df_schema.withColumn(
    "exchange_rate",
    F.col("exchange_rate").cast(DecimalType(10, 4)),
)
df_schema.printSchema()


# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

FX_DQ_RULES = [
    F.when(F.col("rate_date").isNull(), "INVALID_DATE"),
    F.when(
        F.col("exchange_rate").isNull() | (F.col("exchange_rate") <= 0),
        "INVALID_EXCHANGE_RATE",
    ),
]


def flag_fx_quality(df):
    errors = F.concat_ws(", ", *FX_DQ_RULES)
    tag = F.when(errors == "", "OK").otherwise(errors)
    return df.withColumn("dq_error_reason", tag)


df_dq = flag_fx_quality(df_schema)
total = df_dq.count()

df_clean = df_dq.filter(
    F.col("dq_error_reason") == "OK"
).drop("dq_error_reason", "obs_status")
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

# MARKDOWN ********************

# ## [3] DEDUPLICATE

# CELL ********************

df_final = df_clean.dropDuplicates(["rate_date"])
print(f"{df_clean.count()} | {df_final.count()}")

# MARKDOWN ********************

# ## [4] WRITE DELTA

# CELL ********************

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ECB_FX_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ECB_FX_QUARANTINE)
)

print(spark.table(SILVER_ECB_FX_TABLE).count())
print(spark.table(SILVER_ECB_FX_QUARANTINE).count())
