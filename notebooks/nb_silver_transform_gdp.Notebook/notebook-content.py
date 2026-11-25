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

# # Silver Transform: World Bank US GDP

# MARKDOWN ********************

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_GDP_TABLE = "lakehouse_bronze.dbo.bronze_world_bank_gdp"
SILVER_GDP_TABLE = "lakehouse_silver.dbo.silver_world_bank_gdp"
SILVER_GDP_QUARANTINE = (
    "lakehouse_silver.dbo.silver_world_bank_gdp_quarantine"
)

df_raw = spark.table(BRONZE_GDP_TABLE)
df_raw.printSchema()
df_raw.summary().show(truncate=False)
df_raw.agg(
    F.min("date"),
    F.max("date"),
    F.count("date"),
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

RENAMED_COLUMNS = {
    "date": "year",
    "countryiso3code": "country_code",
}
df_schema = df_raw.withColumnsRenamed(RENAMED_COLUMNS)
df_schema = df_schema.select(
    "year",
    "country_code",
    "country_name",
    "indicator_id",
    "indicator_name",
    "value",
)
df_schema = df_schema.withColumn(
    "value",
    F.col("value").cast(DecimalType(18, 2)),
)

df_schema.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

GDP_DQ_RULES = [
    F.when(F.col("country_code").isNull(), "NULL_KEY"),
    F.when(F.col("year").isNull(), "INVALID_YEAR"),
    F.when(
        F.col("value").isNull() | (F.col("value") <= 0),
        "INVALID_GDP",
    ),
]


def flag_gdp_quality(df):
    errors = F.concat_ws(", ", *GDP_DQ_RULES)
    tag = F.when(errors == "", "OK").otherwise(errors)
    return df.withColumn("dq_error_reason", tag)


df_dq = flag_gdp_quality(df_schema)
total = df_dq.count()

df_clean = df_dq.filter(
    F.col("dq_error_reason") == "OK"
).drop("dq_error_reason")
df_quarantine = df_dq.filter(
    F.col("dq_error_reason") != "OK"
)

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
    ["year", "country_code", "indicator_id"]
)
print(f"{df_clean.count()} | {df_final.count()}")

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
    .saveAsTable(SILVER_GDP_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_GDP_QUARANTINE)
)

print(spark.table(SILVER_GDP_TABLE).count())
print(spark.table(SILVER_GDP_QUARANTINE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
