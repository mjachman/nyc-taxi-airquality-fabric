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
BRONZE_LOCATIONS_TABLE = "lakehouse_bronze.dbo.bronze_openaq_locations"
SILVER_LOCATIONS_TABLE = "lakehouse_silver.dbo.silver_openaq_locations"

df_locations_raw = spark.table(BRONZE_LOCATIONS_TABLE)
df_locations_raw.printSchema()
df_locations_raw.orderBy("sensor_id").show(
    df_locations_raw.count(),
    truncate=False
)
print(df_locations_raw.count())
df_locations_raw.summary("count","min","max").show()

df_locations_distinct = df_locations_raw.select("sensor_id").distinct()
df_locations_distinct.show(1000,truncate=False)
print(df_locations_distinct.count())
print(df_locations_raw.select("id").distinct().count())
df_locations_raw.summary().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_locations_raw.select("parameter","units").distinct().show(truncate=False)
df_locations_raw.agg(F.max("datetime_first_utc"),
                     F.max("datetime_last_utc"),
                     F.min("datetime_first_utc"),
                     F.min("datetime_last_utc")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

non_snake_case_columns_replacement_dict = {
    "ID": "location_id",
    "isMonitor" : "is_monitor",
}

def rename_columns(df, rename_dict):
   return df.withColumnsRenamed(rename_dict)

df_locations_silver = rename_columns(df_locations_raw,non_snake_case_columns_replacement_dict)
df_locations_silver = df_locations_silver.dropDuplicates(subset = ["sensor_id"])
df_locations_silver = df_locations_silver.fillna({"locality" : "Unknown"})
df_locations_silver.printSchema()
df_locations_silver.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] WRITE DELTA

# CELL ********************

(df_locations_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_LOCATIONS_TABLE))

spark.sql("SHOW TABLES IN lakehouse_silver.dbo").show()
print(spark.table(SILVER_LOCATIONS_TABLE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
