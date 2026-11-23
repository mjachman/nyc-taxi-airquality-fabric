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

# ## [0] PROFILE

# CELL ********************

from pyspark.sql import functions as F
BRONZE_ZONES_TABLE = "lakehouse_bronze.dbo.taxi_zone_lookup"
SILVER_ZONES_TABLE = "lakehouse_silver.dbo.silver_taxi_zones"

df_zones_raw = spark.table(BRONZE_ZONES_TABLE)
df_zones_raw.printSchema()
df_zones_raw.orderBy("LocationID").show(
    df_zones_raw.count(),
    truncate=False
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [1] SCHEMA & TYPES

# CELL ********************

non_snake_case_columns_replacement_dict = {
    "LocationID": "location_id",
    "Borough" : "borough",
    "Zone" : "zone",
}

def rename_columns(df, rename_dict):
   return df.withColumnsRenamed(rename_dict)

df_zones_silver = df_zones_raw.withColumn("LocationID", F.col("LocationID").cast("int"))
df_zones_silver = rename_columns(df_zones_silver,non_snake_case_columns_replacement_dict)
df_zones_silver.printSchema()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [2] QUALITY GATE

# CELL ********************

df_zones_raw.summary("count", "min", "max").show(truncate=False)
df_zones_raw.select("LocationID").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## [3] WRITE DELTA

# CELL ********************

(df_zones_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(SILVER_ZONES_TABLE))

spark.sql("SHOW TABLES IN lakehouse_silver.dbo").show()
print(spark.table(SILVER_ZONES_TABLE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
