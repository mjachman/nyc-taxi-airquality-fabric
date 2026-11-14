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

# CELL ********************

BRONZE_TAXI_PATH = "abfss://FabricCourseProject@onelake.dfs.fabric.microsoft.com/lakehouse_bronze.Lakehouse/Files/taxi"
df_raw_taxi = spark.read.option("mergeSchema", "true").parquet(BRONZE_TAXI_PATH)
df_raw_taxi.printSchema()
print(df_raw_taxi.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_raw_taxi.summary().toPandas().set_index("summary").T.reset_index())

# CELL ********************

#display(df_raw_taxi.limit(15))
spark.sql("SHOW TABLES IN lakehouse_bronze.dbo").show()
