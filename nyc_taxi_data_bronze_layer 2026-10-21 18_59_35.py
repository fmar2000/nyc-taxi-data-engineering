# Databricks notebook source
# Read your uploaded workspace file directly into PySpark
file_path = "/Workspace/Users/farisramjan@gmail.com/yellow_tripdata_2025-01.parquet"

df = spark.read.parquet(file_path)

# View the schema and first 5 rows
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Point to all 12 monthly parquet files simultaneously
import os
folder = "/Workspace/Users/farisramjan@gmail.com"
parquet_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]

# Read everything into a unified Spark DataFrame
df = spark.read.parquet(*parquet_files)

# Check total row count across all of 2025
total_rows = df.count()
print(f"Total rows loaded for all of 2025: {total_rows:,}")

# Save the full year into your Bronze Delta table
df.write.format("delta").mode("overwrite").saveAsTable("default.bronze_nyc_taxi_2025")

print("Full-year Bronze Delta table created successfully!")

# Preview the data
display(df.limit(5))

# COMMAND ----------

# Read the zone lookup CSV file from your workspace
lookup_path = "file:/Workspace/Users/farisramjan@gmail.com/taxi_zone_lookup.csv"

zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(lookup_path)

# Save it as a Bronze dimension table
zone_df.write.format("delta").mode("overwrite").saveAsTable("default.bronze_taxi_zones")

print("Zone lookup table loaded successfully!")
display(zone_df.limit(5))