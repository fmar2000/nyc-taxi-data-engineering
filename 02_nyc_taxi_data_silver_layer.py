# Databricks notebook source
from pyspark.sql import functions as F

# Load raw bronze table
bronze_df = spark.table("default.bronze_nyc_taxi_2025")

# Filter for any records where the pickup year is 2026 (or anything >= 2026-01-01)
stray_dates_df = bronze_df.filter(
    F.col("tpep_pickup_datetime") >= "2026-01-01"
)

# Count how many stray records exist
stray_count = stray_dates_df.count()
print(f"Total stray records found in 2026 or later: {stray_count:,}")

# If any exist, display distinct dates to see what slipped in
if stray_count > 0:
    print("Previewing distinct dates found outside 2025:")
    stray_dates_df.select(
        F.to_date("tpep_pickup_datetime").alias("stray_date")
    ).distinct().orderBy("stray_date").show(20)
else:
    print("Clean as a whistle! No stray 2026 dates found in your Bronze table.")

# COMMAND ----------

from pyspark.sql import functions as F

# Load your raw bronze table
bronze_df = spark.table("default.bronze_nyc_taxi_2025")

print(f"Total raw records inspected: {bronze_df.count():,}\n")

# 1. Out-of-range / stray dates (outside of 2025)
date_anomalies = bronze_df.filter(
    (F.col("tpep_pickup_datetime") < "2025-01-01") | 
    (F.col("tpep_pickup_datetime") >= "2026-01-01")
).count()

# 2. Drop-off happened before or at the same time as pickup
time_anomalies = bronze_df.filter(
    F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")
).count()

# 3. Zero or negative passenger counts
passenger_anomalies = bronze_df.filter(
    F.col("passenger_count") <= 0
).count()

# 4. Zero or negative trip distances
distance_anomalies = bronze_df.filter(
    F.col("trip_distance") <= 0
).count()

# 5. Zero or negative fare / total amounts
fare_anomalies = bronze_df.filter(
    (F.col("fare_amount") <= 0) | (F.col("total_amount") <= 0)
).count()

# Print out a clean summary report of the anomalies found
print("--- ANOMALY AUDIT REPORT ---")
print(f"Stray Dates (Outside 2025):        {date_anomalies:,}")
print(f"Timeline Bugs (Dropoff <= Pickup): {time_anomalies:,}")
print(f"Invalid Passenger Counts (<= 0):   {passenger_anomalies:,}")
print(f"Invalid Distances (<= 0 miles):    {distance_anomalies:,}")
print(f"Invalid Fares/Totals (<= $0):      {fare_anomalies:,}")

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Load Bronze Tables
bronze_trips = spark.table("default.bronze_nyc_taxi_2025")
bronze_zones = spark.table("default.bronze_taxi_zones")

# 2. Clean & Filter Anomaly Data
silver_cleaned = bronze_trips.filter(
    (F.col("tpep_pickup_datetime") >= "2025-01-01") & 
    (F.col("tpep_pickup_datetime") < "2026-01-01") &
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
    (F.col("passenger_count") > 0) &
    (F.col("trip_distance") > 0) &
    (F.col("fare_amount") > 0) &
    (F.col("total_amount") > 0)
)

# 3. Add Calculated Feature Columns
silver_enriched = silver_cleaned.withColumn(
    "trip_duration_minutes",
    F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2)
).withColumn(
    "pickup_date", F.to_date("tpep_pickup_datetime")
).withColumn(
    "pickup_hour", F.hour("tpep_pickup_datetime")
).withColumn(
    "pickup_day_of_week", F.date_format("tpep_pickup_datetime", "E")
)

# 4. Join with Taxi Zones (Pickup & Dropoff)
silver_final = silver_enriched \
    .join(bronze_zones.alias("pu"), F.col("PULocationID") == F.col("pu.LocationID"), "left") \
    .select(
        silver_enriched["*"],
        F.when(F.col("pu.Borough") == "N/A", "Out of NYC")
         .when(F.col("pu.Borough").isNull(), "Unknown")
         .otherwise(F.col("pu.Borough")).alias("pickup_borough"),
        F.col("pu.Zone").alias("pickup_zone")
    ) \
    .join(bronze_zones.alias("do"), F.col("DOLocationID") == F.col("do.LocationID"), "left") \
    .select(
        silver_enriched["*"],
        F.col("pickup_borough"),
        F.col("pickup_zone"),
        F.when(F.col("do.Borough") == "N/A", "Out of NYC")
         .when(F.col("do.Borough").isNull(), "Unknown")
         .otherwise(F.col("do.Borough")).alias("dropoff_borough"),
        F.col("do.Zone").alias("dropoff_zone")
    )

# 5. Write to Silver Delta Table
silver_final.write.format("delta").mode("overwrite").saveAsTable("default.silver_nyc_taxi_2025")

print("Silver Delta Table successfully created!")

# COMMAND ----------

bronze_count = spark.table("default.bronze_nyc_taxi_2025").count()
silver_count = spark.table("default.silver_nyc_taxi_2025").count()
filtered_out = bronze_count - silver_count

print(f"Bronze Raw Records:     {bronze_count:,}")
print(f"Silver Clean Records:   {silver_count:,}")
print(f"Records Filtered Out:   {filtered_out:,} ({(filtered_out / bronze_count) * 100:.2f}%)")

# Preview clean dataset with joined zones
display(spark.sql("""
    SELECT 
        tpep_pickup_datetime,
        trip_duration_minutes,
        trip_distance,
        fare_amount,
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone
    FROM default.silver_nyc_taxi_2025
    LIMIT 5
"""))