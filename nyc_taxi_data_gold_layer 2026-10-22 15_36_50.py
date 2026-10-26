# Databricks notebook source
from pyspark.sql import functions as F

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Categorize pickup hours and aggregate performance metrics
gold_peak_metrics = (
    silver_df.withColumn(
        "time_category",
        F.when(
            (F.col("pickup_hour") >= 7) & (F.col("pickup_hour") <= 9),
            "Morning Rush",
        )
        .when(
            (F.col("pickup_hour") >= 16) & (F.col("pickup_hour") <= 19),
            "Evening Rush",
        )
        .otherwise("Off-Peak"),
    )
    .groupBy("time_category")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    )
    .orderBy(F.col("total_trips").desc())
)

# 3. Write to a new Gold Delta table
gold_peak_metrics.write.format("delta").mode("overwrite").saveAsTable(
    "default.gold_peak_metrics"
)

print("Gold Peak vs. Off-Peak table successfully created!")

# 4. Display the results
display(spark.table("default.gold_peak_metrics"))
        
    

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Group by both pickup and dropoff boroughs to map commuter flows
gold_corridors = (
    silver_df.groupBy("pickup_borough", "dropoff_borough")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles")
    )
    .orderBy(F.col("total_trips").desc())
    .limit(10)
)
gold_corridors.explain("formatted")
# 3. Write to a new Gold Delta table
gold_corridors.write.format("delta").mode("overwrite").saveAsTable(
    "default.gold_inter_borough_routes"
)

print("Gold Inter-Borough Routes table successfully created!")

# 4. Display the results
display(spark.table("default.gold_inter_borough_routes"))

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Define airport condition (JFK, LaGuardia, EWR in pickup or dropoff zones)
airport_condition = (
    F.col("pickup_zone").contains("Airport")
    | F.col("dropoff_zone").contains("Airport")
    | (F.col("pickup_zone") == "EWR")
    | (F.col("dropoff_zone") == "EWR")
)

# 3. Tag trip categories and calculate performance metrics
gold_airport_metrics = (
    silver_df.withColumn(
        "trip_category",
        F.when(airport_condition, "Airport Trip").otherwise("Standard City Trip"),
    )
    .groupBy("trip_category")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
    )
    .orderBy(F.col("total_trips").desc())
)

# 4. Write to a new Gold Delta table
gold_airport_metrics.write.format("delta").mode("overwrite").saveAsTable(
    "default.gold_airport_performance"
)

print("Gold Airport Performance table successfully created!")

# 5. Display the results
display(spark.table("default.gold_airport_performance"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Define a window partitioned by pickup borough and ordered by fare amount descending
borough_window = Window.partitionBy("pickup_borough").orderBy(
    F.col("fare_amount").desc()
)

# 3. Assign a row number rank within each borough and filter for the top 3
gold_expensive_trips = (
    silver_df.withColumn("rank", F.row_number().over(borough_window))
    .filter(F.col("rank") <= 3)
    # Select key columns to keep the output clean and readable
    .select(
        "rank",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "fare_amount",
        "trip_distance",
        "tpep_pickup_datetime",
    )
)
gold_expensive_trips.explain("formatted")

# 4. Write to a new Gold Delta table
gold_expensive_trips.write.format("delta").mode("overwrite").saveAsTable(
    "default.gold_top_expensive_trips"
)

print("Gold Top Expensive Trips table successfully created!")

# 5. Display the results
display(spark.table("default.gold_top_expensive_trips"))

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Group by both pickup and dropoff boroughs to map commuter flows
gold_borough_efficiency = (
    silver_df.groupBy("pickup_borough")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(
            F.avg(F.col("tip_amount")/F.col("fare_amount"))*100,2
            ).alias("avg_tip_percentage"),
    )
    .orderBy(F.col("total_revenue").desc())
)
# 3. Write to a new Gold Delta table
gold_borough_efficiency.write.format("delta").mode("overwrite").saveAsTable(
    "default.gold_borough_efficiency"
)

print("Gold Borough Efficiency table successfully created!")

# 4. Display the results
display(spark.table("default.gold_borough_efficiency"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read the Silver Delta table
silver_df = spark.table("default.silver_nyc_taxi_2025")

# 2. Define a global window ordered by total trips descending
hourly_rank_window = Window.orderBy(F.col("total_trips").desc())

# 3. Extract hour, aggregate metrics, and assign rank
gold_hourly_trends = (
    silver_df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    )
    .withColumn("busiest_hour_rank", F.row_number().over(hourly_rank_window))
    .orderBy("pickup_hour")
)

# 4. Write to the Gold Delta table with overwriteSchema enabled
gold_hourly_trends.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("default.gold_hourly_trends")

print("Gold Hourly Trends table with rankings successfully created!")

# 5. Display the results
display(spark.table("default.gold_hourly_trends"))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE default.silver_nyc_taxi_2025
# MAGIC ZORDER BY (pickup_borough, fare_amount);