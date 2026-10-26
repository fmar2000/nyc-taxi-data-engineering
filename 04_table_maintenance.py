# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Compact small files in the Silver table
# MAGIC OPTIMIZE default.silver_nyc_taxi_2025;
# MAGIC
# MAGIC -- 2. Clean up old data file versions older than 7 days (168 hours) to save storage
# MAGIC VACUUM default.silver_nyc_taxi_2025 RETAIN 168 HOURS;
# MAGIC
# MAGIC