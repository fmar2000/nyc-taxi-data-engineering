# 🚕 NYC Taxi Data Engineering Pipeline & Analytics

An end-to-end data engineering portfolio project implementing a Databricks medallion-architecture data pipeline. This repository ingests, cleans, models, and optimizes 2025 NYC TLC Trip Record data using PySpark, Delta Lake, Databricks Workflows, and Spark SQL.

---

## 🏗️ Architecture & Tech Stack

* **Orchestration & Workflow:** Databricks Workflows (DAGs)
* **Compute Engine:** Apache Spark (PySpark & Spark SQL on Databricks)
* **Storage & Format:** Delta Lake (Medallion Architecture: Bronze, Silver, Gold)
* **Performance Optimizations:** File Compaction (`OPTIMIZE`), Data Skipping (`ZORDER BY`), Log Pruning (`VACUUM`)

---

## 📂 Repository Structure

```text
nyc-taxi-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Raw batch Parquet ingestion & meta tracking
│   ├── 02_silver_cleansing.py        # Data hygiene, schema normalization & zone joins
│   ├── 03_gold_aggregations.py       # Business metrics, borough rollups & reporting tables
│   └── 04_table_maintenance.py       # OPTIMIZE, ZORDER, and VACUUM maintenance tasks
├── data/
│   └── taxi_zone_lookup.csv          # TLC Zone mapping lookup table
└── README.md                         # Databricks pipeline documentation

```
---

## 📊 Pipeline Data Governance & Metrics

This pipeline processes high volumes of raw data and implements rigorous data hygiene policied before delivering analytical queries.

### Pipeline Execution & Orchestration
**Orchestration:** Configured a Databricks Workflow
with dependent tasks to execute the pipeline stages
in the correct order.

**End-to-end runtime:** Completed a full workflow run
in approximately 3 minutes and 34 seconds during testing.

**Execution:** Configured task dependencies to ensure downstream
stages run only after their prerequisites complete successfully.

1. **High Hygiene Data Filtration**
* **Metric:** Filtered out 13,647,274 anomalous records (28.01% filtration rate) from 48,722,802 raw Bronze records.

* **Purpose:** Stripped out invalid fares (fare_amount <= 0), ghost rides (passenger_count <= 0), and GPS telemetry glitches.

2. **Delta Lake File Optimization**
* **Technique:** Automated file compaction (Optimize) to elminate small-file overhead combined with ZORDER BY (tpep_pickup_datetime) for data skipping.

* **Purpose:** Minimise data scan times during downstream temporal and borough-level analytics.



---

## 📈 Analytical Suite

This repository features aggregated Gold layer analytical queries designed to extract key transportation insights:

1. **Peak Demand & Commuter Rush Hours (`gold_peak_metrics`)**
   * *Technique:* Feature engineering with conditional logic (`F.when`) to categorize pickup hours into Morning Rush, Evening Rush, and Off-Peak tiers.
   * *Purpose:* Evaluates trip volumes, average fares, and distance metrics across peak commute hours.

2. **Inter-Borough Corridor Analysis (`gold_inter_borough_routes`)**
   * *Technique:* Multi-column aggregation grouped by `pickup_borough` and `dropoff_borough`, sorted by trip volume and capped with a `LIMIT 10` boundary.
   * *Purpose:* Maps commuter flow patterns and isolates the 10 busiest transit corridors.

3. **Airport Transit & Yield Profiling (`gold_airport_performance`)**
   * *Technique:* Targeted conditional string matching (contains("Airport") / =="EWR") across pickup and dropoff zones.   
   * *Purpose:* Isolates high-value airport transit routes (JFK, LaGuardia, Newark) to profile premium yields, fare averages, and tip performance against standard city trips.

4. **High-Value Trip & Fare Rankings (`gold_top_expensive_trips`)**
   * *Technique:* Utilizes advanced window functions (Window.partitionBy("pickup_borough").orderBy(F.col("fare_amount").desc())) combined with row-numbering logic (F.row_number() <= 3) to filter and rank the top three highest-fare trips within each borough.   
   * *Purpose:* Highlights most expensive tripper region to examine regional pricing ceilings and premium travel patterns, showcasing advanced window function capabilities beyond basic group-by aggregations.

5. **Borough Revenue & Tip Efficiency (`gold_borough_efficiency`)**

   * *Technique:* Performs rollup aggregations grouped by `pickup_borough`, computing totals, averages, and on-the-fly column calculations to derive tip percentage ratios F.avg(F.col("tip_amount")/F.col("fare_amount"))*100) live during aggregation.
   * *Purpose:* Evaluates the tipping generosity accross different regions, whilst providing macro-level financial reporting on revenue generation.

6. **Hourly Traffic Trends & Volume Ranking (`gold_hourly_trends`)**   
   * *Technique:* Extracts hourly values from trip timestamps, aggregates performance metrics across 24-hour blocks, and applies a global window function (Window.orderBy(F.col("total_trips").desc())) to dynamically assign a rank (busiest_hour_rank) to each hour.   
   * *Purpose:* Maps the congestion profiles within the city by hour, identifying traffic bottlenecks aswell as peak operational hours.

   ---

## 🧠 Key Learnings & Scalability Considerations

**Technical Learnings**

- **Silver Layer Enrichment via Joins:** During the Silver stage, I learned the importance of joining spatial lookup tables early in the pipeline. Resolving location IDs upfront enabled me to catch and drop malformed or out-of-bound zone records before they could skew downstream analytical metrics.
- **Data Skipping & Query Optimization (`ZORDER BY`):** I explored Delta Lake Z-Ordering to reorganise data files and improve data skipping on temporal and borough-related columns. After applying Z-Ordering, I observed the downstream query execution duration for gold_hourly_trends decrease from 17.2 seconds to 8.3 seconds.
However, this was a comparison of two individual runs, so caching and execution variability may have contributed to the difference. Repeated controlled benchmarks would be needed
to isolate the impact of Z-Ordering.
- **Delta Maintenance Scheduling:** I discovered that running `OPTIMIZE` commands right after every small write created unnecessary overhead. Moving file cleanups and `VACUUM` tasks into a separate, scheduled maintenance routine saved time and kept serverless compute resource usage efficient.

### Future Improvements & Scale Considerations
- **Handling Terabyte-Scale Data:**
  * For significantly larger volumes, I would implement **Delta Liquid Clustering** or partition the Silver layer by `year/month` to allow Spark to prune entire storage directories during temporal queries.
- **Production Compute Strategy:**
  * To balance cost and performance at scale, I would transition from single-node execution to a multi-node autoscaling cluster utilizing Spot instances for compute workers during heavy monthly batch joins.

---

## 🚀 How to Run
### Prerequisites

* A Google Cloud Platform (GCP) project with a Cloud Storage bucket.

* A Databricks workspace with Serverless compute enabled.

* Access configured securely between Databricks and GCS.

* The 2025 NYC Yellow Taxi trip records in Parquet format (January–December).

* The taxi_zone_lookup.csv reference file.

### 1. Prepare the Data

* Upload the monthly Yellow Taxi trip record files to your GCS bucket, organised under the raw data directory. Upload taxi_zone_lookup.csv to the reference data directory.

* Update the GCS paths in the notebooks to match your bucket structure.

### 2. Run the Notebooks

**Execute the notebooks in the following order:**

* 01_bronze_ingestion — Reads raw Parquet files from GCS and ingests them into a Bronze Delta table.

* 02_silver_cleansing — Applies validation and cleaning rules, derives analytical columns, and joins the taxi zone reference data.

* 03_gold_aggregations — Builds Gold tables for borough-level metrics, inter-borough routes, fares, and temporal trends.

* 04_table_maintenance — Performs table maintenance operations where configured.

**Run each notebook successfully before proceeding to the next.**

###3. Validate the Results
**After execution, check:**

* Row counts at each pipeline stage.

* Data quality and validation results.

* Unmatched taxi zone lookups and unexpected nulls.

* The output of the Gold analytical tables.

**Security Notes**

* Do not commit service account keys, access tokens, passwords, or other credentials to version control.

* Use an approved, secure authentication method for accessing GCS.

* Ensure notebook paths and table names match your environment.

   

   


