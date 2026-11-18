-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### # 04_analysis
-- MAGIC The three questions from the assessment, answered from the cleaned table `combined_trips`.
-- MAGIC Run after `03_combined_trips`. Each result is shown below and saved as a CSV in the volume's `outputs` folder.
-- MAGIC
-- MAGIC - **Trips** = rows where `is_refund` is false (refund rows are reversals, not trips).
-- MAGIC - **Revenue** = `SUM(total_amount)` over all rows; refund rows are negative, so revenue is net of refunds.
-- MAGIC - **Month** = month of the source file (equals the pickup month after cleaning).

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### #Q1 – Total trips and total revenue per month, split by yellow versus green.

-- COMMAND ----------

SELECT year_month AS month,
       taxi_type,
       SUM(CASE WHEN is_refund THEN 0 ELSE 1 END)   AS total_trips,
       ROUND(SUM(total_amount), 2)                  AS total_revenue
FROM combined_trips
GROUP BY year_month, taxi_type
ORDER BY month, taxi_type;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf.toPandas().to_csv(f"{outputs_path}/q1_monthly_revenue.csv", index=False)   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### #Q2 – The 10 busiest pickup zones by trip count, with their borough

-- COMMAND ----------

SELECT
    pickup_zone,
    pickup_borough,
    COUNT(*) AS total_trips
FROM combined_trips
WHERE NOT is_refund
GROUP BY pickup_zone, pickup_borough
ORDER BY total_trips DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf.toPandas().to_csv(f"{outputs_path}/q2_top_pickup_zones.csv", index=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q3 – Average trip distance and average fare by hour of day

-- COMMAND ----------

SELECT pickup_hour,
       COUNT(*)                      AS total_trips,
       ROUND(AVG(trip_distance), 2)  AS avg_trip_distance_miles,
       ROUND(AVG(fare_amount), 2)    AS avg_fare_amount
FROM combined_trips
WHERE NOT is_refund
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf.toPandas().to_csv(f"{outputs_path}/q3_hourly_metrics.csv", index=False)