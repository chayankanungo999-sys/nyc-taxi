# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # run_pipeline
# MAGIC Runs the whole pipeline in order. Open this notebook and click **Run all**.
# MAGIC Each stage runs inline (`%run`), so its printed output and the three answers appear below.
# MAGIC If any stage raises an error, the run stops there.
# MAGIC
# MAGIC 1. `01_landing` – check the files exist on the TLC website, then download them
# MAGIC 2. `02_bronze` – load the files as published into Delta tables
# MAGIC 3. `03_combined_trips` – clean, combine, join zones, run the data quality checks
# MAGIC 4. `04_analysis` – the three questions (SQL)

# COMMAND ----------

# MAGIC %run ./01_landing

# COMMAND ----------

# MAGIC %run ./02_bronze

# COMMAND ----------

# MAGIC %run ./03_combined_trips

# COMMAND ----------

# MAGIC %run ./04_analysis