# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_bronze
# MAGIC Loads the landed files into Bronze Delta tables **exactly as published**, plus lineage columns
# MAGIC (`source_file`, `year_month`, `ingested_at`).
# MAGIC - `replaceWhere year_month = ...` replaces only that month, atomically -> re-runs never duplicate.
# MAGIC - `mergeSchema` adds new source columns (e.g. `cbd_congestion_fee` from Jan 2025); older rows show NULL.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F


def read_load_bronze_tables(files):

    bronze_yellow_table = f'{catalog}.{schema}.bronze_yellow_trips'
    bronze_green_table = f'{catalog}.{schema}.bronze_green_trips'
    zone_table = f'{catalog}.{schema}.dim_zone'

    for file in files:

        try:

            if file['kind'] == 'trip' and file['taxi_type'] == 'green':

                bronze_df = (
                    spark.read
                    .format('parquet')
                    .load(file['path'])
                    .withColumn('source_file', F.lit(file['file_name']))
                    .withColumn('year_month', F.lit(file['year_month']))
                    .withColumn('ingested_at', F.current_timestamp())
                )

                (
                    bronze_df.write
                    .format('delta')
                    .mode('overwrite')
                    .option('replaceWhere', f"year_month = '{file['year_month']}'")
                    .option('mergeSchema', 'true')
                    .partitionBy('year_month')
                    .saveAsTable(bronze_green_table)
                )

            elif file['kind'] == 'trip' and file['taxi_type'] == 'yellow':
                bronze_df = (
                    spark.read
                    .format('parquet')
                    .load(file['path'])
                    .withColumn('source_file', F.lit(file['file_name']))
                    .withColumn('year_month', F.lit(file['year_month']))
                    .withColumn('ingested_at', F.current_timestamp())
                )

                (
                    bronze_df.write
                    .format('delta')
                    .mode('overwrite')
                    .option('replaceWhere', f"year_month = '{file['year_month']}'")
                    .option('mergeSchema', 'true')
                    .partitionBy('year_month')
                    .saveAsTable(bronze_yellow_table)
                )

            elif file['kind'] == 'zone':

                zone_df = (
                    spark.read
                    .format('csv')
                    .option('header', True)
                    .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
                    .load(file['path'])
                    .withColumn('ingested_at', F.current_timestamp())
                )

                (
                    zone_df.write
                    .format('delta')
                    .mode('overwrite')
                    .option('overwriteSchema', 'true')
                    .saveAsTable(zone_table)
                )

            else:
                raise RuntimeError(f"Unsupported file configuration: {file}")

        except Exception as e:

            raise RuntimeError(
                f"Bronze ingestion failed for file: {file['file_name']}. "
                f"Source path: {file['path']}. Error: {str(e)}"
            ) from e

    print("Bronze ingestion completed successfully.")

# COMMAND ----------

read_load_bronze_tables(get_files_metadata())

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'yellow' AS taxi_type, year_month, COUNT(*) AS row_count
    FROM {catalog}.{schema}.bronze_yellow_trips GROUP BY year_month
    UNION ALL
    SELECT 'green', year_month, COUNT(*)
    FROM {catalog}.{schema}.bronze_green_trips GROUP BY year_month
    ORDER BY taxi_type, year_month
"""))