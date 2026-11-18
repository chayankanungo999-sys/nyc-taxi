# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03_combined_trips
# MAGIC Builds the cleaned, combined table `combined_trips` (yellow + green, with borough and zone names) and runs
# MAGIC the data quality checks.
# MAGIC - Timestamps renamed to one name, then `unionByName(allowMissingColumns=True)`.
# MAGIC - Fixes in the SELECT: RatecodeID NULL -> 99, payment_type NULL -> 5, negative fare -> NULL,
# MAGIC   `is_refund` flag for negative totals.
# MAGIC - Excluded and Quarantined in the WHERE: pickup outside the file's month, dropoff before pickup
# MAGIC - Impossible distance (over 100 miles **and** an implied average speed over 100 mph): trip kept,
# MAGIC   `trip_distance` set to NULL. 
# MAGIC - `INSERT INTO ... REPLACE WHERE` -> re-runs never duplicate.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

combined_table = f"{catalog}.{schema}.combined_trips"
zone_table = f"{catalog}.{schema}.dim_zone"
quarantine_table = f"{catalog}.{schema}.quarantine_trips"

# Average speed a trip implies, in mph (NULLIF avoids dividing by a zero duration).
SPEED_MPH = "trip_distance * 3600.0 / NULLIF(timestampdiff(SECOND, pickup_datetime, dropoff_datetime), 0)"


def check_zone_lookup_is_unique():

    duplicates = spark.sql(f"""
        SELECT LocationID
        FROM {zone_table}
        GROUP BY LocationID
        HAVING COUNT(*) > 1
        LIMIT 1
    """).count()

    if duplicates:
        raise RuntimeError("Duplicate LocationIDs found in zone lookup, this will duplicate the data downstream")

    print("Zone lookup has unique LocationIDs.")


def create_combined_trips_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {combined_table} (
            taxi_type             STRING,
            vendor_id             INT,
            pickup_datetime       TIMESTAMP_NTZ,
            dropoff_datetime      TIMESTAMP_NTZ,
            pickup_hour           INT,
            passenger_count       INT,
            trip_distance         DOUBLE,
            rate_code_id          INT,
            store_and_forward_flag STRING,
            payment_type          INT,
            trip_type             INT,
            pickup_location_id    INT,
            pickup_borough        STRING,
            pickup_zone           STRING,
            dropoff_location_id   INT,
            dropoff_borough       STRING,
            dropoff_zone          STRING,
            fare_amount           DOUBLE,
            extra_charges         DOUBLE,
            mta_tax               DOUBLE,
            tip_amount            DOUBLE,
            tolls_amount          DOUBLE,
            improvement_surcharge DOUBLE,
            congestion_surcharge  DOUBLE,
            airport_fee           DOUBLE,
            cbd_congestion_fee    DOUBLE,
            total_amount          DOUBLE,
            is_refund             BOOLEAN,
            year_month            STRING,
            source_file           STRING
        ) USING DELTA
        PARTITIONED BY (year_month)
    """)
   


def create_bronze_trips_view():
    # Rename the timestamps so both taxi types use the same names, then stack them by column name.
    yellow_df = (spark.table(f"{catalog}.{schema}.bronze_yellow_trips")
                 .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
                 .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
                 .withColumn("taxi_type", F.lit("yellow")))

    green_df = (spark.table(f"{catalog}.{schema}.bronze_green_trips")
                .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
                .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
                .withColumn("taxi_type", F.lit("green")))

    trips_df = (yellow_df.unionByName(green_df, allowMissingColumns=True)
                .filter(F.col("year_month").isin(year_months) & F.col("taxi_type").isin(taxi_types)))

    trips_df.createOrReplaceTempView("bronze_trips")


def load_combined_trips():

    year_months_sql = ', '.join(f"'{m}'" for m in year_months)
    taxi_types_sql = ', '.join(f"'{t}'" for t in taxi_types)

    spark.sql(f"""
        INSERT INTO {combined_table}
        REPLACE WHERE year_month IN ({year_months_sql}) AND taxi_type IN ({taxi_types_sql})
        SELECT
            t.taxi_type,
            CAST(t.VendorID AS INT)                                 AS vendor_id,
            t.pickup_datetime,
            t.dropoff_datetime,
            hour(t.pickup_datetime)                                 AS pickup_hour,
            CAST(t.passenger_count AS INT)                          AS passenger_count,
            CASE WHEN t.trip_distance > 100 AND {SPEED_MPH} > 100 THEN NULL
                 ELSE abs(t.trip_distance) END                           AS trip_distance,  -- impossible distance: trip kept, value removed
            CAST(COALESCE(t.RatecodeID, 99) AS INT)                 AS rate_code_id,
            t.store_and_fwd_flag                                    AS store_and_forward_flag,
            CAST(COALESCE(t.payment_type, 5) AS INT)                AS payment_type,
            CAST(t.trip_type AS INT)                                AS trip_type,
            t.PULocationID                                          AS pickup_location_id,
            pz.Borough                                              AS pickup_borough,
            pz.Zone                                                 AS pickup_zone,
            t.DOLocationID                                          AS dropoff_location_id,
            dz.Borough                                              AS dropoff_borough,
            dz.Zone                                                 AS dropoff_zone,
            CASE WHEN t.fare_amount < 0 THEN NULL ELSE t.fare_amount END AS fare_amount,  -- a meter fare can't be negative
            t.extra                                                 AS extra_charges,
            t.mta_tax,
            t.tip_amount,
            t.tolls_amount,
            t.improvement_surcharge,
            t.congestion_surcharge,
            t.Airport_fee                                           AS airport_fee,
            t.cbd_congestion_fee,
            t.total_amount,
            t.total_amount < 0                                      AS is_refund,
            t.year_month,
            t.source_file
        FROM bronze_trips t
        LEFT JOIN {zone_table} pz ON t.PULocationID = pz.LocationID
        LEFT JOIN {zone_table} dz ON t.DOLocationID = dz.LocationID
        WHERE date_format(t.pickup_datetime, 'yyyy-MM') = t.year_month   -- pickup in the file's month
          AND t.dropoff_datetime >= t.pickup_datetime                     -- dropoff not before pickup (0-second trips kept: paid trips, wrong dropoff time)
    """)
    bronze_rows = spark.table("bronze_trips").count()
    loaded_rows = spark.sql(f"""
        SELECT COUNT(1)  FROM {combined_table}
        WHERE year_month IN ({year_months_sql}) AND taxi_type IN ({taxi_types_sql})
    """).first()[0]

    if loaded_rows == 0:
        raise RuntimeError(f"{combined_table}: 0 rows loaded for {taxi_types} / {year_months}")

    print(f"{combined_table}: {bronze_rows} rows in Bronze, {loaded_rows} loaded, "
          f"{bronze_rows - loaded_rows} excluded by the cleaning rules")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC Each check is a rule plus what the cleaning does with rows that break it. The checks run twice:
# MAGIC 1. **On the incoming data (`bronze_trips`) – report:** print how many rows fail each check and how they are
# MAGIC 2. **On the output (`combined_trips`) – gate:** expected 0 bad rows; any failure stops the pipeline.

# COMMAND ----------

# (check name, SQL condition that is TRUE for a bad row, what the cleaning does with those rows)
CHECKS = [
    ("no_trips_outside_file_month", "date_format(pickup_datetime, 'yyyy-MM') <> year_month", "excluded and quarantined"),
    ("no_dropoff_before_pickup",    "dropoff_datetime < pickup_datetime",                    "excluded and quarantined"),
    ("no_negative_fares",           "fare_amount < 0",                                       "fare set to NULL, trip kept"),
    ("valid_trip_distance",         f"(trip_distance > 100 AND {SPEED_MPH} > 100)",
                                    "over 100 miles at over 100 mph: distance set to NULL, trip kept"),
]


def run_quality_checks(table, stop_on_failure):
    failed = []
    total_rows = spark.table(table).count()

    for check_name, bad_row_condition, handling in CHECKS:

        bad_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {bad_row_condition}").first()["n"]
        bad_percent = bad_rows * 100 / total_rows

        if bad_rows == 0:
            print("PASS", check_name)
        elif stop_on_failure:
            print(f"FAIL {check_name}: {bad_rows} bad rows ({bad_percent:.3f}%)")
            failed.append(check_name)
        else:
            print(f"WARNING {check_name}: {bad_rows} rows ({bad_percent:.3f}%) fail this check -> {handling}")

    if len(failed) > 0:
        raise RuntimeError(f"Data quality checks failed on {table}: {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

create_combined_trips_table()
create_bronze_trips_view()
check_zone_lookup_is_unique()

run_quality_checks("bronze_trips", stop_on_failure=False)    # report: what is wrong in the incoming data
load_combined_trips()
run_quality_checks(combined_table, stop_on_failure=True)     # gate: the output must be clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## What a failure looks like
# MAGIC Running the gate on the uncleaned data shows the error the pipeline stops with if bad rows reach the output.

# COMMAND ----------

try:
    run_quality_checks("bronze_trips", stop_on_failure=True)
except Exception as error:
    print("The pipeline would stop here with:", error)

# COMMAND ----------

def create_quarantine_trips_table():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table} (
            taxi_type              STRING,
            vendor_id              INT,
            pickup_datetime        TIMESTAMP_NTZ,
            dropoff_datetime       TIMESTAMP_NTZ,
            pickup_hour            INT,
            passenger_count        INT,
            trip_distance          DOUBLE,
            rate_code_id           INT,
            store_and_forward_flag STRING,
            payment_type           INT,
            trip_type              INT,
            pickup_location_id     INT,
            pickup_borough         STRING,
            pickup_zone            STRING,
            dropoff_location_id    INT,
            dropoff_borough        STRING,
            dropoff_zone           STRING,
            fare_amount            DOUBLE,
            extra_charges          DOUBLE,
            mta_tax                DOUBLE,
            tip_amount             DOUBLE,
            tolls_amount           DOUBLE,
            improvement_surcharge  DOUBLE,
            congestion_surcharge   DOUBLE,
            airport_fee            DOUBLE,
            cbd_congestion_fee     DOUBLE,
            total_amount           DOUBLE,
            is_refund              BOOLEAN,
            year_month             STRING,
            source_file            STRING,

            -- Quarantine metadata
            rejection_reason       STRING,
            quarantined_at         TIMESTAMP

        )
        USING DELTA
        PARTITIONED BY (year_month)
    """) 

# COMMAND ----------

def load_quarantine_trips():

    year_months_sql = ', '.join(f"'{m}'" for m in year_months)
    taxi_types_sql = ', '.join(f"'{t}'" for t in taxi_types)

    spark.sql(f"""
        INSERT INTO {quarantine_table}

        REPLACE WHERE
            year_month IN ({year_months_sql})
            AND taxi_type IN ({taxi_types_sql})

        SELECT
            t.taxi_type,

            CAST(t.VendorID AS INT) AS vendor_id,

            t.pickup_datetime,
            t.dropoff_datetime,

            hour(t.pickup_datetime) AS pickup_hour,

            CAST(t.passenger_count AS INT) AS passenger_count,

            CAST(t.trip_distance AS DOUBLE) AS trip_distance,

            CAST(t.RatecodeID AS INT) AS rate_code_id,

            t.store_and_fwd_flag AS store_and_forward_flag,

            CAST(t.payment_type AS INT) AS payment_type,

            CAST(t.trip_type AS INT) AS trip_type,

            t.PULocationID AS pickup_location_id,
            pz.Borough AS pickup_borough,
            pz.Zone AS pickup_zone,

            t.DOLocationID AS dropoff_location_id,
            dz.Borough AS dropoff_borough,
            dz.Zone AS dropoff_zone,

            CAST(t.fare_amount AS DOUBLE) AS fare_amount,

            t.extra AS extra_charges,
            t.mta_tax,
            t.tip_amount,
            t.tolls_amount,
            t.improvement_surcharge,
            t.congestion_surcharge,
            t.Airport_fee AS airport_fee,
            t.cbd_congestion_fee,

            t.total_amount,

            t.total_amount < 0 AS is_refund,

            t.year_month,
            t.source_file,

            -- Explain why the record was rejected
            CONCAT_WS(' | ',
                CASE
                    WHEN t.pickup_datetime IS NOT NULL
                         AND date_format(t.pickup_datetime, 'yyyy-MM') <> t.year_month
                    THEN 'PICKUP_MONTH_MISMATCH'
                END,

                CASE
                    WHEN t.pickup_datetime IS NOT NULL
                         AND t.dropoff_datetime IS NOT NULL
                         AND t.dropoff_datetime < t.pickup_datetime
                    THEN 'NEGATIVE_TRIP_DURATION'
                END

            ) AS rejection_reason,

            current_timestamp() AS quarantined_at

        FROM bronze_trips t

        LEFT JOIN {zone_table} pz
            ON t.PULocationID = pz.LocationID

        LEFT JOIN {zone_table} dz
            ON t.DOLocationID = dz.LocationID

        WHERE
            date_format(t.pickup_datetime, 'yyyy-MM') <> t.year_month

            OR t.dropoff_datetime < t.pickup_datetime
    """)

    quarantine_rows = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {quarantine_table}
        WHERE year_month IN ({year_months_sql})
          AND taxi_type IN ({taxi_types_sql})
    """).first()["n"]

    print(
        f"{quarantine_table}: "
        f"{quarantine_rows} records quarantined "
        f"for {taxi_types} / {year_months}"
    )

# COMMAND ----------

create_quarantine_trips_table()
load_quarantine_trips()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quarantine_trips limit(10)

# COMMAND ----------

