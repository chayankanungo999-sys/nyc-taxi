# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_config
# MAGIC Shared settings for every notebook (loaded with `%run ./00_config`).
# MAGIC Defines the parameters, validates them, builds all names and paths, and makes sure the schema and volume exist.

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

# Parameters: which taxi types and months to process. Change these lists to process other data.
taxi_types = ["yellow", "green"]
year_months = ["2024-12", "2025-01"]

# COMMAND ----------

catalog = "nyc_taxi_catalog"
schema = "nyc_taxi"
volume = "raw"

base_url = "https://d37ci6vzurychx.cloudfront.net"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

landing_path = f"{volume_path}/landing_zone"
outputs_path = f"{volume_path}/outputs"
zone_folder = f"{landing_path}/zones"

os.makedirs(outputs_path, exist_ok=True)
os.makedirs(zone_folder, exist_ok=True)

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

valid_taxi_types = {"yellow", "green"}


def normalize_taxi_types(taxi_types):
    normalize_taxi_types = []
    for taxi_type in taxi_types:
        normalize_taxi_types.append(taxi_type.strip().lower())
    invalid_taxi_types = set(normalize_taxi_types) - valid_taxi_types
    if invalid_taxi_types:
        print(f'{invalid_taxi_types} not a valid taxi type. Accepted values are {valid_taxi_types}.')
        raise RuntimeError('Invalid Taxi Types...')
    else:
        print('taxi types are valid...continuing')
        return list(set(normalize_taxi_types))


def normalize_year_months(year_months):

    invalid_year_month  = []
    normalized_year_month = []
    current_year_month = datetime.now().strftime('%Y-%m')
    current_year = current_year_month[:4]
    current_month = current_year_month[5:]

    for year_month in year_months:
        year_month = year_month.strip()
        year = year_month[:4]
        month = year_month[5:]

        if len(year_month) != 7 or year_month[4] != '-':
            invalid_year_month.append(year_month)
        elif not year.isdigit() or not month.isdigit():
            invalid_year_month.append(year_month)
        elif int(month)<1 or int(month)>12:
            invalid_year_month.append(year_month)
        elif int(year) > int(current_year) or (int(year) == int(current_year) and int(month) > int(current_month)):
            invalid_year_month.append(year_month)
        else:
            normalized_year_month.append(year_month)

    if invalid_year_month:
        print(f'{invalid_year_month} not valid year_month. Enter valid year month in yyyy-MM format. future year month not valid')       
        raise RuntimeError('Invalid Year Month') 
    else:
        print('year months are valid...continuing')
        return list(set(normalized_year_month))



taxi_types = normalize_taxi_types(taxi_types)
year_months = normalize_year_months(year_months)

# COMMAND ----------

# The list of all source files and there metadata, defined once and used by every notebook


def get_files_metadata():
    files = []
    for taxi_type in taxi_types:
        for year_month in year_months:
            file_name = f'{taxi_type}_tripdata_{year_month}.parquet'
            folder_name = f'{landing_path}/{taxi_type}/{year_month[:4]}/{year_month[5:]}'
            files.append({
                'kind':'trip',
                'taxi_type':taxi_type,
                'year_month':year_month,
                'file_name':file_name,
                'folder_name':folder_name,
                'url':f'{base_url}/trip-data/{file_name}',
                'path':f'{folder_name}/{file_name}'

            })
    zone_file_name= 'taxi_zone_lookup.csv'
    files.append({
        'kind':'zone',
        'file_name':zone_file_name,
        'folder_name': zone_folder,
        'url':f'{base_url}/misc/{zone_file_name}',
        'folder_path':zone_folder,
        'path':f'{zone_folder}/{zone_file_name}'
    })    
    return files                 


# COMMAND ----------

