# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_landing
# MAGIC Downloads the requested TLC files into the landing_zone folder of the volume:
# MAGIC `landing_zone/<taxi_type>/<yyyy>/<mm>/<file>` and `landing_zone/zones/taxi_zone_lookup.csv`.
# MAGIC 1. **Check first:** ask the website (HEAD request, no download) whether every requested file exists.
# MAGIC    If any is missing, stop with one error listing all of them – nothing is downloaded.
# MAGIC 2. **Then download** all files. Re-running overwrites the same files.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
import urllib.error
import urllib.request


def is_available(url):
    request = urllib.request.Request(url,method='HEAD')
    try:
        urllib.request.urlopen(request)
        return True
    except Exception:
        return False


def check_all_files_available(files):
    available = []
    missing = []

    for file in files:
        if is_available(file['url']):
            available.append(file['url'])
        else:
            missing.append(file['url']) 
    
    if missing:
        print(f'files not available or TLC website not reachable {missing}')
        raise RuntimeError('No file downloaded')
    else:
        print('All files available, attempting to download')

def download_files(files):
    for file in files:
        url = file['url']
        os.makedirs(file['folder_name'],exist_ok=True)
        with urllib.request.urlopen(url) as response:
            with open(file['path'],'wb') as output_file:
                while True:
                    chunk = response.read(1024*1024)
                    if not chunk:
                        break 
                    output_file.write(chunk)
        print('downloaded', url)

# COMMAND ----------

# files defined in 00_config
check_all_files_available(get_files_metadata())
download_files(get_files_metadata())