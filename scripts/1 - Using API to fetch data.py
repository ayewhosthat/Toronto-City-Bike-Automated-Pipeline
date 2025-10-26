# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 - Using Python to fetch bike data using API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import necessary libraries

# COMMAND ----------

import requests # for fetching data from API'S
from collections import defaultdict # for extracting data fields from json
import json # for converting API response to json object
import pandas as pd # for converting to tabular format to be read by tables later on
from datetime import datetime
from zoneinfo import ZoneInfo

# since DataBricks uses volumes, and pandas cannot directly write to volumes, we will leverage Spark's builtin capabilities
# the spark session is already initialized within the notebook
from pyspark.sql.functions import col

# COMMAND ----------

# define url that will fetch API response
URL = "http://api.citybik.es/v2/networks/bixi-toronto"

def fetch_bikeshare_data():
    response = requests.get(URL)
    data = response.json()
    stations_info = defaultdict(list)
    stations = data['network']['stations']

    # Get the current date and time
    now = datetime.now(ZoneInfo('America/New_York'))

    # Extract the current date
    current_date = now.date()

    # Extract the current hour
    current_hour = now.hour

    for station in stations:
        name = station['name']
        latitude = station['latitude']
        longitude = station['longitude']
        # some stations will not have payment data available
        free_bikes = station['free_bikes']
        empty_slots = station['empty_slots']
        slots = station['extra']['slots']

        stations_info['station_name'].append(name)
        stations_info['latitude'].append(latitude)
        stations_info['longitude'].append(longitude)
        stations_info['date'].append(current_date.strftime('%Y-%m-%d'))
        stations_info['hour'].append(current_hour)
        stations_info['free_bikes'].append(free_bikes)
        stations_info['empty_slots'].append(empty_slots)
        stations_info['slots'].append(slots)

    captured_data_df = spark.createDataFrame(pd.DataFrame(stations_info))

    captured_data_df.write.csv(
        f"/Volumes/workspace/first-project-dev/bike_data/{current_date}_Hour-{current_hour}.csv",
        mode='overwrite',
        header=True)

# COMMAND ----------

fetch_bikeshare_data()