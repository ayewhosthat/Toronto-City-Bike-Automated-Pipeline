# Databricks notebook source
# MAGIC %md
# MAGIC ## Importing Libraries

# COMMAND ----------

# lets install streamlit and altair
%pip install streamlit
%pip install altair
%pip install reportlab
%pip install kaleido

# COMMAND ----------

import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.io as pio
from zoneinfo import ZoneInfo
import os

# for sending emails automatically
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# COMMAND ----------

current_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
# setting the current date so that we can filter only the data for the current day

# this is because the data is being updated every hour, but we only create one dashboard per day
st.title(f"CityBike Dashboard: {current_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data from database and fetch only today's data

# COMMAND ----------

sdf_connect = spark.table("`first-project-dev`.silver_transformed")
# read the silver transformed table and then convert it to a regular Pandas dataframe
# create a view 
sdf_connect.createOrReplaceTempView("silver_view")

# COMMAND ----------

# since we only want to display a dashboard for today's information, we filter based on today's date
todays_data = spark.sql(
    f"SELECT * FROM silver_view WHERE date = '{current_date}'"
)

todays_data.show()
todays_data.createOrReplaceTempView("todays_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now, we can create our visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization\# 1: Top 5 Stations by Number of Bikes Rented

# COMMAND ----------

# query to get the top 5 stations with most bikes rented
top_5_stations = spark.sql(
    f"""
    SELECT station_name, 
    SUM(empty_slots) AS total_bikes_rented 
    FROM todays_data 
    GROUP BY station_name 
    ORDER BY total_bikes_rented DESC 
    LIMIT 5"""
)

top_5_stations.show()

# COMMAND ----------

# convert the spark dataframe to a pandas dataframe
top_5_stations_df = top_5_stations.toPandas()

top_5_chart = alt.Chart(top_5_stations_df, title=f'5 Busiest Stations: {current_date}').mark_bar().encode(
    x=alt.X('station_name', sort='-y'),
    y=alt.Y('total_bikes_rented'),
    tooltip=['station_name', 'total_bikes_rented']
).properties(
    width=300,
    height=400,
).configure_view(
    strokeWidth=0  # remove chart border
)

top_5_chart

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization \#2: Line Chart of Avg Percentage of Rentals by Hour

# COMMAND ----------

avg_percentage_table = spark.sql(
    f"""
    SELECT AVG(percent_free_bikes) AS avg_percentage_free_bikes,
    ROUND(AVG(percent_empty_slots), 2) AS avg_percentage_empty_slots,
    hour
    FROM todays_data
    GROUP BY hour
    ORDER BY hour ASC
    """
)
avg_percentage_table.show()

# COMMAND ----------

avg_pct_df = avg_percentage_table.toPandas()

avg_pct_chart = alt.Chart(avg_pct_df, title=f'Avg Pct of Station Capacity Rented by Hour: {current_date}').mark_line(
    color='#008080'
).encode(
    x=alt.X('hour', sort='-y'),
    y=alt.Y('avg_percentage_empty_slots'),
    tooltip=['hour', 'avg_percentage_empty_slots']
).properties(
    width=300,
    height=400,
).configure_view(
    strokeWidth=0  # remove chart border
)

avg_pct_chart

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization \#3: Map/Heatmap of Avg of Station Capacity Rented by Coordinates

# COMMAND ----------

# query to get average percentage of bikes rented throughout the day by coordinates
avg_percentage_bikes_rented_coordinates = spark.sql(
    f"""
    SELECT station_name, latitude, longitude, ROUND(AVG(percent_empty_slots), 2) AS avg_percentage_bikes_rented
    FROM todays_data
    GROUP BY station_name, latitude, longitude
    """
)

avg_percentage_bikes_rented_coordinates.show()

# COMMAND ----------

coordinates_df = avg_percentage_bikes_rented_coordinates.toPandas()

# COMMAND ----------

fig = px.scatter_map(coordinates_df, lat="latitude", lon="longitude", color="avg_percentage_bikes_rented", color_continuous_scale=px.colors.diverging.Picnic, size_max=15, zoom=10, hover_name="station_name")

fig.update_traces(marker=dict(size=12))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization \#4: Accumulation of bikes rented over the day

# COMMAND ----------

total_by_hour = spark.sql(
    f"""
    SELECT hour, SUM(empty_slots) AS total_bikes_rented FROM todays_data GROUP BY hour ORDER BY hour ASC
    """
)

total_by_hour.createOrReplaceTempView("total_by_hour")

running_total_bikes = spark.sql(
    f"""
    SELECT hour, SUM(total_bikes_rented)
    OVER (
        ORDER BY hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
    AS running_total_bikes
    FROM total_by_hour
    """
)
running_total_bikes.show()

# COMMAND ----------

running_total_df = running_total_bikes.toPandas()
running_total_bike_chart = alt.Chart(running_total_df, title=f"Running Total of Bikes Rented by Hour - {current_date}").mark_line().encode(
    x=alt.X('hour', sort='y'),
    y=alt.Y('running_total_bikes'),
    tooltip=['hour', 'running_total_bikes']
).properties(
    width=600,
    height=400,
).configure_view(
    strokeWidth=0  # remove chart border
)

running_total_bike_chart

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization \#5: Number of Bikes Rented KPI

# COMMAND ----------

total_bikes_rented = spark.sql(
    f"""
    SELECT SUM(empty_slots) AS total_bikes_rented FROM todays_data
    """
)
total_bikes_rented.show()

# COMMAND ----------

total_bikes_df = total_bikes_rented.toPandas()
total_bikes_num = total_bikes_df['total_bikes_rented'].iloc[0]
total_bikes_num

# COMMAND ----------

st.metric('Total Bikes Rented', f'{total_bikes_num:,}')
st.plotly_chart(fig)
st.altair_chart(top_5_chart)
st.altair_chart(running_total_bike_chart)
st.altair_chart(avg_pct_chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now to export the created dashboard as either an html snapshot or individual images

# COMMAND ----------

# MAGIC %pip install vl-convert-python

# COMMAND ----------

pio.write_image(
    fig,
    "map_chart.png",
    format="png",
    width=1000,
    height=800,
    scale=2
)

top_5_chart.save("top_5_chart.png")
running_total_bike_chart.save("running_total_bike_chart.png")
avg_pct_chart.save("avg_pct_chart.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now send an email containing all the saved charts/metrics

# COMMAND ----------

# get email app password secret
email_password = dbutils.secrets.get(scope="gmail-app-password", key="gmailapppass")

# COMMAND ----------

pdf_file = "daily_dashboard.pdf"
doc = SimpleDocTemplate(pdf_file, pagesize=letter)
styles = getSampleStyleSheet()
story = []

story.append(Paragraph("Daily CityBike Dashboard", styles['Title']))
story.append(Spacer(1, 12))
story.append(Paragraph(f"Total Bikes Rented: {total_bikes_num:,}", styles['Normal']))
story.append(Spacer(1, 24))

# Add charts
for chart_path in ["map_chart.png", "top_5_chart.png", "running_total_bike_chart.png", "avg_pct_chart.png"]:
    story.append(Image(chart_path, width=500, height=350))
    story.append(Spacer(1, 24))

doc.build(story)
print("PDF created successfully")

# COMMAND ----------

sender_email = "alan.notiverson@gmail.com"
receiver_email = "alan.notiverson@gmail.com"
password = email_password # Use an App Password, NOT your real password!

# Create email
msg = MIMEMultipart()
msg["From"] = sender_email
msg["To"] = receiver_email
msg["Subject"] = "Daily Bike Dashboard Report"

# Email body
body = "Hi,\n\nPlease find attached today's bike dashboard report.\n\nBest,\nDatabricks Job"
msg.attach(MIMEText(body, "plain"))

# Attach PDF file
pdf_filename = "daily_dashboard.pdf"
filename = pdf_filename
with open(filename, "rb") as attachment:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())

encoders.encode_base64(part)
part.add_header(
    "Content-Disposition",
    f"attachment; filename={filename}",
)
msg.attach(part)

context = ssl.create_default_context()

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.send_message(msg)
        print("Email sent successfully!")
except Exception as e:
    print("Error sending email:", e)
