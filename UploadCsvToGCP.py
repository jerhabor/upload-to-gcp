# In this script, I am uploading the latest Customer Satisfaction (CSAT) Data for Xumo Stream Box (a puck for TV apps in the US) to Google Cloud Platform (GCP)

import time
from google.cloud import bigquery
from pathlib import Path
import os
import datetime as dt
import pandas as pd

# Get the latest month
latest_month_and_year = ((pd.Period(dt.datetime.now(), 'M') - 1).strftime('%B %Y')).upper()
downloads_path = str(Path.home() / "Downloads")
print(downloads_path)

# Rename file with the latest CSAT data. Note that the filename is always saved as "Xumo Stream Box CSAT - <MONTH> <YEAR>"
for filename in os.listdir(downloads_path):
    if filename.startswith(("Xumo Stream Box CSAT - {}").format(latest_month_and_year)):
        os.rename(filename,("Xumo Stream Box CSAT - {}.csv").format(latest_month_and_year))

# Construct a BigQuery client object
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = [ - HIDDEN DUE TO DATA PRIVACY REASONS - ]
client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    allow_quoted_newlines=True
)
job_config.write_disposition = "WRITE_APPEND" # append to the existing table in GCP rather than overwrite it with WRITE_TRUNCATE

# Set table_id to the ID of the table to create
table_id = [ - HIDDEN DUE TO DATA PRIVACY REASONS - ]

with open(("Xumo Stream Box CSAT - {}.csv").format(latest_month_and_year), "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

# Upload table to BigQuery
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

table = client.get_table(table_id)   # Make an API request
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
