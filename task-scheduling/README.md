# Taximizer Automation

Airflow was used to automate updating the historical data batch job and creation of the current forecast tables. Airflow will send email alert for any failed tasks. The task to update the current forecast runs every 15 minutes. The batch process tasks run monthly:

* email sent reminding user to update weather data

    The weather dataset used is not yet available through the NOAA API
* current year cab data downloaded from Chicago Data Portal
* downloaded cab data uploaded to S3 bucket
* Spark job run on cluster (cluster assumed to be running already)