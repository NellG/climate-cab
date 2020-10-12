from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
import csv

# Make this quarterly, include email to download more weather data - keep weather data ~3 mo back so cab data is mostly complete (check cab completeness to see appropriate delay)
# In email flag to switch years in June or whenever


def read_config():
    """Read config file and return dict."""
    with open('/home/ubuntu/code/.airflow-config.csv') as infile:
        reader = csv.reader(infile)
        config = {row[0]: row[1] for row in reader}
    return config

config = read_config()
cab_url = 'https://data.cityofchicago.org/api/views/r2u4-wwk3/rows.csv?accessType=DOWNLOAD'
cab_file = 'chi_2020.csv'
wthr_url = '???'
wthr_file = 'chi-weather_2020.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,10,7),
    'email': [config['email']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15)
}

dag = DAG(
    dag_id = 'monthly_batch_dag',
    description = 'Download cab and weather data and move to S3.',
    schedule_interval = timedelta(months=1),
    default_args = default_args
)

wthr_download = BashOperator(
    task_id = 'download_weather_data',
    bash_command = '???',
    dag = dag 
)

wther_upload = BashOperator(
    task_id = 'upload_weather_data_to_s3',
    bash_command = 'aws s3 cp ~/data/' + wthr_file \
                 + ' s3://chi-cab-bucket/weather/' + wthr_file,
    dag = dag
)

cab_download = BashOperator(
    task_id = 'download_cab_data',
    retries = 0,
    bash_command = 'wget -O ~/data/' + cab_file + ' ' + cab_url,
    dag = dag 
)

cab_upload = BashOperator(
    task_id = 'upload_cab_data_to_s3',
    bash_command = 'aws s3 cp ~/data/' + cab_file \
                 + ' s3://chi-cab-bucket/taxi/' + cab_file,
    dag = dag
)

run_spark = BashOperator(
    task_id = 'run_spark',
    bash_command = '???',
    dag = dag
)

# setting dependencies
wthr_download >> wthr_upload >> cab_download >> cab_upload >> run_spark
