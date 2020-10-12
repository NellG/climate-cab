from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
import csv


def read_config():
    """Read config file and return dict."""
    with open('/home/ubuntu/code/.airflow-config.csv') as infile:
        reader = csv.reader(infile)
        config = {row[0]: row[1] for row in reader}
    return config

config = read_config()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,10,7),
    'email': [config['email']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'forecast_dag',
    description = 'Airflow DAG to update cab forecast and dashboard.',
    schedule_interval = timedelta(minutes=15),
    default_args = default_args
)

forecast = BashOperator(
    task_id = 'update_forecast',
    bash_command = 'python3 /home/ubuntu/code/owm-data.py',
    dag = dag 
)