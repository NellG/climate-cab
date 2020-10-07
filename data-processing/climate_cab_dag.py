from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def update_forecast():
    """Update cab forecast."""


def update_dashboard():
    """Update cab dashboard."""


dag = DAG(
    dag_id = 'climate_cab_dag',
    description = 'Airflow DAG to update cab forecast and dashboard.',
    default_args = default_args
)

forecast = PythonOperator(
    task_id = 'update_forecast',
    python_callable = update_forecast,
    dag = dag 
)

dashboard = PythonOperator(
    task_id = 'update_dashboard',
    python_callable = update_dashboard,
    dag = dag 
)

# setting dependencies
forecast >> dashboard