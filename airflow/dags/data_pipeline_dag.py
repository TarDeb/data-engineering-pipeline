from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

ingest_task = BashOperator(
    task_id='ingest_data',
    bash_command='python3 /path/to/etl/ingest_data.py',
    dag=dag
)

transform_task = BashOperator(
    task_id='transform_data',
    bash_command='python3 /path/to/spark/spark_transform.py',
    dag=dag
)

load_task = BashOperator(
    task_id='load_data',
    bash_command='python3 /path/to/etl/load_data.py',
    dag=dag
)

ingest_task >> transform_task >> load_task
