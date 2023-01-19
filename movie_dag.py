from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from movie_etl import run_movie_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 18),
    'email': ['airflow_project@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'movie_dag',
    default_args=default_args,
    description='Movie ETL Project'
)

run_etl = PythonOperator(
    task_id='complete_movie_etl',
    python_callable=run_movie_etl,
    dag=dag,
)

run_etl