
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_github_sync_dag',
    default_args=default_args,
    description='A simple DAG synced from GitHub',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World from GitHub Synced DAG!"',
    )

    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t1 >> t2
