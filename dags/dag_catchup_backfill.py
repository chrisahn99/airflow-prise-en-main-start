from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import timedelta, datetime

default_args = {
    'owner': 'chris',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="dag_catchup_backfill",
    default_args=default_args,
    description="Catchup backfill",
    start_date=datetime(2024, 3, 20),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="print_simple_message",
        bash_command="echo hello"
    )


