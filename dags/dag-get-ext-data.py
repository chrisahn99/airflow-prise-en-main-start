"""
DAG dans Airflow qui utilise un BashOperator pour
télécharger les données depuis une source externe et le PythonOperator pour les traiter.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
from datetime import timedelta
from pendulum import datetime

def process_data():
    df = pd.read_csv("/tmp/flowerdataset.csv")
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())

    largest_sepal_flower = df.sort_values(by="sepal_length", ascending=False).iloc[0]

    return {
        "somme": largest_sepal_flower["somme"],
        "sepal_length": largest_sepal_flower["sepal_length"],
        "sepal_width": largest_sepal_flower["sepal_width"],
    }

def largest_length():
    df = pd.read_csv("/tmp/flowerdataset.csv")
    largest_sepal_flower = df.sort_values(by="sepal_length", ascending=False).iloc[0]
    return largest


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    "dag-get-ext-data",
    default_args=default_args,
    description="Get ext data",
    schedule_interval="@daily"
) 

download_data_task = BashOperator(
    task_id="download_data",
    bash_command="curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv",
    dag=dag
)

process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag
)

create_task = PostgresOperator(
    task_id='create_postgres_table',
    postgres_conn_id='postgres_localhost',
    sql="""
        create table if not exists flower (
            somme FLOAT,
            sepal_length FLOAT,
            sepal_width FLOAT
        )
    """,
    dag=dag
)

insert_task = PostgresOperator(
    task_id='insert_into_table',
    postgres_conn_id='postgres_localhost',
    sql="""
        insert into flower (somme, sepal_length, sepal_width) 
        values (
            {{task_instance.xcom_pull(task_ids='process_data')['somme']}}, 
            {{task_instance.xcom_pull(task_ids='process_data')['sepal_length']}}, 
            {{task_instance.xcom_pull(task_ids='process_data')['sepal_width']}}
        )
    """
)

download_data_task >> process_data_task 
create_task >> insert_task
process_data_task >> insert_task