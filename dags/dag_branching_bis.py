"""
DAG dans Airflow qui utilise un BashOperator pour
télécharger les données depuis une source externe et le PythonOperator pour les traiter.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
from datetime import timedelta
from pendulum import datetime
import numpy as np

def process_data(ti):
    df = pd.read_csv("/tmp/flowerdataset.csv")
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())

    key="longueur"
    valeur=len(df)
    ti.xcom_push(key, valeur)

def print_message(message):
    print(message)

def decision_making(ti):
    valeur = ti.xcom_pull(task_ids='process_data', key='longueur')
    if valeur > 1000:
        return "big_data"
    return "small_data"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    "dag_branching_bis",
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

branching_task = BranchPythonOperator(
    task_id="branching",
    python_callable=decision_making,
    dag=dag
)

fun_task = PythonOperator(
    task_id="big_data",
    python_callable=print_message,
    op_kwargs={"message": "Gros dataset"},
    dag=dag
)

bored_task = PythonOperator(
    task_id="small_data",
    python_callable=print_message,
    op_kwargs={"message": "Petit dataset"},
    dag=dag
)



download_data_task >> process_data_task >> branching_task >> [fun_task, bored_task]
