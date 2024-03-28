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

def print_message(message):
    print(message)

def decision_making():
    if np.random.randint(0,1) == 0:
        return "bored"
    return "fun"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    "dag_branching",
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
    task_id="fun",
    python_callable=print_message,
    op_kwargs={"message": "malheureusement je dois travailler"},
    dag=dag
)

bored_task = PythonOperator(
    task_id="bored",
    python_callable=print_message,
    op_kwargs={"message": "cool je peux recueillir des fleurs"},
    dag=dag
)



download_data_task >> process_data_task >> branching_task >> [fun_task, bored_task]
