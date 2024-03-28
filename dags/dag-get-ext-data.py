"""
DAG dans Airflow qui utilise un BashOperator pour
télécharger les données depuis une source externe et le PythonOperator pour les traiter.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import timedelta
from pendulum import datetime

def process_data():
    df = pd.read_csv("/tmp/flowerdataset.csv")
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())


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

dowload_data_task = BashOperator(
    task_id="download_data",
    bash_command="curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv",
    dag=dag
)

process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag
)

dowload_data_task >> process_data_task