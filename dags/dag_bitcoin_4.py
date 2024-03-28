import logging
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import random
import requests
from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}localization=fr"


@dag(dag_id="dag_bitcoin_4", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price(task_instance) -> Dict[str, float]:
        execution_date = task_instance.execution_date
        dates = []
        prices = []
        formatted_date = execution_date.strftime("%d-%m-%Y")
        response = requests.get(API.format(formatted_date)).json()["market_data"]
        initial_price = response["current_price"]["usd"]

        for n_day_before in range(0, 10):
            date = execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(date)
            prices.append(initial_price + random_variation)
        return {"dates": dates, "prices": prices}

    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        logging.info(extraction)
        prices = extraction["prices"][::-1]
        print(prices)
        deltas = np.diff(prices)
        print(deltas)
        ups = [delta for delta in deltas if delta > 0]
        downs = [-delta for delta in deltas if delta < 0]
        up_avg = sum(ups) / len(ups)
        print(up_avg)
        down_avg = sum(downs) / len(downs)
        print(down_avg)
        rsi = 100 - 100/(1 + up_avg/down_avg)
        return {
            "last_price": prices[-1],
            "rsi": rsi,
        }

    @task
    def store_data(data: Dict[str, float]):
        logging.info(
            f"Store: Last price {data['last_price']} with rsi {data['rsi']}"
        )

    store_data(process_data(extract_bitcoin_price()))

taskflow()