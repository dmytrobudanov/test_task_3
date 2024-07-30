from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

API_KEY = os.getenv('OPENEXCHANGE_API_KEY')
BASE_URL = 'https://openexchangerates.org/api'

def fetch_currency_rates():
    url = f"{BASE_URL}/historical/{datetime.now().strftime('%Y-%m-%d')}.json"
    params = {'app_id': API_KEY}
    response = requests.get(url, params=params)
    data = response.json()
    return data['rates']

def save_to_parquet(data):
    df = pd.DataFrame(data.items(), columns=['currency', 'rate'])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f'/opt/airflow/data/currency_rates_{datetime.now().strftime("%Y-%m-%d")}.parquet')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 1,
}

dag = DAG(
    'currency_rates_etl',
    default_args=default_args,
    description='Fetch and store currency rates as PARQUET files',
    schedule_interval='@daily',
)

fetch_task = PythonOperator(
    task_id='fetch_currency_rates',
    python_callable=fetch_currency_rates,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_parquet',
    python_callable=lambda: save_to_parquet(fetch_currency_rates()),
    dag=dag,
)

fetch_task >> save_task
