import json
import requests
import boto3
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import datetime

start_date = airflow.utils.dates.days_ago(2)  # 2 days ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["onidajo99@gmail.com"],
    "email_on_failure": False,
    "email_on_rety": False,
    "reries": 1,
    "retry_delay": datetime.timedelta(minutes=5)
}

def json_scraper(url, file_name, bucket):
    print('start running')
    response = requests.request("GET", url)
    json_data=response.json()

    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)


    print('end running')
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket,f"predict/{file_name}")

with DAG(
    "raw_predictit",
    default_args=default_args,
    description="",
    schedule_interval = datetime.timedelta(days=3),
    start_date=start_date,
    catchup=False,
    tags=["sdg"]
) as dag:
    

    extract_predictit = PythonOperator(
    task_id='extract_predictit',
    python_callable=json_scraper,
    op_kwargs={
                'url':"https://www.predictit.org/api/marketdata/all/",
                'file_name': 'predict_markets.json',
                'bucket':"data-mwaa-predict"},
    dag=dag
    )

    ready = DummyOperator(task_id='ready')

    extract_predictit >> ready
