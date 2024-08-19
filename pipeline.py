import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(parent_dir)

from extract import multi_threaded_download
from transform_and_load import transform_and_load

from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

from pytz import timezone

local_tz = timezone('Asia/Bangkok')

# URL of the file to download
url = "https://cdn.searchspring.net/help/feeds/searchspring.json"
output_file = "downloaded_file.ndjson"

with DAG(
    "ETL",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple ETL",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    tags=["etl"],
) as dag:
    
    multi_threaded_download_task = PythonOperator(
        task_id='multi_threaded_download',
        python_callable=multi_threaded_download,
        op_args=[url, output_file]
    )

    transform_and_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
    )

    multi_threaded_download_task >> transform_and_load_task