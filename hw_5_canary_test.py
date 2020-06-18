from datetime import timedelta
import os
import time
import random
import requests

from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from telegram_eventer import TelegramEventer

ENV_FILE = '/home/dimk/airflow/.env'

telegram_eventer = TelegramEventer(env_path=ENV_FILE)


def canary_test(ds, **kwargs):
    time.sleep(15)
    the_first_letter = random.choice(["y", "", "", "", "", ""])
    url = f'https://{the_first_letter}a.ru/'
    time.sleep(random.choice([15, 30, 45]))
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    time.sleep(random.choice([15, 30, 45]))


default_args = {
    'owner': 'Dimk_smith',
    'start_date': days_ago(7),
}

with DAG(dag_id='hw_5_canary_test',
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         sla_miss_callback=telegram_eventer.send_sla,
         on_failure_callback=telegram_eventer.send_message,
         ) as dag:

    starting_point = DummyOperator(task_id='start_here', dag=dag)

    task_canary_op = PythonOperator(
        task_id='canary_test',
        sla=timedelta(seconds=23),
        python_callable=canary_test,
    )

    all_success_op = DummyOperator(task_id='all_success', dag=dag)

    starting_point >> task_canary_op >> all_success_op
