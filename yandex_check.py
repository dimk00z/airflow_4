from datetime import timedelta
import os
import time
import random
import requests

from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

from airflow.hooks.http_hook import HttpHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from telegram_eventer import TelegramEventer

ENV_FILE = '/home/dimk/airflow/.env'

telegram_eventer = TelegramEventer(env_path=ENV_FILE)


def yandex_check(ds, **kwargs):
    time.sleep(15)
    the_first_letter = random.choice(["y", "", "", "", "", ""])
    url = f'https://{the_first_letter}a.ru/'
    time.sleep(random.choice([15, 30, 45]))
    response = requests.get(url, timeout=3)
    response.raise_for_status()


default_args = {
    'owner': 'Dimk_smith',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email': ['dimk00z@mail.com'],
    'on_success_callback': telegram_eventer.send_message,
    'on_retry_callback': telegram_eventer.send_message,
    'on_failure_callback': telegram_eventer.send_message,
    #    'sla_miss_callback': telegram_eventer.send_sla,
    'sla': timedelta(seconds=10),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='yandex_checker',
    default_args=default_args,
    #    schedule_interval='@once'
    schedule_interval=timedelta(hours=1)
)

starting_point = DummyOperator(task_id='start_here', dag=dag)
yandex_check_op = PythonOperator(
    task_id='yandex_check',
    provide_context=True,
    python_callable=yandex_check,
    dag=dag,
    sla=timedelta(seconds=10),
    sla_miss_callback=telegram_eventer.send_sla,
)
all_success_op = DummyOperator(task_id='all_success', dag=dag)

starting_point >> yandex_check_op >> all_success_op
