from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, \
    BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from data_collector_hw4_operators import load_orders_csv, \
    load_transactions_operations
from data_collector_hw4_operators import load_from_postgres, \
    save_data, send_error_to_chat

from data_collector_hw4_check_operators import PostgreCheckOperator, DataCheckOperator

default_args = {
    'owner': 'Dimk_smith',
    'start_date': days_ago(2)}

dag = DAG(dag_id='data_collector_hw4',
          schedule_interval='@once',
          default_args=default_args)

check_postgre_db_op = PostgreCheckOperator(
    task_id='check_postgre_db',
    correct_way='load_csv',
    wrong_way='all_failed',
    connections=['postgres_goods_customers', 'postgres_final_data_set'],
    provide_context=True,
    dag=dag,
)

all_failed_op = DummyOperator(task_id='all_failed', dag=dag)

load_csv_op = PythonOperator(
    task_id='load_csv',
    provide_context=True,
    python_callable=load_orders_csv,
    dag=dag,
)

load_transactions_operations_op = PythonOperator(
    task_id='load_transactions_operations',
    provide_context=True,
    python_callable=load_transactions_operations,
    dag=dag,
)

load_from_postgres_op = PythonOperator(
    task_id='load_from_postgres',
    provide_context=True,
    python_callable=load_from_postgres,
    dag=dag,
)


check_data_op = DataCheckOperator(
    task_id='check_data',
    correct_way='save_data',
    wrong_way='send_error_to_chat',
    connections=['postgres_goods_customers', 'postgres_final_data_set'],
    provide_context=True,
    dag=dag,
)

save_data_op = PythonOperator(
    task_id='save_data',
    provide_context=True,
    python_callable=save_data,
    dag=dag,
)

send_error_to_chat_op = PythonOperator(
    task_id='send_error_to_chat',
    provide_context=True,
    python_callable=send_error_to_chat,
    dag=dag,
)

all_success_op = DummyOperator(task_id='all_success', dag=dag)


check_postgre_db_op >> [load_csv_op, all_failed_op]
load_csv_op >> load_transactions_operations_op >> load_from_postgres_op \
    >> check_data_op >> [save_data_op, send_error_to_chat_op]
save_data_op >> all_success_op
