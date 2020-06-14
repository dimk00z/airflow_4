from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from data_collector_hw4_operators import LoadGoodsCustomersOperator, \
    TelegramErrorSendOperator, FinalSaveDataOperator, \
    LoadOrdersOperator, LoadTransactionsOperator
from data_collector_hw4_check_operators import PostgreCheckOperator, \
    DataCheckOperator

DIR_FOR_CSV_FILES = '/home/dimk/airflow/data/'
FILES = ['transactions', 'orders', 'goods',
         'temp', 'customers', 'final_dataset']
FILE_NAMES = {key: f'{DIR_FOR_CSV_FILES}{key}.csv' for key in FILES}

default_args = {
    'owner': 'Dimk_smith',
    'env_path': '/home/dimk/airflow/.env',
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

load_csv_op = LoadOrdersOperator(
    task_id='load_csv',
    temp_file_name=FILE_NAMES['temp'],
    orders_file_name=FILE_NAMES['orders'],
    dag=dag,
)

load_transactions_operations_op = LoadTransactionsOperator(
    task_id='load_transactions_operations',
    transactions_file_name=FILE_NAMES['transactions'],
    dag=dag,
)

load_from_postgres_op = LoadGoodsCustomersOperator(
    task_id='load_from_postgres',
    goods_file_name=FILE_NAMES['goods'],
    customers_file_name=FILE_NAMES['customers'],
    dag=dag,
)

check_data_op = DataCheckOperator(
    task_id='check_data',
    correct_way='save_data',
    wrong_way='send_error_to_chat',
    file_names=FILE_NAMES,
    provide_context=True,
    dag=dag,
)

save_data_op = FinalSaveDataOperator(
    task_id='save_data',
    final_data_set_file=FILE_NAMES['final_dataset'],
    dag=dag,
)

send_error_to_chat_op = TelegramErrorSendOperator(
    task_id='send_error_to_chat',
    dag=dag,
)

all_success_op = DummyOperator(task_id='all_success', dag=dag)


check_postgre_db_op >> [load_csv_op, all_failed_op]
load_csv_op >> load_transactions_operations_op >> load_from_postgres_op \
    >> check_data_op >> [save_data_op, send_error_to_chat_op]
save_data_op >> all_success_op
