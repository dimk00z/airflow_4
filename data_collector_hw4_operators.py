import requests
import os
import telebot
from dotenv import load_dotenv
from pathlib import Path
from psycopg2.extras import DictCursor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from data_collector_hw4_utils import write_csv, get_table_data, csv_dict_reader


class LoadOrdersOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 temp_file_name: str,
                 orders_file_name: str,
                 url='https://airflow101.python-jitsu.club/orders.csv',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.temp_file_name = temp_file_name
        self.orders_file_name = orders_file_name

    def execute(self, *args, **kwargs):
        response = requests.get(self.url)
        response.raise_for_status()
        with open(self.temp_file_name, 'wb') as file:
            file.write(response.content)
        with open(self.temp_file_name, 'r') as in_file, \
                open(self.orders_file_name, 'w') as out_file:
            seen = set()
            for line in in_file:
                if line in seen:
                    continue
                seen.add(line)
                out_file.write(line)
        os.remove(self.temp_file_name)


class LoadTransactionsOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 transactions_file_name: str,
                 url='https://api.jsonbin.io/b/5ed7391379382f568bd22822',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.transactions_file_name = transactions_file_name

    def execute(self, *args, **kwargs):
        response = requests.get(self.url)
        response.raise_for_status()
        transaction_json = response.json()
        seen_transactions = []
        for transaction, transaction_data in transaction_json.items():
            if transaction and transaction_data:
                status = 'Successful operation' if transaction_data[
                    'success'] else \
                    f'Error: {". ".join(transaction_data["errors"])}'
                seen_transactions.append({
                    'transaction_uuid': transaction,
                    'transaction_status': status
                })
        write_csv(['transaction_uuid', 'transaction_status'],
                  seen_transactions, self.transactions_file_name)


class LoadGoodsCustomersOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 goods_file_name: str,
                 customers_file_name: str,
                 connection='postgres_goods_customers',
                 goods_table='goods',
                 goods_customers='customers',
                 url='https://api.jsonbin.io/b/5ed7391379382f568bd22822',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.customers_file_name = customers_file_name
        self.goods_file_name = goods_file_name
        self.goods_table = goods_table
        self.goods_customers = goods_customers
        self.connection = connection

    def load_postgres_table(self, table_name: str, file_name: str):
        request = f'SELECT * FROM {table_name}'
        with PostgresHook(
            postgres_conn_id=self.connection). \
                get_conn() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(request)
                headers, result_table = get_table_data(cursor.fetchall())
                write_csv(headers, result_table, file_name)

    def execute(self, *args, **kwargs):
        self.load_postgres_table(self.goods_table,
                                 self.goods_file_name)
        self.load_postgres_table(self.goods_customers,
                                 self.customers_file_name)


class FinalSaveDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 final_data_set_file: str,
                 connection_name="postgres_final_data_set",
                 table_name='home_work_2_data_set',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_name = connection_name
        self.table_name = table_name
        self.final_data_set_file = final_data_set_file

    def execute(self, *args, **kwargs):
        columns = ['uuid', 'name', 'age', 'good_title', 'date',
                   'payment_status', 'total_price', 'amount', 'last_modified_at']

        check_request = f"""
                SELECT *
                FROM information_schema.tables
                WHERE table_name='{self.table_name}'"""
        create_request = f"""
                    CREATE TABLE public.{self.table_name} (
                        uuid UUID PRIMARY KEY,
                        name varchar(255),
                        age int,
                        good_title varchar(255),
                        date timestamp,
                        payment_status text,
                        total_price numeric(10,2),
                        amount int,
                        last_modified_at timestamp)"""
        with PostgresHook(
            postgres_conn_id=self.connection_name). \
                get_conn() as connection:
            connection.autocommit = True
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                check_table = cursor.execute(check_request)
                if not bool(cursor.rowcount):
                    cursor.execute(create_request)
                final_data_set = csv_dict_reader(
                    self.final_data_set_file, 'uuid')
                fields = (',').join(columns)
                insert_update_requests = ''
                for uuid, data in final_data_set.items():
                    age = data['age'] if data['age'] else 'NULL'
                    values = f"""'{uuid}', '{data['name']}', {age}, '{data['good_title']}', '{data['date']}',
        '{data['payment_status']}', {data['total_price']}, {data['amount']}, '{data['last_modified_at']}'
                    """
                    values_for_update = ', '.join(
                        [f'{key} = EXCLUDED.{key}'
                            for key in data if key != 'uuid'])
                    insert_update_request = f"""
        INSERT INTO {self.table_name} ({fields})
        VALUES ({values})
        ON CONFLICT(uuid) DO UPDATE SET {values_for_update};"""
                    insert_update_requests = '\n'.join(
                        (insert_update_requests, insert_update_request))
                cursor.execute(insert_update_requests)


class TelegramErrorSendOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 use_proxy=True,
                 env_path=Path('.') / '.env',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        load_dotenv(dotenv_path=env_path)
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.use_proxy = use_proxy
        self.chat_id_for_send = os.getenv("TELEGRAM_CHAT_ID")
        if use_proxy:
            self.proxy = os.getenv("TELEGRAM_PROXY")

    def execute(self, *args, **kwargs):
        if self.proxy:
            telebot.apihelper.proxy = {
                'https': self.proxy}
        bot = telebot.TeleBot(self.token)
        context = kwargs['context']
        dag_id = context['dag'].dag_id
        ti = context['ti']
        previous_task_id = ti.xcom_pull(key=None, task_ids='check_data')
        message = f"""The data check task has worked incorrectly.
The dag_id is '{dag_id}', the previous task_id is '{previous_task_id}'.
by Dimk_Smith"""
        bot.send_message(self.chat_id_for_send, message)
