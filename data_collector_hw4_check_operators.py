import logging
import datetime
import re
from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from psycopg2 import OperationalError
from typing import List
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import DictCursor
from dateutil import relativedelta
from data_collector_hw4_utils import write_csv, csv_dict_reader


class PostgreCheckOperator(BaseBranchOperator):

    def __init__(self, correct_way: str, wrong_way: str,
                 connections: List[str], * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.correct_way = correct_way
        self.wrong_way = wrong_way
        self.connections = connections

    def choose_branch(self, *args, **kwargs):
        try:
            for connection in self.connections:
                with PostgresHook(
                        postgres_conn_id=connection).get_conn() as connection:
                    with connection.cursor(cursor_factory=DictCursor) as cursor:
                        continue
            return [self.correct_way]
        except (AirflowException, OperationalError) as exception:
            logging.error(exception)
            return [self.wrong_way]


class DataCheckOperator(BaseBranchOperator):

    def __init__(self, correct_way: str, wrong_way: str,
                 file_names, * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.correct_way = correct_way
        self.wrong_way = wrong_way
        self.file_names = file_names

    def sanitize_name(self, name):
        regex = re.compile('[@_!#$%^&*()<>?/.,\|}{~:-]')
        sanitized_name = ' '.join(
            [word for word in name.split() if not regex.search(word)])
        return (sanitized_name, len(sanitized_name.split()) < 2)

    def create_final_dataset(self, **kwargs):
        transactions = csv_dict_reader(
            self.file_names['transactions'], 'transaction_uuid')
        goods = csv_dict_reader(self.file_names['goods'], 'name')
        orders = csv_dict_reader(self.file_names['orders'], 'uuid заказа')
        customers = csv_dict_reader(self.file_names['customers'], 'email')
        today = datetime.datetime.today()
        headers = ['uuid', 'name', 'age', 'good_title', 'date',
                   'payment_status', 'total_price', 'amount', 'last_modified_at']
        result_data_set = []
        for uuid_order, order in orders.items():
            try:
                should_skip = False
                row = {}
                customer_email = order['email']
                row['uuid'] = uuid_order
                if customer_email in customers:
                    customer_birth_date = customers[customer_email]['birth_date']
                    row['age'] = relativedelta.relativedelta(
                        today, datetime.datetime.strptime(customer_birth_date,
                                                          '%Y-%m-%d')).years
                    row['name'], should_skip = self.sanitize_name(
                        customers[customer_email]['name'])
                else:
                    row['age'] = None
                    row['name'], should_skip = self.sanitize_name(order['ФИО'])

                row['last_modified_at'] = datetime.datetime.now()

                row['good_title'] = order['название товара'] \
                    if order['название товара'] else None
                if row['good_title'] is None:
                    should_skip = True
                row['date'] = order['дата заказа']
                price = float(goods[order['название товара']]['price'])

                if int(order['количество']) < 1 or price <= 0:
                    should_skip = True
                row['amount'] = int(order['количество'])
                row['total_price'] = round(
                    row['amount'] * price, 2)
                row['payment_status'] = transactions[uuid_order]['transaction_status']
                if not should_skip:
                    result_data_set.append(row)
            except (ValueError, TypeError):
                continue
        if len(transactions) * 0.7 > len(result_data_set) \
                or len(result_data_set) < 10:
            return False

        write_csv(headers, result_data_set, self.file_names['final_dataset'])
        return True

    def choose_branch(self, context, *args, **kwargs):
        if self.create_final_dataset():
            return [self.correct_way]
        ti = context['ti']
        ti.xcom_push(key='task_id', value=ti.task_id)
        return [self.wrong_way]
