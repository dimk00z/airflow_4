import logging
from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from psycopg2 import OperationalError
from typing import List
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import DictCursor


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
                 connections: List[str], * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.correct_way = correct_way
        self.wrong_way = wrong_way
        self.connections = connections

    def choose_branch(self, *args, **kwargs):
        return [self.wrong_way]
