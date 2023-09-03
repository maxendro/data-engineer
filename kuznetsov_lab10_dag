# Лабораторная работа №10.
# Инструмент Apache Airflow и создание DAG файлов

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import configuration
from airflow.operators import PostgresOperator
from airflow.operators import PythonOperator
import requests

DAG_NAME = 'kuznetsov_dag'  #
GP_CONN_ID = 'kuznetsov_conn'

MESS = ' Живите долго и процветайте!'
SQL = fr"insert into lab10_kuznetsov(message) values (E'\xf0\x9f\x96\x96'||' {MESS}');"

args = {'owner': 'kuznetsov',
        'start_date': datetime(2023, 9, 3),
        'retries': 3,
        'retry_delay': timedelta(seconds=600)}


def start_task(**kwargs):
    print('Start')


def finish_task(**kwargs):
    # send_text = f'https://api.telegram.org/bot968097013:AAGfYL_p6CJmfcZctBN81MwEsmgZ4zeENX0/sendMessage?chat_id=-1001915901409&parse_mode=Markdown&text=\U0001F596{MESS}'
    # response = requests.get(send_text)
    print('Finish')


with DAG(DAG_NAME, description='kuznetsov_DAG',
         schedule_interval='* * * * *',
         catchup=False,
         max_active_runs=1,
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    start_operator = PythonOperator(task_id='start',
                                    python_callable=start_task,
                                    provide_context=True)
    finish_operator = PythonOperator(task_id='finish',
                                     python_callable=finish_task,
                                     provide_context=True)
    sql_stat = PostgresOperator(task_id='greenplum',
                                sql=SQL,
                                postgress_conn_id=GP_CONN_ID,
                                autocommit=True)
    start_operator >> sql_stat >> finish_operator
