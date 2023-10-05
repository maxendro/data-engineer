# Аттестационная работа. Вариант №1

import datetime
import requests
from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator


# Название DAG-файла
DAG_NAME = 'final_kuznetsov_dag'
# Коннект, созданный web-интерфейсе в Airflow
GP_CONN_ID = 'kuznetsov_conn'

sql_upd = "REFRESH MATERIALIZED VIEW mv_final_kuznetsov"

args = {'owner': 'kuznetsov',
        'start_date': datetime(2023, 10, 5), # дата запуска
        'retries': 3,    # количество попыток в случае аварии
        'retry_delay': timedelta(seconds=600)}  # время между попытками

# конструктор DAG
with DAG(DAG_NAME, description='update_mv_kuznetsov_DAG',
         schedule_interval='0 0 1 * *', #расписание, формат CRON
         catchup=False, # если упал DAG, догонять очередь не надо
         max_active_runs=1, # не запускать пока висит не завершенный
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    sql_upd = PostgresOperator(task_id="mview_update_kuznetsov",
                                sql=SQL_STAT,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)
sql_upd
