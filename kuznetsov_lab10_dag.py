# Лабораторная работа №10.
# Инструмент Apache Airflow и создание DAG файлов

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import requests

# Название DAG-файла
DAG_NAME = 'kuznetsov_dag'
# Коннект, созданный в web-интерфейсе в Airflow
GP_CONN_ID = 'kuznetsov_conn'

MESS = ' Live long and prosper!' # Живите долго и процветайте!
sql_text = f"insert into public.lab10_kuznetsov(message) values ('\U0001F596'||'{MESS}');"

args = {'owner': 'kuznetsov',
        'start_date': datetime(2023, 9, 3), # дата запуска
        'retries': 3,    # количество попыток в случае аварии
        'retry_delay': timedelta(seconds=300)}  # время между попытками

def send_mess(**kwargs):
     send_text = f'https://api.telegram.org/bot968097013:AAGfYL_p6CJmfcZctBN81MwEsmgZ4zeENX0/sendMessage?chat_id=-1001915901409&parse_mode=Markdown&text=\U0001F596{MESS}'
     response = requests.get(send_text)

# конструктор DAG
with DAG(DAG_NAME, description='kuznetsov_DAG',
         schedule_interval='*/2 * * * *', #расписание, формат CRON
         catchup=False, # если упал DAG, догонять очередь не надо
         max_active_runs=1, # не запускать пока висит не завершенный
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    #PythonOperator  - отправляем сообщение в Telegramm
    send_message_to_tg = PythonOperator(task_id='send_tg',
                                        python_callable=send_mess,
                                        provide_context=True)
    # PostgresOperator  - вставляем запись в таблицу
    insert_to_gp = PostgresOperator(task_id='insert_gp',
                                sql=sql_text,
                                postgres_conn_id=GP_CONN_ID,
                                autocommit=True)
    # Граф выполнения (выстраивание процесса последовательности)
    send_message_to_tg >> insert_to_gp
