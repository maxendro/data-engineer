# Аттестационная работа. Вариант №1
# "Создание системы аналитики для обработки и визуализации данных использования услуги интерактивного телевидения Ростелеком".

from kafka import KafkaProducer
# библиотека для генерации "фальшивых" данных
from faker import Faker
import random
import json
import time
from time import sleep
from datetime import datetime, timedelta
import psycopg2
import pandas as pd


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))

def random_date(start, end, prop):
    return str_time_prop(start, end, '%d-%m-%Y %H:%M:%S', prop)

dbname = 'postgres'
user = 'kuznetsov'
password = 'w7ton'
host = '192.168.77.21'
port = 5432

try:
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)
    producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')

    fake = Faker('ru_RU')
    df_content = pd.read_sql( """select * from public.final_kuznetsov_content where content_id<=30 order by content_id""", conn)
    df_regions = pd.read_sql("""select * from public.final_kuznetsov_regions where region_id<=10 order by region_id""",     conn)

    for row_r in df_regions.iterrows():
        for row_c in df_content.iterrows():
            for i in range(int(row_r[1][2] * (row_c[1][5]) / 5000)):
                start_s = str(fake.date_time_this_year().strftime("%d-%m-%Y"))
                # if stop session next day
                if row_c[1][3].strftime("%H:%M:%S") < row_c[1][2].strftime("%H:%M:%S"):
                    stop_s = datetime.strptime(start_s + " " + row_c[1][3].strftime("%H:%M:%S"),
                                               "%d-%m-%Y %H:%M:%S") + timedelta(days=1)
                else:
                    stop_s = datetime.strptime(start_s + " " + row_c[1][3].strftime("%H:%M:%S"), "%d-%m-%Y %H:%M:%S")

                message = {'user_id': fake.pyint((row_r[1][0] - 1) * 10000 + 1, row_r[1][0] * 10000),
                           'content_id': row_c[1][0],
                           'start_s': datetime.strptime(start_s + " " + row_c[1][2].strftime("%H:%M:%S"),
                                                        "%d-%m-%Y %H:%M:%S"),
                           'stop_s': datetime.strptime(random_date(start_s + " " + row_c[1][2].strftime("%H:%M:%S"),
                                                                   stop_s.strftime("%d-%m-%Y %H:%M:%S"),
                                                                   random.random()), "%d-%m-%Y %H:%M:%S"),
                           'channel_id': int(row_c[1][7]),
                           'region_id': row_r[1][0]
                           }
                # сериализируем объект dict в строку str формата JSON.
                message_json = json.dumps(message, default=str).encode('utf-8')
                # отправляем сообщение в очередь kafka
                producer.send('final.kuznetsov', value=message_json)
                print("[INFO] Действие абонета интерактивного ТВ:  {}".format(message))
                sleep(1)
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    producer.close()
    conn.close()
