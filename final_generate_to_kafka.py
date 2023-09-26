# Аттестационная работа. Вариант №1
# "Создание системы аналитики для обработки и визуализации данных использования услуги интерактивного телевидения Ростелеком".

from kafka import KafkaProducer
# библиотека для генерации "фальшивых" данных
from faker import Faker
import random
import json
import time
from time import sleep
from datetime import date
import psycopg2
import pandas as pd


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))

def random_date(start, end, prop):
    return str_time_prop(start, end, '%d-%m-%Y %H:%M', prop)

dbname = 'postgres'
user = 'kuznetsov'
password = 'w7ton'
host = '192.168.77.21'
port = 5432

try:
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)

    producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')
    fake = Faker('ru_RU')
    query = '''SELECT *  FROM final_kuznetsov_content  WHERE content_id = %(content_id)s'''

    while True:
        reg_id = fake.random_int(1, 85)
        us_id  = fake.pyint((reg_id - 1) * 10000 + 1, reg_id * 10000)
        cont_id = fake.random_int(1, 55)
        cf = pd.read_sql(query, con=conn, params={"content_id": cont_id})
        start_s = date.today().strftime("%d-%m-%Y") + " " + cf['start_s'].values[0].strftime("%H:%M")
        stop_s = date.today().strftime("%d-%m-%Y")  + " " + cf['stop_s'].values[0].strftime("%H:%M")
        channel = cf['channel'].values[0]

        message = {'user_id': us_id ,
                   'content_id': cont_id,
                   'start_s': start_s,
                   'stop_s': random_date(start_s, stop_s, random.random()),
                   'channel': channel,
                   'region_id': reg_id
                   }
        # сериализируем объект dict в строку str формата JSON.
        message_json = json.dumps(message).encode('utf-8')
        # отправляем сообщение в очередь kafka
        producer.send('final.kuznetsov', value=message_json)
        print("[INFO] Действие абонета интерактивного ТВ:  {}".format(message))
        sleep(2)
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    producer.close()
    conn.close()
