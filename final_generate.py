# Аттестационная работа. Вариант №1
# "Создание системы аналитики для обработки и визуализации данных использования услуги интерактивного телевидения Ростелеком".

# библиотека для генерации "фальшивых" данных
from faker import Faker
import random
import json
import time
import pg8000
import pandas as pd


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))


dbname = 'postgres'
user = 'kuznetsov'
password = 'w7ton'
host = '192.168.77.21'
port = 5432


def random_date(start, end, prop):
    return str_time_prop(start, end, '%d-%m-%Y %H:%M', prop)

try:
    conn = pg8000.connect(database=dbname, user=user, password=password, host=host, port=port)
       # Выставляем локаль для faker
    fake = Faker('ru_RU')
    df_content = pd.read_sql("""select * from public.final_kuznetsov_content order by content_id""", conn)
    df_regions = pd.read_sql("""select * from public.final_kuznetsov_regions order by region_id""", conn)

    # folow by region and rating table
    for row_r in df_regions.iterrows():
        for row_c in df_content.iterrows():
            for i in range(int(row_r[1][2] * (row_c[1][5]) / 100)):
                start_s = str(fake.date_time_this_year().strftime("%d-%m-%Y"))
                message = {'user_id':  fake.pyint((row_r[1][0]-1)*10000+1,row_r[1][0]*10000),
                           'content_id': row_c[1][0],
                           'start_s': random_date(start_s + " " + row_c[1][2].strftime("%H:%M"), start_s + " " + row_c[1][2].strftime("%H:%M"), random.random()),
                           'stop_s': start_s + " " + row_c[1][3].strftime("%H:%M"),
                           'channel': row_c[1][4],
                           'region_id': row_r[1][0]
                           }

                sql = (
                    f"INSERT INTO  public.final_kuznetsov_log_temp VALUES ('{(message['user_id'])}', '{message['content_id']}','{message['start_s']}', '{message['stop_s']}', '{message['channel']}', '{message['region_id']}')"
                )
                with conn.cursor() as cur:
                    cur.execute(sql)
                    conn.commit() #  Faster if commit every 1000 messages
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    # producer.close()
    conn.close()
