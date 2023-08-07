# Get and insert test data for HomeWork #7

import csv
import pandas as pd
import numpy as np
import psycopg2

# Параметры для подключения к PostgreSQL
host = "192.168.77.21"
user = "kuznetsov"
password = "w7ton"
db_name = "postgres"
# port default = 5432
port = 5432

users_cnt = 10

try:
    # открываем соединение с PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name
    )
    # Включим для нашей задачи авто-коммит по умолчанию
    conn.autocommit = True

    for x in range(1, users_cnt+1):
        df = pd.read_fwf('opendns-random-domains.txt', names=['site'], header=None)
        df.insert(0, 'user_id', x, True)
        df['bytes'] = np.random.randint(1024, 40960000, df.shape[0])
        df.sample(frac=0.50, replace=True)
        for i, row in df.iterrows():
            # Вытаскиваем данные из строки списка
            user_id = row[0]
            site = row[1]
            bytes = row[2]
            with conn.cursor() as cur:
                cur.execute("INSERT INTO public.user_log_kuznetsov  VALUES (%s,%s,%s);", (user_id, site, bytes))
        print("[INFO] Загружено {} записей:".format(i))
except Exception as _ex:
    print("[ERR] Ошибка работы с PostgreSQL: ", _ex)
finally:
    # Закрываем соединение с БД
    if conn:
        conn.close()
        print("\n[INFO] Соединение с PostgreSQL закрыто.")
