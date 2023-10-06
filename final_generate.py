# Аттестационная работа. Вариант №1
# "Создание системы аналитики для обработки и визуализации данных использования услуги интерактивного телевидения Ростелеком".
# Модуль генерации данных напрямую в parquet-файл.  Для быстрого заполнения тестовыми данными.

# библиотека для генерации "фальшивых" данных
from faker import Faker
import random
import pandas as pd
import time
from datetime import datetime, timedelta
# USE SPARK for   can also be used pyarrow.parquet
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))

def random_date(start, end, prop):
    return str_time_prop(start, end, '%d-%m-%Y %H:%M:%S', prop)

try:
    fake = Faker()
    df_content = pd.read_sql("""select * from public.final_kuznetsov_content where content_id<=30 order by content_id""", conn)
    df_regions = pd.read_sql("""select * from public.final_kuznetsov_regions where region_id<=10 order by region_id""", conn)

    rows = []
    k = 0
    spark = SparkSession.builder.master("local[*]").appName('kuznetsov').getOrCreate()
    sc = SparkContext.getOrCreate()
    dataschema = StructType([
        StructField('user_id', IntegerType(), False),
        StructField('content_id', IntegerType(), False),
        StructField('start_s', TimestampType(), False),
        StructField('stop_s', TimestampType(), False),
        StructField('channel_id', IntegerType(), False),
        StructField('region_id', IntegerType(), False)
    ])
    struct = StructType(fields=dataschema)

    # folow by region and rating table
    for row_r in df_regions.iterrows():
        for row_c in df_content.iterrows():
            for i in range(int(row_r[1][2] * (row_c[1][5]) / 5000)):
                start_s = str(fake.date_time_this_year().strftime("%d-%m-%Y"))
                # if stop session next day
                if row_c[1][3].strftime("%H:%M:%S")  < row_c[1][2].strftime("%H:%M:%S") :
                   stop_s = datetime.strptime( start_s  + " " + row_c[1][3].strftime("%H:%M:%S"), "%d-%m-%Y %H:%M:%S") + timedelta(days=1)
                else:
                   stop_s = datetime.strptime( start_s  + " " + row_c[1][3].strftime("%H:%M:%S"), "%d-%m-%Y %H:%M:%S")

                message = {'user_id':  fake.pyint((row_r[1][0]-1)*10000+1,row_r[1][0]*10000),
                           'content_id': row_c[1][0],
                           'start_s': datetime.strptime( start_s + " " + row_c[1][2].strftime("%H:%M:%S"), "%d-%m-%Y %H:%M:%S" ),
                           'stop_s': datetime.strptime(random_date(start_s + " " + row_c[1][2].strftime("%H:%M:%S") , stop_s.strftime("%d-%m-%Y %H:%M:%S"), random.random()), "%d-%m-%Y %H:%M:%S" ),
                           'channel_id': int(row_c[1][7]),
                           'region_id': row_r[1][0]
                           }
                if message is not None:
                    rows.append(message)
                    k = k + 1
                    if k == 10000:
                        rdd = sc.parallelize(rows)
                        df = spark.createDataFrame(rdd.collect(), schema=struct)
                        df.write.mode('append').parquet(f'hdfs://vm-dlake2-m-1.test.local/user/kuznetsov/data/final_kuznetsov.parquet')
                        rows = []
                        k = 0
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:

    conn.close()
