# Аттестационная работа. Вариант №1
# Модуль создания consumer, который будет вычитывать данные из очереди сообщений и класть их в parquet-file на HDFS


from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import json

try:
    consumer = KafkaConsumer('final.kuznetsov', bootstrap_servers='vm-strmng-s-1.test.local:9092',
                             group_id='kuznetsov_group')
    rows = []
    i = 0
    for message in consumer:
        # Преобразовать строку JSON в объект Python (dict)
        message_dict = json.loads(message.value)
        print(message_dict)
        if message is not None:
            rows.append(message_dict)
            i = i + 1
            if i == 10:
                spark = SparkSession.builder.master("local[*]").appName('kuznetsov').getOrCreate()
                sc = SparkContext.getOrCreate()
                print(rows)
                dataschema = StructType([
                    StructField('user_id', IntegerType(), False),
                    StructField('content_id', IntegerType(), False),
                    StructField('start_s', StringType(), False),
                    StructField('stop_s', StringType(), False),
                    StructField('channel_id', IntegerType(), False),
                    StructField('region_id', IntegerType(), False)    
                ])
                struct = StructType(fields=dataschema)
                rdd = sc.parallelize(rows)
                df = spark.createDataFrame(rdd.collect(), schema=struct)
                df.write.mode('append').parquet(f'hdfs://vm-dlake2-m-1.test.local/user/kuznetsov/data/final_kuznetsov.parquet')
                rows = []

except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    consumer.close()
