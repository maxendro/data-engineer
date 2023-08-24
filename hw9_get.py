# Лабораторная работа №9
# Создание consumer, который будет вычитывать данные из очереди сообщений “lab10_kuznetsov” и заливка данных по таблицам в зависимости от значения поля priority в json данных.

from kafka import KafkaConsumer
import pg8000
from pg8000.native import identifier
import json

dbname = 'postgres'
user = 'kuznetsov'
password = 'w7ton'
host = '192.168.77.21'
port = 5432

try:
    conn = pg8000.connect(database=dbname, user=user, password=password, host=host, port=port)
    consumer = KafkaConsumer('lab10.kuznetsov', bootstrap_servers='vm-strmng-s-1.test.local:9092', group_id='kuznetsov_group')
    for message in consumer:
        # Преобразовать строку JSON в объект Python (dict)
        message_dict = json.loads(message.value)
     # раскидываем данные в зависимости от priority
        # таблица куда заливаем
        table_name = 'lab10_kuznetsov_'+str(message_dict['priority'])
        sql = (
                f"INSERT INTO  {identifier(table_name)} VALUES ('{message_dict['client']}', to_timestamp('{message_dict['opened']}','DD-MM-YYYY H24:MI'),{message_dict['priority']})"
              )
        with conn.cursor() as cur:
             cur.execute(sql)
        conn.commit()
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    consumer.close()
    conn.close()
