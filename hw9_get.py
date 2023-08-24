from kafka import KafkaConsumer
import pg8000
import json

dbname = 'postgres'
user = 'kuznetsov'
password = '****'
host = '192.168.77.21'
port = 5432

try:
    conn = pg8000.connect(database=dbname, user=user, password=password, host=host, port=port)
    consumer = KafkaConsumer('lab10.kuznetsov', bootstrap_servers='vm-strmng-s-1.test.local:9092',
                             group_id='kuznetsov_group')

    for message in consumer:
        # Преобразовать строку JSON в объект Python (dict)
        message_dict = json.loads(message.value)
        # раскидываем данные в зависимости от priority
        if message_dict['priority'] == 0:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.lab10_kuznetsov_0  VALUES (%s, to_timestamp(%s,'DD-MM-YYYY H24:MI'),%s);",
                    (message_dict['client'], message_dict['opened'], message_dict['priority']))
        elif message_dict['priority'] == 1:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.lab10_kuznetsov_1  VALUES (%s,to_timestamp(%s,'DD-MM-YYYY H24:MI'),%s);",
                    (message_dict['client'], message_dict['opened'], message_dict['priority']))
        elif message_dict['priority'] == 2:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.lab10_kuznetsov_2  VALUES (%s,to_timestamp(%s,'DD-MM-YYYY H24:MI'),%s);",
                    (message_dict['client'], message_dict['opened'], message_dict['priority']))
        else:
            pass
        conn.commit()
except Exception as _ex:
    print("[ERR] Ошибка : ", _ex)
finally:
    consumer.close()
    conn.close()
