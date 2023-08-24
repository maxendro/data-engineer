# Лабораторная работа №9
# Генерация потока данных (1000 сообщений) в формате json и направление данных в topic  “lab10.kuznetsov” очереди сообщений Kafka.

from kafka import KafkaProducer
# библиотека для генерации "фальшивых" данных
from faker import Faker
import random
import json

# количество строк данных
n = 1000
try:
    producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')
    # Выставляем локаль для faker
    fake = Faker('en_US')
    #выставление весов параметра priority для использования в random.choice
    priority_list = ['0'] * int(n*0.75) + ['1'] * int(n*0.2) + ['2'] * int(n*0.05)
    for i in range(n):
        message = {'client': fake.name(),
                   'opened': fake.date_time_this_year().strftime("%d-%m-%Y %H:%M"),
                   'priority': int(random.choice(priority_list))
                   }
        # сериализируем объект dict в строку str формата JSON.
        message_json = json.dumps(message).encode('utf-8')
        # отправляем сообщение в очередь kafka
        producer.send('lab10.kuznetsov', value=message_json)
        print(message_json)
    print("[INFO] В очередь ушло {} записей:".format(i + 1))
except Exception as _ex:
      print("[ERR] Ошибка : ", _ex)
finally:
      producer.close()
