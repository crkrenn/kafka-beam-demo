#!/usr/bin/env python3

from time import sleep
from json import dumps
from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x: 
#                          dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: str(x).encode('utf-8'))

for e in range(1000):
    # data = {'number' : e}
    data = e
    producer.send('numtest', value=data)
    print(e)
    print(dumps({"x":e}).encode('utf-8'))
    sleep(.2)
