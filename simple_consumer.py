#!/usr/bin/env python3

# # Import KafkaConsumer from Kafka library
# from kafka import KafkaConsumer

# # Import sys module
# import sys

# # Define server with port
# bootstrap_servers = ['localhost:9092']

# # Define topic name from where the message will recieve
# topicName = 'numtest'

# # Initialize consumer variable
# consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
#    bootstrap_servers)

# # Read and print message from consumer
# for msg in consumer:
#     print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))

# # Terminate the script
# sys.exit()

from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    # collection.insert_one(message)
    print('{} added to {}'.format(message, collection))
    

# for msg in consumer:
#     print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))

    