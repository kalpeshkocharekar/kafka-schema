import json
import time

from bson import json_util
from kafka import KafkaProducer

topic = "sample"
server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[server])
'''data format 1:'''
data1 = {'tag': 'test',
         'name': 'hashtag'
         }

'''data format 2'''
data2 = {
    'device_id': '1',
    'device_name': 'mobile',
}

while True:
    producer.send(topic, key=b"data1", value=json.dumps(data1, default=json_util.default).encode('utf-8'))
    producer.send(topic, key=b'data2', value=json.dumps(data2, default=json_util.default).encode('utf-8'))
    producer.flush()
    time.sleep(10)
