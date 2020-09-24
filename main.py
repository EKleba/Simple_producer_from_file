from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import pandas as pd

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_PRODUCER = 'TO_ML'

def json_serializer(data):
   return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
   bootstrap_servers=BOOTSTRAP_SERVERS, 
   value_serializer=json_serializer
   )

def on_send_success(record_metadata):
   print(record_metadata.topic)
   print(record_metadata.partition)
   print(record_metadata.offset)

def on_send_error(excp):
   print('excp')

df = pd.read_csv('./data.csv', sep=",", quotechar="'", encoding='utf-8')
for i in df.index:
   result = df.loc[i].to_json()
   input_data = json.loads(result)
   producer.send(TOPIC_PRODUCER, input_data).add_callback(on_send_success).add_errback(on_send_error)
   producer.flush()