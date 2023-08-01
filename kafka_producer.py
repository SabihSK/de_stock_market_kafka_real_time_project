from json import dumps
from kafka import KafkaProducer
from time import sleep
import pandas as pd

print("making consumer")
producer = KafkaProducer(bootstrap_servers=['3.83.128.220:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
data = pd.read_csv('data/indexProcessed.csv')

print("sending to consumer")
while True:
    msg = data.sample(1).to_dict(orient="records")[0]
    producer.send('demo_testing2', value=msg)
    producer.flush()
    sleep(2)
