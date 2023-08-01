from json import loads, dump
from kafka import KafkaConsumer
from s3fs import S3FileSystem
consumer = KafkaConsumer('demo_testing2', bootstrap_servers='3.83.128.220:9092',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

# consumer = KafkaConsumer('demo_testing2', bootstrap_servers='3.83.128.220:9092',
#                          value_deserializer=lambda x: x)
# for c in consumer:
#     print(c.value)

s3 = S3FileSystem()
for count, i in enumerate(consumer):
    with s3.open("s3://de-kafka-project-dev/stock_market_{}.json".format(count), 'w') as file:
        dump(i.value, file)
