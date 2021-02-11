from kafka import KafkaConsumer
consumer = KafkaConsumer(
    'names',
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    auto_offset_reset='earliest', enable_auto_commit=True
)

for message in consumer:
    print(message.value)