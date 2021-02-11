import time
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092',
    'localhost:9093', 'localhost:9094'],
    group_id='names-consumer-group'
    )
    
for _ in range(100):
    name = fake.name()
    producer.send('names', name.encode('utf-8'))
    print(name)
time.sleep(2)