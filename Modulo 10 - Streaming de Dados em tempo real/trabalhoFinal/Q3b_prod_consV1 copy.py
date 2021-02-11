import json
import random
import threading
import time
from faker import Faker

fake = Faker()

from kafka import KafkaConsumer, KafkaProducer

class Producer(threading.Thread):

    def run(self):

        producer = KafkaProducer(bootstrap_servers=['localhost:9092',
            'localhost:9093','localhost:9094'], 
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        #producer2 = KafkaProducer(bootstrap_servers=['localhost:9092',
        #    'localhost:9093','localhost:9094'])
            #value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            data = {}
            id_ = random.randint(0,1000)
            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'nome':fake.name(),
                              'salario':random.randint(1000,3000)}
                message = [id_, streaming]
                data[id_] = message
            print(message)
            producer.send('funcionarios', message)
            time.sleep(4)

        while True:
            data = {}
            id_ = random.randint(0, 1000)
            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'idade': random.randint(10, 50), 
                            'altura': random.randint(100, 200),
                            'peso': random.randint(30, 100)}
                message = [id_, streaming]
                data[id_] = message

            producer.send('pessoas', message)
            time.sleep(random.randint(0, 5))

class Consumer(threading.Thread):

    def run(self):
        stream = KafkaConsumer(bootstrap_servers='localhost:9092', 
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        stream.subscribe(['pessoas'])
        for mensagem in stream:
            pessoa = mensagem.value[1]
            imc = pessoa['peso'] / ((pessoa['altura'] / 100) ** 2)
            if imc > 35:
                print(pessoa,", IMC = ",imc)

if __name__ == '__main__':
    threads = [
        Producer(),
        Producer(),
        # Producer(),
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()