import json
import pymongo
import pandas as pd
import sys
from   confluent_kafka import Consumer, KafkaError
from   pymongo import MongoClient

MIN_COMMIT_COUNT = 1000

# extract command line args: host, port and database
USAGE = "python sub_agg_mongo.py <MongoDB_Host> <MongoDB_Port> <MongoDB_Database>"


host  = sys.argv[1]
#host = 9092
port  = int(sys.argv[2])
database = sys.argv[3]

'''
Set up MongoDB Client
'''
client = MongoClient(host, port)
db = client[database]
coll = db.agg_dados

'''
Kafka Consumer settings
'''
c = Consumer({'bootstrap.servers': 'localhost', 
              'group.id': 'mygroup',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['topicoDataStreaming'])
                

def aggregation_basic(msgs):
    df = pd.DataFrame(msgs) 
    aggDF = df.groupby("airline_id").count()
    coll.insert_many(aggDF.to_dict('records'))

def consume():
    try:
        msg_count = 0
        msgs = []
        i = 0
        while (i < 10): 
            msg = c.poll()
            if not msg.error():
                print('Received message: %s' % msg.value().decode('utf-8'))
                msgs.append(json.loads(msg.value()))
                msg_count += 1
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
            if i == 9: # aggregate 10 messages at a time
                aggregation_basic(msgs)
                if msg_count % MIN_COMMIT_COUNT == 0:
                    #c.commit(async=False)
                    c.commit(asynchronous=False)

                i = 0
                msgs=[]
            else:
                i += 1
    finally:
        c.close()

def main():
    if len(sys.argv) < 4:
        print ("ERROR: Insufficient command line arguments supplied")
        print ("       usage: '" + USAGE + "'")
        sys.exit(2)
    
    consume()

if __name__ == "__main__": main()

# TODO: Add more complex aggregations
