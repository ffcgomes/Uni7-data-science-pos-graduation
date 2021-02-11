from confluent_kafka import Producer
import json

with open('dataset/data.json') as data_file:    
    mydata = json.load(data_file)


# OBS.: 
# The .produce() method is asynchronous. 
# When called it adds the message to a queue of pending messages and immediately returns. 
# This allows the Producer to batch together individual messages for efficiency.


p = Producer({'bootstrap.servers': 'localhost'})
for data in mydata:
    #print(data['airline'])
    airline = data['airline']
    data['airline_alias'] = airline['alias']
    data['airline_iata'] = airline['iata']
    data['airline_id'] = airline['id']
    data['airline_name'] = airline['name']
    data.pop('airline')
    data.pop('_id')
    print('Producing message: %s' % data)
    p.produce('topicoDataStreaming', json.dumps(data))

p.flush()

