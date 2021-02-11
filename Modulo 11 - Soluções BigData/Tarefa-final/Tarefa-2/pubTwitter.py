from confluent_kafka import Producer
import json

with open('dataset/twitterSentiments.json', encoding='utf-8-sig') as data_file:    
    mydata = json.load(data_file)


# OBS.: 
# The .produce() method is asynchronous. 
# When called it adds the message to a queue of pending messages and immediately returns. 
# This allows the Producer to batch together individual messages for efficiency.


p = Producer({'bootstrap.servers': 'localhost'})
for data in mydata:
    data['sentimento']=data['airline_sentiment']
    data['companhia aerea']=data['airline']
    data['confianca']=data['airline_sentiment_confidence']
    data['comentario']=data['text']
    data.pop('tweet_id')
    print('Producing message: %s' % data)
    p.produce('topicoTwitter', json.dumps(data))
    
p.flush()

