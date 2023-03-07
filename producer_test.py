from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(
    acks=0, 
    compression_type='gzip', 
    bootstrap_servers=['192.168.122.17:19092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    
start = time.time()
for i in range(10):
    data = {'str' : 'result'+ str(i)}
    print("Let's send messegae:"+ str(i))
    producer.send('test-topic-1', value=data)
    producer.flush()

print("elapsed :", time.time() - start)