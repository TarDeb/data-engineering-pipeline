from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('data/sample_data.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        producer.send('orders', row)
        print("Produced order:", row)
        time.sleep(1)  #  delay between 
producer.flush()
