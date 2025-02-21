from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

with open('data/raw_orders.json', 'a') as outfile:
    for message in consumer:
        order = message.value
        outfile.write(json.dumps(order) + "\n")
        print("Ingested order:", order)
