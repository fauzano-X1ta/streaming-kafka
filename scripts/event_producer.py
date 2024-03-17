import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
_instance = Faker()
global faker
faker = Faker()

import uuid
from faker import Faker
import json
from time import sleep

class PurchaseEventProducer(object):
    @staticmethod
    def generate_purchase_event():
        faker = Faker()
        products = ['Product A', 'Product B', 'Product C']
        return {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': faker.unix_time(),
            'product': faker.random_element(elements=products),
            'amount': faker.random_int(min=1, max=100),
            'customer_id': faker.random_number(digits=6)
        }

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:9092')

# Define Kafka topic
kafka_topic_partition = 'purchase_topic'

# Send purchase events to Kafka topic
for i in range(1, 400):
    event_data = PurchaseEventProducer.generate_purchase_event()
    _payload = json.dumps(event_data).encode("utf-8")
    response = producer.send(topic=kafka_topic_partition, value=_payload)
    print(f"Sent message: {event_data['transaction_id']}, response: {response.get()}")
    sleep(10)
