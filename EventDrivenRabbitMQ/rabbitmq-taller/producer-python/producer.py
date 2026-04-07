import pika
import time
import json
import os
import random
from datetime import datetime

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
EXCHANGE_NAME  = "events"
ROUTING_KEY    = "sensor.data"

def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters  = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        connection_attempts=10,
        retry_delay=3,
    )
    return pika.BlockingConnection(parameters)

def main():
    print("[Python Producer] Connecting to RabbitMQ...")
    connection = connect()
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    print(f"[Python Producer] Connected. Publishing to exchange='{EXCHANGE_NAME}' key='{ROUTING_KEY}'")

    counter = 1
    while True:
        payload = {
            "producer": "python",
            "event":    "sensor.data",
            "value":    round(random.uniform(20.0, 100.0), 2),
            "unit":     "celsius",
            "timestamp": datetime.utcnow().isoformat(),
            "seq":      counter,
        }
        body = json.dumps(payload)
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"[Python Producer] Sent #{counter}: {body}")
        counter += 1
        time.sleep(2)

if __name__ == "__main__":
    main()
