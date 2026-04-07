import pika
import json
import os
import time

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
EXCHANGE_NAME = "events"
QUEUE_NAME    = "queue.python"
BINDING_KEY   = "sensor.#"

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

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f"[Python Consumer] Received: {json.dumps(data, indent=2)}")
    except json.JSONDecodeError:
        print(f"[Python Consumer] Received (raw): {body}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    print("[Python Consumer] Connecting to RabbitMQ...")
    connection = connect()
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"[Python Consumer] Waiting for messages on queue='{QUEUE_NAME}' binding='{BINDING_KEY}'...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
