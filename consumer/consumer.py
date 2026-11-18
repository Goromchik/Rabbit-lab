import json
import logging
import os
import time

import pika

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.environ.get("RABBIT_USER", "app")
RABBIT_PASSWORD = os.environ.get("RABBIT_PASSWORD", "secret")

EXCHANGE = "tasks"
QUEUE = "tasks_queue"
DLX = "dlx"
DLQ = "tasks_dlq"
OPERATIONS = ["add", "sub", "mul", "div"]

os.makedirs("/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("/logs/consumer.log")],
)
log = logging.getLogger("consumer")


def connect():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, credentials=credentials, heartbeat=60
    )
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ not ready, retry in 3s...")
            time.sleep(3)


def declare_topology(channel):
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX, routing_key="failed")

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "failed",
        },
    )
    for op in OPERATIONS:
        channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=op)


def calculate(op, a, b):
    if op == "add":
        return a + b
    if op == "sub":
        return a - b
    if op == "mul":
        return a * b
    if op == "div":
        return a / b  # при b == 0 будет ZeroDivisionError
    raise ValueError(f"unknown operation: {op}")


def on_message(channel, method, properties, body):
    try:
        task = json.loads(body)
        result = calculate(task["op"], task["a"], task["b"])
        log.info(
            "OK   id=%s: %s %s %s = %s",
            task["id"], task["a"], task["op"], task["b"], result,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        log.error("FAIL body=%s error=%r -> sent to DLQ", body.decode(), exc)
        # requeue=False: сообщение уходит в dead letter exchange, а не в ту же очередь
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = connect()
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    log.info("Consumer started, waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()