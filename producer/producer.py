import json
import os
import random
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


def connect():
    """Подключается к RabbitMQ, пока он не станет доступен"""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, credentials=credentials, heartbeat=60
    )
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not ready, retry in 3s...", flush=True)
            time.sleep(3)


def declare_topology(channel):
    """Объявляет обменники, очереди и привязки"""
    # Dead letter: обменник и очередь для отклонённых сообщений
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX, routing_key="failed")

    # Основной обменник и очередь, привязанная к DLX
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


def main():
    connection = connect()
    channel = connection.channel()
    declare_topology(channel)
    print("Producer started", flush=True)

    task_id = 0
    while True:
        task_id += 1
        op = random.choice(OPERATIONS)
        a = random.randint(1, 100)
        # для деления b может оказаться нулём: это вызовет ошибку у consumer
        b = random.randint(0, 5) if op == "div" else random.randint(1, 100)
        task = {"id": task_id, "op": op, "a": a, "b": b}

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=op,
            body=json.dumps(task),
            properties=pika.BasicProperties(
                delivery_mode=2,  # сообщение сохраняется на диск
                content_type="application/json",
            ),
        )
        print(f"Sent: key={op} body={task}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()