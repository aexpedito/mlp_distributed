import json
from time import sleep
from typing import Any

import pika
import psycopg2

CONN = psycopg2.connect(
    dbname="chinook",
    user="user",
    password="pass",
    host="localhost",
    port="5432",
)
CURSOR = CONN.cursor()

def exec_sql(query: str) -> list[tuple[Any, ...]]:
    CURSOR.execute(query)

    CONN.commit()

    try:
        return CURSOR.fetchall()
    except Exception as err:
        print(err)
        return


def work(msg: dict) -> None:
    """processing"""
    print(msg)
    """Atualiza o Postgres"""
    exec_sql(query)

    sleep(2)


def callback(ch, method, properties, body):
    """msg callback"""
    print(f" [x] Received {body}")
    msg = json.loads(body)
    work(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)  # <- !!! confirmação


def main():
    parameters = pika.ConnectionParameters(
        "localhost",
        5672,
        "/",
        pika.PlainCredentials("user", "pass"),
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = "tasks.queue.afonso"
    channel.queue_declare(queue=queue_name)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=False
    )

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    main()
