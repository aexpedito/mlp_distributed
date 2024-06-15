import json
import os
from dotenv import load_dotenv
from time import sleep
from typing import Any
import json
import pika
import psycopg2

load_dotenv()

CONN = psycopg2.connect(
    dbname=os.getenv("PG_NAME"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
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
    """Atualiza o Postgres"""

    update = msg.get("update")
    exec_sql(update)

def callback(ch, method, properties, body):
    """msg callback"""
    #print(f"Received: {body}")
    msg = json.loads(body)
    work(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    parameters = pika.ConnectionParameters(os.getenv("REDIS_HOST"),os.getenv("REDIS_PORT"),"/",pika.PlainCredentials(os.getenv("REDIS_USER"), os.getenv("REDIS_PASS")))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = os.getenv("REDIS_QUEUE")
    channel.queue_declare(queue=queue_name)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
