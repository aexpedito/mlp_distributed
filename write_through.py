import os
from typing import Any
import json
from uuid import uuid4
from dotenv import load_dotenv
import psycopg2
import redis
import pika
import multiprocessing as mp
import random
from time import sleep

semaphore = mp.Semaphore(1)

load_dotenv()

CONN = psycopg2.connect(
    dbname=os.getenv("PG_NAME"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
)
CURSOR = CONN.cursor()

REDIS_CLIENT = redis.Redis()

def exec_sql(query: str) -> list[tuple[Any, ...]]:
    CURSOR.execute(query)

    CONN.commit()

    try:
        return CURSOR.fetchall()
    except Exception as err:
        print(err)
        return

def send_to_queue(message, semaphore):
    #publish na fila controlado pelo semaforo
    with semaphore:
        parameters = pika.ConnectionParameters(os.getenv("REDIS_HOST"),os.getenv("REDIS_PORT"),"/",pika.PlainCredentials(os.getenv("REDIS_USER"),os.getenv("REDIS_PASS")))
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        queue_name = os.getenv("REDIS_QUEUE")
        channel.queue_declare(queue=queue_name)

        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f"Sent: {message}")
        connection.close()


def update(album_id, title, semaphore):
    query = f"UPDATE album SET title = '{title}' WHERE album_id = {album_id}"

    #atualiza a cache
    REDIS_CLIENT.set(f"sql:album-title:{album_id}", title, ex=3600)

    # disparar a atualizacao assíncrona, para atualizar o Postgres
    #cria a mensagem para a fila:
    message = json.dumps(
        {
            "id": f"{uuid4()}",
            "update": f"{query}"
        }
    )

    #envia a mensagem para a fila
    send_to_queue(message, semaphore)

def process_updates(max, semaphore):
    #atualiza o album_id aleatorio
    album_id = random.randint(1, max)
    title = "Updated!!!"
    update(album_id, title, semaphore)

def main():
    print("START")
    #carrega dados do banco no redis
    rows = exec_sql("select album_id, title from album")
    for row in rows:
        REDIS_CLIENT.set(f"sql:album-title:{row[0]}", row[1], ex=3600)

    query = "select max(album_id) from album"
    tuple = exec_sql(query)
    max = tuple[0][0]

    #update para titles de alguns albuns
    processes = []
    while True:
        for i in range(5):
            proc = mp.Process(target=process_updates, args=(max, semaphore))
            proc.start()
            processes.append(proc)
        for proc in processes:
            proc.join()
        processes = []
        sleep(5)

    print("END")



if __name__ == "__main__":
    main()
