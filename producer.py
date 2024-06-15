import json
import random
import time

import pika


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

    while True:
        message = json.dumps(
            {
                "id": random.randint(0, 99999),
                "text": "MessageContent!",
            }
        )
        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f" [x] Sent {message}")
        time.sleep(0.1)

    connection.close()


if __name__ == "__main__":
    main()
