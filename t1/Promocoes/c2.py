#!/usr/bin/env python
import pika
import sys
import json

class C1:

    def __init__(self):

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='promocoes', exchange_type='topic')

        self.result = self.channel.queue_declare('', exclusive=True)
        self.queue_name = self.result.method.queue

        binding_key = "promocao.categoria.*"

        self.channel.queue_bind(
            exchange='promocoes', queue=self.queue_name, routing_key=binding_key)

        print(' [*] Waiting for logs. To exit press CTRL+C')


        def callback(ch, method, properties, body):
            info = json.loads(body)
            print(f" [x] {method.routing_key}:{info["message"]}")


        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=True)

        self.channel.start_consuming()

if __name__ == "__main__":
    c = C1()