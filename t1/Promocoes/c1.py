#!/usr/bin/env python
import pika
import sys
import json

from key_manager.generate_keys import KeyManager

class C1:

    def __init__(self):

        self.key_manager = KeyManager()

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='promocoes', exchange_type='topic')

        self.result = self.channel.queue_declare('', exclusive=True)
        self.queue_name = self.result.method.queue

        binding_keys = ["promocao.categoria.A", "promocao.categoria.B"]

        for binding_key in binding_keys:
            self.channel.queue_bind(
                exchange='promocoes', queue=self.queue_name, routing_key=binding_key)

        print(' [*] Waiting for logs. To exit press CTRL+C')


        def callback(ch, method, properties, body):
            info = json.loads(body)

            if self.key_manager.check_signature(info["message"], info["signature"], "ads"):
                print(f" [x] {method.routing_key}:{info["message"]}")
            else:
                print("Falsified signature")


        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=True)

        self.channel.start_consuming()

if __name__ == "__main__":
    c = C1()