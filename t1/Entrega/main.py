#!/usr/bin/env python
import pika
import sys

class Entrega:
    def __init__(self):
        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='consumer_queue', exclusive=True)
        self.consumer_queue = result.method.queue

        result = self.channel.queue_declare(queue='producer_queue', exclusive=True)
        self.producer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pagamento.aprovado")
        self.channel.queue_bind(exchange='ecommerce', queue=self.producer_queue,
                   routing_key="pedido.enviado")

        self.consume()


    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body}")
        print(f" Gerando nota...")
        msg = "Teste"
        self.publish(msg)
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.consumer_queue, on_message_callback=self.callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self,message):
        self.channel.basic_publish(
        exchange='direct_logs', routing_key="pedido.enviado", body=message)