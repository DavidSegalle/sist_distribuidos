#!/usr/bin/env python
import pika
import sys

class EstoqueTester:
    def __init__(self):
        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.consumer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pedido.criado")
        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pedido.excluido")

        #self.consume()

        self.publish("Feijoada", "pedido.criado")
        self.publish("Feijoada", "pedido.excluido")
        self.publish("Feijoada", "pedido.criado")


    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body}")
        key = method.routing_key
        if (key == "pedido.criado"):
            print(f"Adicionando produto {body}")
        else:
            #pedido.excluido
            print(f"Excluindo produto {body}")
        
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.consumer_queue, on_message_callback=self.callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self,message, routing_key):
        self.channel.basic_publish(
        exchange='ecommerce', routing_key=routing_key, body=message)

if __name__ == "__main__":
    e = EstoqueTester()