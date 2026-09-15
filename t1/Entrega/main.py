#!/usr/bin/env python
import pika
import sys
import time
import json

from key_manager.generate_keys import KeyManager

# O Microsserviço Entrega é responsável pelo gerenciamento da emissão de notas e da
# entrega dos produtos.
# O serviço deverá consumir o evento pagamento.aprovado.
# Após receber esse evento, deverá realizar as operações necessárias para emissão da nota
# e preparação da entrega. Após o processamento, deverá publicar um novo evento
# utilizando a routing key pedido.enviado, informando que o pedido foi enviado.


class Entrega:
    def __init__(self):

        self.key_manager = KeyManager("entrega")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.consumer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pagamento.aprovado")

        self.consume()


    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)
        
        if self.key_manager.check_signature(info["message"], info["signature"], "pagamento"):
            print(f" [x] The message is real, sending pedido.enviado")
            self.publish(info["message"])
        else:
            print(" [x] Falsified signature, ignoring")
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.consumer_queue, on_message_callback=self.callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self, message):
        # Message deve possuir as informações do pedido
        signature = self.key_manager.sign(message)
        
        signed_message = {"message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key="pedido.enviado", body=json.dumps(signed_message))

if __name__ == "__main__":
    e = Entrega()