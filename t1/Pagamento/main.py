#!/usr/bin/env python
import pika
import sys
import time
import json
import random

from key_manager.generate_keys import KeyManager

# O Microsserviço Pagamento é responsável pelo processamento dos pagamentos dos pedidos. O serviço deverá consumir o evento pedido.estoque_ok. Ao receber esse evento, deverá iniciar o processo de pagamento para o pedido correspondente. O processamento do pagamento deverá ser simulado por meio do uso de variáveis aleatórias para determinar se o pagamento será aprovado ou recusado. Quando o pagamento for aprovado, o microsserviço Pagamento deverá publicar um evento utilizando a routing key pagamento.aprovado. Quando o pagamento for recusado, deverá publicar um evento utilizando a routing key pagamento.recusado.

class Pagamento:
    def __init__(self):

        self.key_manager = KeyManager("pagamento")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.consumer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pedido.estoque_ok")

        random.seed(time.time())

        self.consume()


    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)
        signed_info = str(info["id"]) + info["message"]
        if self.key_manager.check_signature(signed_info, info["signature"], "estoque"):
            print(f" [x] The message is real, checking if the payment gets accepted")

            accepted = bool(random.getrandbits(1))
            if accepted:
                print(" [x] Payment was accepted, sending to: pagamento.aprovado")
                self.publish("pagamento.aprovado", info["message"])
            else:
                print(" [x] Payment failed, sending to: pagamento.reprovado")
                self.publish("pagamento.reprovado", info["id"], info["message"])
        else:
            print(" [x] Falsified signature, ignoring")
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.consumer_queue, on_message_callback=self.callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self, key, id, message):
        # Message deve possuir as informações do pedido
        signature = self.key_manager.sign(str(id) + message)
        
        signed_message = {"id": id, "message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key=key, body=json.dumps(signed_message))

if __name__ == "__main__":
    e = Pagamento()