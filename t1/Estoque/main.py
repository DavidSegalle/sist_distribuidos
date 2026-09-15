#!/usr/bin/env python
import pika
import sys
import json
import time

from key_manager.generate_keys import KeyManager

class Estoque:
    def __init__(self):

        self.stock = {
            "Suco de Laranja": 2,
            "Suco de Maçã": 4,
            "Suco de Uva": 5,
            "Arayes": 3,
            "Feijoada": 4,
            "Oniguiri": 1,
            "Lamen": 3,
            "Rum": 2,
            "Vodka": 1,
        }
        
        self.key_manager = KeyManager("estoque")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        criado = self.channel.queue_declare(queue='', exclusive=True)
        self.criado_queue = criado.method.queue

        excluido = self.channel.queue_declare(queue='', exclusive=True)
        self.excluido_queue = excluido.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.criado_queue,
                   routing_key="pedido.criado")
        self.channel.queue_bind(exchange='ecommerce', queue=self.excluido_queue,
                   routing_key="pedido.excluido")

        self.consume()

    def criado_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(info["message"], info["signature"], "principal"):
            message = info["message"]
            print(f" [x] The message is real: {message}")
            time.sleep(2)

            produto = info["message"] #TROCAR DEPOIS
            if (self.stock[produto] > 0):
                print(f"Estoque.ok")
                self.publish("pedido.estoque_ok", message) #colocar id
            else:
                print(f"Estoque.indisponivel")
                self.publish("estoque.indisponivel", message) #colocar id
                 
        else:
            print(" [x] Falsified signature, ignoring")

    def excluido_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(info["message"], info["signature"], "principal"):
            message = info["message"]
            print(f" [x] The message is real: {message}")
            time.sleep(2)
            
            produto = message
            if (self.stock[produto] - 1 >= 0):
                self.stock[produto] = self.stock[produto] - 1
                print(f"Estoque.ok")
                self.publish("pedido.estoque_ok", message) #colocar id  
            else:
                print(f"Estoque.indisponivel")
                self.publish("estoque.indisponivel", message) #colocar id


        else:
            print(" [x] Falsified signature, ignoring")

        
    def consume(self):
        self.channel.basic_consume(
            queue=self.criado_queue, on_message_callback=self.criado_callback, auto_ack=True)
        self.channel.basic_consume(
            queue=self.excluido_queue, on_message_callback=self.excluido_callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self,key, message):
        signature = self.key_manager.sign(message)
        
        signed_message = {"message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key=key, body=json.dumps(signed_message))

if __name__ == "__main__":
    e = Estoque()