import pika
import sys
import json
import time

from key_manager.generate_keys import KeyManager

class EstoqueTest:
    def __init__(self):

        self.key_manager = KeyManager("principal")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        estoque_ok = self.channel.queue_declare(queue='', exclusive=True)
        self.estoque_ok_queue = estoque_ok.method.queue

        estoque_indisponivel = self.channel.queue_declare(queue='', exclusive=True)
        self.estoque_indisponivel_queue = estoque_indisponivel.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.estoque_ok_queue,
                   routing_key="pedido.estoque_ok")
        self.channel.queue_bind(exchange='ecommerce', queue=self.estoque_indisponivel_queue,
                           routing_key="estoque.indisponivel")

        self.publish("Feijoada", "pedido.criado")
        self.publish("Feijoada", "pedido.excluido")
        self.publish("Feijoada", "pedido.criado")
        self.publish("Vodka", "pedido.excluido")
        self.publish("Vodka", "pedido.excluido")


        self.consume()


    def estoque_ok_callback(self,ch, method, properties, body):
        info = json.loads(body)
        time.sleep(2)
        if self.key_manager.check_signature(info["message"], info["signature"], "estoque"):
            print(f" [x] The message is real, pedido aprovado, sending another test")
            print(info["message"])
        else:
            print(" [x] Falsified signature")

    def estoque_indisponivel_callback(self,ch, method, properties, body):
        #print(f" [x] {method.routing_key}:{body}")
        info = json.loads(body)
        time.sleep(2)
        if self.key_manager.check_signature(info["message"], info["signature"], "estoque"):
            print(f" [x] The message is real, pedido recusado, sending another test")
            print(info["message"])
        else:
            print(" [x] Falsified signature")
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.estoque_ok_queue, on_message_callback=self.estoque_ok_callback, auto_ack=True)
        self.channel.basic_consume(
                queue=self.estoque_indisponivel_queue, on_message_callback=self.estoque_indisponivel_callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self,message, key):

        signature = self.key_manager.sign(message)
                
        signed_message = {"message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key=key, body=json.dumps(signed_message))

if __name__ == "__main__":
    e = EstoqueTest()