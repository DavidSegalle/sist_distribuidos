import pika
import sys
import json
import time

from key_manager.generate_keys import KeyManager

class PagamentoTest:
    def __init__(self):

        self.key_manager = KeyManager("estoque")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        aprovado = self.channel.queue_declare(queue='', exclusive=True)
        self.aprovado_queue = aprovado.method.queue

        reprovado = self.channel.queue_declare(queue='', exclusive=True)
        self.reprovado_queue = reprovado.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.aprovado_queue,
                   routing_key="pagamento.aprovado")
        self.channel.queue_bind(exchange='ecommerce', queue=self.reprovado_queue,
                           routing_key="pagamento.reprovado")

        self.publish("Hello")

        self.consume()


    def aprovado_callback(self,ch, method, properties, body):
        info = json.loads(body)
        time.sleep(2)
        if self.key_manager.check_signature(info["message"], info["signature"], "pagamento"):
            print(f" [x] The message is real, pedido aprovado, sending another test")
            self.publish(info["message"] + "1")
        else:
            print(" [x] Falsified signature")

    def reprovado_callback(self,ch, method, properties, body):
        #print(f" [x] {method.routing_key}:{body}")
        info = json.loads(body)
        time.sleep(2)
        if self.key_manager.check_signature(info["message"], info["signature"], "pagamento"):
            print(f" [x] The message is real, pedido recusado, sending another test")
            self.publish(info["message"] + "1")
        else:
            print(" [x] Falsified signature")
    
    def consume(self):
        self.channel.basic_consume(
        queue=self.aprovado_queue, on_message_callback=self.aprovado_callback, auto_ack=True)
        self.channel.basic_consume(
                queue=self.reprovado_queue, on_message_callback=self.reprovado_callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

    def publish(self,message):

        signature = self.key_manager.sign(message)
                
        signed_message = {"message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key="pedido.estoque_ok", body=json.dumps(signed_message))

if __name__ == "__main__":
    e = PagamentoTest()