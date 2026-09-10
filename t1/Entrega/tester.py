import pika
import sys
import json

from key_manager.generate_keys import KeyManager

class EntregaTest:
    def __init__(self):

        self.key_manager = KeyManager("pagamento")

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.consumer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="pedido.enviado")

        self.publish("Hello")

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

        signature = self.key_manager.sign(message)
                
        signed_message = {"message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key="pagamento.aprovado", body=json.dumps(signed_message))

if __name__ == "__main__":
    e = EntregaTest()