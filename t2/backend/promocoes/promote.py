import resend
import os

from dotenv import load_dotenv

load_dotenv()

resend.api_key = os.environ["RESEND_API_KEY"]

r = resend.Emails.send({
  "from": "onboarding@resend.dev",
  "to": "davidsegalle@alunos.utfpr.edu.br",
  "subject": "NOVA PROMOÇÃO, APAGAR DEPOIS AAA",
  "html": "<p>Congrats on sending your <strong>first email</strong>!</p>"
})

registered_emails = []

#!/usr/bin/env python
import pika
import sys
import time
import json

import threading
import time

from key_manager.generate_keys import KeyManager

# O Microsserviço Entrega é responsável pelo gerenciamento da emissão de notas e da
# entrega dos produtos.
# O serviço deverá consumir o evento pagamento.aprovado.
# Após receber esse evento, deverá realizar as operações necessárias para emissão da nota
# e preparação da entrega. Após o processamento, deverá publicar um novo evento
# utilizando a routing key pedido.enviado, informando que o pedido foi enviado.


import threading
import time
import random

def worker():
    def ad_maker(self):
        categories = ["A", "B", "C"]
        cat = random.randint(0, 2)
        category = categories[cat]
        
        print(f"Emailing a product of category: promocao.categoria.{category}")

        message = "Could not generate a promotion"

        if category == "A":
            juices = ["Orange juice 20%% off", "Apple juice 15%% off", "Grape juice 10%% off"]
            promotion_index = random.randint(0, len(juices) - 1)
            message = juices[promotion_index]

        elif category == "B":
            foods = ["Arayes 15%% off", "Feijoada 40%% off", "Oniguiri 10%% off", "Lamen 20%% off"]
            promotion_index = random.randint(0, len(foods) - 1)
            message = foods[promotion_index]

        elif category == "C":
            drinks = ["Rum 5%% off", "Vodka 15%% off"]
            promotion_index = random.randint(0, len(drinks) - 1)
            message = drinks[promotion_index]

        return message, category
    
    time.sleep(60)

    sl_time = random.randint(3, 7)
    print(f"Waiting {sl_time} seconds until making a new advert")
    time.sleep(sl_time)
    message, category = ad_maker()

    for email in registered_emails:
        r = resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": email,
            "subject": f"New add, category: {category}",
            "html": f"The ad is for: {message}"
        })

t = threading.Thread(target=worker, daemon=True) # Daemon pra matar com control c
t.start()


class Promote:
    def __init__(self):

        self.key_manager = KeyManager()

        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='ecommerce', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.consumer_queue = result.method.queue

        self.channel.queue_bind(exchange='ecommerce', queue=self.consumer_queue,
                   routing_key="interesse.promocao")

        self.consume()


    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        time.sleep(2)

        info = json.loads(body)
        signed_info = str(info["id"]) + info["message"]
        if self.key_manager.check_signature(signed_info, info["signature"], "principal"):
            print(f" [x] The message is real, registering email")
            global registered_emails
            registered_emails.append(info["message"])

    def consume(self):
        self.channel.basic_consume(
        queue=self.consumer_queue, on_message_callback=self.callback, auto_ack=True)
        print("Started consuming")
        self.channel.start_consuming()

if __name__ == "__main__":
    e = Promote()