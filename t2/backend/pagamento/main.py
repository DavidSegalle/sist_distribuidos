#!/usr/bin/env python
import pika
import sys
import time
import json
import random

from fastapi import FastAPI
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests

from contextlib import asynccontextmanager
import threading

from key_manager.generate_keys import KeyManager

pagamento_object = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pagamento_object, rabbitmq_thread

    pagamento_object = Pagamento()

    rabbitmq_thread = threading.Thread(
        target=pagamento_object.consume,
        daemon=True
    )
    rabbitmq_thread.start()

    yield

    # Se pá tem que fazer um stop
    #pagamento_object.stop()

    rabbitmq_thread.join(timeout=5)


app = FastAPI(lifespan=lifespan)

all_payments = []

global_key_manager = KeyManager("pagamento")

global_connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
global_channel = global_connection.channel()

def publish(key, id, message):
    # Message deve possuir as informações do pedido
    signature = global_key_manager.sign(str(id) + message)
    
    signed_message = {"id": id, "message": message, "signature": signature}

    global_channel.basic_publish(
    exchange='ecommerce', routing_key=key, body=json.dumps(signed_message))

@app.patch("/payment/{id}/{status}")
def generate_payment_link(id: int, status: str): # Added type hint
    exists = False
    for payment in all_payments:
        
        if payment["id"] == str(id) and payment["status"] == "pending":
            payment["status"] = status
            exists = True
            break

    if not exists:
        raise HTTPException(status_code=400, detail="Id already in payment list")

    print(status)
    print(payment)
    if status == "paid":
        print(" [x] Payment was accepted, sending to: pagamento.aprovado")
        publish("pagamento.aprovado", str(id), payment["message"])
    else:
        print(" [x] Payment failed, sending to: pagamento.reprovado")
        publish("pagamento.reprovado", str(id), payment["message"])

    return

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

    def callback(self,ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        # Verifica validade
        info = json.loads(body)
        signed_info = str(info["id"]) + info["message"]
        if not self.key_manager.check_signature(signed_info, info["signature"], "estoque"):
            return

        global all_payments
        info ["status"] = "pending"
        all_payments.append(info) 
        print(all_payments)
        # Request the payment backend api to generate a URL
        api_url = f"http://localhost:8001/payment/{str(info["id"])}"
        response = requests.post(api_url)
        
        url = response.json()

        # Envia a url (principal deve consumir e disponibilizar no frontend para que o frontend faça request dessa url)
        self.publish("pagamento.url", str(info["id"]), url["url"])

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

    
