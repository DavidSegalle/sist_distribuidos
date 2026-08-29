# O Microsserviço Promoções é responsável pela geração e publicação de promoções de produtos. O serviço deverá gerar promoções aleatórias de produtos e publicá-las no RabbitMQ, utilizando routing keys que indiquem a categoria do produto, como promocao.categoria.A, promocao.categoria.B, promocao.categoria.C.

import pika
import sys

import time
import random

class Ads:

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='promocoes', exchange_type='topic')

        random.seed(time.time())
        self.categories = ["A", "B", "C"]

    def start_promoting(self):
        
        while True:
            sl_time = random.randint(5, 10)
            print(f"Waiting {sl_time} seconds until making a new advert")
            time.sleep(sl_time)
            self.ad_maker()


    def ad_maker(self):
        cat = random.randint(0, 2)
        category = self.categories[cat]
        print(f"Publishing a product of category: promocao.categoria.{category}")

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

        routing_key = f"promocao.categoria.{category}"
        self.channel.basic_publish(
                exchange='promocoes', routing_key=routing_key, body=message)

        print(f" [x] Sent {routing_key}:{message}")

if __name__ == "__main__":
    promoter = Ads()
    promoter.start_promoting()