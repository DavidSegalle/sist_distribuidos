#!/usr/bin/env python
import pika
import sys
from threading import Thread
import json


from key_manager.generate_keys import KeyManager

EXCHANGE = "ecommerce"

PRODUCTS = [
    "Suco de Laranja", "Suco de Maçã", "Suco de Uva",
    "Arayes", "Feijoada", "Oniguiri", "Lamen",
    "Rum", "Vodka"
]

class Actions():
    see_products = 0
    buy_products = 1
    see_purchase_status = 2
    quit = 3

# beverages = ["Orange juice 20%% off", "Apple juice 15%% off", "Grape juice 10%% off"]
# foods = ["Arayes 15%% off", "Feijoada 40%% off", "Oniguiri 10%% off", "Lamen 20%% off"]
# drinks = ["Rum 5%% off", "Vodka 15%% off"]

class Principal:

    def __init__(self):

        self.key_manager = KeyManager("entrega")

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        self.pedidos = []

    def terminal_interaction(self):
        while(True):
            print(Actions.see_products)
            print(f"[{Actions.see_products}] See products")
            print(f"[{Actions.buy_products}] Buy product")
            print(f"[{Actions.see_purchase_status}] See purchases status")
            print(f"[{Actions.quit}] Quit")
            selection = int(input("Select an option:"))

            if selection == Actions.see_products:
                # Fazer algum meio de mostrar os produtos (não está claro se isso pode ser armazenado nessa classe mesmo)
                for product in PRODUCTS:
                    print(product)

            if selection == Actions.buy_products:
                # Dá um publish em pedido.criado e adiciona o pedido a uma lista
                i = 0
                for product in PRODUCTS:
                    print(f"Select [{i}] for product: {product}")
                    i += 1
                print(f"Select [{i}] to cancel")

                selected = int(input("select: "))

                # Caso tenha sido cancelado
                if selected < i and selected >= 0:
                    print(F"You chose {PRODUCTS[selected]}, buying the product")

                    pedido = {"status": "sent", "product": PRODUCTS[selected]}
                    self.pedidos.append(pedido)

                    id = len(self.pedidos) - 1

                    self.publish("pedido.criado", id, PRODUCTS[selected])
                else:
                    print("You did not choose a product, returning to menu")

            if selection == Actions.see_purchase_status:
                print(self.pedidos)

            if "cancelar pedido":
                # Professora disse que não será cobrado no trabalho
                pass

            if selection == Actions.quit:
                print("Exiting")
                break

    def publish(self, key, id, message):
        # Message deve possuir as informações do pedido
        signature = self.key_manager.sign(str(id) + message)
        
        signed_message = {"id": id, "message": message, "signature": signature}

        self.channel.basic_publish(
        exchange='ecommerce', routing_key=key, body=json.dumps(signed_message))

if __name__ == "__main__":
    a = Principal()
    a.terminal_interaction()