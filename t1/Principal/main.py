#!/usr/bin/env python
import pika
import sys
from threading import Thread, Lock
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

class Status():
    requested = "requested"
    estoque_ok = "estoque_ok"
    indisponivel = "indisponivel"
    aprovado = "aprovado"
    recusado = "recusado"
    enviado = "enviado"
    excluido = "excluido"

# beverages = ["Orange juice 20%% off", "Apple juice 15%% off", "Grape juice 10%% off"]
# foods = ["Arayes 15%% off", "Feijoada 40%% off", "Oniguiri 10%% off", "Lamen 20%% off"]
# drinks = ["Rum 5%% off", "Vodka 15%% off"]
class Principal:

    def __init__(self):

        self.key_manager = KeyManager("principal")

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        self.pedidos = []
        self.pedidos_lock = Lock()

        consumer_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.consumer_channel = consumer_connection.channel()

        self.consumer_channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        estoque_ok = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.estoque_ok_queue = estoque_ok.method.queue

        estoque_indisponivel = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.estoque_indisponivel_queue = estoque_indisponivel.method.queue

        pagamento_aprovado = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.pagamento_aprovado_queue = pagamento_aprovado.method.queue

        pagamento_reprovado = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.pagamento_reprovado_queue = pagamento_reprovado.method.queue

        pedido_enviado = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.pedido_enviado_queue = pedido_enviado.method.queue

        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.estoque_ok_queue,
                   routing_key="pedido.estoque_ok")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.estoque_indisponivel_queue,
                   routing_key="estoque.indisponivel")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pagamento_aprovado_queue,
                   routing_key="pagamento.aprovado")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pagamento_reprovado_queue,
                   routing_key="pagamento.reprovado")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pedido_enviado_queue,
                   routing_key="pedido.enviado")

        self.consumer_thread = Thread(target=self.consume)
        self.consumer_thread.start()

    def estoque_ok_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], "estoque"):
            self.update_status(info["id"], Status.estoque_ok)
        else:
            print(" [x] Falsified signature, ignoring")

    def estoque_indisponivel_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], "estoque"):
            self.update_status(info["id"], Status.indisponivel)
            self.publish("pedido.excluido", info["id"], info["message"])
        else:
            print(" [x] Falsified signature, ignoring")

    def pagamento_aprovado_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], "pagamento"):
            self.update_status(info["id"], Status.aprovado)
        else:
            print(" [x] Falsified signature, ignoring")

    def pagamento_reprovado_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], "pagamento"):
            self.update_status(info["id"], Status.recusado)
            self.publish("pedido.excluido", info["id"], info["message"])
        else:
            print(" [x] Falsified signature, ignoring")

    def pedido_enviado_callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key} sent a message")

        info = json.loads(body)

        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], "entrega"):
            self.update_status(info["id"], Status.enviado)
        else:
            print(" [x] Falsified signature, ignoring")

    def update_status(self, id, status):
        with self.pedidos_lock:
            for pedido in self.pedidos:
                if pedido["id"] == id:
                    pedido["status"] = status
                    return

    def consume(self):
        self.consumer_channel.basic_consume(
            queue=self.estoque_ok_queue, on_message_callback=self.estoque_ok_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.estoque_indisponivel_queue, on_message_callback=self.estoque_indisponivel_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pagamento_aprovado_queue, on_message_callback=self.pagamento_aprovado_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pagamento_reprovado_queue, on_message_callback=self.pagamento_reprovado_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pedido_enviado_queue, on_message_callback=self.pedido_enviado_callback, auto_ack=True)
        print("Started consuming")
        self.consumer_channel.start_consuming()

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

                    with self.pedidos_lock:
                        id = len(self.pedidos)
                        pedido = {"id": id, "status": Status.requested, "product": PRODUCTS[selected]}
                        self.pedidos.append(pedido)

                    self.publish("pedido.criado", id, PRODUCTS[selected])
                else:
                    print("You did not choose a product, returning to menu")

            if selection == Actions.see_purchase_status:
                with self.pedidos_lock:
                    for pedido in self.pedidos:
                        print(pedido)

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
        exchange=EXCHANGE, routing_key=key, body=json.dumps(signed_message))

if __name__ == "__main__":
    a = Principal()
    a.terminal_interaction()