#!/usr/bin/env python
import pika
import sys
from threading import Thread

EXCHANGE = "ecommerce"

CONSUME_FROM_KEYS = {
    "pagamento": ["aprovado", "recusado"],
    "pedido": ["enviado", "estoque_ok"],
    "estoque": ["indisponivel"]
}

# CONSUME_FROM_KEYS = [
# "pagamento.aprovado",
# "pagamento.recusado",
# "pedido.enviado",
# "pedido.estoque_ok",
# "estoque.indisponivel"
# ]

class Principal:

    def __init__(self):

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = self.result.method.queue

        self.severities = sys.argv[1:]
        if not self.severities:
            sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
            sys.exit(1)

        for key in CONSUME_FROM_KEYS.keys():
            for sub_key in CONSUME_FROM_KEYS[key]:
                self.channel.queue_bind(
                    exchange=EXCHANGE, queue=queue_name, routing_key=f"{key}.{sub_key}"
                )

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=True
        )

        # Lança a thread para ler o rabbitMQ
        t = Thread(target = self.process_consume)
        t.start()
        # Precisa fazer sistema pra thread morrer quando fechar o app

        self.pedidos = {}

    def terminal_interaction():
        while(True):
            if "tá olhando pedidos":
                # Fazer algum meio de mostrar os produtos (não está claro se isso pode ser armazenado nessa classe mesmo)
                pass
            if "realizar pedidos":
                # Dá um publish em pedido.criado e adiciona o pedido a uma lista
                pass
            if "cancelar pedido":
                # Dá um publish em pedido.cancelado e retira ele dá lista
                pass

    # Saporra tem que ser lançada em uma thread pq start_consuming é blocking e temos que poder enviar pedidos
    def process_consume(self):
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body}")

        if method.routing_key.split(".")[0] == "pagamento":
            if method.routing_key.split(".")[1] == "aprovado":
                print("O pagamento foi aprovado")
            elif method.routing_key.split(".")[1] == "recusado":
                print("O pagamento foi recusado")
                # Tirar o pedido da lista de pedidos e publicar em pedido.excluido

        if method.routing_key.split(".")[0] == "estoque":
            if method.routing_key.split(".")[1] == "indisponivel":
                print("Não há estoque disponível")
                # Tirar o pedido da lista de pedidos e publicar em pedido.excluido

            # Estoque ok estava na key pedido., ou seja, talvez aqui seja para que o publisher de pedido envie uma lista de produtos ocasionalmente
            elif method.routing_key.split(".")[1] == "estoque_ok":
                print("Há estoque disponível")

        if method.routing_key.split(".")[0] == "pedido":
                if method.routing_key.split(".")[1] == "enviado":
                    print("O pedido foi enviado")
