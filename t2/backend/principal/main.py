#!/usr/bin/env python
import pika
import json
import asyncio
from threading import Thread, Lock
from contextlib import asynccontextmanager
import os

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel

from key_manager.generate_keys import KeyManager

EXCHANGE = "ecommerce"
ESTOQUE_URL = "http://localhost:8003"

PRODUCTS = [
    "Suco de Laranja", "Suco de Maçã", "Suco de Uva",
    "Arayes", "Feijoada", "Oniguiri", "Lamen",
    "Rum", "Vodka"
]

CATEGORIAS = ["A", "B", "C"]

class Status():
    requested = "Pedido criado"
    estoque_ok = "Estoque confirmado"
    indisponivel = "Estoque indisponivel"
    aprovado = "Pagamento aprovado"
    recusado = "Pagamento recusado"
    enviado = "Pedido enviado"

class Principal:

    def __init__(self, loop):

        self.key_manager = KeyManager("principal")

        # Event loop do FastAPI
        self.loop = loop

        self.pedidos = {}
        self.pedidos_lock = Lock()

        # Fila da conexão SSE aberta (só é acessada dentro do event loop)
        self.fila_sse = None

        # Conexão só de publicação: fica ociosa entre pedidos e não processa heartbeats,
        # então o heartbeat é desligado para o RabbitMQ não fechá-la
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=0))
        self.channel = self.connection.channel()
        # Os endpoints e a thread consumidora publicam pelo mesmo canal
        self.publish_lock = Lock()

        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        consumer_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.consumer_channel = consumer_connection.channel()

        self.consumer_channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

        estoque_ok = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.estoque_ok_queue = estoque_ok.method.queue

        estoque_indisponivel = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.estoque_indisponivel_queue = estoque_indisponivel.method.queue

        pagamento_url = self.consumer_channel.queue_declare(queue='', exclusive=True)
        self.pagamento_url_queue = pagamento_url.method.queue

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
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pagamento_url_queue,
                   routing_key="pagamento.url")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pagamento_aprovado_queue,
                   routing_key="pagamento.aprovado")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pagamento_reprovado_queue,
                   routing_key="pagamento.reprovado")
        self.consumer_channel.queue_bind(exchange=EXCHANGE, queue=self.pedido_enviado_queue,
                   routing_key="pedido.enviado")

    def verify(self, body, publisher):
        info = json.loads(body)
        if self.key_manager.check_signature(str(info["id"]) + info["message"], info["signature"], publisher):
            return info
        print(" [x] Falsified signature, ignoring")
        return None

    def estoque_ok_callback(self, ch, method, properties, body):
        info = self.verify(body, "estoque")
        if info:
            self.update_pedido(info["id"], status=Status.estoque_ok)

    def estoque_indisponivel_callback(self, ch, method, properties, body):
        info = self.verify(body, "estoque")
        if info:
            self.update_pedido(info["id"], status=Status.indisponivel)
            self.publish("pedido.excluido", info["id"], info["message"])

    def pagamento_url_callback(self, ch, method, properties, body):
        print(
            f"pagamento.url received: "
            f"delivery_tag={method.delivery_tag}, "
            f"consumer_tag={method.consumer_tag}, "
            f"queue={self.pagamento_url_queue}, "
            f"body={body!r}"
        )
        info = self.verify(body, "pagamento")
        if info:
            self.update_pedido(info["id"], url_pagamento=info["message"])

    def pagamento_aprovado_callback(self, ch, method, properties, body):
        info = self.verify(body, "pagamento")
        if info:
            self.update_pedido(info["id"], status=Status.aprovado)

    def pagamento_reprovado_callback(self, ch, method, properties, body):
        info = self.verify(body, "pagamento")
        if info:
            pedido = self.update_pedido(info["id"], status=Status.recusado)
            if pedido:
                # Libera a reserva feita pelo estoque
                self.publish("pedido.excluido", pedido["id"], pedido["produto"])

    def pedido_enviado_callback(self, ch, method, properties, body):
        info = self.verify(body, "entrega")
        if info:
            self.update_pedido(info["id"], status=Status.enviado)

    def update_pedido(self, id, **fields):
        with self.pedidos_lock:
            pedido = self.pedidos.get(str(id))
            if pedido is None:
                print(f" [x] Unknown order {id}, ignoring")
                return None
            pedido.update(fields)
            pedido = dict(pedido)

        print(f" [x] Order {pedido['id']}: {pedido['status']}")
        self.notify(pedido)
        return pedido

    def criar_pedido(self, produto):
        with self.pedidos_lock:
            id = len(self.pedidos)
            pedido = {"id": id, "produto": produto,
                      "status": Status.requested, "url_pagamento": None}
            self.pedidos[str(id)] = pedido
            pedido = dict(pedido)

        self.publish("pedido.criado", id, produto)
        return pedido

    def listar_pedidos(self):
        with self.pedidos_lock:
            return [dict(p) for p in self.pedidos.values()]

    # SSE

    def notify(self, pedido):
        # envia para a outra thread
        self.loop.call_soon_threadsafe(self.dispatch, pedido)

    def dispatch(self, pedido):
        if self.fila_sse is not None:
            self.fila_sse.put_nowait(pedido)

    def subscribe(self):
        # Uma nova conexão (ex.: recarregar a página) substitui a anterior
        self.fila_sse = asyncio.Queue()
        return self.fila_sse

    def unsubscribe(self, queue):
        # Só limpa se ainda for a conexão atual
        if self.fila_sse is queue:
            self.fila_sse = None

    # RABITMQ

    def consume(self):
        self.consumer_channel.basic_consume(
            queue=self.estoque_ok_queue, on_message_callback=self.estoque_ok_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.estoque_indisponivel_queue, on_message_callback=self.estoque_indisponivel_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pagamento_url_queue, on_message_callback=self.pagamento_url_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pagamento_aprovado_queue, on_message_callback=self.pagamento_aprovado_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pagamento_reprovado_queue, on_message_callback=self.pagamento_reprovado_callback, auto_ack=True)
        self.consumer_channel.basic_consume(
            queue=self.pedido_enviado_queue, on_message_callback=self.pedido_enviado_callback, auto_ack=True)
        print("Started consuming")
        self.consumer_channel.start_consuming()

    def publish(self, key, id, message):
        # Message deve possuir as informações do pedido
        signature = self.key_manager.sign(str(id) + message)

        signed_message = {"id": id, "message": message, "signature": signature}

        with self.publish_lock:
            self.channel.basic_publish(
            exchange=EXCHANGE, routing_key=key, body=json.dumps(signed_message))


principal = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global principal

    principal = Principal(asyncio.get_running_loop())

    rabbitmq_thread = Thread(
        target=principal.consume,
        daemon=True
    )
    rabbitmq_thread.start()

    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PedidoRequest(BaseModel):
    produto: str

class InteresseRequest(BaseModel):
    email: str
    categorias: list[str]


@app.get("/produtos")
def listar_produtos():
    try:
        response = requests.get(f"{ESTOQUE_URL}/produtos", timeout=5)
        response.raise_for_status()
    except requests.RequestException:
        raise HTTPException(status_code=503, detail="MS Estoque indisponivel")
    return response.json()

@app.post("/pedidos")
async def criar_pedido(pedido: PedidoRequest):
    if pedido.produto not in PRODUCTS:
        raise HTTPException(status_code=404, detail="Produto não existe")
    return principal.criar_pedido(pedido.produto)

@app.get("/pedidos")
async def listar_pedidos():
    return principal.listar_pedidos()

@app.get("/eventos", response_class=EventSourceResponse)
async def eventos():
    # Notifica o cliente sobre toda mudança de estado dos pedidos
    queue = principal.subscribe()
    try:
        while True:
            pedido = await queue.get()
            yield ServerSentEvent(data=pedido, event="pedido")
    finally:
        principal.unsubscribe(queue)

@app.get("/categorias")
async def listar_categorias():
    return CATEGORIAS

@app.post("/interesses")
async def criar_interesse(interesse: InteresseRequest):
    categorias_invalidas = [c for c in interesse.categorias if c not in CATEGORIAS]
    if categorias_invalidas:
        raise HTTPException(status_code=400, detail=f"Categorias inválidas: {categorias_invalidas}")

    message = json.dumps({"acao": "registrar", "email": interesse.email,
                          "categorias": interesse.categorias})
    principal.publish("interesse.promocao", interesse.email, message)
    return {"ok": True}

@app.delete("/interesses")
async def cancelar_interesse(interesse: InteresseRequest):
    message = json.dumps({"acao": "cancelar", "email": interesse.email,
                          "categorias": interesse.categorias})
    principal.publish("interesse.promocao", interesse.email, message)
    return {"ok": True}
