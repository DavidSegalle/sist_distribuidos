<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

const API_BASE_URL = 'http://localhost:8000'

interface Produto {
  produto: string
  quantidade: number
}

interface Pedido {
  id: number
  produto: string
  status: string
  url_pagamento: string | null
}

const produtos = ref<Produto[]>([])
const pedidos = ref<Pedido[]>([])
const categorias = ref<string[]>([])
const email = ref('')
const selecionadas = ref<string[]>([])
const mensagem = ref('')
const conectado = ref(false)

// Pedidos cuja aba de pagamento já foi aberta
const abertos = new Set<number>()
let eventos: EventSource | null = null

const api = async (path: string, options: RequestInit = {}) => {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  const data = await response.json()
  if (!response.ok) throw new Error(data.detail ?? 'Erro na requisição')
  return data
}

const carregarProdutos = async () => {
  try {
    produtos.value = await api('/produtos')
  } catch (err) {
    mensagem.value = `Erro ao listar produtos: ${(err as Error).message}`
  }
}

const comprar = async (produto: string) => {
  try {
    const pedido: Pedido = await api('/pedidos', {
      method: 'POST',
      body: JSON.stringify({ produto }),
    })
    atualizarPedido(pedido)
    mensagem.value = `Pedido #${pedido.id} criado`
  } catch (err) {
    mensagem.value = `Erro ao criar pedido: ${(err as Error).message}`
  }
}

const abrirPagamento = (pedido: Pedido) => {
  if (!pedido.url_pagamento) return
  abertos.add(pedido.id)
  window.open(pedido.url_pagamento, '_blank')
}

const atualizarPedido = (pedido: Pedido) => {
  const i = pedidos.value.findIndex((p) => p.id === pedido.id)
  if (i >= 0) pedidos.value[i] = pedido
  else pedidos.value.push(pedido)
}

const conectarSSE = () => {
  eventos = new EventSource(`${API_BASE_URL}/eventos`)
  eventos.onopen = () => (conectado.value = true)
  eventos.onerror = () => (conectado.value = false)
  eventos.addEventListener('pedido', (e) => {
    const pedido: Pedido = JSON.parse((e as MessageEvent).data)
    atualizarPedido(pedido)
    mensagem.value = `Pedido #${pedido.id}: ${pedido.status}`
    // Ao receber a URL do checkout, abre o pagamento em nova aba
    if (pedido.url_pagamento && !abertos.has(pedido.id)) abrirPagamento(pedido)
    carregarProdutos()
  })
}

const interesse = async (method: 'POST' | 'DELETE') => {
  if (!email.value || selecionadas.value.length === 0) {
    mensagem.value = 'Informe o e-mail e ao menos uma categoria'
    return
  }
  try {
    await api('/interesses', {
      method,
      body: JSON.stringify({ email: email.value, categorias: selecionadas.value }),
    })
    mensagem.value = method === 'POST' ? 'Interesse registrado' : 'Interesse cancelado'
  } catch (err) {
    mensagem.value = `Erro: ${(err as Error).message}`
  }
}

onMounted(async () => {
  conectarSSE()
  carregarProdutos()
  try {
    pedidos.value = await api('/pedidos')
    pedidos.value.forEach((p) => p.url_pagamento && abertos.add(p.id))
    categorias.value = await api('/categorias')
  } catch (err) {
    mensagem.value = `Erro ao conectar no gateway: ${(err as Error).message}`
  }
})

onUnmounted(() => eventos?.close())
</script>

<template>
  <div class="loja">
    <h1>E-commerce</h1>
    <p class="status">
      SSE: {{ conectado ? 'conectado' : 'desconectado' }}
      <span v-if="mensagem"> | {{ mensagem }}</span>
    </p>

    <section>
      <h2>Catálogo <button @click="carregarProdutos">↻</button></h2>
      <table>
        <tr><th>Produto</th><th>Estoque</th><th></th></tr>
        <tr v-for="p in produtos" :key="p.produto">
          <td>{{ p.produto }}</td>
          <td>{{ p.quantidade }}</td>
          <td><button @click="comprar(p.produto)">Comprar</button></td>
        </tr>
        <tr v-if="produtos.length === 0"><td colspan="3">Nenhum produto disponível</td></tr>
      </table>
    </section>

    <section>
      <h2>Meus pedidos</h2>
      <table>
        <tr><th>#</th><th>Produto</th><th>Status</th><th></th></tr>
        <tr v-for="p in pedidos" :key="p.id">
          <td>{{ p.id }}</td>
          <td>{{ p.produto }}</td>
          <td>{{ p.status }}</td>
          <td>
            <button v-if="p.url_pagamento && p.status === 'Estoque confirmado'" @click="abrirPagamento(p)">
              Pagar
            </button>
          </td>
        </tr>
        <tr v-if="pedidos.length === 0"><td colspan="4">Nenhum pedido</td></tr>
      </table>
    </section>

    <section>
      <h2>Promoções</h2>
      <input v-model="email" type="email" placeholder="seu@email.com" />
      <label v-for="c in categorias" :key="c">
        <input v-model="selecionadas" type="checkbox" :value="c" /> {{ c }}
      </label>
      <div>
        <button @click="interesse('POST')">Inscrever</button>
        <button @click="interesse('DELETE')">Cancelar inscrição</button>
      </div>
    </section>
  </div>
</template>

<style scoped>
.loja {
  max-width: 700px;
  margin: 0 auto;
}

section {
  margin-top: 1.5rem;
}

table {
  width: 100%;
  border-collapse: collapse;
}

th,
td {
  text-align: left;
  padding: 0.3rem;
  border-bottom: 1px solid #ccc;
}

button {
  margin: 0.2rem;
  cursor: pointer;
}

label {
  margin-left: 0.5rem;
}

.status {
  color: #888;
}
</style>
