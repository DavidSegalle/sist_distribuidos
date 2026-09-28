from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/produtos")
async def listar_produtos():
    return {"Produtos": "Vazio"}

@app.post("/pedidos")
async def criar_pedido(pedido: str):
    return {"ok": pedido}

@app.post("/interesses")
async def criar_interesse(interesse: str):
    return {"ok": interesse,}

@app.delete("/interesses")
async def cancelar_interesse(interesse: str):
    return {"ok": True}
