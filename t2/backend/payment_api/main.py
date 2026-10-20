from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from pathlib import Path

import requests

app = FastAPI()

# Add CORS middleware to allow your Vue frontend to communicate with the backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your Vue URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PAYMENT_API_URL = "http://localhost:8001"

CHECKOUT_PAGE = (Path(__file__).parent / "checkout.html").read_text(encoding="utf-8")

all_ids = []
active_payments = {}
accepted_payments = {}
rejected_payments = {}

@app.post("/payment/{id}")
def generate_payment_link(id: int): # Added type hint
    if id in all_ids:
        raise HTTPException(status_code=400, detail="Id already in payment list")
         
    all_ids.append(id)
    active_payments[id] = "Pending"

    return {"url": f"{PAYMENT_API_URL}/perform_payment/{id}"}

@app.get("/perform_payment/{id}", response_class=HTMLResponse)
def checkout_page(id: int):
    if id not in active_payments:
        return HTMLResponse("<h1>Pagamento não encontrado ou já processado</h1>", status_code=404)
    return CHECKOUT_PAGE.replace("{id}", str(id))

@app.patch("/perform_payment/{id}/{performed}")
def perform_payment(id: int, performed: str): # Added type hints
    if id not in active_payments:
        raise HTTPException(status_code=404, detail="Item not found")
    
    del active_payments[id]
    
    if performed == "paid":
        accepted_payments[id] = "Approved"
    else:
        rejected_payments[id] = "Not Approved"


    api_url = f"http://localhost:8002/payment/{id}/{performed}"
    response = requests.patch(api_url)
    
    print(response)
    
    # Added a return statement so it returns valid JSON
    return {"status": "success", "id": id, "action": performed}

@app.get("/get_active")
def get_active():
    return {
        "active": active_payments,
        "accepted": accepted_payments,
        "rejected": rejected_payments
    }