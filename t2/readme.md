Para rodar o RabbitMQ:

``` cd backend ```
``` docker-compose up -d ```

Para rodar o frontend:

```bash
cd frontend
npm run dev
```

Para rodar backend de pagamentos:

```bash
t2/backend/payment_api$ fastapi run main.py --port 8001
```

```bash
cd backend
python3 -m uvicorn pagamento.main:app --reload --port 8002
```

Para rodar backend de estoque:

```bash
cd backend
python3 -m uvicorn estoque.main:app --port 8003
```